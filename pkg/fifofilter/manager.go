// Package fifofilter contains a FIFO-based Mavlink message filter.
package fifofilter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/tlog"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const (
	queueSize = 128
	// FIONREAD is not exported by golang.org/x/sys/unix for all architectures.
	fionread = 0x541B
	// 管理出口写失败的日志节流窗口。
	mgmtErrLogInterval = 5 * time.Second
	// 管理出口单次写的上界。UDP send buffer 满时 Write 会**等待空间**，不设上界就会把
	// 本 goroutine —— 同时也是 FIFO/fallback 的唯一消费者 —— 一起卡住。
	mgmtWriteTimeout = 200 * time.Millisecond
)

// Manager is a FIFO-based Mavlink message filter.
type Manager struct {
	Ctx          context.Context
	Wg           *sync.WaitGroup
	Dialect      *dialect.Dialect
	FifoPath     string
	ConfigPath   string
	FallbackPath string
	// MgmtEndpoint 管理出口：写 FIFO 的同时，用 UDP 把**同一份报文**镜像到该
	// ip:port。空串表示不启用。
	//
	// 规范 10_deviceID与payload加密公共规范.md 附录 A.1 里确有 MANAGEMENT_ENDPOINT
	// 这一项，但它被标注为「待定……不在本协议范围，由后端管理系统单独定义」（§3.2.5）
	// ——本字段的报文格式（8B 时间戳 + 帧字节，与 FIFO 逐字节相同）是本仓库自定的
	// 约定，不是规范给定的。
	MgmtEndpoint string

	// private
	messageIDs   map[uint32]struct{}
	dialectRW    *dialect.ReadWriter
	encodeBuf    bytes.Buffer
	frameWriter  *frame.Writer
	fifoFd       *os.File
	fallbackFile *os.File
	fallbackTlog *tlog.Writer
	mgmtConn     *net.UDPConn
	chEntry      chan *tlog.Entry

	// 管理出口写失败的日志节流（UDP 无连接，对端不在时 connected socket 会持续
	// 返回 ECONNREFUSED，而帧率可达 20Hz，不节流会刷屏）。
	mgmtErrAt  time.Time
	mgmtErrMsg string
}

// Initialize initializes the Manager.
func (m *Manager) Initialize() error {
	// 1. Load YAML config file: list of uint32 message IDs, one per line
	data, err := os.ReadFile(m.ConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read filter config %q: %w", m.ConfigPath, err)
	}
	var ids []uint32
	if err := yaml.Unmarshal(data, &ids); err != nil {
		return fmt.Errorf("failed to parse filter config %q: %w", m.ConfigPath, err)
	}
	m.messageIDs = make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		m.messageIDs[id] = struct{}{}
	}

	// 2. Initialize dialect ReadWriter
	m.dialectRW = &dialect.ReadWriter{Dialect: m.Dialect}
	if err := m.dialectRW.Initialize(); err != nil {
		return err
	}

	// 3. Initialize frame writer backed by bytes.Buffer for marshaling
	m.frameWriter = &frame.Writer{
		ByteWriter: &m.encodeBuf,
		DialectRW:  m.dialectRW,
	}
	if err := m.frameWriter.Initialize(); err != nil {
		return err
	}

	// 4. Ensure FIFO exists
	if err := m.ensureFifo(); err != nil {
		return err
	}

	// 5. Try to open FIFO (non-blocking; succeeds only if a reader is present)
	m.fifoFd, _ = openFifoWrite(m.FifoPath)

	// 6. Open fallback file
	m.fallbackFile, err = os.Create(m.FallbackPath)
	if err != nil {
		return fmt.Errorf("failed to create fallback file %q: %w", m.FallbackPath, err)
	}
	m.fallbackTlog = &tlog.Writer{
		ByteWriter: m.fallbackFile,
		DialectRW:  m.dialectRW,
	}
	if err := m.fallbackTlog.Initialize(); err != nil {
		return fmt.Errorf("failed to init fallback tlog: %w", err)
	}

	// 7. 管理出口（预留 ip+port）：与 FIFO 并行的 UDP 镜像。空 = 关闭。
	// 解析失败必须在这里报错——配置写错要当场暴露，不能静默降级成「没有下游」。
	if m.MgmtEndpoint != "" {
		addr, err := net.ResolveUDPAddr("udp", m.MgmtEndpoint)
		if err != nil {
			// 第 5/6 步已开出 fifoFd / fallbackFile；此处返回错误会让调用方放弃本
			// Manager，两个 fd 无人再关（run() 尚未启动，cleanup 不会替它们兜底）。
			m.cleanup()
			return fmt.Errorf("failed to resolve mgmt endpoint %q: %w", m.MgmtEndpoint, err)
		}
		m.mgmtConn, err = net.DialUDP("udp", nil, addr)
		if err != nil {
			m.cleanup()
			return fmt.Errorf("failed to dial mgmt endpoint %q: %w", m.MgmtEndpoint, err)
		}
	}

	// 8. Allocate buffered channel
	m.chEntry = make(chan *tlog.Entry, queueSize)

	// 9. Spawn background goroutine
	m.Wg.Add(1)
	go m.run()

	return nil
}

func (m *Manager) ensureFifo() error {
	info, err := os.Stat(m.FifoPath)
	if err == nil {
		if info.Mode()&os.ModeNamedPipe == 0 {
			return fmt.Errorf("%q exists but is not a FIFO", m.FifoPath)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat %q: %w", m.FifoPath, err)
	}
	if err := unix.Mkfifo(m.FifoPath, 0o666); err != nil {
		return fmt.Errorf("failed to create FIFO %q: %w", m.FifoPath, err)
	}
	return nil
}

func openFifoWrite(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func (m *Manager) run() {
	defer m.Wg.Done()
	defer m.cleanup()

	for {
		select {
		case entry := <-m.chEntry:
			if err := m.handleEntry(entry); err != nil {
				log.Printf("WARN: fifofilter: %s", err)
			}

		case <-m.Ctx.Done():
			return
		}
	}
}

func (m *Manager) handleEntry(entry *tlog.Entry) error {
	// Marshal frame to internal buffer
	m.encodeBuf.Reset()
	if err := m.frameWriter.Write(entry.Frame); err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	frameBytes := m.encodeBuf.Bytes()

	// Build tlog format: 8-byte timestamp (µs, big-endian) + frame bytes.
	// Same format as the fallback .tlog file, so FIFO and fallback are interchangeable.
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(entry.Time.UnixMicro()))
	tlogData := append(ts[:], frameBytes...)

	// 管理出口镜像：与 FIFO 并行发同一份 tlogData。UDP 是尽力而为，失败只记日志
	// （并节流），绝不影响下面的 FIFO / fallback 路径。
	if m.mgmtConn != nil {
		// 有界写：见 mgmtWriteTimeout 的说明——超时即放弃这一份镜像、继续走 FIFO，
		// 保住「管理出口绝不影响 FIFO/fallback」这条契约。
		_ = m.mgmtConn.SetWriteDeadline(time.Now().Add(mgmtWriteTimeout))
		if _, err := m.mgmtConn.Write(tlogData); err != nil {
			m.logMgmtErr(err)
		}
	}

	// Try to write to FIFO
	if err := m.writeToFifo(tlogData); err != nil {
		// FIFO unavailable or full — fall back to .tlog file.
		// The frame has already been encoded to MessageRaw by frameWriter.Write above;
		// tlog.Writer handles that correctly (re-marshals without re-encoding).
		return m.fallbackTlog.Write(entry)
	}
	return nil
}

func (m *Manager) writeToFifo(data []byte) error {
	// Reopen FIFO if not currently open (reader may have appeared)
	if m.fifoFd == nil {
		fd, err := openFifoWrite(m.FifoPath)
		if err != nil {
			return fmt.Errorf("fifo not available: %w", err)
		}
		m.fifoFd = fd
	}

	// Check FIFO buffer space before writing
	if !m.fifoHasSpace(len(data)) {
		return fmt.Errorf("FIFO buffer full (need %d bytes)", len(data))
	}

	// Write tlog-format data (8-byte timestamp + frame) to FIFO
	if _, err := m.fifoFd.Write(data); err != nil {
		// Write failed - reader may have disconnected
		m.fifoFd.Close()
		m.fifoFd = nil
		return fmt.Errorf("fifo write failed: %w", err)
	}

	return nil
}

func (m *Manager) fifoHasSpace(frameSize int) bool {
	fd := int(m.fifoFd.Fd())

	// Get total pipe capacity
	capacity, err := unix.FcntlInt(uintptr(fd), unix.F_GETPIPE_SZ, 0)
	if err != nil {
		return false
	}

	// Get current bytes in pipe via ioctl(FIONREAD)
	var unread int32
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL, uintptr(fd), fionread, uintptr(unsafe.Pointer(&unread)),
	)
	if errno != 0 {
		return false
	}

	available := capacity - int(unread)
	return available >= frameSize
}

// logMgmtErr 节流打印管理出口写失败：同一错误在 mgmtErrLogInterval 内只打一次。
func (m *Manager) logMgmtErr(err error) {
	msg := err.Error()
	if msg == m.mgmtErrMsg && time.Since(m.mgmtErrAt) < mgmtErrLogInterval {
		return
	}
	m.mgmtErrMsg = msg
	m.mgmtErrAt = time.Now()
	log.Printf("WARN: fifofilter: mgmt endpoint write failed: %s", msg)
}

// ProcessFrame 处理一个**已通过会话路由全部过滤**的帧，由 messageman.Manager 经
// FrameSink 注入。本组件只负责最后一层筛选：哪些 msgID 需要送到下游。
//
// 注入点有两处，都在 messageman 的全部早退分支之后：
//   - processEncrypted 末尾——加密下行帧，已过防重放 + 方向 + px4Map 命中；
//   - processStandbyHeartbeat 末尾——PX4 明文待命心跳（msgID=0）。这一路**不走**
//     processEncrypted，因而没有防重放、也不查 px4Map：它由「PX4 段 deviceID +
//     msgID 0 + payload<28」的形态直接确定。⇒ 对这类帧，**白名单是唯一的闸**，
//     过滤配置漏掉 0 会让 data_writer 收不到无人机在线状态（§3.2.5 要求 FIFO 必须
//     含明文待命心跳，故默认配置应含 0）。
//
// 改前它是 main.go 事件循环里的平级旁路，绕过全部过滤——QGC 上行的加密心跳照样被
// 写进 FIFO，该帧随后在 data_writer 推进其**当时的**单水位判重，把下行遥测饿死
// （2026-09-27 云端实测零入库）。那层判重已于 2026-09-28 按 §2.6「data_writer 例外」
// 删除，故今天再走旁路不会重现同一症状——这正是本组件不能靠「下游反正会拦」自证清白
// 的原因：注入点与白名单都得自己守住，依据是 §3.2.5 对 FIFO 内容的定义。
func (m *Manager) ProcessFrame(fr frame.Frame) {
	msgID := fr.GetMessage().GetID()
	if _, ok := m.messageIDs[msgID]; !ok {
		return
	}

	select {
	case m.chEntry <- &tlog.Entry{
		Time:  time.Now(),
		Frame: fr,
	}:
	case <-m.Ctx.Done():
	default:
		log.Printf("WARN: fifofilter queue is full, discarding frame")
	}
}

func (m *Manager) cleanup() {
	if m.mgmtConn != nil {
		m.mgmtConn.Close()
	}
	if m.fifoFd != nil {
		m.fifoFd.Close()
	}
	if m.fallbackFile != nil {
		m.fallbackFile.Close()
	}
}
