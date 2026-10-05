// Package fifofilter contains a FIFO-based Mavlink message filter.
package fifofilter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
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
	// 丢帧/写失败类告警的日志节流窗口。管理出口与 FIFO 两条路径共用：它们的丢帧都由
	// 上游帧率（可达 20Hz）驱动，不节流就是刷屏，而刷屏恰好会把「在丢数据」这件事淹掉。
	throttleLogInterval = 5 * time.Second
	// 管理出口自己的投递队列深度。它与 queueSize 是**两条独立的队列**：mgmt 侧
	// 拥塞只丢 mgmt 的帧，不占也不堵 FIFO 那条。
	mgmtQueueSize = 256
	// 管理出口 socket 重建失败的退避间隔（见 mgmtConnForWrite）。
	mgmtRedialInterval = 1 * time.Second
)

// throttledLog 把同一文案的告警限制在窗口内只打印一次。
//
// 本组件有多个互相独立的丢帧/写失败点：管理出口的写失败、投递队列满、重建退避，
// 以及 FIFO 投递队列满。它们共用这一套写法，但**每条文案各持一个实例** ——
// 单个实例只有一条 msg 当节流键，两条文案共用一个会互相顶掉（见 warnKeyed）。
type throttledLog struct {
	at  time.Time
	msg string
}

func (l *throttledLog) warn(interval time.Duration, format string, args ...any) {
	l.warnKeyed(interval, fmt.Sprintf(format, args...), format, args...)
}

// warnKeyed 与 warn 相同，但**节流键由调用方单独给出**。
//
// 需要它，是因为 warn 拿格式化后的全文当键：输出里只要含**每次都变**的值（累计丢帧数、
// 错误详情、时刻），键就每帧都不同 ⇒ 节流形同虚设，日志反被刷屏（帧率可达 20Hz）。
// warnKeyed 把键固定成一句稳定的文案，变化的量照样打进输出。
func (l *throttledLog) warnKeyed(interval time.Duration, key, format string, args ...any) {
	if key == l.msg && time.Since(l.at) < interval {
		return
	}
	l.msg = key
	l.at = time.Now()
	log.Printf("WARN: fifofilter: %s", fmt.Sprintf(format, args...))
}

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
	chEntry      chan *tlog.Entry

	// 管理出口的连接、目标与投递队列。mgmtCh 由 handleEntry **非阻塞**投递、
	// mgmtRun 独占消费；mgmtConn 加锁保护——mgmtRun 会重建它、cleanup 会关它。
	mgmtCh       chan []byte
	mgmtAddr     *net.UDPAddr
	mgmtConnMu   sync.Mutex
	mgmtConn     *net.UDPConn
	mgmtWg       sync.WaitGroup
	mgmtRedialAt time.Time

	// mgmtDropped 累计「因管理出口不可用而丢弃的帧数」，只增不减。两个来源各记各的
	// （见 handleEntry 的 default 分支、mgmtConnForWrite 的退避分支），但共用这一个
	// 计数：运维要知道的是「镜像漏了多少」，不是漏在哪一环。
	mgmtDropped atomic.Uint64

	// 管理出口三处告警各自节流（UDP 无连接，对端不在时 connected socket 会持续
	// 返回 ECONNREFUSED，而帧率可达 20Hz，不节流会刷屏）。
	mgmtErrLog  throttledLog // 写失败（可恢复类，见 mgmtWrite）
	mgmtDropLog throttledLog // 投递队列满、丢帧
	mgmtDialLog throttledLog // socket 重建失败
	// 重建退避窗口内丢帧。必须与 mgmtDropLog **分开**：throttledLog 只存一条 msg，
	// 两条不同文案共用一个实例会互相顶掉节流状态、交替出现 ⇒ 节流等于没有。
	mgmtBackoffLog throttledLog

	// fifoDropped 累计「因 FIFO 投递队列满而丢弃的帧数」，只增不减。
	//
	// 与 mgmtDropped **刻意分开**：两者语义不同，混在一起运维就分不清丢的是哪一路。
	// FIFO 这条尤其要紧 —— 它丢的是 data_writer 的遥测，不是尽力而为的镜像。
	fifoDropped atomic.Uint64

	// FIFO 队列满的告警。同样必须独立于 mgmt 侧那三个实例（见上）。
	fifoDropLog throttledLog
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
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			m.cleanup()
			return fmt.Errorf("failed to dial mgmt endpoint %q: %w", m.MgmtEndpoint, err)
		}
		m.mgmtAddr = addr
		m.mgmtConn = conn
		// 管理出口有自己的队列和**专属 goroutine**——见 mgmtRun 的说明。
		m.mgmtCh = make(chan []byte, mgmtQueueSize)
		m.mgmtWg.Add(1)
		go m.mgmtRun()
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
	//
	// 必须**独立分配**：这份数据要异步交给 mgmtRun（它可能持有到下一帧之后），不能与
	// encodeBuf 共享底层数组——下一帧一来 encodeBuf 就被 Reset 了。
	// （这里 make+copy 而非 append，只是让「8 字节头 + 帧」的长度一目了然；就脱钩而言
	//   两者等价：frameBytes 来自 bytes.Buffer.Bytes()，append 到那个 8 字节切片上必然
	//   重新分配。曾经把「不用 append」写成脱钩的理由，那个理由是错的。）
	tlogData := make([]byte, 8+len(frameBytes))
	binary.BigEndian.PutUint64(tlogData[:8], uint64(entry.Time.UnixMicro()))
	copy(tlogData[8:], frameBytes)

	// 管理出口镜像：把**同一份** tlogData 交给专属 goroutine 发 UDP。
	//
	// 这里必须**非阻塞投递**：本 goroutine 同时是 FIFO/fallback 的唯一消费者，一旦
	// 在这里等 mgmt 队列腾空间，data_writer 就跟着断粮。队列满即丢这一份——UDP 本
	// 就是尽力而为，在这一层丢与内核在发送缓冲里丢没有语义差别。
	//
	// 2026-10-05 改：原先 mgmt 写就在本 goroutine 里同步做，靠 200ms 写超时防止
	// 「UDP 发送缓冲满 ⇒ Write 阻塞 ⇒ FIFO 一起卡住」。拆出 mgmtRun 之后那条约束
	// 不再需要，超时随之删掉——mgmt 侧可以无上界地等，谁也拖累不到。
	if m.mgmtCh != nil {
		select {
		case m.mgmtCh <- tlogData:
		default:
			// 走 warnKeyed 而不是 warn：输出里带了累计计数，用 warn 的话节流键每帧都
			// 不同，等于把节流关掉（见 warnKeyed）。
			n := m.mgmtDropped.Add(1)
			m.mgmtDropLog.warnKeyed(throttleLogInterval,
				"mgmt endpoint queue full, discarding frame",
				"mgmt endpoint queue full, discarding frame (累计已丢 %d 帧)", n)
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

// ---- 管理出口（航管系统 ip+port）----

// mgmtRun 是管理出口的专属写循环，与 FIFO/fallback 路径**完全分离**。
//
// 分开的理由只有一条：UDP 发送缓冲满时 Write 会等内核腾空间，而 FIFO/fallback
// 那条路径不能等——它是 data_writer 的唯一粮道。拆开之后：
//   - 投递侧（handleEntry）非阻塞，队列满即丢帧；
//   - 本循环内**不设写超时**，可以一直等——反正谁也拖累不到。
func (m *Manager) mgmtRun() {
	defer m.mgmtWg.Done()
	for {
		select {
		case data := <-m.mgmtCh:
			m.mgmtWrite(data)
		case <-m.Ctx.Done():
			return
		}
	}
}

// mgmtWrite 往管理出口发一份报文，错误分两类处理：
//
//   - **可恢复**：ECONNREFUSED（对端未起，内核回 ICMP port unreachable）、
//     EHOSTUNREACH / ENETUNREACH（路由中断）、ENOBUFS / ENOMEM（内核缓冲不足）、
//     EAGAIN。只节流记日志，**不重建 socket**——connected UDP socket 的下一次
//     Write 会自动重试，对端恢复即自愈；重建反而会丢掉既有的本地绑定。
//   - **不可恢复**（isFatalSocketErr）：丢弃连接，由下一条报文触发重建。
//
// 关闭期要单独认：cleanup 正是靠关掉连接来打断可能阻塞在这里的 Write，那次失败是
// **预期的**，静默丢弃即可，不该报成写失败。
func (m *Manager) mgmtWrite(data []byte) {
	conn := m.mgmtConnForWrite()
	if conn == nil {
		// 连接当前不可用（上次重建失败、或退避窗口未到）——本条丢弃，不做阻塞重试：
		// 宁可丢这一份报文，也不让队列后面的帧跟着一起等。
		return
	}

	if _, err := conn.Write(data); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if isFatalSocketErr(err) {
			m.mgmtDiscardConn(err)
			return
		}
		m.logMgmtErr(err)
	}
}

// mgmtConnForWrite 返回可用的管理出口连接；连接缺失时按 mgmtRedialInterval 退避重建。
// 返回 nil 表示本次不可用，调用方丢帧。
func (m *Manager) mgmtConnForWrite() *net.UDPConn {
	m.mgmtConnMu.Lock()
	defer m.mgmtConnMu.Unlock()

	if m.mgmtConn != nil {
		return m.mgmtConn
	}
	// ‼️ 已经进入关闭流程就不再重建（2026-10-05 补，见 mgmt_shutdown_test.go）。
	//
	// 漏洞长这样：cleanup 关掉 mgmtConn 并置 nil 之后，mgmtRun 未必已经退出 —— 它的
	// select 在 Ctx.Done() 已就绪时若随机选中了数据分支，就会走到这里。少了这一条，
	// 那次重建出来的 socket 会被赋回 m.mgmtConn 并返回，而 cleanup 的关闭动作**已经
	// 执行过**、mgmtWg.Wait() 之后也没有第二个关闭点 ⇒ 这个 fd 到进程结束没人关。
	//
	// 判据用 Ctx 而不是「mgmtConn 是否被关过」：Ctx 取消**先于** cleanup 关连接成立
	// （run() 正是因 Ctx.Done 才退出、才 defer 到 cleanup），只有它能区分「还没起来」
	// 与「正在拆」这两种 mgmtConn == nil。
	if m.Ctx.Err() != nil {
		return nil
	}
	if time.Since(m.mgmtRedialAt) < mgmtRedialInterval {
		// 这条分支曾经完全静默：重建失败后的一秒里，每一帧都无声消失。计数 + 节流日志
		// 让「镜像在漏」看得见。（上面的 Ctx 那条不报：那种丢弃是关闭期的正常行为。）
		n := m.mgmtDropped.Add(1)
		m.mgmtBackoffLog.warnKeyed(throttleLogInterval,
			"mgmt endpoint in redial backoff, discarding frames",
			"mgmt endpoint %q in redial backoff, discarding frames (累计已丢 %d 帧)",
			m.MgmtEndpoint, n)
		return nil
	}
	m.mgmtRedialAt = time.Now()

	conn, err := net.DialUDP("udp", nil, m.mgmtAddr)
	if err != nil {
		m.mgmtDialLog.warn(throttleLogInterval, "mgmt endpoint %q rebuild failed: %s", m.MgmtEndpoint, err)
		return nil
	}
	m.mgmtConn = conn
	log.Printf("INFO: fifofilter: mgmt endpoint %q socket rebuilt", m.MgmtEndpoint)
	return conn
}

// mgmtDiscardConn 丢弃一个已被判定失效的连接，让下一条报文立刻触发重建。
func (m *Manager) mgmtDiscardConn(cause error) {
	m.mgmtConnMu.Lock()
	defer m.mgmtConnMu.Unlock()

	if m.mgmtConn == nil {
		return
	}
	m.mgmtConn.Close()
	m.mgmtConn = nil
	// 立刻允许重建、不等退避：这是**第一次**发现 socket 失效；退避留给「重建本身也
	// 失败」的情形（见 mgmtConnForWrite）。
	m.mgmtRedialAt = time.Time{}
	// 文案别写 "rebuilding"：这里**没有**重建，只是丢弃。重建发生在下一条报文到达时
	// （mgmtConnForWrite 里懒做）。写 rebuilding 会让人以为 socket 已换好。
	m.mgmtDialLog.warn(throttleLogInterval,
		"mgmt endpoint socket unusable, discarded; will rebuild on next frame: %s", cause)
}

// isFatalSocketErr 判断错误是否属于「这个 socket 再也不可能发出去了」。
//
// 判据只认 socket 本身失效的那几种：EBADF（fd 已关）、ENOTSOCK（不是 socket）、
// EINVAL（参数非法，同一个 conn 再试也不会变）。
//
// **反面同样重要**：ECONNREFUSED / EHOSTUNREACH / ENETUNREACH / ENOBUFS / EAGAIN
// 都会自愈，必须留在可恢复一侧——把它们也拿去重建 socket 是错的。
func isFatalSocketErr(err error) bool {
	return errors.Is(err, syscall.EBADF) ||
		errors.Is(err, syscall.ENOTSOCK) ||
		errors.Is(err, syscall.EINVAL)
}

// logMgmtErr 节流打印管理出口写失败：同一错误在 throttleLogInterval 内只打一次。
func (m *Manager) logMgmtErr(err error) {
	m.mgmtErrLog.warn(throttleLogInterval, "mgmt endpoint write failed: %s", err)
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
		// 计数 + 节流。改前这里是一句裸 log.Printf：队列 128、上游帧率可达 20Hz 时
		// 每秒打 20 条，既刷屏又不留数字 —— 「data_writer 正在收不到遥测」反而被淹掉。
		// 用 warnKeyed 而非 warn：输出里带了累计丢帧数，拿全文当节流键会每帧都不同，
		// 等于把节流关掉（见 warnKeyed）。
		n := m.fifoDropped.Add(1)
		m.fifoDropLog.warnKeyed(throttleLogInterval,
			"fifo queue full, discarding frame",
			"fifo queue is full, discarding frame (累计已丢 %d 帧)", n)
	}
}

func (m *Manager) cleanup() {
	// 先关管理出口连接：mgmtRun 可能正**无超时地**阻塞在 Write（对端不读、发送缓冲
	// 满），Close 是唯一能打断它的手段。随后等它退出，免得它继续碰已关的 fd。
	// （那次被打断的 Write 返回 net.ErrClosed，由 mgmtWrite 静默吞掉。）
	m.mgmtConnMu.Lock()
	if m.mgmtConn != nil {
		m.mgmtConn.Close()
		m.mgmtConn = nil
	}
	m.mgmtConnMu.Unlock()
	m.mgmtWg.Wait()

	if m.fifoFd != nil {
		m.fifoFd.Close()
	}
	if m.fallbackFile != nil {
		m.fallbackFile.Close()
	}
}
