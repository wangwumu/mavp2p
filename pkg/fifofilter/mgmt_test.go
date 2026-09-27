package fifofilter_test

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/bluenviron/mavp2p/pkg/fifofilter"
)

// ---- 测试夹具 ----

// mgmtReceiver 起一个本地 UDP socket 充当「管理出口」的对端。
// 返回的 read 带超时；超时返回 nil（用于「不该收到」的阴性格）。
func mgmtReceiver(t *testing.T) (string, func(time.Duration) []byte) {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { pc.Close() })

	read := func(timeout time.Duration) []byte {
		_ = pc.SetReadDeadline(time.Now().Add(timeout))
		buf := make([]byte, 4096)
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			return nil
		}
		return buf[:n]
	}
	return pc.LocalAddr().String(), read
}

// newManager 组装一个 fifofilter.Manager：白名单由 ids（YAML 列表文本）给出，
// FIFO/fallback 落在 tmpFolder 下，管理出口指向 mgmtAddr（空串表示不启用）。
// 返回的 stop 负责取消 ctx 并等后台 goroutine 退出。
func newManager(t *testing.T, tmpFolder, ids, mgmtAddr string) *fifofilter.Manager {
	t.Helper()

	configPath := filepath.Join(tmpFolder, "filter.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(ids), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	m := &fifofilter.Manager{
		Ctx:          ctx,
		Wg:           &wg,
		Dialect:      common.Dialect,
		FifoPath:     filepath.Join(tmpFolder, "test.fifo"),
		ConfigPath:   configPath,
		FallbackPath: filepath.Join(tmpFolder, "fallback.tlog"),
		MgmtEndpoint: mgmtAddr,
	}
	require.NoError(t, m.Initialize())

	return m
}

func v2Heartbeat(seq byte) *frame.V2Frame {
	return &frame.V2Frame{
		SequenceNumber: seq,
		SystemID:       1,
		ComponentID:    1,
		Message: &common.MessageHeartbeat{
			Type:           common.MAV_TYPE_GCS,
			Autopilot:      common.MAV_AUTOPILOT_INVALID,
			SystemStatus:   4,
			MavlinkVersion: 3,
		},
	}
}

func v2SysStatus(seq byte) *frame.V2Frame {
	return &frame.V2Frame{
		SequenceNumber: seq,
		SystemID:       1,
		ComponentID:    1,
		Message:        &common.MessageSysStatus{OnboardControlSensorsPresent: 1},
	}
}

// tlogMsgID 从 tlog 条目（8B 时间戳 + V2 帧）里取 msgID：帧内偏移 7，24 位小端。
// 让「收到了帧」这类断言带上帧身份，而不是只数条数。
func tlogMsgID(t *testing.T, data []byte) uint32 {
	t.Helper()

	require.GreaterOrEqual(t, len(data), 18, "tlog 条目应含完整 V2 帧头")
	require.Equal(t, byte(0xFD), data[8], "时间戳之后应是 V2 帧魔数")
	return uint32(data[15]) | uint32(data[16])<<8 | uint32(data[17])<<16
}

// ---- 测试 ----

// TestMgmtEndpointMirrorsPayload 守管理出口（预留 ip+port）的两条契约：
//  1. 内容与 FIFO/fallback **完全同一份报文**（8B 时间戳 + 帧字节）——下游
//     解析器与 FIFO 消费者可以共用一套解码代码；
//  2. 与 FIFO 路径**互相独立**——本格没有 FIFO 读端（writeToFifo 必失败、走
//     fallback），管理出口仍须照常收到。
//
// 2026-09-28 用户要求：「把过滤过的，且在白名单中的报文写入FIFO和那个预留ip」
// 「mavp2p会配置一个ip和端口，它在写入FIFO的同时udp协议把同样报文写入到该ip+port中」。
func TestMgmtEndpointMirrorsPayload(t *testing.T) {
	tmpFolder := t.TempDir()
	addr, recv := mgmtReceiver(t)

	// 不装 reader：ensureFifo 会建出 FIFO 文件，但 openFifoWrite 用 O_WRONLY|O_NONBLOCK，
	// 无读端时 open 直接 ENXIO ⇒ fifoFd 为 nil ⇒ writeToFifo 必失败 ⇒ 走 fallback。
	m := newManager(t, tmpFolder, "- 0\n", addr)

	m.ProcessFrame(v2Heartbeat(7))

	got := recv(2 * time.Second)
	require.NotNil(t, got, "管理出口应收到白名单内的帧")
	require.GreaterOrEqual(t, len(got), 9, "tlog 格式＝8 字节时间戳 + 至少 1 字节帧")
	require.Equal(t, byte(0xFD), got[8], "时间戳之后应是 Mavlink V2 帧头")

	// FIFO 不可用 ⇒ 同一份报文落在 fallback；两边必须逐字节相同。
	time.Sleep(200 * time.Millisecond)
	fb, err := os.ReadFile(filepath.Join(tmpFolder, "fallback.tlog"))
	require.NoError(t, err)
	require.Equal(t, fb, got,
		"管理出口与 FIFO/fallback 必须是同一份报文（含同一个 8 字节时间戳）")
}

// TestMgmtEndpointHonoursWhitelist 守白名单仍在 fifofilter 这一层生效：
// 会话过滤（防重放/方向/px4Map）由 messageman 负责，**哪些 msgID 需要进下游**
// 仍是本组件自己的职责，不能因为上游已过滤就把白名单丢掉。
//
// 判定力：先喂白名单外的一条、再喂白名单内的一条。队列是单 goroutine 串行处理的，
// 故第二条到达 ⇒ 第一条已处理完 ⇒ 此时「只收到一条」即为第一条被白名单挡住的证据。
//
// ❗ 收到的这一条**必须认帧身份**。只断言「收到了一条」时，把白名单取反（写成
// `- 0` 让 msgID=0 通过）照样绿——两条都放行、先到的正是第一条，测试通过而白名单
// 已整个失效。所以下面钉死 msgID==1。
func TestMgmtEndpointHonoursWhitelist(t *testing.T) {
	tmpFolder := t.TempDir()
	addr, recv := mgmtReceiver(t)

	m := newManager(t, tmpFolder, "- 1\n", addr)

	m.ProcessFrame(v2Heartbeat(1)) // msgID=0，白名单外
	m.ProcessFrame(v2SysStatus(2)) // msgID=1，白名单内 —— 阳性对照

	got := recv(2 * time.Second)
	require.NotNil(t, got, "阳性对照：白名单内的帧必须到达管理出口")
	require.Equal(t, uint32(1), tlogMsgID(t, got),
		"收到的必须是白名单内那条（msgID=1）——不认身份时白名单取反仍绿")

	require.Nil(t, recv(300*time.Millisecond),
		"白名单外（msgID=0）的帧不得进管理出口")
}

// TestMgmtEndpointWithFifoReader 守「写入 FIFO 的同时」这半句：
// FIFO 可用时也要照发管理出口，且两条路径载荷一致。
func TestMgmtEndpointWithFifoReader(t *testing.T) {
	tmpFolder := t.TempDir()
	addr, recv := mgmtReceiver(t)

	// 在 Initialize **之前**预建 FIFO 并挂上 reader。成败取决于**有没有读端**，不是
	// 文件在不在——ensureFifo 自己就会建文件。openFifoWrite 用 O_WRONLY|O_NONBLOCK，
	// Initialize 那一刻若无读端即 ENXIO、fifoFd 留在 nil（那正是上一个用例走的路径）。
	// 故必须先挂 reader 再 Initialize，顺序反过来本格就退化成 fallback 格。
	fifoPath := filepath.Join(tmpFolder, "test.fifo")
	require.NoError(t, unix.Mkfifo(fifoPath, 0o666))

	fifoCh := make(chan []byte, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		f, err := os.OpenFile(fifoPath, os.O_RDONLY, 0)
		if err != nil {
			fifoCh <- nil
			return
		}
		defer f.Close()

		var buf [4096]byte
		n, _ := f.Read(buf[:])
		fifoCh <- buf[:n]
	}()
	<-ready
	time.Sleep(50 * time.Millisecond)

	m := newManager(t, tmpFolder, "- 0\n", addr)
	m.ProcessFrame(v2Heartbeat(9))

	got := recv(2 * time.Second)
	require.NotNil(t, got, "管理出口应收到帧")

	var viaFifo []byte
	select {
	case viaFifo = <-fifoCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for FIFO read")
	}
	require.NotNil(t, viaFifo, "FIFO reader 应读到数据")
	require.Equal(t, viaFifo, got, "FIFO 与管理出口必须是同一份报文")
}

// logReader / syncBuf：捕获 log 输出的并发安全缓冲。
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// countSub 数 substr 在已捕获日志里出现的次数。
func (b *syncBuf) countSub(sub string) int {
	return strings.Count(b.String(), sub)
}

// TestMgmtEndpointWriteFailureIsThrottled 守管理出口写失败的**日志节流**：UDP 无连接，
// 对端不在时 connected socket 会持续返回 ECONNREFUSED，而帧率可达 20Hz——不节流会
// 刷屏，把 FIFO 路径的正常日志一起淹掉。
//
// 判定力：把 `logMgmtErr` 的节流判断删掉（或把 `return` 换成继续打印），连喂多帧后
// 计数会变成「有几帧就几次」，本断言必红。**不能只断言「出现过 ≥1 次」**——那只证明
// 失败被记了，证明不了被节流。
//
// 对端用一个**先监听再关闭**的 UDP socket：端口随即无人接收，ICMP port unreachable
// 回到 connected socket 上，后续 send 即 ECONNREFUSED。第一帧多半仍"成功"（内核还没
// 收到 ICMP），故必须连喂多帧并留出间隔，只喂一两帧会得到零次失败而误判。
func TestMgmtEndpointWriteFailureIsThrottled(t *testing.T) {
	tmpFolder := t.TempDir()

	// 拿到一个空闲端口再关掉监听 ⇒ 该端口上必无接收者。
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := pc.LocalAddr().String()
	require.NoError(t, pc.Close())

	logs := &syncBuf{}
	log.SetOutput(logs)
	// 后注册的先执行：先让 newManager 的 cancel+wg.Wait 把后台 goroutine 收干净，
	// 再还原全局 log 输出，避免残留 goroutine 写到别的测试的输出里。
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	m := newManager(t, tmpFolder, "- 0\n", deadAddr)

	// 连喂 8 帧、每帧 60ms。第一帧很可能"成功"（ICMP 尚未返回），失败从其后开始。
	for i := 0; i < 8; i++ {
		m.ProcessFrame(v2Heartbeat(byte(i)))
		time.Sleep(60 * time.Millisecond)
	}

	// 条件等待：确认失败**确实发生过**（否则下面那个 ==1 会变成对"零次"的误判）。
	deadline := time.Now().Add(3 * time.Second)
	for logs.countSub("mgmt endpoint write failed") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if logs.countSub("mgmt endpoint write failed") == 0 {
		t.Fatalf("前提不成立：8 帧内一次写失败都没发生，本用例退化为空判据（"+
			"ICMP 未按预期返回？对端 %s）。日志：%s", deadAddr, logs.String())
	}

	// 再等一个足以容纳若干帧的窗口：若不节流，这里会继续累加。
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, 1, logs.countSub("mgmt endpoint write failed"),
		"同一错误在 %s 的节流窗口内只应打印一次；多于一次说明节流失效，日志会被刷屏",
		5*time.Second)
}

// TestMgmtEndpointBadAddressFailsInit 守第 7 步的「配置写错当场暴露」：管理出口地址
// 非法时 Initialize 必须返回错误，不能静默降级成「没有下游」——降级是无声的，FIFO
// 照常工作，只有镜像出口悄悄消失。
//
// 用端口越界（99999）而非非法域名：后者要走 DNS，会把网络与超时拖进这个判据。
func TestMgmtEndpointBadAddressFailsInit(t *testing.T) {
	tmpFolder := t.TempDir()
	configPath := filepath.Join(tmpFolder, "filter.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("- 0\n"), 0o644))

	m := &fifofilter.Manager{
		Ctx:          context.Background(),
		Wg:           &sync.WaitGroup{},
		Dialect:      common.Dialect,
		FifoPath:     filepath.Join(tmpFolder, "test.fifo"),
		ConfigPath:   configPath,
		FallbackPath: filepath.Join(tmpFolder, "fallback.tlog"),
		MgmtEndpoint: "127.0.0.1:99999",
	}
	require.Error(t, m.Initialize(), "管理出口地址非法必须让 Initialize 报错，不得静默降级")
}
