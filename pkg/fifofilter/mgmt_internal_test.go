package fifofilter

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/bluenviron/gomavlib/v4/pkg/tlog"
	"github.com/stretchr/testify/require"
)

// 本文件是**内部**测试（package fifofilter，与 mgmt_test.go 的 fifofilter_test 外部包
// 同目录共存）。理由：要钉的三条判据全在未导出面上 —— mgmtConn / mgmtDropped / mgmtWg
// 是私有字段，cleanup 与 handleEntry 的丢帧分支也没有对外的观测出口。
//
// ‼️ 三条用例都**刻意不调 Initialize()**：那会 mkfifo、开 fallback 文件、起 run() 与
// mgmtRun() 两个 goroutine。用例各自只用得上一小块，拉起全套反而把判据泡进无关的时序里
// （mgmtRun 会立刻把队列排空，队列满那一格就永远造不出来）。缺什么就手工填什么。

// newMgmtHarness 造一个只有「管理出口连接」这一条的 Manager：Ctx 有效、mgmtAddr 指向一个
// 真实的本地 UDP 接收端、mgmtConn 已建好。
func newMgmtHarness(t *testing.T) *Manager {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rcv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rcv.Close() })
	addr := rcv.LocalAddr().(*net.UDPAddr)

	conn, err := net.DialUDP("udp", nil, addr)
	require.NoError(t, err)

	m := &Manager{Ctx: ctx, mgmtAddr: addr, mgmtConn: conn}

	// 关连接的责任收在这里，而不是各用例自己记：有用例会中途把 m.mgmtConn 换成另一个
	// socket，跟着旧指针去关就漏了新的。
	t.Cleanup(func() {
		m.mgmtConnMu.Lock()
		if m.mgmtConn != nil {
			_ = m.mgmtConn.Close()
		}
		m.mgmtConnMu.Unlock()
	})
	return m
}

// closeUnderlyingFd 绕过 Go runtime、从 syscall 层直接关掉 conn 的底层 fd。
//
// 为什么不能用 conn.Close()：那样 Write 返回的是 net.ErrClosed，而 mgmtWrite **刻意**
// 静默吞掉它（关闭期 cleanup 正是靠关连接来打断阻塞中的 Write，那次失败是预期的）。
// 要走 fatal 分支就得拿到真的 EBADF —— syscall.Close 绕开 runtime，errno 原样透上来。
// （已实测：这样拿到的错误 Is(EBADF)=true 且 Is(ErrClosed)=false。）
func closeUnderlyingFd(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	raw, err := conn.SyscallConn()
	require.NoError(t, err)
	var fd int
	require.NoError(t, raw.Control(func(f uintptr) { fd = int(f) }))
	require.NoError(t, syscall.Close(fd))
}

// TestFatalMgmtWriteDropsConnSoNextFrameCanRebuild 钉住致命写失败之后的**接续动作**。
//
// isFatalSocketErr 的分类本身已由 mgmt_classify_test.go 钉住；本用例钉的是它接没接到
// mgmtDiscardConn 上、以及丢弃之后退避有没有一并清零。两件事各自都会坏：
//   - 判成致命却只记日志（不丢连接）⇒ 下一条报文继续往死 socket 里写，静默全丢；
//   - 丢了连接但不清 mgmtRedialAt ⇒ 重建被 1s 退避挡住，白白多丢一秒的帧。
func TestFatalMgmtWriteDropsConnSoNextFrameCanRebuild(t *testing.T) {
	m := newMgmtHarness(t)
	require.NotNil(t, m.mgmtConn)
	closeUnderlyingFd(t, m.mgmtConn)

	m.mgmtWrite([]byte("payload"))

	require.Nil(t, m.mgmtConn,
		"致命写失败（EBADF 一类）必须丢弃连接，否则下一条报文还会往这个死 socket 里写")
	require.True(t, m.mgmtRedialAt.IsZero(),
		"致命失败要**清零**重建时间戳：这是第一次发现 socket 失效，退避只留给「重建本身也失败」")

	// 行为判据：清零点必须真的让**下一条报文**立刻重建，而不只是把一个字段抹成零值。
	require.NotNil(t, m.mgmtConnForWrite(),
		"下一条报文取连接时仍被 %s 的退避挡住 —— 清零点没起到作用", mgmtRedialInterval)
}

// TestRecoverableMgmtWriteKeepsConn 是本文件的阴性对照：**可恢复**错误绝不能丢连接。
//
// 少了它，把 mgmtWrite 改成「Write 出错就 mgmtDiscardConn」也能让上一格全绿 —— 而那是
// 错的：ECONNREFUSED / EHOSTUNREACH / ENOBUFS 这一类下一次 Write 会自愈，重建反而丢掉
// 既有的本地绑定。
//
// 判据分两层：循环里每一轮都断言连接**还在**（防误丢）；循环结束后再断言 mgmtWrite
// **自己**确实记下了那次可恢复失败（防本格空转成恒绿）。
//
// ‼️ 第二层刻意**不**用「另发一个探针包看它报不报错」来判定。试过，那是错的：往同一个
// dead port 多发一个包只是又触发一次 ICMP，而 socket 上的 pending 错误（sk_err）会被
// **任意一次** Write 取走 —— 探针自己拿走了那个错误，mgmtWrite 那一次就什么也没拿到，
// 于是循环里的连接断言全程在空转，本格却因为探针报错而变绿（实测过一次：整段跑完
// 一行日志都没有）。判据必须钉在**被考察的那次调用**上，而不是同一 socket 上另一次调用。
// 日志是 logMgmtErr 唯一的出口，这里把它接管过来当作证据。
func TestRecoverableMgmtWriteKeepsConn(t *testing.T) {
	m := newMgmtHarness(t)

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf) // 同包内测试默认串行，接管是安全的
	t.Cleanup(func() { log.SetOutput(prev) })

	// 目标换成一个刚被关掉的本地 UDP 端口：往那儿发，内核回 ICMP port unreachable，
	// 于是这个 connected socket 的下一次 Write 拿到 ECONNREFUSED（可恢复类）。
	dead, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	deadAddr := dead.LocalAddr().(*net.UDPAddr)
	require.NoError(t, dead.Close())

	conn, err := net.DialUDP("udp", nil, deadAddr)
	require.NoError(t, err)
	m.mgmtConnMu.Lock()
	_ = m.mgmtConn.Close()
	m.mgmtConn = conn
	m.mgmtConnMu.Unlock()

	// ICMP 是异步到的：第一发只负责触发，之后才可能拿到错误。循环等它出现。
	sawRecoverable := false
	for i := 0; i < 200 && !sawRecoverable; i++ {
		time.Sleep(5 * time.Millisecond)
		m.mgmtWrite([]byte("payload"))
		require.NotNil(t, m.mgmtConn,
			"可恢复错误被当成致命错误、连接被丢弃了 —— 这类错误下一次 Write 会自愈，"+
				"重建反而丢掉既有的本地绑定")
		sawRecoverable = strings.Contains(logBuf.String(), "mgmt endpoint write failed")
	}
	require.True(t, sawRecoverable,
		"1 秒内 mgmtWrite 一次可恢复失败都没记到 —— 本格其实没跑到目标分支，绿是空的。日志：\n%s",
		logBuf.String())

	// 记了错误就必须是**可恢复**那一类：致命错误走的是 mgmtDiscardConn，会打另一句文案。
	require.NotContains(t, logBuf.String(), "socket unusable",
		"探针造出来的错误被判成了致命错误：%s", logBuf.String())
	require.Contains(t, logBuf.String(), "connection refused",
		"造出来的应当是 ECONNREFUSED（可恢复类的代表）；实际日志：%s", logBuf.String())
}

// newEntryHarness 造一个够 handleEntry 跑完的 Manager：frameWriter + fallback 文件齐备，
// FifoPath 指向一个**不存在**的路径（writeToFifo 必然失败 ⇒ 稳定走 fallback 那一支）。
// 刻意不建渠道：与 FIFO 那条路径的成败无关，本用例只关心投递侧那三行 select。
//
// messageIDs 只放本帧那一个 msgID —— 不是偷懒，是让 ProcessFrame 的两道闸（白名单、
// 队列）里只留下被测的那一道；白名单没配全时，用它的用例会以前提断言的形式立刻报出来，
// 而不是安静地空转过去。
func newEntryHarness(t *testing.T) (*Manager, *tlog.Entry) {
	t.Helper()
	dir := t.TempDir()

	rw := &dialect.ReadWriter{Dialect: &dialect.Dialect{}}
	require.NoError(t, rw.Initialize())

	fb, err := os.Create(filepath.Join(dir, "fallback.tlog"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = fb.Close() })

	const msgID = uint32(30)
	m := &Manager{
		Ctx:          context.Background(),
		FifoPath:     filepath.Join(dir, "absent.fifo"),
		fallbackTlog: &tlog.Writer{ByteWriter: fb, DialectRW: rw},
		messageIDs:   map[uint32]struct{}{msgID: {}},
	}
	m.frameWriter = &frame.Writer{ByteWriter: &m.encodeBuf, DialectRW: rw}
	require.NoError(t, m.frameWriter.Initialize())
	require.NoError(t, m.fallbackTlog.Initialize())

	entry := &tlog.Entry{
		Time: time.Now(),
		Frame: &frame.V2Frame{
			SystemID:    1,
			ComponentID: 1,
			Message:     &message.MessageRaw{ID: msgID, Payload: []byte{1, 2, 3}},
		},
	}
	return m, entry
}

// TestMgmtQueueFullDiscardsInsteadOfBlocking 钉住投递侧的**非阻塞**契约，以及丢了要记账。
//
// handleEntry 与 FIFO/fallback 共用同一个 goroutine —— 它是 data_writer 的唯一粮道。
// 所以队列满时只能丢，绝不能等：一旦在这里阻塞，上游帧率的每一次抖动都会变成下游的断粮。
//
// 队列容量取 1 并预先占满，且**不起 mgmtRun**（没有消费者 ⇒ 不可能被排空），
// default 分支因此是必然而非概率。
func TestMgmtQueueFullDiscardsInsteadOfBlocking(t *testing.T) {
	m, entry := newEntryHarness(t)
	m.mgmtCh = make(chan []byte, 1)
	m.mgmtCh <- []byte("占位：让队列处于满状态")

	done := make(chan struct{})
	go func() {
		_ = m.handleEntry(entry)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("mgmt 队列满时 handleEntry 卡住了 —— 它与 FIFO/fallback 共用同一个 goroutine，" +
			"在这里等队列腾空间会让 data_writer 一起断粮")
	}

	require.Len(t, m.mgmtCh, 1, "队列已满，不该有东西被塞进去")
	require.Equal(t, uint64(1), m.mgmtDropped.Load(),
		"丢了一帧却没记账 ⇒ 静默丢帧：运维看到的是「镜像好像少了点」，没有任何数字可对")
}

// TestCleanupWaitsForMgmtRunToExit 钉住 cleanup 的 mgmtWg.Wait()。
//
// ‼️ 这是个**反向**用例：绿 = 「cleanup 没有返回」。
//
// 为什么要反过来：正常路径上（Ctx 先取消、mgmtRun 随即退出）Wait 与不 Wait 分不出来 ——
// mgmtRun 会在微秒级退出。要让它可判，就得构造一个「mgmtRun 必然还活着」的状态，而
// 那正是**不取消 Ctx**：mgmtRun 会一直等在 select 上。此时调 cleanup：
//   - 有 mgmtWg.Wait() ⇒ 它必须卡住（本用例观察到超时 ⇒ 绿）；
//   - 删掉 Wait        ⇒ 它立刻返回（本用例看到 done ⇒ 红）。
//
// 这不是刁难行为：cleanup 的调用时机本就是 run() 因 Ctx.Done 退出之后（defer），
// Ctx 取消在先 —— 用例只是把这个前提摆到台面上，让 Wait 的存在与否变成可观测的。
//
// 收尾必须 cancel()，否则 mgmtRun 永不退出、测试进程收不干净。
func TestCleanupWaitsForMgmtRunToExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &Manager{Ctx: ctx, mgmtCh: make(chan []byte, 1)}
	m.mgmtWg.Add(1)
	go m.mgmtRun()

	done := make(chan struct{})
	go func() {
		m.cleanup()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Ctx 还没取消、mgmtRun 还在 select 上活着，cleanup 却已经返回了 —— " +
			"mgmtWg.Wait() 不在了。返回之后 mgmtRun 仍可能醒来碰已关的 fd，甚至把它重建回来")
	case <-time.After(300 * time.Millisecond):
		// 正确：cleanup 正卡在 mgmtWg.Wait() 上
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Ctx 取消之后 cleanup 仍未返回 —— mgmtRun 没退出")
	}
}

// TestMgmtDropIsCountedAndLogsDoNotCannibaliseEachOther 钉住两条**丢帧**路径的记账与节流。
//
// 两件事一起钉，因为它们共用一套机制：
//
//  1. 重建退避窗口内的丢帧（mgmtConnForWrite 返回 nil）也要计数。这条分支曾经完全静默 ——
//     重建失败后的整整一秒里每一帧都无声消失，运维只看得出「镜像好像少了点」。
//
//  2. 两条丢帧文案**各自独立节流**。throttledLog 只有一个 msg 字段当节流键：两条不同文案
//     共用一个实例会互相把对方的键顶掉 —— A 打完 B 打，B 把键换成自己，于是下一条 A 又
//     认为「键变了、该打」，交替刷新、双双失去节流（帧率可达 20Hz，等于刷屏）。判据因此
//     取「每条文案都**只**出现一次」：共用一个实例时，第三次调用（又一次队列满）会把
//     queue full 那行打第二遍。
//
// 顺带钉住 warnKeyed 的存在理由：退避那句的输出里带了**累计丢帧数**，若拿全文当节流键，
// 键每帧都不同，三次调用会打出三条（累计 1/2/3 帧）。断言「只出现一次」把这条也盖住了。
func TestMgmtDropIsCountedAndLogsDoNotCannibaliseEachOther(t *testing.T) {
	m, entry := newEntryHarness(t)

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	t.Cleanup(func() { log.SetOutput(prev) })

	// 队列容量 1 并预先占满，且不起 mgmtRun ⇒ 投递必然落进 default。
	m.mgmtCh = make(chan []byte, 1)
	m.mgmtCh <- []byte("占位：让队列处于满状态")

	// 连接缺失 + 退避窗口未到 ⇒ mgmtConnForWrite 返回 nil，走退避丢帧那一支。
	require.Nil(t, m.mgmtConn)
	m.mgmtRedialAt = time.Now()

	require.NoError(t, m.handleEntry(entry)) // 丢帧来源 1：队列满
	// 丢帧来源 2：重建退避，**连发三次**。三次是刻意的：累计丢帧数每次都在变，若节流键
	// 取自格式化后的全文（即误用 warn 而非 warnKeyed），三次会各打一条 —— 只发一次的话
	// 这个缺陷看不出来。
	m.mgmtWrite([]byte("x"))
	m.mgmtWrite([]byte("x"))
	m.mgmtWrite([]byte("x"))
	require.NoError(t, m.handleEntry(entry)) // 再来一次队列满 —— 用来验节流没被顶掉

	require.Equal(t, uint64(5), m.mgmtDropped.Load(),
		"两条丢帧路径都要记账；这里期望 5（队列满 ×2 + 退避 ×3）")

	logs := logBuf.String()
	require.Equal(t, 1, strings.Count(logs, "mgmt endpoint queue full, discarding frame"),
		"队列满的文案打了不止一次 ⇒ 节流键被另一条文案顶掉了（两条文案必须各用一个 throttledLog）。日志：\n%s", logs)
	require.Equal(t, 1, strings.Count(logs, "in redial backoff"),
		"退避丢帧的文案打了不止一次 ⇒ 要么节流失效，要么输出里的累计计数被打进了节流键。日志：\n%s", logs)
}

// TestFifoQueueFullIsCountedAndThrottled 钉住 **FIFO** 那条队列的丢帧可见性。
//
// 这一条比 mgmt 侧更要紧：它丢的是 data_writer 的遥测，不是尽力而为的镜像。改前这里是
// 一句裸 log.Printf —— 队列 128、上游 20Hz 时每秒 20 条，刷屏之外还一个数字都不留，
// 运维只能靠肉眼从日志海洋里看出「遥测少了」。
//
// 判据与 mgmt 侧同形：连丢 3 帧 ⇒ 计数 3、日志 1 条。计数用 mgmtDropped 之外的**独立**
// 字段，所以顺便钉住「两路各记各的」：本用例只丢 FIFO，mgmtDropped 必须纹丝不动。
func TestFifoQueueFullIsCountedAndThrottled(t *testing.T) {
	m, entry := newEntryHarness(t)

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	t.Cleanup(func() { log.SetOutput(prev) })

	// 投递队列容量 1 并预先占满，且不起 run()（没有消费者 ⇒ 不可能被排空）。
	m.chEntry = make(chan *tlog.Entry, 1)
	m.chEntry <- entry

	// 白名单必须含本帧的 msgID，否则 ProcessFrame 在队列判断之前就早退了，本格空转。
	require.Contains(t, m.messageIDs, entry.Frame.GetMessage().GetID(),
		"夹具前提不成立：帧的 msgID 不在白名单里，ProcessFrame 会在这一步早退，判据全是空的")

	for i := 0; i < 3; i++ {
		m.ProcessFrame(entry.Frame)
	}

	require.Len(t, m.chEntry, 1, "队列已满，不该有东西被塞进去")
	require.Equal(t, uint64(3), m.fifoDropped.Load(), "FIFO 丢帧没记账")
	require.Zero(t, m.mgmtDropped.Load(),
		"FIFO 丢帧记进了 mgmtDropped —— 两路必须各记各的，否则运维分不清丢的是哪条链")
	require.Equal(t, 1, strings.Count(logBuf.String(), "fifo queue is full"),
		"三条丢帧只该有一条日志（节流 + 节流键不能含累计值）。日志：\n%s", logBuf.String())
}
