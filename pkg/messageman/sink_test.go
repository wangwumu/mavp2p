package messageman_test

import (
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/stretchr/testify/require"
)

// fakeSink 记录交给下游（FIFO / 管理出口）的全部帧。
// ProcessFrame 由 messageman 在喂帧的同一 goroutine 内同步调用，
// 故 feed() 返回后即可直接断言，无需等待。
type fakeSink struct {
	mu     sync.Mutex
	msgIDs []uint32
}

func (s *fakeSink) ProcessFrame(fr frame.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgIDs = append(s.msgIDs, fr.GetMessage().GetID())
}

func (s *fakeSink) got() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint32(nil), s.msgIDs...)
}

// TestSinkDownlinkOnly 守下游出口（FIFO / 管理出口）的**落点**：只有「已通过全部过滤的
// 下行帧」能抵达，QGC 上行一律不能——尤其是 QGC 的加密 GCS 心跳。
//
// 2026-09-28 用户要求：「qgc的心跳过滤掉，不要发给data_writer」「mavp2p既然已经过滤的
// 报文，没有道理让data_writer再过滤一遍」。
//
// 改前 FIFO 是 main.go 事件循环里的**平级旁路**（`messageMan.ProcessFrame(evt)` 与
// `fifoFilter.ProcessFrame(evt)` 各调一次）：mavp2p 在 processEncrypted 里早已识别并
// 拦下 QGC 加密心跳，但旁路照样把它写进 FIFO —— 该帧随后在 data_writer 推进其**当时的**
// 单水位判重，把下行遥测饿死（2026-09-27 10:40 实测零入库）。下游那层判重已于
// 2026-09-28 按 §2.6 删除，故本用例钉的是**本组件自己的**过滤义务（§3.2.5 对 FIFO
// 内容的定义），而不是「下游反正会拦」。
//
// 判定力：把 sink 调用挪回任何「过滤前」位置（ProcessFrame 开头、或各早退分支之前），
// 断言必红——但**红在最先受影响的那一条**：sink 提前后第一个进 sink 的是 80005 登记帧，
// 于是红的是「80005 不得进下游」那格，而不是格 2/4/5。require 是 fail-fast，红了就停，
// 后面几格根本不执行 ⇒ 描述变异后果要按「哪条断言先失败」，不能按「哪几格会红」。
func TestSinkDownlinkOnly(t *testing.T) {
	node, m, stop := newServer(t, "3370")
	defer stop()

	qgc := connectPeer(t, node, "3370")
	px4 := connectPeer(t, node, "3370")

	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 明文待命心跳（未握手）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	require.Empty(t, sink.got(), "80005 登记心跳仅由 mavp2p 消费，不得进下游")

	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出，消费
	require.Equal(t, []uint32{0}, sink.got(),
		"格 1：PX4 明文待命心跳必须进下游（规范 §3.2.5：FIFO 内容＝原样加密帧＋明文待命心跳）")

	// ---- 格 2「未握手期 QGC 加密 GCS 心跳」：照常转发给 PX4，但不得进下游 ----
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(1)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(1), frameCounter(t, ef))
	require.Equal(t, []uint32{0}, sink.got(),
		"格 2：QGC 上行心跳不得进下游（§3.2.5：FIFO 内容只含下行帧 + PX4 明文待命心跳）")

	// ---- 格 3「PX4 加密心跳」：置位握手；它本身是下行，必须进下游 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(2)))
	expectFrame(t, qgc, 1*time.Second) // 送达配对 QGC，消费
	require.Equal(t, []uint32{0, 0}, sink.got(), "格 3：PX4 加密心跳是下行，必须进下游")

	// ---- 格 4「已握手期 QGC 加密 GCS 心跳」：被 mavp2p 拦截，同样不得进下游 ----
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(5)))
	expectNoFrame(t, px4, 300*time.Millisecond)
	require.Equal(t, []uint32{0, 0}, sink.got(), "格 4：被拦的 QGC 加密心跳不得进下游")

	// ---- 格 5「QGC 普通加密上行」：非心跳，转发给 PX4；仍不得进下游 ----
	// counter=3 顺带钉住「被拦心跳不推进上行水位」：若格 4 的 counter=5 推进了水位，
	// 3<=5 会被判重丢弃，本格的上行到不了 PX4、expectFrame 超时。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(3)))
	expectFrame(t, px4, 1*time.Second)
	require.Equal(t, []uint32{0, 0}, sink.got(), "格 5：QGC 普通上行指令不得进下游")

	// ---- 格 6「PX4 加密下行」：必须进下游 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(8)))
	expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, []uint32{0, 0, 33}, sink.got(), "格 6：PX4 加密下行必须进下游")
}

// TestSinkSkipsDroppedFrames 守下游出口的**否定面**：被丢弃的帧不得进下游。
// 落点若放在防重放判定之前、或放在 px4Map 命中判定之前，断言必红——红在最先受影响的
// 那一条（80005/待命心跳提前进 sink ⇒ 下面那个 `before == 2` 的前置先失败），
// 此时三格根本不执行，所以「三格必红」是错的读法。
func TestSinkSkipsDroppedFrames(t *testing.T) {
	node, m, stop := newServer(t, "3371")
	defer stop()

	qgc := connectPeer(t, node, "3371")
	px4 := connectPeer(t, node, "3371")

	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出，消费

	// 建立下行水位
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(10)))
	expectFrame(t, qgc, 1*time.Second)
	before := len(sink.got())
	require.Equal(t, 2, before, "前置：待命心跳 + 首条下行")

	// 格 1：重放同一 counter 的下行帧 —— 被边缘防重放丢弃
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(10)))
	require.Equal(t, before, len(sink.got()), "格 1：被防重放丢弃的帧不得进下游")

	// 格 2：未登记 PX4 的下行帧 —— px4Map 查不到，丢弃
	oInc, oCom, oSys, oComp := didBytes(px4DeviceID + 1)
	feed(m, px4.ch, makeFrame(oInc, oCom, oSys, oComp, 33, encryptedPayload(4)))
	require.Equal(t, before, len(sink.got()), "格 2：未知 deviceID 的下行帧不得进下游")

	// 格 3：未登记来源的 QGC 上行（奇数 counter，非 QGC 来源表）—— 忽略
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(7)))
	require.Equal(t, before, len(sink.got()), "格 3：未登记来源的上行帧不得进下游")
}

// TestSinkNilIsNoOp 守 **Sink 未接线**（nil）这一形态——即**默认部署**：不带
// `--fifo-enable` 时 main.go 根本不注入 Sink，生产上跑的大多数就是这条路径。
// 两处的 `if m.Sink != nil` 守卫若被删，nil 接口上调用方法会直接 panic，把整个
// mavp2p 进程带走；而上面两个用例都设了 Sink，删掉守卫它们照样全绿。
//
// 本用例必须同时证明「帧确实被处理了」，不能只断言「没 panic」——提前 return 也
// 满足「没 panic」。故两条路径各留一条阳性对照：待命心跳必须扇出到 QGC，加密下行
// 必须送达配对的 QGC。两条路径分别覆盖 manager.go 里的两处守卫（待命心跳一处、
// 加密下行一处），只喂其中一条会漏掉另一处。
func TestSinkNilIsNoOp(t *testing.T) {
	node, m, stop := newServer(t, "3372")
	defer stop()

	qgc := connectPeer(t, node, "3372")
	px4 := connectPeer(t, node, "3372")

	// 刻意不设 m.Sink：保持零值 nil。

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	// 路径 1：PX4 明文待命心跳（processStandbyHeartbeat 末尾的守卫）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	require.Equal(t, uint32(0), expectFrame(t, qgc, 1*time.Second).Frame.GetMessage().GetID(),
		"阳性对照：Sink 为 nil 不得影响待命心跳扇出")

	// 路径 2：已登记 PX4 的加密下行（processEncrypted 末尾的守卫）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1))) // 上行建链
	_ = expectFrame(t, px4, 1*time.Second)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2))) // 下行
	require.Equal(t, uint32(33), expectFrame(t, qgc, 1*time.Second).Frame.GetMessage().GetID(),
		"阳性对照：Sink 为 nil 不得影响加密下行投递")
}
