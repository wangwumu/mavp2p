package messageman_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEntryGateVehicleHeartbeatBelowThresholdDropped 钉住载具端点的 T=50 分流：
// 37B 的 QGC 加密心跳原样投到载具端点时必须整帧丢弃，不能进入加密下行、不能喂 Sink。
// 9B 明文待命是阳性对照——同一条路径上的合法形状仍须工作。
func TestEntryGateVehicleHeartbeatBelowThresholdDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)
	_ = qgc

	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// QGC 心跳 37B 落在 [28B, T=50B)：载具端点只接受 9B 待命，这一帧必须丢。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(1)))
	require.Empty(t, sink.got(), "载具端点的 37B msgID=0 不得进入加密下行")

	// 阳性对照：9B 明文待命仍须到达下游。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	require.Equal(t, []uint32{0}, sink.got(), "9B 明文待命必须照常处理")
}

// TestEntryGateCounterZeroDropped 钉住「counter=0 一律丢弃」：入口即丢，不判重、不写水位、
// 不转发、不喂 Sink。阳性对照用 counter=1 的同一帧，证明丢弃的是 counter 而不是整类帧。
func TestEntryGateCounterZeroDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	require.Len(t, sink.got(), 1, "待命心跳是下游基线")

	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(0)))
	require.Len(t, sink.got(), 1, "counter=0 必须在入口丢弃")

	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(1)))
	// P5 起建链期下行是异步验签，结果晚于本帧返回。
	require.Eventually(t, func() bool { return len(sink.got()) == 2 },
		time.Second, time.Millisecond, "counter=1 的同一帧必须照常处理（且水位未被 0 污染）")
}

// TestEntryGateShortFramesDropped 钉住「其它 msgID payload<28B 两端丢弃」。
// 27B 帧带 counter=0：旧行为里 counter=0 不判重放，这类短帧会被一路转发。
func TestEntryGateShortFramesDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费待命扇出

	// GCS 端点 27B 普通帧：不得上行到 PX4。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, make([]byte, 27)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 阳性对照：28B 以上照常上行。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(2)))
	expectFrame(t, px4, 1*time.Second)

	// 载具端点 27B 普通帧：不得进入下游。
	before := len(sink.got())
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, make([]byte, 27)))
	require.Len(t, sink.got(), before, "载具端点的 27B 帧不得进入下游")
}

// TestEntryGateZeroDeviceIDRegistrationDropped 钉住「帧头 deviceID=0 任意 msgID 丢弃」。
// 80005 帧头为 0 时旧行为仍会登记该通道；登记后上行就能被转发。
func TestEntryGateZeroDeviceIDRegistrationDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 先建 px4Map（否则上行会在「未知 PX4」处被丢，测不到登记闸）。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 帧头 deviceID=0 的登记：必须丢，不得把该通道加入 qgcOnline。
	feed(m, qgc.ch, makeFrame(0, 0, 0, 0, 80005, registrationPayload(px4DeviceID)))
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(1)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 阳性对照：正常帧头的登记后，上行照常送达。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(2)))
	expectFrame(t, px4, 1*time.Second)
}

// TestEntryGateRelabeledMsgIDDoesNotEstablishHandshake 钉 §8.6 第 43 条：把 QGC 37B
// 加密心跳的帧头 msgID 从 0 改成 33 ⇒ 不再走「待命 / T 分流」，按加密下行路径处理
// （37B ≥ 28B，形状合法）。T 是形状闸，不是重放/认证防线（§5.7）：这一帧不得因此置
// handshaked，也不得走 §4.5 的待命处理。
//
// 判定力：若实现把「37B」当成握手帧（按长度而非 msgID 判定），格末的 QGC 加密心跳
// 会被判为「已建链」而拦截，expectFrame 超时。
func TestEntryGateRelabeledMsgIDDoesNotEstablishHandshake(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)
	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费待命扇出
	require.Equal(t, []uint32{0}, sink.got(), "待命心跳是下游基线（待命路径也喂 Sink）")

	// 改标 msgID=33 的 37B 帧：进入加密下行并建映射（异步验签）。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedHeartbeatPayload(2)))
	// ‼️ 必须等 sink **增到 2**，不能只等「非空」：待命帧已占掉第 1 格，等到非空
	// 时验签结果可能还没应用 —— 那时映射未建、候选仍在，QGC 心跳会经候选多播放行，
	// 下一格就与 handshaked 无关了（假绿）。
	require.Eventually(t, func() bool { return len(sink.got()) == 2 },
		2*time.Second, time.Millisecond, "改标后的帧应按加密下行被接受（形状合法）")

	// 未握手 ⇒ QGC 的加密 GCS 心跳必须仍被放行。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(5)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(5), frameCounter(t, ef),
		"改标 msgID 的帧不得置 handshaked：否则 QGC 心跳被误拦，PX4 收不到地面站存活")
}

// TestEntryGateHeartbeatCounterZeroDropped 钉 §8.6 第 44 条：counter=0 的**加密心跳**
// （msgID=0 ∧ payload≥T）同样在入口丢弃：不调 oracle、不置 handshaked、不喂 Sink。
//
// 判定力来自「同形状、仅 counter 不同」的阴性对照：counter=0 的 92B 心跳不得进 Sink，
// counter=2 的同一帧必须进。若 counter=0 只在「非心跳」分支被拦，前者会走验签、建映射、
// 置 handshaked 并喂 Sink，第一格必红。
//
// ‼️ 不拿「QGC 心跳是否被拦」当判据：那需要先有 px4Map，而一旦有映射，错误放行的
// counter=0 帧还会撞上「水位已推进」的重检（0 ≤ 旧水位）被丢弃 —— 结果与正确实现一致，
// 判据就失去了判别力。这里用「无前置映射」让错误路径的结果**可见**。
func TestEntryGateHeartbeatCounterZeroDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)
	sink := &fakeSink{}
	m.Sink = sink

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费待命扇出
	require.Equal(t, []uint32{0}, sink.got(), "待命心跳是下游基线")

	// counter=0 的加密心跳：入口即丢。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, px4EncryptedHeartbeatPayload(0)))
	// ‼️ 不能只在 feed 之后同步断言一次：若该帧被错误地放进异步验签，结果要过一会儿
	// 才落地，此刻 sink 仍是 1 —— 同步断言会假绿。观察满一个窗口，确保它**始终**不来。
	require.Never(t, func() bool { return len(sink.got()) != 1 },
		300*time.Millisecond, 10*time.Millisecond,
		"counter=0 的加密心跳不得喂 Sink（含异步验签路径）")

	// 阴性对照：同形状、仅 counter=2 ⇒ 必须被接受。缺了它，本用例可以靠「任何 92B
	// 心跳都被丢」蒙混过关。它同时证明这条路径确实会喂 Sink（第一格才有意义）。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, px4EncryptedHeartbeatPayload(2)))
	require.Eventually(t, func() bool { return len(sink.got()) == 2 },
		2*time.Second, time.Millisecond, "counter=2 的同一帧必须被接受")
}

// TestEntryGateZeroDeviceIDStandbyDropped 钉 §8.6 第 48 条前半：deviceID=0 的明文待命
// 心跳必须走「deviceID=0 一律丢弃」，不得被待命路径放行（否则能种出 deviceID=0 的候选/
// 映射并开始扇出）。这是独立于号段闸的第一道闸。
func TestEntryGateZeroDeviceIDStandbyDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)
	sink := &fakeSink{}
	m.Sink = sink

	feed(m, px4.ch, makeFrame(0, 0, 0, 0, 0, standbyHeartbeatPayload()))

	require.Empty(t, sink.got(), "deviceID=0 的待命心跳不得进 Sink")
	expectNoFrame(t, qgc, 300*time.Millisecond) // 也不得扇出
}
