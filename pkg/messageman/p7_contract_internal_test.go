package messageman

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// 本文件补 03 §8.2 第 20 条、§8.5 第 36 条与 §8.6 第 43–49、51 条中需要观察内部
// 状态的用例；43/44/48 的入口形状部分在 entry_gate_test.go（外部包，走真实 ProcessFrame）。

// TestOracleMalformedRepliesFailClosed 覆盖 §8.6 第 45 条：应答长度不足 5B、
// 状态码越界，都按 fail-closed 处理，不写任何状态。
// （reqID 不匹配已由 TestVerifyTimeoutAndMalformedReply 覆盖。）
func TestOracleMalformedRepliesFailClosed(t *testing.T) {
	t.Run("reply shorter than 5B", func(t *testing.T) {
		o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
			// 只回 reqID（4B），缺状态字节 ⇒ 实现侧 io.ReadFull 失败。
			raw := make([]byte, 4)
			binary.BigEndian.PutUint32(raw, 1)
			return fakeOracleReply{raw: raw}
		})
		m := newVerifyManager(t, o)
		ch := &gomavlib.Channel{}

		m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 2, 36), dirDownlink)
		waitVerifyIdle(t, m)

		require.Empty(t, m.px4Map, "应答长度不对必须 fail-closed，不得建映射")
		require.Empty(t, m.lastNonce, "应答长度不对不得写水位")
		// 诊断归一：读不满按「服务错误」，不能冒出一个未定义的状态。
		_, logged := m.verifyDiag[verifyDiagKey{deviceID: standbyTestDID, status: verifyInternal}]
		require.True(t, logged, "应答读失败必须记为 service-error，而不是静默或记成别的状态")
	})

	t.Run("status out of range", func(t *testing.T) {
		o := startFakeOracle(t, func(r fakeOracleRequest) fakeOracleReply {
			// reqID 必须**对得上**，否则先命中「reqID 不匹配」分支，测不到状态归一段。
			raw := make([]byte, 5)
			binary.BigEndian.PutUint32(raw[:4], r.reqID)
			raw[4] = 7 // 超出 0–6 的状态码
			return fakeOracleReply{raw: raw}
		})
		m := newVerifyManager(t, o)
		ch := &gomavlib.Channel{}

		m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 2, 36), dirDownlink)
		waitVerifyIdle(t, m)

		require.Empty(t, m.px4Map, "状态码越界必须按「无法解析」fail-closed")
		require.Empty(t, m.lastNonce, "状态码越界不得写水位")
		// ‼️ 这里必须查**归一后的**状态：任何非 pass 状态本身都会被 fail-closed 丢掉，
		// 所以「不写状态」对上没上归一化那条分支是零判别力。真正要钉的是诊断可读：
		// 越界状态码应记成 version（无法解析），而不是一个未定义的数字。
		_, logged := m.verifyDiag[verifyDiagKey{deviceID: standbyTestDID, status: verifyVersionUnsupported}]
		require.True(t, logged, "越界状态码必须归一为 version，否则排障时看到一个不存在的状态名")
	})
}

// TestVerifyPerDeviceInFlightIsSerial 覆盖 §8.6 第 46 条：同设备同一时刻只有 1 个
// oracle 请求在途；慢请求在前时，快请求必须等它，且结果按入队顺序应用。
func TestVerifyPerDeviceInFlightIsSerial(t *testing.T) {
	var active, maxActive atomic.Int64

	o := startFakeOracle(t, func(r fakeOracleRequest) fakeOracleReply {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		defer active.Add(-1)
		// counter 在 payload 前 8B；第一帧（counter=2）故意慢。
		if binary.BigEndian.Uint64(r.payload[:8]) == 2 {
			time.Sleep(60 * time.Millisecond)
		}
		return passReply()
	})
	m := newVerifyManager(t, o)
	sink := &syncSink{}
	m.Sink = sink
	ch := &gomavlib.Channel{}
	// 条目存在但 handshaked=false ⇒ D13 不放行，两帧都要验签（建链期典型形态）。
	// 不从「无条目」起步：第一帧新建映射会改变条目指针，第二帧入队时的 nil 快照
	// 会被 §4.7 第 7 步的重检作废（fail-closed，另有专门用例钉它）。
	m.px4Map[standbyTestDID] = &px4Entry{channel: ch}

	m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 2, 36), dirDownlink)
	m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 3, 36), dirDownlink)
	waitVerifyIdle(t, m)

	require.Equal(t, int64(1), maxActive.Load(),
		"同设备同时只能有 1 个 in-flight；并发会让两个结果互相覆盖水位")
	require.Equal(t, int64(2), o.requests.Load())
	require.Equal(t, 2, sink.count(), "两帧都必须被接受（第二帧不是被丢弃）")

	// 结果按入队顺序应用 ⇒ 水位停在第二帧的 counter。
	wm, ok := m.lastNonce[nonceKey{deviceID: standbyTestDID, dir: dirDownlink}]
	require.True(t, ok)
	require.Equal(t, uint64(3), wm.counter, "结果必须按请求顺序应用，不能串帧")

	o.mu.Lock()
	order := append([]fakeOracleRequest(nil), o.seen...)
	o.mu.Unlock()
	require.Len(t, order, 2)
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(order[0].payload[:8]))
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(order[1].payload[:8]),
		"oracle 收到的请求顺序必须与入队顺序一致")
}

// TestCrossDirectionReplayIsKnownResidual 记录 §5.1 的已知残余（§8.6 第 49 条）：
// 把 QGC 的上行加密遥测原样投到载具端点，密文本身合法、GCM 不绑定方向，帧按
// 「入口即方向」被当作下行并推进**下行**水位。
//
// ‼️ 本用例钉的是「残余仍然存在」，不是「已拦截」。若将来协议加入方向绑定，
// 本格应改为期望丢弃 —— 改之前请先改 03 §5.1。
func TestCrossDirectionReplayIsKnownResidual(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m := newVerifyManager(t, o)
	ch := &gomavlib.Channel{}

	// 这条帧的原始身份是 QGC 上行遥测：counter=7、msgID=33、36B。
	m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 7, 36), dirDownlink)
	waitVerifyIdle(t, m)

	wm, ok := m.lastNonce[nonceKey{deviceID: standbyTestDID, dir: dirDownlink}]
	require.True(t, ok, "跨方向重放会推进下行水位 —— 这是已知残余，不是被拦下了")
	require.Equal(t, uint64(7), wm.counter)
	_, uplinkWatermark := m.lastNonce[nonceKey{deviceID: standbyTestDID, dir: dirUplink}]
	require.False(t, uplinkWatermark, "它只影响下行水位；上行水位由 QGC 入口单独维护")
}

// TestSessionGateExpiryDoesNotDeleteWatermark 覆盖 §8.6 第 51 条：会话门按
// StateIdleTimeout 回收，与防重放水位的回收是两条独立判据 —— 会话门过期本身
// **不得**顺带删掉水位（水位只由它自己的 lastSeen 决定）。
func TestSessionGateExpiryDoesNotDeleteWatermark(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	idle := m.stateIdleTimeout()

	const did = uint32(10001001)
	// 全会话门过期；水位是新鲜的（模拟「门关着，但刚有帧被拒/被接受后刷新」）。
	m.sessionGate[did] = gateState{endedAt: now.Add(-idle - time.Second)}
	m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}] = nonceEntry{counter: 9, lastSeen: now}

	m.prune()

	_, gateLeft := m.sessionGate[did]
	require.False(t, gateLeft, "空闲超过阈值的会话门必须被回收")
	wm, wmLeft := m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}]
	require.True(t, wmLeft,
		"会话门过期不得连坐水位：两条回收互不隐含，否则一次门回收等于把重放窗口重开")
	require.Equal(t, uint64(9), wm.counter)
}

// TestD13SurvivesOracleOutage 覆盖 03 §8.5 第 36 条：oracle 长期不可用时，已建链
// 同通道的下行**照常转发**（免验签路径根本不碰 socket）；需要验签的帧（换通道）
// fail-closed 丢弃。fail-closed 只作用于需要验签的帧，不能退化成全链路中断。
func TestD13SurvivesOracleOutage(t *testing.T) {
	m := newBareManager()
	m.VerifySocket = "/nonexistent/mavp2p-verify.sock"
	m.VerifyTimeout = 5 * time.Millisecond
	m.Sink = &syncSink{}
	sink := m.Sink.(*syncSink)

	ch := &gomavlib.Channel{}
	m.px4Map[standbyTestDID] = &px4Entry{channel: ch, handshaked: true}

	// 同通道 + 已握手 ⇒ 免验签，同步放行。
	m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 2, 36), dirDownlink)
	require.Equal(t, 1, sink.count(), "oracle 不可用时，免验签的同通道下行必须照常转发")

	// 换通道 ⇒ 必须验签 ⇒ socket 不存在 ⇒ 丢弃，权威通道保持不变。
	ch2 := &gomavlib.Channel{}
	m.processEncrypted(ch2, standbyTestDID, verifyFrame(standbyTestDID, 33, 3, 36), dirDownlink)
	waitVerifyIdle(t, m)

	require.Same(t, ch, m.px4Map[standbyTestDID].channel,
		"验签服务不可用时换通道帧必须 fail-closed，不得晋升")
	require.Equal(t, 1, sink.count(), "被丢弃的换通道帧不得喂 Sink")
}

// TestOracleRecoversAfterOutage 覆盖 03 §8.2 第 20 条：oracle 短暂不可用后恢复，
// 下一帧验签下行应当恢复（连接失败触发的退避不得变成永久熔断）。
// 期间被丢弃的帧不污染水位（失败帧在重检之前就返回）。
func TestOracleRecoversAfterOutage(t *testing.T) {
	var down atomic.Bool
	down.Store(true) // handler 返回时连接被关闭且不回包 ⇒ 实现侧读失败

	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
		if down.Load() {
			return fakeOracleReply{closeWithoutReply: true}
		}
		return passReply()
	})
	m := newVerifyManager(t, o)

	ch := &gomavlib.Channel{}
	m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 2, 36), dirDownlink)
	waitVerifyIdle(t, m)
	require.Empty(t, m.px4Map, "oracle 不可用时必须 fail-closed")
	require.Empty(t, m.lastNonce, "失败的帧不得写水位")

	down.Store(false)
	require.Eventually(t, func() bool {
		// counter 逐次递增：退避期内请求会被直接判失败，重试帧不能撞上水位（此处水位仍空，
		// 递增只是保持与真实发送端一致）。
		m.processEncrypted(ch, standbyTestDID, verifyFrame(standbyTestDID, 33, 3, 36), dirDownlink)
		waitVerifyIdle(t, m)
		return len(m.px4Map) == 1
	}, 3*time.Second, 100*time.Millisecond, "oracle 恢复后下一帧必须能重新晋升（退避不能变成永久熔断）")
}
