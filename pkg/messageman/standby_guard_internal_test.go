package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// 本文件钉住 03 §4.5 的待命三路径：任务结束 / 可疑丢弃 / 候选。
//
// 关键判据只有两条证据：`lastTrustedDownlink`（最近一次被接受的可信下行）与下行水位
// （当前会话内是否还有加密下行）。明文待命心跳本身无认证，只有「同一可信通道」这一次
// 能获得做权威变更的权限，且必须是在当前会话内（下行水位仍存在）发生一次。

const standbyTestDID uint32 = 10001048

func newStandbyManager() (*Manager, *countSink) {
	m := newBareManager()
	sink := &countSink{}
	m.Sink = sink
	return m, sink
}

// TestStandbyTaskEndDeletesSessionState 任务结束：删全部配对、清双水位、删映射、关会话门、
// 强制登记保护候选、立即发布；最近可信通道保留。
func TestStandbyTaskEndDeletesSessionState(t *testing.T) {
	m, sink := newStandbyManager()
	now := time.Now()
	ch := &gomavlib.Channel{}

	m.px4Map[standbyTestDID] = &px4Entry{channel: ch, lastSeen: now, handshaked: true}
	m.lastTrustedDownlink[standbyTestDID] = trustedMark{
		at: now.Add(-time.Minute), channel: ch, sourceIP: "10.0.0.1",
	}
	up := nonceKey{deviceID: standbyTestDID, dir: dirUplink}
	down := nonceKey{deviceID: standbyTestDID, dir: dirDownlink}
	m.lastNonce[up] = nonceEntry{counter: 11, lastSeen: now}
	m.lastNonce[down] = nonceEntry{counter: 22, lastSeen: now}
	// 发布窗口处于生效状态：任务结束路径必须无视它。
	m.lastStandbyPublish[standbyTestDID] = now

	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))

	require.NotContains(t, m.px4Map, standbyTestDID, "任务结束必须删除映射（handshaked 随之消失）")
	require.NotContains(t, m.lastNonce, up)
	require.NotContains(t, m.lastNonce, down)
	gate, ok := m.sessionGate[standbyTestDID]
	require.True(t, ok)
	require.False(t, gate.endedAt.IsZero())
	require.True(t, gate.openedAt.IsZero(), "新会话尚未被 QGC 上行打开")

	cs := m.standbyCandidates[standbyTestDID]
	require.Len(t, cs, 1)
	require.True(t, cs[0].protected)
	require.Equal(t, "10.0.0.1", cs[0].sourceIP)
	require.Same(t, ch, cs[0].channel)

	require.Contains(t, m.lastTrustedDownlink, standbyTestDID, "可信证据保留给后续可疑判定")
	require.Contains(t, m.lastStandbyPublish, standbyTestDID)
	require.Equal(t, 1, sink.n, "任务结束必须发布（不受窗口/预算限制）")
}

// TestStandbySameChannelWithoutWatermarkDoesNotEndTask 同通道但下行水位已不存在
// （任务结束后的持续 1Hz 待命）⇒ 只能走候选路径，不得再次清理。
func TestStandbySameChannelWithoutWatermarkDoesNotEndTask(t *testing.T) {
	m, sink := newStandbyManager()
	now := time.Now()
	ch := &gomavlib.Channel{}

	m.px4Map[standbyTestDID] = &px4Entry{channel: ch, lastSeen: now}
	m.lastTrustedDownlink[standbyTestDID] = trustedMark{
		at: now.Add(-time.Minute), channel: ch, sourceIP: "10.0.0.1",
	}
	up := nonceKey{deviceID: standbyTestDID, dir: dirUplink}
	m.lastNonce[up] = nonceEntry{counter: 11, lastSeen: now}

	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))

	require.Contains(t, m.px4Map, standbyTestDID, "候选路径不得删除映射")
	require.Contains(t, m.lastNonce, up, "候选路径不得动水位")
	require.NotContains(t, m.sessionGate, standbyTestDID)
	cs := m.standbyCandidates[standbyTestDID]
	require.Len(t, cs, 1)
	require.False(t, cs[0].protected)
	require.Equal(t, 1, sink.n)
}

// TestStandbySuspiciousOtherChannelIsNoop 窗口内异通道 ⇒ 整帧丢弃，零状态变更。
func TestStandbySuspiciousOtherChannelIsNoop(t *testing.T) {
	m, sink := newStandbyManager()
	now := time.Now()
	trusted, attacker := &gomavlib.Channel{}, &gomavlib.Channel{}
	lastSeen := now.Add(-2 * time.Minute)

	m.px4Map[standbyTestDID] = &px4Entry{channel: trusted, lastSeen: lastSeen, handshaked: true}
	m.lastTrustedDownlink[standbyTestDID] = trustedMark{
		at: now.Add(-time.Minute), channel: trusted, sourceIP: "10.0.0.1",
	}
	up := nonceKey{deviceID: standbyTestDID, dir: dirUplink}
	down := nonceKey{deviceID: standbyTestDID, dir: dirDownlink}
	m.lastNonce[up] = nonceEntry{counter: 11, lastSeen: now}
	m.lastNonce[down] = nonceEntry{counter: 22, lastSeen: now}

	m.processStandbyHeartbeat(attacker, standbyTestDID, standbyFrame(standbyTestDID))

	e := m.px4Map[standbyTestDID]
	require.Same(t, trusted, e.channel, "可疑帧不得改写映射通道")
	require.Equal(t, lastSeen, e.lastSeen, "可疑帧不得续期 lastSeen")
	require.Equal(t, nonceEntry{counter: 11, lastSeen: now}, m.lastNonce[up])
	require.Equal(t, nonceEntry{counter: 22, lastSeen: now}, m.lastNonce[down])
	require.Empty(t, m.standbyCandidates, "可疑帧不得登记候选")
	require.NotContains(t, m.sessionGate, standbyTestDID)
	require.Empty(t, m.lastStandbyPublish, "被丢弃的帧不得推进发布窗口")
	require.Zero(t, sink.n, "可疑帧不得进下游")
}

// TestStandbyAfterGuardWindowBecomesCandidate 超出 5m 窗口的异通道 ⇒ 候选路径。
func TestStandbyAfterGuardWindowBecomesCandidate(t *testing.T) {
	m, sink := newStandbyManager()
	now := time.Now()
	trusted, other := &gomavlib.Channel{}, &gomavlib.Channel{}

	m.lastTrustedDownlink[standbyTestDID] = trustedMark{
		at: now.Add(-6 * time.Minute), channel: trusted, sourceIP: "10.0.0.1",
	}
	up := nonceKey{deviceID: standbyTestDID, dir: dirUplink}
	m.lastNonce[up] = nonceEntry{counter: 11, lastSeen: now}

	m.processStandbyHeartbeat(other, standbyTestDID, standbyFrame(standbyTestDID))

	cs := m.standbyCandidates[standbyTestDID]
	require.Len(t, cs, 1)
	require.False(t, cs[0].protected)
	require.Same(t, other, cs[0].channel)
	require.NotContains(t, m.px4Map, standbyTestDID, "候选路径不得建权威映射")
	require.Contains(t, m.lastNonce, up, "候选路径不得动水位")
	require.Equal(t, 1, sink.n)
}

// TestStandbyCandidatePublishThrottledByDevice 候选路径每设备 2s 聚合：两帧只发布一次。
func TestStandbyCandidatePublishThrottledByDevice(t *testing.T) {
	m, sink := newStandbyManager()
	ch := &gomavlib.Channel{}

	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))
	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))

	require.Equal(t, 1, sink.n, "2s 窗口内的第二帧不得重复发布")
	require.Len(t, m.standbyCandidates[standbyTestDID], 1, "同 IP 只刷新，不新增")
}

// TestStandbyTaskEndIgnoresFanoutBudget 任务结束不受全局发布预算限制。
func TestStandbyTaskEndIgnoresFanoutBudget(t *testing.T) {
	m, sink := newStandbyManager()
	now := time.Now()
	ch := &gomavlib.Channel{}

	m.px4Map[standbyTestDID] = &px4Entry{channel: ch, lastSeen: now}
	m.lastTrustedDownlink[standbyTestDID] = trustedMark{
		at: now.Add(-time.Minute), channel: ch, sourceIP: "10.0.0.1",
	}
	m.lastNonce[nonceKey{deviceID: standbyTestDID, dir: dirDownlink}] = nonceEntry{counter: 22, lastSeen: now}
	m.fanoutBucket.tokens = 0
	m.fanoutBucket.last = now

	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))

	require.Equal(t, 1, sink.n, "任务结束必须发布，即使全局预算已耗尽")
}

// TestDownlinkRefreshesTrustedMark 被接受的下行刷新最近可信通道。
// 用 D13（已握手 + 同通道）构造「被接受」：免验签帧同样刷新信任表，且不产生 IPC。
func TestDownlinkRefreshesTrustedMark(t *testing.T) {
	m, _ := newStandbyManager()
	ch := &gomavlib.Channel{}
	m.px4Map[standbyTestDID] = &px4Entry{
		channel: ch, lastSeen: time.Now(), handshaked: true,
	}

	m.processEncrypted(ch, standbyTestDID, downlinkFrame(standbyTestDID, 2), dirDownlink)

	mark, ok := m.lastTrustedDownlink[standbyTestDID]
	require.True(t, ok)
	require.Same(t, ch, mark.channel)
	require.False(t, mark.at.IsZero())
}

// TestDownlinkEstablishmentClearsCandidates 一帧**验签通过**的加密下行建立权威映射后，
// 歧义期结束 —— 候选必须清空，否则上行会一直多播到早先登记的来源，且「已建链」后的
// 心跳拦截条件永远不成立。
func TestDownlinkEstablishmentClearsCandidates(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m, _ := newStandbyManager()
	m.VerifySocket = o.path()
	m.VerifyTimeout = 500 * time.Millisecond
	ch := &gomavlib.Channel{}

	// 无映射、无可信证据 ⇒ 明文待命只登记候选，不建权威映射
	m.processStandbyHeartbeat(ch, standbyTestDID, standbyFrame(standbyTestDID))
	require.NotEmpty(t, m.standbyCandidates[standbyTestDID])
	require.NotContains(t, m.px4Map, standbyTestDID)

	m.processEncrypted(ch, standbyTestDID, downlinkFrame(standbyTestDID, 2), dirDownlink)
	waitVerifyIdle(t, m)

	require.Contains(t, m.px4Map, standbyTestDID)
	require.Empty(t, m.standbyCandidates[standbyTestDID],
		"建立权威映射后候选必须清空，上行回到单播")
}
