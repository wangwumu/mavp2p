package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// TestPrunePurgesIdleStandbyCandidates 候选表按 MapTTL 过期；新鲜候选必须保留。
func TestPrunePurgesIdleStandbyCandidates(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	ttl := m.mapTTL()

	oldCh, freshCh := &gomavlib.Channel{}, &gomavlib.Channel{}
	m.standbyCandidates = map[uint32][]candidate{
		10001001: {{channel: oldCh, sourceIP: "10.0.0.1", lastSeen: now.Add(-2 * ttl)}},
		10001002: {{channel: freshCh, sourceIP: "10.0.0.2", lastSeen: now}},
	}

	m.prune()

	require.NotContains(t, m.standbyCandidates, uint32(10001001), "过期候选必须回收")
	require.Contains(t, m.standbyCandidates, uint32(10001002), "新鲜候选必须保留")
}

// TestPrunePurgesIdleTrustedMark 最近可信通道按 StateIdleTimeout 回收；它不随 MapTTL 消失。
func TestPrunePurgesIdleTrustedMark(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	idle := m.stateIdleTimeout()

	oldCh, freshCh := &gomavlib.Channel{}, &gomavlib.Channel{}
	m.lastTrustedDownlink = map[uint32]trustedMark{
		10001001: {at: now.Add(-idle - time.Second), channel: oldCh, sourceIP: "10.0.0.1"},
		10001002: {at: now.Add(-m.mapTTL()), channel: freshCh, sourceIP: "10.0.0.2"},
	}

	m.prune()

	require.NotContains(t, m.lastTrustedDownlink, uint32(10001001))
	require.Contains(t, m.lastTrustedDownlink, uint32(10001002),
		"即便超过 MapTTL，只要没到 StateIdleTimeout 就必须保留（待命闸仍需要证据）")
}

// TestPrunePurgesIdleSessionGate 会话门按 StateIdleTimeout 回收。
func TestPrunePurgesIdleSessionGate(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	idle := m.stateIdleTimeout()

	m.sessionGate = map[uint32]gateState{
		10001001: {endedAt: now.Add(-idle - time.Second)},
		10001002: {endedAt: now.Add(-idle - time.Second), openedAt: now},
	}

	m.prune()

	require.NotContains(t, m.sessionGate, uint32(10001001))
	require.Contains(t, m.sessionGate, uint32(10001002),
		"刚被上行打开的会话门不得因为 endedAt 陈旧而回收")
}

// TestPrunePurgesIdleStandbyPublish 扇出节流时间按 StateIdleTimeout 回收。
func TestPrunePurgesIdleStandbyPublish(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	idle := m.stateIdleTimeout()

	m.lastStandbyPublish = map[uint32]time.Time{
		10001001: now.Add(-idle - time.Second),
		10001002: now.Add(-time.Second),
	}

	m.prune()

	require.NotContains(t, m.lastStandbyPublish, uint32(10001001))
	require.Contains(t, m.lastStandbyPublish, uint32(10001002))
}

// TestChannelCloseDropsCandidatesForThatChannel 通道关闭只删该通道的候选，同设备其它来源保留。
func TestChannelCloseDropsCandidatesForThatChannel(t *testing.T) {
	m := newBareManager()
	now := time.Now()

	closed, alive := &gomavlib.Channel{}, &gomavlib.Channel{}
	m.standbyCandidates = map[uint32][]candidate{
		10001001: {
			{channel: closed, sourceIP: "10.0.0.1", lastSeen: now},
			{channel: alive, sourceIP: "10.0.0.2", lastSeen: now},
		},
	}

	m.ProcessChannelClose(&gomavlib.EventChannelClose{Channel: closed})

	require.Len(t, m.standbyCandidates[10001001], 1)
	require.Same(t, alive, m.standbyCandidates[10001001][0].channel)
}

// TestValidateRejectsInvalidNewConfig 入口校验：负值一律拒绝；MaxUplinkTargets 必须 ≥2。
// 0 仍表示「用默认值」（Go 零值约定），CLI 层另行拒绝用户显式传 0（§4.11）。
func TestValidateRejectsInvalidNewConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Manager)
	}{
		{"负 StandbyFanoutInterval", func(m *Manager) { m.StandbyFanoutInterval = -time.Second }},
		{"负 CandidateAdmitInterval", func(m *Manager) { m.CandidateAdmitInterval = -time.Second }},
		{"负 VerifyTimeout", func(m *Manager) { m.VerifyTimeout = -time.Second }},
		{"负 MaxCandidatesPerDevice", func(m *Manager) { m.MaxCandidatesPerDevice = -1 }},
		{"负 MaxCandidatesGlobal", func(m *Manager) { m.MaxCandidatesGlobal = -1 }},
		{"负 GlobalCandidateAdmitRate", func(m *Manager) { m.GlobalCandidateAdmitRate = -1 }},
		{"负 GlobalFanoutRate", func(m *Manager) { m.GlobalFanoutRate = -1 }},
		{"负 MaxUplinkTargets", func(m *Manager) { m.MaxUplinkTargets = -1 }},
		{"MaxUplinkTargets=1", func(m *Manager) { m.MaxUplinkTargets = 1 }},
		{"负 VerifyQueuePerDevice", func(m *Manager) { m.VerifyQueuePerDevice = -1 }},
		{"负 VerifyQueueGlobal", func(m *Manager) { m.VerifyQueueGlobal = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manager{}
			tc.mutate(m)
			require.Error(t, m.validate())
		})
	}
}

// TestConfigAccessorDefaults 零值 → 文档默认值；显式值优先。
func TestConfigAccessorDefaults(t *testing.T) {
	m := &Manager{}
	require.Equal(t, DefaultStandbyFanoutInterval, m.standbyFanoutInterval())
	require.Equal(t, DefaultMaxCandidatesPerDevice, m.maxCandidatesPerDevice())
	require.Equal(t, DefaultMaxCandidatesGlobal, m.maxCandidatesGlobal())
	require.Equal(t, DefaultCandidateAdmitInterval, m.candidateAdmitInterval())
	require.Equal(t, DefaultGlobalCandidateAdmitRate, m.globalCandidateAdmitRate())
	require.Equal(t, DefaultGlobalFanoutRate, m.globalFanoutRate())
	require.Equal(t, DefaultMaxUplinkTargets, m.maxUplinkTargets())
	require.Equal(t, DefaultVerifySocket, m.verifySocket())
	require.Equal(t, DefaultVerifyTimeout, m.verifyTimeout())
	require.Equal(t, DefaultVerifyQueuePerDevice, m.verifyQueuePerDevice())
	require.Equal(t, DefaultVerifyQueueGlobal, m.verifyQueueGlobal())

	m2 := &Manager{Config: Config{
		StandbyFanoutInterval:    time.Second,
		MaxCandidatesPerDevice:   2,
		MaxCandidatesGlobal:      10,
		CandidateAdmitInterval:   2 * time.Second,
		GlobalCandidateAdmitRate: 7,
		GlobalFanoutRate:         9,
		MaxUplinkTargets:         2,
		VerifySocket:             "/tmp/x.sock",
		VerifyTimeout:            time.Millisecond,
		VerifyQueuePerDevice:     1,
		VerifyQueueGlobal:        4,
	}}
	require.Equal(t, time.Second, m2.standbyFanoutInterval())
	require.Equal(t, 2, m2.maxCandidatesPerDevice())
	require.Equal(t, 10, m2.maxCandidatesGlobal())
	require.Equal(t, 2*time.Second, m2.candidateAdmitInterval())
	require.Equal(t, 7, m2.globalCandidateAdmitRate())
	require.Equal(t, 9, m2.globalFanoutRate())
	require.Equal(t, 2, m2.maxUplinkTargets())
	require.Equal(t, "/tmp/x.sock", m2.verifySocket())
	require.Equal(t, time.Millisecond, m2.verifyTimeout())
	require.Equal(t, 1, m2.verifyQueuePerDevice())
	require.Equal(t, 4, m2.verifyQueueGlobal())
}

// TestChannelCloseKeepsNonChannelTables 钉住 03 §8.4 第 29 条的后半：通道关闭只删
// 「以通道为键」的表（候选、regDiag、dropDiag）；lastTrustedDownlink / sessionGate /
// lastStandbyPublish **按 deviceID 存，不随通道关闭删**。
//
// 为什么是刻意的：这些表回答的是「这台飞机最近有没有过可信下行 / 任务是否刚结束」，
// 通道断开（NAT 重建、QGC 重启）不改变这些事实。跟着删掉等于把待命闸的证据清了 ——
// 下一次换端口的伪造待命就会被当成「首次出现」，直接改映射。
func TestChannelCloseKeepsNonChannelTables(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	ch := &gomavlib.Channel{}
	const did = uint32(10001001)

	m.lastTrustedDownlink[did] = trustedMark{at: now, channel: ch, sourceIP: "10.0.0.1"}
	m.sessionGate[did] = gateState{endedAt: now}
	m.lastStandbyPublish[did] = now

	m.ProcessChannelClose(&gomavlib.EventChannelClose{Channel: ch})

	require.Contains(t, m.lastTrustedDownlink, did,
		"通道关闭不得清最近可信下行：它按 deviceID 存，是换端口后待命闸的证据")
	require.Contains(t, m.sessionGate, did,
		"通道关闭不得清会话门：任务结束这一事实与 QGC 通道无关")
	require.Contains(t, m.lastStandbyPublish, did,
		"通道关闭不得清候选发布时间戳：回收走 StateIdleTimeout，不跟通道走")
}
