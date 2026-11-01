package messageman

import (
	"net"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

func TestUpsertCandidateSameIPRefreshesChannel(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	ch1, ch2 := &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, ch1, "10.0.0.1", now, false))
	// 同 IP 换端口：只刷新 channel，不新增，也不受准入间隔限制。
	require.True(t, m.upsertStandbyCandidate(10001001, ch2, "10.0.0.1", now, false))

	cs := m.standbyCandidates[10001001]
	require.Len(t, cs, 1)
	require.Same(t, ch2, cs[0].channel)
}

func TestCandidatePerDeviceCapEvictsOldestNonProtected(t *testing.T) {
	m := newBareManager() // 每设备 3 条
	now := time.Now()
	chP, chA, chB, chC := &gomavlib.Channel{}, &gomavlib.Channel{}, &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, chP, "10.0.0.1", now, true))
	require.True(t, m.upsertStandbyCandidate(10001001, chA, "10.0.0.2", now.Add(2*time.Second), false))
	require.True(t, m.upsertStandbyCandidate(10001001, chB, "10.0.0.3", now.Add(4*time.Second), false))
	// 第 4 条：淘汰最久未刷新的非保护候选（10.0.0.2），保护候选保留。
	require.True(t, m.upsertStandbyCandidate(10001001, chC, "10.0.0.4", now.Add(6*time.Second), false))

	cs := m.standbyCandidates[10001001]
	require.Len(t, cs, 3)
	ips := map[string]bool{}
	for _, c := range cs {
		ips[c.sourceIP] = true
	}
	require.True(t, ips["10.0.0.1"], "保护候选不得被普通候选挤出")
	require.True(t, ips["10.0.0.3"])
	require.True(t, ips["10.0.0.4"])
	require.False(t, ips["10.0.0.2"])
}

func TestProtectedCandidateForcedInsertWhenFull(t *testing.T) {
	m := newBareManager()
	m.MaxCandidatesPerDevice = 1
	now := time.Now()
	ordinary, protected := &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, ordinary, "10.0.0.1", now, false))
	// 任务结束的保护候选是强制插入：同一时刻也能挤进满表。
	require.True(t, m.upsertStandbyCandidate(10001001, protected, "10.0.0.2", now, true))

	cs := m.standbyCandidates[10001001]
	require.Len(t, cs, 1)
	require.True(t, cs[0].protected)
	require.Equal(t, "10.0.0.2", cs[0].sourceIP)
}

func TestCandidateGlobalCapRejectsNewDevice(t *testing.T) {
	m := newBareManager()
	m.MaxCandidatesGlobal = 2
	now := time.Now()
	ch1, ch2, ch3 := &gomavlib.Channel{}, &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, ch1, "10.0.0.1", now, false))
	require.True(t, m.upsertStandbyCandidate(10001002, ch2, "10.0.0.2", now.Add(2*time.Second), false))
	require.False(t, m.upsertStandbyCandidate(10001003, ch3, "10.0.0.3", now.Add(4*time.Second), false),
		"全局表满时必须拒绝新 deviceID 的普通候选")
	// 已有候选的同 IP 刷新不受影响。
	require.True(t, m.upsertStandbyCandidate(10001001, ch1, "10.0.0.1", now.Add(5*time.Second), false))
}

func TestCandidateAdmissionIntervalPerDevice(t *testing.T) {
	m := newBareManager() // 每设备 1s
	now := time.Now()
	ch1, ch2 := &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, ch1, "10.0.0.1", now, false))
	require.False(t, m.upsertStandbyCandidate(10001001, ch2, "10.0.0.2", now.Add(500*time.Millisecond), false),
		"同一设备新增来源 IP 受 1s 准入间隔限制")
	require.True(t, m.upsertStandbyCandidate(10001001, ch2, "10.0.0.2", now.Add(time.Second), false))
}

func TestGlobalCandidateAdmitRate(t *testing.T) {
	m := newBareManager()
	m.GlobalCandidateAdmitRate = 1
	now := time.Now()
	ch1, ch2 := &gomavlib.Channel{}, &gomavlib.Channel{}

	require.True(t, m.upsertStandbyCandidate(10001001, ch1, "10.0.0.1", now, false))
	require.False(t, m.upsertStandbyCandidate(10001002, ch2, "10.0.0.2", now, false),
		"全局准入令牌耗尽时必须拒绝")
	require.True(t, m.upsertStandbyCandidate(10001002, ch2, "10.0.0.2", now.Add(time.Second), false))
}

func TestStandbyPublishThrottlePerDevice(t *testing.T) {
	m := newBareManager() // 2s 窗口
	now := time.Now()

	require.True(t, m.shouldPublishStandby(10001001, now), "初值零 ⇒ 第一帧立即发布")
	require.False(t, m.shouldPublishStandby(10001001, now.Add(time.Second)))
	require.True(t, m.shouldPublishStandby(10001001, now.Add(2*time.Second)))
}

func TestStandbyPublishGlobalBudget(t *testing.T) {
	m := newBareManager()
	m.GlobalFanoutRate = 1
	now := time.Now()

	require.True(t, m.shouldPublishStandby(10001001, now))
	require.False(t, m.shouldPublishStandby(10001002, now), "全局预算按聚合发布计，耗尽时丢候选发布")
	require.True(t, m.shouldPublishStandby(10001002, now.Add(time.Second)))
}

func TestStandbyFanoutTargetsOnlyDeclaredFreshPairs(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	ttl := m.mapTTL()

	fresh, stale, otherDevice := &gomavlib.Channel{}, &gomavlib.Channel{}, &gomavlib.Channel{}
	m.pairs[pairKey{qgcCh: fresh, px4DeviceID: 10001001}] = &pairEntry{qgcSeen: now}
	m.pairs[pairKey{qgcCh: stale, px4DeviceID: 10001001}] = &pairEntry{qgcSeen: now.Add(-2 * ttl)}
	m.pairs[pairKey{qgcCh: otherDevice, px4DeviceID: 10001002}] = &pairEntry{qgcSeen: now}

	require.Equal(t, []*gomavlib.Channel{fresh}, m.standbyFanoutTargets(10001001, now),
		"只发给声明过该 deviceID 且 qgcSeen 新鲜的 QGC")
}

func TestSourceIPFromAddr(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr net.Addr
		want string
	}{
		{"nil", nil, ""},
		{"IPv4", &net.UDPAddr{IP: net.ParseIP("10.1.2.3"), Port: 14550}, "10.1.2.3"},
		{"IPv6", &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 14550}, "2001:db8::1"},
		{"无端口", &net.IPAddr{IP: net.ParseIP("10.1.2.3")}, "10.1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sourceIPFromAddr(tc.addr))
		})
	}
}

// TestProtectedCandidateGlobalEvictionDoesNotResurrect 钉住全局淘汰的切片一致性：
// 淘汰可能删掉的正是**本设备**刚写回 map 的候选，而函数里的局部切片 cs 不会跟着变。
// 若淘汰后直接用旧 cs 追加并写回，被淘汰的候选会复活 ⇒ 全局/每设备双超限，
// 严重时同一切片里出现两条相同 sourceIP。
func TestProtectedCandidateGlobalEvictionDoesNotResurrect(t *testing.T) {
	m := newBareManager()
	m.MaxCandidatesGlobal = 2
	now := time.Now()

	chOld, chOther, chNew := &gomavlib.Channel{}, &gomavlib.Channel{}, &gomavlib.Channel{}
	// did1 的普通候选最旧，正是全局淘汰会挑中的那条。
	require.True(t, m.upsertStandbyCandidate(10001001, chOld, "10.0.0.1", now, false))
	require.True(t, m.upsertStandbyCandidate(10001002, chOther, "10.0.0.2", now.Add(time.Second), false))

	// 任务结束：did1 强制插入保护候选 ⇒ 全局淘汰会删掉 did1 自己刚写回的那条。
	require.True(t, m.upsertStandbyCandidate(10001001, chNew, "10.0.0.9", now.Add(2*time.Second), true))

	require.LessOrEqual(t, m.candidateCountLocked(), 2, "全局上限被突破")
	cs := m.standbyCandidates[10001001]
	require.Len(t, cs, 1, "被全局淘汰的普通候选不得复活")
	require.Equal(t, "10.0.0.9", cs[0].sourceIP)
	require.True(t, cs[0].protected)
	ips := map[string]bool{}
	for _, c := range m.standbyCandidates[10001002] {
		ips[c.sourceIP] = true
	}
	require.True(t, ips["10.0.0.2"], "其他设备的候选不得被误删")
}

// TestUplinkTargetsPriorityDedupAndLimit 钉住 03 §4.6 的上行目标集：
// 已验签通道 → 保护候选 → 普通候选（lastSeen 新到旧），去重后裁到 MaxUplinkTargets。
// 之前只测了配置访问器，判据本身（顺序/去重/裁剪）没有用例。
func TestUplinkTargetsPriorityDedupAndLimit(t *testing.T) {
	m := newBareManager()
	m.MaxUplinkTargets = 3
	now := time.Now()
	const did = 10001001

	verified := &gomavlib.Channel{}
	protected := &gomavlib.Channel{}
	ordinaryNew := &gomavlib.Channel{}
	ordinaryOld := &gomavlib.Channel{}
	candidateTooMany := &gomavlib.Channel{}

	m.px4Map[did] = &px4Entry{channel: verified}
	m.standbyCandidates[did] = []candidate{
		{channel: ordinaryOld, sourceIP: "10.0.0.1", lastSeen: now.Add(-time.Minute)},
		{channel: protected, sourceIP: "10.0.0.2", lastSeen: now.Add(-2 * time.Minute), protected: true},
		{channel: ordinaryNew, sourceIP: "10.0.0.3", lastSeen: now},
		{channel: verified, sourceIP: "10.0.0.4", lastSeen: now.Add(time.Second)}, // 与权威通道重复
		{channel: candidateTooMany, sourceIP: "10.0.0.5", lastSeen: now.Add(-3 * time.Minute)},
	}

	got := m.uplinkTargetsLocked(did)
	require.Equal(t, []*gomavlib.Channel{verified, protected, ordinaryNew}, got,
		"顺序必须是 权威通道 → 保护候选 → 最新普通候选；重复通道去重；超出上限裁剪")
}

// TestStandbyPublishNotConsumedWithoutFanout 钉住 03 §4.5.3：只有**实际发布**才推进
// 2s 窗口与全局预算。没有扇出目标且 Sink=nil 时，这一帧什么也没发出去，
// 若照样消耗窗口，后续真正需要扇出的心跳会被无谓地节流掉。
func TestStandbyPublishNotConsumedWithoutFanout(t *testing.T) {
	const did = uint32(10001001)
	srcCh := &gomavlib.Channel{}
	fr := verifyFrame(did, 0, 0, 9)

	m := newBareManager() // 无配对、无 Sink ⇒ 零落点
	m.processStandbyHeartbeat(srcCh, did, fr)
	_, published := m.lastStandbyPublish[did]
	require.False(t, published, "没有任何落点时不得推进发布窗口/消耗全局预算")

	// 阴性对照：有 Sink 就必须算一次发布（data_writer 的在线状态来源）。
	m2 := newBareManager()
	sink := &syncSink{}
	m2.Sink = sink
	m2.processStandbyHeartbeat(srcCh, did, fr)
	_, published = m2.lastStandbyPublish[did]
	require.True(t, published, "有 Sink 时待命心跳必须发布")
	require.Equal(t, 1, sink.count())
}
