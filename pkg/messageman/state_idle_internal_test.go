package messageman

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// idleTestUplinkFrame 构造一帧 QGC 加密上行：帧头 deviceID = 目标 PX4 的 did（§3.2.3），
// 奇数 counter（上行特征）。msgID 取非心跳值，避开 §2.5 的加密心跳拦截。
func idleTestUplinkFrame(did uint32, counter uint64) *frame.V2Frame {
	payload := make([]byte, 8+4)
	binary.BigEndian.PutUint64(payload[:8], counter)
	return &frame.V2Frame{
		IncompatibilityFlag: byte(did >> 24),
		CompatibilityFlag:   byte(did >> 16),
		SystemID:            byte(did >> 8),
		ComponentID:         byte(did),
		Message:             &message.MessageRaw{ID: 33, Payload: payload},
	}
}

// standbyFrame 构造一帧 PX4 明文待命心跳：msgID=0、payload 9 字节（< encryptedPayloadMinLen），
// 帧头 deviceID = PX4 自身（§3.2.2 步骤 1/10）。
func standbyFrame(did uint32) *frame.V2Frame {
	return &frame.V2Frame{
		IncompatibilityFlag: byte(did >> 24),
		CompatibilityFlag:   byte(did >> 16),
		SystemID:            byte(did >> 8),
		ComponentID:         byte(did),
		Message:             &message.MessageRaw{ID: 0, Payload: make([]byte, 9)},
	}
}

// 本文件钉住「长期状态表在 10 分钟真实流量静默后回收」这一组规则。
//
// **改前**：pairs 原有的两道闸（lastSeen ≤ TTL、qgcSeen ≤ 10×TTL）都会被 80005 周期登记
// （每 10s）刷新 ⇒「声明过、PX4 从未上线」的配对（px4Ch 恒 nil、永不扇出）永远不会命中，
// 只能等 QGC 通道关闭；lastNonce 只在 PX4 回待命时清；hbDiag/fanoutDiag 没有删除路径；
// regDiag 只随通道关闭删除。本用例组把新闸的**方向**与**边界**都钉住。
//
// ‼️ 凡调用 `prune()` 的用例都显式设 StateIdleTimeout，不依赖默认的 10 分钟；唯一例外是
// `TestStateIdleTimeoutDefault`（它测的就是零值兜底）。判据一律靠字段龄期构造、不靠睡眠，
// 避免把 CI 调度抖动变成假红。

// TestStateIdleTimeoutDefault 钉住零值兜底与显式值优先。
func TestStateIdleTimeoutDefault(t *testing.T) {
	// 默认值本身也钉住：它同时是文档口径与运维预期（"10 分钟"），改了要有意识地改这一格。
	require.Equal(t, 10*time.Minute, DefaultStateIdleTimeout,
		"默认空闲阈值不是 10 分钟 ⇒ 文档/运维预期与本实现分叉")

	m := newBareManager()
	require.Equal(t, DefaultStateIdleTimeout, m.stateIdleTimeout(),
		"零值没有退回 10 分钟默认值 ⇒ 调用方漏配时这张表又变成永不过期")

	m.StateIdleTimeout = 3 * time.Second
	require.Equal(t, 3*time.Second, m.stateIdleTimeout(),
		"显式配置被默认值盖掉 ⇒ 参数形同虚设")
}

// TestPrunePurgesIdlePairDespiteFreshRegistration 是核心格：80005 登记**救不活**一个
// 长期没有真实流量的配对。
//
// 两个存活时间戳刻意取「刚刚」，只有 lastTraffic 陈旧 —— 这正是改前的形状：登记每 10s
// 刷新 lastSeen/qgcSeen，于是上面两道闸永不命中。若实现里 lastTraffic 被登记一并刷新
// （最容易顺手写错的一行），本格必红。
func TestPrunePurgesIdlePairDespiteFreshRegistration(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	m.StateIdleTimeout = time.Minute

	ch := &gomavlib.Channel{}
	now := time.Now()
	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{
		px4Ch:       ch,
		lastSeen:    now,
		qgcSeen:     now,
		lastTraffic: now.Add(-2 * time.Hour),
	}

	// 真的先登记一次（而不是只把两个时间戳写成 now）——本格要钉的是「登记刷新了
	// lastSeen/qgcSeen，却救不活 lastTraffic 陈旧的配对」。少了这一步，用例名里的
	// 「fresh registration」只是夹具自说自话。
	regPayload := make([]byte, 1+4)
	regPayload[0] = 1
	binary.BigEndian.PutUint32(regPayload[1:], did)
	m.processRegistration(ch, DefaultGCSDeviceID, &message.MessageRaw{ID: 80005, Payload: regPayload})

	m.prune()

	_, ok := m.pairs[pk]
	require.False(t, ok,
		"80005 刚登记过、但真实流量已静默超过阈值 ⇒ 配对必须回收。"+
			"若还活着，说明新闸读的不是 lastTraffic（多半被登记刷新了），幽灵配对依旧只增不减")
}

// TestPruneKeepsPairWithRecentTraffic 是上一格的阴性对照：最近有真实流量的配对**不得**被删。
// 缺这一格，「prune 无条件清空 pairs」也能让核心格全绿。
func TestPruneKeepsPairWithRecentTraffic(t *testing.T) {
	const (
		active = uint32(10000001)
		idle   = uint32(10000002)
	)

	m := newBareManager()
	m.StateIdleTimeout = time.Minute

	ch := &gomavlib.Channel{}
	now := time.Now()
	activeKey := pairKey{qgcCh: ch, px4DeviceID: active}
	idleKey := pairKey{qgcCh: ch, px4DeviceID: idle}
	m.pairs[activeKey] = &pairEntry{px4Ch: ch, lastSeen: now, qgcSeen: now, lastTraffic: now}
	m.pairs[idleKey] = &pairEntry{px4Ch: ch, lastSeen: now, qgcSeen: now, lastTraffic: now.Add(-2 * time.Hour)}

	m.prune()

	_, kept := m.pairs[activeKey]
	require.True(t, kept,
		"最近有真实流量的配对被删了 ⇒ 新闸判据写反/写错，正在飞的链路会被无端拆掉")

	_, keptIdle := m.pairs[idleKey]
	require.False(t, keptIdle,
		"同轮的陈流量配对照样活着 ⇒ 本格阳性判据没有判别力：prune 压根没在按 lastTraffic 删东西")
}

// TestRegistrationDoesNotRefreshLastTraffic 钉住「80005 不刷新 lastTraffic」这条分工。
//
// 它是核心格能成立的前提：只要登记顺手刷一下 lastTraffic，幽灵配对就永远年轻。
// 前提自检同时断言登记**确实**刷新了 lastSeen —— 否则本格会因为「登记什么都没做」而假绿。
func TestRegistrationDoesNotRefreshLastTraffic(t *testing.T) {
	const (
		qgcDid = uint32(1)        // GCS 段（§2.2）
		px4Did = uint32(10000001) // 80005 里声明的那个
	)

	m := newBareManager()
	require.Greater(t, uint64(m.gcsDeviceIDMax()), uint64(qgcDid),
		"夹具前提不成立：帧头 deviceID 不在 GCS 段，80005 会被直接丢弃，本格空转")

	ch := &gomavlib.Channel{}
	stale := time.Now().Add(-2 * time.Hour)
	traffic := time.Now().Add(-20 * time.Minute)
	pk := pairKey{qgcCh: ch, px4DeviceID: px4Did}
	m.pairs[pk] = &pairEntry{px4Ch: ch, lastSeen: stale, qgcSeen: stale, lastTraffic: traffic}

	payload := make([]byte, 1+4)
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:], px4Did)
	m.processRegistration(ch, qgcDid, &message.MessageRaw{ID: 80005, Payload: payload})

	e, ok := m.pairs[pk]
	require.True(t, ok, "被声明的配对不该被这帧登记删掉")
	require.True(t, e.lastSeen.After(stale),
		"前提自检失败：登记没有刷新 lastSeen，本格证不了『登记刷新了别的、唯独不刷 lastTraffic』")
	require.Equal(t, traffic, e.lastTraffic,
		"80005 登记刷新了 lastTraffic ⇒ 幽灵配对靠每 10s 的登记永远年轻，新闸形同虚设")
}

// TestEncryptedDownlinkRefreshesPairTraffic 钉住真实流量的两个刷新点之一（下行）。
// 上行那一支由紧邻的 `TestEncryptedUplinkRefreshesPairTraffic` 覆盖（真正调用
// `processEncrypted`）；`TestPruneKeepsPairWithRecentTraffic` 只验证 prune 的读取方向，
// 不覆盖任何写点。这里单独测下行，因为它是「QGC 只收不发」的监控员场景里唯一的保活信号。
func TestEncryptedDownlinkRefreshesPairTraffic(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	ch := &gomavlib.Channel{}
	stale := time.Now().Add(-2 * time.Hour)
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: stale}
	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: ch, lastSeen: stale, qgcSeen: time.Now(), lastTraffic: stale}

	// 同一 channel 指针同时当 PX4 来源与配对 QGC ⇒ 扇出末尾的 `qgc != srcCh` 跳过写，
	// Node 可留 nil（与 TestPX4DownlinkKeepsPairButNotFanout 同一手法）。
	m.processEncrypted(ch, did, downlinkFrame(did, 2))

	e, ok := m.pairs[pk]
	require.True(t, ok, "配对项不该被下行帧删掉")
	require.True(t, e.lastTraffic.After(stale),
		"PX4 加密下行没有刷新 lastTraffic ⇒ 只收不发的监控员 QGC，其配对每 10 分钟被误删一次")
}

// TestEncryptedUplinkRefreshesPairTraffic 是下行那一格的对照：上行同样算真实流量。
func TestEncryptedUplinkRefreshesPairTraffic(t *testing.T) {
	const did = uint32(10000002)

	m := newBareManager()
	// 同一 channel 指针同时当 QGC 来源与 PX4 目标：方向由 qgcOnline 成员资格判定，
	// 而转发末尾 `target != srcCh` 会跳过写 ⇒ Node 可留 nil（同 TestPX4Downlink… 的手法）。
	ch := &gomavlib.Channel{}
	stale := time.Now().Add(-2 * time.Hour)
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}
	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: ch, lastSeen: stale, qgcSeen: stale, lastTraffic: stale}

	m.processEncrypted(ch, did, idleTestUplinkFrame(did, 3))

	e, ok := m.pairs[pk]
	require.True(t, ok, "配对项不该被上行帧删掉")
	require.True(t, e.lastTraffic.After(stale),
		"加密上行没有刷新 lastTraffic ⇒ 活跃控制的配对也可能在 10 分钟后被回收")
}

// TestPrunePurgesIdleNonce 钉住防重放水位的空闲回收。
// 代价（2026-10-24 用户裁定接受）：空闲超过阈值后，该方向的防重放窗口**重开**——
// 不是「只放行一条」，而是直到该方向再次出现真实帧为止，任意递增的旧 counter 序列
// 都能通过（首条重放会把水位写成它自己，之后的更大值继续通过）。
func TestPrunePurgesIdleNonce(t *testing.T) {
	m := newBareManager()
	m.StateIdleTimeout = time.Minute

	idle := nonceKey{deviceID: 10000001, odd: true}
	fresh := nonceKey{deviceID: 10000002, odd: false}
	m.lastNonce[idle] = nonceEntry{counter: 5, lastSeen: time.Now().Add(-2 * time.Hour)}
	m.lastNonce[fresh] = nonceEntry{counter: 6, lastSeen: time.Now()}

	m.prune()

	_, keptIdle := m.lastNonce[idle]
	require.False(t, keptIdle,
		"长期没流量的设备，其防重放水位仍留着 ⇒ 这张表只增不减（deviceID 是 32 位空间）")

	_, keptFresh := m.lastNonce[fresh]
	require.True(t, keptFresh,
		"活跃设备的水位被清 ⇒ 防重放窗口被无端重开（窗口一直开到下一条真实帧为止）")
}

// TestPrunePurgesIdleDiagTables 钉住三张诊断/节流表的回收。
// regDiag 用「通道是否还在 qgcOnline 里」判，判据随 qgcOnline 的 TTL 回收；
// hbDiag/fanoutDiag 直接按时间戳判。
func TestPrunePurgesIdleDiagTables(t *testing.T) {
	m := newBareManager()
	m.StateIdleTimeout = time.Minute

	onlineCh := &gomavlib.Channel{}
	goneCh := &gomavlib.Channel{}
	m.qgcOnline[onlineCh] = &qgcEntry{lastSeen: time.Now()}
	m.regDiag[onlineCh] = regDiagEntry{sig: "1|[10000001]"}
	m.regDiag[goneCh] = regDiagEntry{sig: "1|[10000002]"}

	m.hbDiag[10000001] = time.Now().Add(-2 * time.Hour)
	m.hbDiag[10000002] = time.Now()
	m.fanoutDiag[10000003] = time.Now().Add(-2 * time.Hour)
	m.fanoutDiag[10000004] = time.Now()

	m.prune()

	// ‼️ 一律按**键**显式查。testify v1.11 的 Contains/NotContains 对 map 比较的是**键**；
	// 但元素类型与键类型不一致时（例如拿 time.Time 去查 map[uint32]time.Time）结果恒为
	// 「找不到」⇒ `NotContains` 永远绿。显式 `v, ok := m[k]` 不受这类错配影响。
	_, stillGone := m.regDiag[goneCh]
	require.False(t, stillGone,
		"通道已不在 QGC 在线表里，regDiag 还留着 ⇒ 它持有已死 channel 指针，且只增不减")
	_, stillOnline := m.regDiag[onlineCh]
	require.True(t, stillOnline,
		"在线 QGC 的登记签名被删 ⇒ 「内容变化才打」的节流失效，每 2s 一条日志")

	_, hbIdle := m.hbDiag[10000001]
	require.False(t, hbIdle, "hbDiag 的陈旧条目没有被回收")
	_, hbFresh := m.hbDiag[10000002]
	require.True(t, hbFresh, "hbDiag 的新鲜条目被误删")
	_, fanoutIdle := m.fanoutDiag[10000003]
	require.False(t, fanoutIdle, "fanoutDiag 的陈旧条目没有被回收")
	_, fanoutFresh := m.fanoutDiag[10000004]
	require.True(t, fanoutFresh, "fanoutDiag 的新鲜条目被误删")
}

// TestInitializeRejectsNegativeDurations 钉住入口校验：负时长不得被当成「用默认值」。
// 负值会让两道闸恒真，每轮 prune 把整张表清空——表现成「一切正常但什么都不缓存」。
func TestInitializeRejectsNegativeDurations(t *testing.T) {
	require.Error(t, (&Manager{Config: Config{MapTTL: -time.Second}}).Initialize(),
		"负 MapTTL 被接受 ⇒ 每轮 prune 删光映射表，现象是「帧收得到但路由不到」")
	require.Error(t, (&Manager{Config: Config{StateIdleTimeout: -time.Second}}).Initialize(),
		"负 StateIdleTimeout 被接受 ⇒ 每 10s 清空配对/水位表")

	// 阴性对照：零值 = 使用默认值，必须能正常初始化（否则上面两格可以靠「Initialize
	// 永远报错」假绿）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	m := &Manager{Ctx: ctx, Wg: &wg}
	require.NoError(t, m.Initialize(), "零值配置被拒绝 ⇒ 默认路径不可用")
	cancel()
	wg.Wait()
}

// TestNonceSiblingDirectionRefreshed 钉住 touchSiblingNonce：任一方向有真实帧时，
// 同 deviceID 的另一方向水位一并续期。
//
// 背景（§2.5 拦截的副作用）：建链后 QGC 的 1 Hz 上行心跳被拦、不推进上行水位，而飞行中
// 的加密下行一直推进下行水位 ⇒ 上行水位的 lastSeen 停在最后一条**真实**上行上，10 分钟后
// 被 prune 回收、窗口随之重开。这条链路其实一直活跃。
func TestNonceSiblingDirectionRefreshed(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}

	stale := time.Now().Add(-2 * time.Hour)
	downKey := nonceKey{deviceID: did, odd: false}
	m.lastNonce[downKey] = nonceEntry{counter: 2, lastSeen: stale}

	m.processEncrypted(ch, did, idleTestUplinkFrame(did, 3))

	up, ok := m.lastNonce[nonceKey{deviceID: did, odd: true}]
	require.True(t, ok, "上行水位没有推进")
	require.Equal(t, uint64(3), up.counter, "上行水位 counter 写错")

	down, ok := m.lastNonce[downKey]
	require.True(t, ok, "下行水位被这帧上行删了")
	require.True(t, down.lastSeen.After(stale),
		"上行活跃时下行水位没有续期 ⇒ 对称失效：只发不收的链路其下行水位会被空闲回收")
	require.Equal(t, uint64(2), down.counter, "续期不得改动 counter（那会破坏防重放判据）")
}

// TestNonceSiblingDirectionRefreshedByDownlink 钉住反向的那一半：**下行**活跃时上行水位
// 一并续期。
//
// 这正是 §2.5 拦截造成的真实形态：建链后 QGC 的 1 Hz 上行心跳被拦、上行水位不再前进，
// 而飞行中的加密下行一直在刷下行水位。若下行不续上行，上行水位会在 10 分钟后被 prune
// 回收 —— 删掉 manager.go 下行分支里那次 touchSiblingNonce，本用例必须红。
func TestNonceSiblingDirectionRefreshedByDownlink(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	// 同一 channel 指针同时当 PX4 来源与配对 QGC ⇒ 扇出末尾的 `qgc != srcCh` 跳过写，
	// Node 可留 nil（与 TestEncryptedDownlinkRefreshesPairTraffic 同一手法）。
	// 注意：该 channel 不能进 qgcOnline，否则方向判定会把它当上行。
	ch := &gomavlib.Channel{}
	now := time.Now()
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: now}
	m.pairs[pairKey{qgcCh: ch, px4DeviceID: did}] = &pairEntry{
		px4Ch: ch, lastSeen: now, qgcSeen: now, lastTraffic: now,
	}

	stale := time.Now().Add(-2 * time.Hour)
	upKey := nonceKey{deviceID: did, odd: true}
	m.lastNonce[upKey] = nonceEntry{counter: 3, lastSeen: stale}

	m.processEncrypted(ch, did, downlinkFrame(did, 2))

	up, ok := m.lastNonce[upKey]
	require.True(t, ok, "下行水位写入时把上行水位删了")
	require.True(t, up.lastSeen.After(stale),
		"下行活跃时上行水位没有续期 ⇒ §2.5 拦截期间上行水位 10 分钟后被空闲回收、窗口无端重开")
	require.Equal(t, uint64(3), up.counter, "续期不得改动 counter（那会破坏防重放判据）")
}

// TestActiveNonceWatermarkSurvivesPrune 把「续期」与「prune」接起来：一条上下行都活跃的
// 链路，它的上行水位不得因为 §2.5 拦截而先变旧、再被回收。
func TestActiveNonceWatermarkSurvivesPrune(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	m.StateIdleTimeout = time.Minute
	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}

	downKey := nonceKey{deviceID: did, odd: false}
	m.lastNonce[downKey] = nonceEntry{counter: 2, lastSeen: time.Now().Add(-2 * time.Hour)}

	// 一帧真实上行（非心跳 ⇒ 不被 §2.5 拦截）：推进上行水位并给下行续期。
	m.processEncrypted(ch, did, idleTestUplinkFrame(did, 3))
	m.prune()

	upKey := nonceKey{deviceID: did, odd: true}
	_, upKept := m.lastNonce[upKey]
	require.True(t, upKept, "刚有真实上行的水位被 prune 回收了")
	_, downKept := m.lastNonce[downKey]
	require.True(t, downKept,
		"下行水位被回收 ⇒ touchSiblingNonce 没生效或 prune 读的时间戳不对，窗口被无端重开")
}

// TestStandbyHeartbeatDoesNotRefreshLastTraffic 钉住待命心跳**不算流量**：
// 它只在「该 deviceID 有下行水位」时清配对，其余情况下连配对都不碰。
// 若它也刷 lastTraffic，PX4 关机前发的那串待命心跳会把幽灵配对续到下一次开机。
func TestStandbyHeartbeatDoesNotRefreshLastTraffic(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	px4Ch := &gomavlib.Channel{}
	staleSeen := time.Now().Add(-2 * time.Hour)
	traffic := time.Now().Add(-20 * time.Minute)
	m.px4Map[did] = &px4Entry{channel: px4Ch, lastSeen: staleSeen}
	qgcCh := &gomavlib.Channel{}
	pk := pairKey{qgcCh: qgcCh, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: px4Ch, lastSeen: staleSeen, qgcSeen: staleSeen, lastTraffic: traffic}

	m.processStandbyHeartbeat(px4Ch, did, standbyFrame(did))

	pe, ok := m.pairs[pk]
	require.True(t, ok, "没有下行水位的待命心跳不该清配对（那是「开机」不是「任务结束」）")
	require.Equal(t, traffic, pe.lastTraffic, "待命心跳刷新了 lastTraffic ⇒ 不活跃的配对靠心跳永不过期")
	// 阳性对照：同一帧确实刷新了 px4Map（否则「没刷 lastTraffic」可以归因于「什么都没做」）。
	m.mu.Lock()
	px4 := m.px4Map[did]
	m.mu.Unlock()
	require.True(t, px4.lastSeen.After(staleSeen), "前提自检失败：待命心跳没有刷新 px4Map.lastSeen")
}

// TestIdlePurgedPairIsRebuiltOnlyByRegistration 钉住 10 分钟回收的**可用性代价**：
// 下行分支只查配对、不建配对 ⇒ 回收后到下一次 80005 之间的首帧下行会被丢弃。
// 这是有意接受的（回收是卫生机制，不是投递闸），本格把代价与恢复路径一起钉住。
func TestIdlePurgedPairIsRebuiltOnlyByRegistration(t *testing.T) {
	const (
		qgcDid = uint32(1) // GCS 段（§2.2）
		did    = uint32(10000001)
	)

	m := newBareManager()
	m.StateIdleTimeout = time.Minute
	require.Greater(t, uint64(m.gcsDeviceIDMax()), uint64(qgcDid),
		"夹具前提不成立：80005 帧头不在 GCS 段会被直接丢弃")

	qgcCh := &gomavlib.Channel{}
	px4Ch := &gomavlib.Channel{}
	now := time.Now()
	m.qgcOnline[qgcCh] = &qgcEntry{lastSeen: now}
	m.px4Map[did] = &px4Entry{channel: px4Ch, lastSeen: now}
	pk := pairKey{qgcCh: qgcCh, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: px4Ch, lastSeen: now, qgcSeen: now, lastTraffic: now.Add(-2 * time.Hour)}

	m.prune()
	_, ok := m.pairs[pk]
	require.False(t, ok, "前提自检失败：配对没被空闲闸回收，后面两格都在空转")

	// 回收后的第一帧下行：不重建配对（已知代价）。
	m.processEncrypted(px4Ch, did, downlinkFrame(did, 2))
	_, ok = m.pairs[pk]
	require.False(t, ok, "下行分支重建了配对 ⇒ 与实现口径不符，回收代价的前提变了")

	// 80005 登记负责重建（§3.2.4.1）：链路在下一个登记周期内恢复。
	payload := make([]byte, 1+4)
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:], did)
	m.processRegistration(qgcCh, qgcDid, &message.MessageRaw{ID: 80005, Payload: payload})
	_, ok = m.pairs[pk]
	require.True(t, ok, "80005 没有重建被回收的配对 ⇒ 链路回收后再也回不来")
}

// TestUplinkCreatedPairSurvivesPrune 钉住「上行建配对」也会写 lastTraffic。
// 漏掉这一处，一个 QGC 主动控制、但 PX4 一时的下行还没到 的会话会被空闲闸立刻回收。
func TestUplinkCreatedPairSurvivesPrune(t *testing.T) {
	const did = uint32(10000002)

	m := newBareManager()
	m.StateIdleTimeout = time.Minute
	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}

	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	// ‼️ 按**键**显式查（理由见 TestPrunePurgesIdleDiagTables 的注释）：元素类型与键类型
	// 不一致时，`NotContains` 会恒绿。
	_, preExists := m.pairs[pk]
	require.False(t, preExists, "夹具前提：配对应由本帧上行新建")

	m.processEncrypted(ch, did, idleTestUplinkFrame(did, 3))

	e, ok := m.pairs[pk]
	require.True(t, ok, "加密上行没有建立配对（§3.2.2 步骤 3）")
	require.False(t, e.lastTraffic.IsZero(),
		"上行新建的配对没有 lastTraffic ⇒ 下一轮 prune 立刻回收，链路永远建不起来")

	m.prune()
	_, kept := m.pairs[pk]
	require.True(t, kept, "刚由上行建立的配对被空闲闸立刻回收了")
}

// TestChannelCloseDropsRegDiag 钉住诊断表的另一条清路：通道关闭时随即回收。
// prune 里那条按「是否还在 qgcOnline」的判据只在下一轮 ticker 生效，通道关闭这条更早。
func TestChannelCloseDropsRegDiag(t *testing.T) {
	m := newBareManager()
	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.regDiag[ch] = regDiagEntry{sig: "1|[10000001]|[]"}

	m.ProcessChannelClose(&gomavlib.EventChannelClose{Channel: ch})

	_, ok := m.regDiag[ch]
	require.False(t, ok, "通道已关闭，regDiag 还持有它的指针 ⇒ 该表只增不减")
}

// TestRegistrationDropsInvalidDeclaredDeviceIDs 钉住 mavp2p 侧的纵深防御：
// 80005 payload 里的非法 deviceID（GCS 段 / bit24 置位）不得建出配对。
// QGC 侧解析入口已过滤（2026-10-24），但 mavp2p 是最后一道，且脏配对的清理代价很高。
func TestRegistrationDropsInvalidDeclaredDeviceIDs(t *testing.T) {
	const (
		qgcDid = uint32(1)
		valid  = uint32(10000001)
	)

	invalid := []uint32{
		91002,      // GCS 段
		0x01000001, // bit24（签名位）置位，号段合法
		0,          // 缺省值
	}

	m := newBareManager()
	require.Greater(t, uint64(m.gcsDeviceIDMax()), uint64(qgcDid),
		"夹具前提不成立：80005 帧头不在 GCS 段会被直接丢弃")

	ch := &gomavlib.Channel{}
	payload := make([]byte, 1+4*(1+len(invalid)))
	payload[0] = byte(1 + len(invalid))
	binary.BigEndian.PutUint32(payload[1:], valid)
	for i, d := range invalid {
		binary.BigEndian.PutUint32(payload[5+i*4:], d)
	}
	m.processRegistration(ch, qgcDid, &message.MessageRaw{ID: 80005, Payload: payload})

	_, ok := m.pairs[pairKey{qgcCh: ch, px4DeviceID: valid}]
	require.True(t, ok, "合法 deviceID 没被建配对 ⇒ 纵深防御写成了「全丢」")
	for _, d := range invalid {
		_, created := m.pairs[pairKey{qgcCh: ch, px4DeviceID: d}]
		require.False(t, created, "非法 deviceID=%d 进了配对表 ⇒ 建出 px4Ch 恒 nil 的幽灵配对", d)
	}
	require.Len(t, m.pairs, 1, "配对表里除了那一个合法 ID 之外还有别的条目")
}

// TestPruneReapsQGCSilentPairAfterTenTTL 钉住**另一条**清理闸：QGC 侧安静超过 10×TTL。
//
// 为什么它不能被新闸取代：PX4 在飞时，下行会把该 deviceID 名下**所有**配对的 lastTraffic
// 刷成 now（宽松口径）⇒ 签出方那一条的 StateIdleTimeout 闸永远不命中。只有这条按 qgcSeen
// 计的兜底闸能清掉它（否则 QGC 端口每漂移一次就多一批条目，只增不减）。
// ⚠️ 本格把 StateIdleTimeout 设成 1 小时，**刻意**让新闸不参与，专测 10×TTL 这道。
func TestPruneReapsQGCSilentPairAfterTenTTL(t *testing.T) {
	m := newBareManager()
	m.MapTTL = 2 * time.Second
	m.StateIdleTimeout = time.Hour

	ch := &gomavlib.Channel{}
	now := time.Now()
	// 15s：> TTL（2s），但 < 10×TTL（20s）⇒ 配对活着、只是不扇出。
	quiet := pairKey{qgcCh: ch, px4DeviceID: 10000001}
	m.pairs[quiet] = &pairEntry{
		px4Ch:       ch,
		lastSeen:    now, // PX4 一直在刷，first gate 不命中
		qgcSeen:     now.Add(-15 * time.Second),
		lastTraffic: now,
	}
	// 25s：> 10×TTL ⇒ 必须回收。
	gone := pairKey{qgcCh: ch, px4DeviceID: 10000002}
	m.pairs[gone] = &pairEntry{
		px4Ch:       ch,
		lastSeen:    now,
		qgcSeen:     now.Add(-25 * time.Second),
		lastTraffic: now,
	}

	m.prune()

	_, kept := m.pairs[quiet]
	require.True(t, kept, "QGC 安静 15s（< 10×TTL）的配对提前被回收 ⇒ 链路抖动会误杀刚要恢复的会话")
	_, reaped := m.pairs[gone]
	require.False(t, reaped,
		"QGC 安静 25s（> 10×TTL）的配对还活着 ⇒ 签出方会随 QGC 端口漂移无限累积")
}

// TestPruneReapsRegDiagWhenChannelExpiresInSameRound 钉住 prune 内部的**顺序约束**：
// regDiag 的回收必须排在 qgcOnline 清理之后。顺序反了，本轮刚该消失的通道此刻还在
// qgcOnline 里，签名就被漏掉——而 map 键持有已死 channel 指针，表只增不减。
// 夹具刻意让通道在**同一轮**过期（改前用例用的通道从不在 qgcOnline 里，抓不到顺序）。
func TestPruneReapsRegDiagWhenChannelExpiresInSameRound(t *testing.T) {
	m := newBareManager()
	m.MapTTL = 2 * time.Second
	m.StateIdleTimeout = time.Hour
	m.GCSDeviceIDMax = 10000000

	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now().Add(-3 * time.Second)} // > TTL
	m.regDiag[ch] = regDiagEntry{sig: "1|[10000001]|[]"}

	m.prune()

	_, online := m.qgcOnline[ch]
	require.False(t, online, "前提自检失败：在线表没有按 TTL 过期，本格空转")
	_, diag := m.regDiag[ch]
	require.False(t, diag,
		"通道在同一轮过期，regDiag 却留了下来 ⇒ 两个循环顺序反了（先删 regDiag 后删 qgcOnline）")
}

// TestChannelCloseKeepsNonceWatermark 钉住 ProcessChannelClose 的明确约定：**不清** lastNonce。
// PX4 断线重连（NAT 重建）counter 连续，清了就是无端重开防重放窗口；真正的清路是
// 「PX4 回待命」（§2.5）与 prune 的空闲闸。
func TestChannelCloseKeepsNonceWatermark(t *testing.T) {
	m := newBareManager()
	key := nonceKey{deviceID: 10000001, odd: true}
	m.lastNonce[key] = nonceEntry{counter: 7, lastSeen: time.Now()}

	m.ProcessChannelClose(&gomavlib.EventChannelClose{Channel: &gomavlib.Channel{}})

	_, kept := m.lastNonce[key]
	require.True(t, kept,
		"通道关闭清掉了防重放水位 ⇒ 断线重连后的第一条旧帧可被重放（NAT 重建 counter 是连续的）")
}

// TestSiblingNonceIsNotCreated 钉住 touchSiblingNonce 只**续期**、不**新建**：
// 从没出现过的方向没有可续的水位，伪造一条 counter=0 的条目会掩盖真实的首帧。
func TestSiblingNonceIsNotCreated(t *testing.T) {
	const did = uint32(10000003)

	m := newBareManager()
	ch := &gomavlib.Channel{}
	m.qgcOnline[ch] = &qgcEntry{lastSeen: time.Now()}
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}

	m.processEncrypted(ch, did, idleTestUplinkFrame(did, 5))

	_, upOK := m.lastNonce[nonceKey{deviceID: did, odd: true}]
	require.True(t, upOK, "前提自检失败：上行水位没有建立，本格空转")
	_, downOK := m.lastNonce[nonceKey{deviceID: did, odd: false}]
	require.False(t, downOK,
		"只出现过上行的设备被伪造了一条下行水位 ⇒ 「从没出现过的方向」与「活跃方向」被混为一谈")
}
