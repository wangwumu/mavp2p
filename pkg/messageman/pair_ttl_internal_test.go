package messageman

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// 本文件是**内部**测试（package messageman，与 manager_test.go 的 messageman_test 外部包
// 并存，Go 允许同目录两包共存）。理由：本用例要钉的两样东西都不可见 —— 配对的 lastSeen
// 是未导出字段、prune() 是未导出方法。
//
// 不走外部包的端到端观测（「QGC 到底收没收到帧」）是因为 prune 由 manager.go 里**硬编码
// 10s** 的 ticker 驱动（run()），等它一次就要 10 秒以上，还把判据泡进时序噪声 —— 用例会
// 又慢又飘。直接调 prune() 把「TTL 到了 ⇒ 配对消失」这条因果钉得又准又快。

// newBareManager 构造一个**不起后台 goroutine** 的 Manager，手工建齐 processEncrypted 与
// prune 会碰到的五张表（在 nil map 上写入会 panic）。
//
// 刻意不走 Initialize()：那会起 run() 的 10s ticker goroutine，而本用例的判据不依赖它、
// 也不该被它影响。
func newBareManager() *Manager {
	return &Manager{
		px4Map:              map[uint32]*px4Entry{},
		qgcOnline:           map[*gomavlib.Channel]*qgcEntry{},
		pairs:               map[pairKey]*pairEntry{},
		lastNonce:           map[nonceKey]uint64{},
		emptyDownlinkLogged: map[uint32]time.Time{},
	}
}

// downlinkFrame 构造一帧 PX4 加密下行：帧头 deviceID = 发送方自身 did（§3.2.1 核心规则），
// 偶数 counter（下行特征），msgID 取非心跳值——避免触发握手置位（§2.5）与心跳拦截判据，
// 让本用例只测「PX4 普通下行对配对 lastSeen 的影响」这一件事。
func downlinkFrame(did uint32, counter uint64) *frame.V2Frame {
	payload := make([]byte, 8+4)
	binary.BigEndian.PutUint64(payload[:8], counter)
	return &frame.V2Frame{
		IncompatibilityFlag: byte(did >> 24),
		CompatibilityFlag:   byte(did >> 16),
		SystemID:            byte(did >> 8),
		ComponentID:         byte(did),
		Message:             &message.MessageRaw{ID: 33, Payload: payload},
		Checksum:            0,
	}
}

// TestPairExpiresDespitePX4Downlink 钉住「PX4 持续下行不得让配对永不过期」。
//
// 改前的实现（2026-10-05 之前）在 PX4 下行分支里刷新该 deviceID 所有配对的 lastSeen，
// 注释自陈是为「QGC 保活中断但 PX4 持续下行时配对不因 MAP_TTL 静默过期」。后果却是配对
// **永不过期**：飞机在飞 ⇒ PX4 持续下行 ⇒ 每帧刷活 ⇒ `now.Sub(e.lastSeen) >= ttl` 永不
// 成立，于是 MAP_TTL 设 30s 还是 60s 都结构性失效，QGC 签出后仍无限期收到该机的加密帧
// （终端每帧一条 `no key for device … dropping encrypted frame`）。
//
// 修法让配对的保活只剩 QGC 侧两源（80005 周期登记、1Hz 加密上行心跳）；二者在飞时都在、
// 签出后都不在 —— 这正是「签出 ⇒ 配对按 MAP_TTL 过期」所需的语义。
//
// 两格判据都不依赖 ticker：第一格直接比对字段，第二格直接调 prune()。
func TestPairExpiresDespitePX4Downlink(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	// 2s 而非几十毫秒：第二格要断言「px4Map 那一格**不**被 prune 删掉」（PX4 映射表靠
	// PX4 自身下行保活是对的），而那是靠「两次调用间隔 < TTL」成立的。取 50ms 这种量级
	// 会把 CI 上的调度抖动变成假红。
	m.MapTTL = 2 * time.Second

	// ‼️ QGC 侧 channel 刻意与 PX4 来源 channel 取**同一个**指针。
	// 下行扇出的结尾是 `if qgc != srcCh { m.Node.WriteFrameTo(...) }`，同指针即扇出为空
	// ⇒ 本用例不必造真实的 gomavlib.Node（Node 留 nil）。本用例断言的是 lastSeen、不是
	// 转发，扇出为空对判据没有影响；万一将来判据被改坏到真的去转发，会以 nil 解引用
	// 崩掉而不是静默通过。
	ch := &gomavlib.Channel{}

	// 配对早已过期：lastSeen 取一小时前，远超 2s 的 TTL。
	stale := time.Now().Add(-time.Hour)
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: stale}
	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: ch, lastSeen: stale}

	// PX4 发来一帧加密下行。改前这里会把 e.lastSeen 刷成 now。
	m.processEncrypted(ch, did, downlinkFrame(did, 2))

	// ---- 第一格：下行**不得**刷新配对 lastSeen ----
	e, ok := m.pairs[pk]
	require.True(t, ok, "配对项本身不该被下行帧删掉（本格只钉 lastSeen）")
	require.Equal(t, stale, e.lastSeen,
		"PX4 下行刷新了配对 lastSeen ⇒ MAP_TTL 结构性失效：飞机在飞就永不过期，QGC 签出后仍会无限期收帧")

	// ---- 第二格：后果（行为级）—— TTL 已过，prune 必须把配对清掉 ----
	// 只有第一格的话，证的是「这个字段没被写」，证不了「于是配对真的会过期」。
	m.prune()
	_, stillThere := m.pairs[pk]
	require.False(t, stillThere,
		"lastSeen 已超 TTL，prune 仍留着该配对 ⇒ 它还会被下行路由命中并转发")

	// ---- 阴性对照：px4Map 那一格应当**存活** ----
	// 两张表的 lastSeen 语义不同，别混为一谈：PX4 映射表就该靠 PX4 自身下行保活（PX4 真的
	// 不发了才该消失），配对表才该靠 QGC 侧保活。少了这一格，把「下行不刷任何 lastSeen」
	// 当成修法也能全绿 —— 而那会让 PX4 一有遥测间隙就被判掉线。
	require.Len(t, m.px4Map, 1,
		"PX4 映射表刚被本帧刷新过，不该被 prune 删掉（2s TTL 远大于两次调用的间隔）")
}

// TestRegistrationKeepsPairAlive 钉住配对的**保活源**之一：80005 周期登记。
//
// 上一格证明了「PX4 下行**不**刷配对 lastSeen」，那个修法把配对的保活全押在 QGC 侧两源上
// （80005 周期登记、1Hz 加密上行心跳）。本用例钉住其中之一 —— 否则那个修法可以被「顺手
// 把 processRegistration 里的刷新也删掉」弄成全绿，后果是配对在任务**进行中**也按 MAP_TTL
// 过期：QGC 还在飞、还在照常登记，却再也收不到这架飞机的下行。
//
// 阴性对照不可省：只声明「配对还在」证不了是登记起了作用 —— prune 若因任何原因没在删
// 东西，阳性判据照样全绿。所以同时放一条**没被声明**的配对进去，它必须被删掉。
func TestRegistrationKeepsPairAlive(t *testing.T) {
	const (
		qgcDid = uint32(1)        // GCS 段（§2.2）
		px4Did = uint32(10000001) // 80005 里声明的那个，配对**已存在**（走刷新分支）
		fresh  = uint32(10000003) // 80005 里声明的那个，配对**不存在**（走新建分支）
		other  = uint32(10000002) // 刻意不声明 —— 阴性对照
	)

	m := newBareManager()
	m.MapTTL = 2 * time.Second

	// ‼️ 夹具前提自检：deviceID 必须真落在 GCS 段。否则 processRegistration 的第一件事就是
	// 「非 GCS 段 ⇒ 丢弃并返回」，后面每一格都在空转，而且会以「配对没被刷新」的形式假红，
	// 把我引到错误的根因上。零值兜底确实是 DefaultGCSDeviceIDMax，但那是实现细节，这里按
	// **实际返回值**判，不靠记忆。
	require.Greater(t, uint64(m.gcsDeviceIDMax()), uint64(qgcDid),
		"夹具前提不成立：本帧 deviceID 不在 GCS 段，80005 会被直接丢弃，用例全格空转")

	ch := &gomavlib.Channel{}
	stale := time.Now().Add(-time.Hour)

	m.qgcOnline[ch] = &qgcEntry{lastSeen: stale}
	aliveKey := pairKey{qgcCh: ch, px4DeviceID: px4Did}
	otherKey := pairKey{qgcCh: ch, px4DeviceID: other}
	m.pairs[aliveKey] = &pairEntry{px4Ch: ch, lastSeen: stale}
	m.pairs[otherKey] = &pairEntry{px4Ch: ch, lastSeen: stale}

	// 80005 登记：num=2，声明 px4Did 与 fresh；other 刻意不出现。
	payload := make([]byte, 1+2*4)
	payload[0] = 2
	binary.BigEndian.PutUint32(payload[1:], px4Did)
	binary.BigEndian.PutUint32(payload[5:], fresh)
	m.processRegistration(ch, qgcDid, &message.MessageRaw{ID: 80005, Payload: payload})

	// ---- 第一格：登记刷新 QGC 在线表（键 = 来源 socketID） ----
	qe, ok := m.qgcOnline[ch]
	require.True(t, ok, "QGC 在线表项被这帧登记顺手删了")
	require.NotEqual(t, stale, qe.lastSeen,
		"80005 登记是在线表的保活源之一，没刷 ⇒ 联网正常但一时没发加密上行的 QGC 会被误判掉线")

	// ---- 第二格：登记刷新被声明配对的 lastSeen ----
	pe, ok := m.pairs[aliveKey]
	require.True(t, ok, "被声明的配对不该被这帧登记删掉")
	require.NotEqual(t, stale, pe.lastSeen,
		"80005 声明了该 PX4 却没刷配对 ⇒ 保活源被腰斩，QGC 在任务进行中也会按 MAP_TTL 过期")

	// ---- 第三格：声明里出现、但本地还没有的配对要被**新建**（§3.2.4.1 重启恢复）----
	// 这一支与第二格是同一个循环里的两个分支，缺了它，「刷新」的判据盖不住「新建」——
	// 把新建那两行删掉，配对表里会安静地少一条，而第二格照样绿。
	freshKey := pairKey{qgcCh: ch, px4DeviceID: fresh}
	fe, ok := m.pairs[freshKey]
	require.True(t, ok, "80005 声明了一个本地没有的 PX4，配对却没被建起来 ⇒ 重启恢复路径断了")
	require.False(t, fe.lastSeen.IsZero(), "新建的配对没有 lastSeen，第一次 prune 就会把它当成陈年项删掉")

	// ---- 第四格（行为级）：prune 之后，被声明与未被声明的下场必须**不同** ----
	m.prune()
	_, kept := m.pairs[aliveKey]
	_, keptFresh := m.pairs[freshKey]
	_, expired := m.pairs[otherKey]
	require.True(t, kept, "刚被 80005 声明过的配对，TTL 未过却没了")
	require.True(t, keptFresh, "刚被 80005 新建的配对，TTL 未过却没了")
	require.False(t, expired,
		"没被声明的那个配对也活着 ⇒ 本用例的阳性判据没有判别力：prune 压根没在删东西，"+
			"「配对还在」不能归因于登记")
	require.Len(t, m.qgcOnline, 1, "刚登记过的 QGC 在线表不该被 prune 清空")
}
