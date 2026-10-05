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

// TestPX4DownlinkKeepsPairButNotFanout 钉住配对表两个时间戳的分工（2026-10-06 恢复）。
//
// 演进经过，三段都要看，否则很容易把其中一段当成「正确实现」：
//
//  1. 最初（2026-10-05 之前）PX4 下行刷新配对 lastSeen，**同时**它也是唯一的保活源。后果：
//     配对永不过期（飞机在飞 ⇒ 每帧刷活），QGC 签出后仍无限期收到该机的加密帧（终端每帧
//     一条 `no key for device … dropping encrypted frame`）。
//  2. 2026-10-05（1a6da49）把 PX4 下行那一行删掉，配对保活只剩 QGC 侧两源。签出确实会停，
//     但**引入了对航线监控员 QGC 的误伤**：QGC 侧一时安静（链路抖动、80005 丢包）配对就被
//     prune 删掉，必须等下一次 80005 登记（周期 10s）才重建 —— 这段空窗里 PX4 下行扇出为
//     空，监控员界面「飞机不动了」。
//  3. 本次（2026-10-06）恢复 PX4 下行对 **lastSeen** 的刷新（配对因此常驻，不再有「删除 →
//     重建」空窗），同时把「要不要真的投给某个 QGC」拆出来交给 **qgcSeen** 单独决定。
//
// 于是两件事分开了：PX4 活跃只能让配对**活着**，不能让一个已经安静的 QGC 继续**收帧**。
// 本用例四格分别钉住这条分工的四面。
func TestPX4DownlinkKeepsPairButNotFanout(t *testing.T) {
	const did = uint32(10000001)

	m := newBareManager()
	// 2s 而非几十毫秒：判据 3 靠「两次调用的间隔 < TTL」成立，取 50ms 这种量级会把 CI 上的
	// 调度抖动变成假红。
	m.MapTTL = 2 * time.Second

	// ‼️ 这里刻意**只用一个** channel 指针，同时充当 PX4 下行来源与配对里的 QGC 侧。
	// 扇出末尾是 `if qgc != srcCh { m.Node.WriteFrameTo(...) }`，同指针即扇出为空 ⇒
	// processEncrypted 不会碰 m.Node（Node 留 nil，真去转发会 nil 解引用 panic）。这条约束
	// 换来的是：判据 1、2 的字段观测不会被 panic 打断 —— 变异落在断言上，而不是崩掉整个测试
	// 二进制（崩了后面那个用例根本不跑，诊断价值就没了）。
	// 扇出本身由判据 3、4 直接调 fanoutTargets 观测，那正是「算谁该收」与「真去写」分开的
	// 意义；到那一步，同不同指针已经无所谓。
	ch := &gomavlib.Channel{}

	// 两个时间戳刻意取不同龄期，让判据可以分开观测：
	//   - lastSeen 一小时前：远超 TTL，若 PX4 不刷它，prune 一定该删。
	//   - qgcSeen 3×TTL 前：**已超 TTL**（扇出该被挡）但**未到 10×TTL 的兜底闸**（配对不该
	//     被兜底清理）—— 取同一时刻的话，「配对还在」就分不清是 lastSeen 保住了它还是兜底
	//     还没触发，判据失去判别力。
	staleSeen := time.Now().Add(-time.Hour)
	staleQGC := time.Now().Add(-3 * m.MapTTL)

	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: staleSeen}
	pk := pairKey{qgcCh: ch, px4DeviceID: did}
	m.pairs[pk] = &pairEntry{px4Ch: ch, lastSeen: staleSeen, qgcSeen: staleQGC}

	// PX4 发来一帧加密下行。
	m.processEncrypted(ch, did, downlinkFrame(did, 2))

	// ---- 第一格：PX4 下行**恢复**刷新配对 lastSeen（配对存活） ----
	e, ok := m.pairs[pk]
	require.True(t, ok, "配对项本身不该被下行帧删掉")
	require.NotEqual(t, staleSeen, e.lastSeen,
		"PX4 下行没有保活配对 ⇒ 误伤未修复：QGC 侧一时安静就会让配对按 MAP_TTL 被删，"+
			"要等下一次 80005 登记（周期 10s）才重建，这段空窗里监控员界面数据冻结")

	// ---- 第二格：但**不得**刷新 qgcSeen（可用性只由 QGC 侧证明） ----
	require.Equal(t, staleQGC, e.qgcSeen,
		"PX4 下行刷新了 qgcSeen ⇒ PX4 活跃被当成 QGC 仍在用这台飞机，"+
			"场地操作员签出后仍会被无限期投喂")

	// ---- 第三格（行为级）：qgcSeen 已超 TTL ⇒ 扇出必须为空 ----
	// 只有前两格的话，证的是「这两个字段各自被写/没被写」，证不了「于是真的不发」。
	require.Empty(t, m.fanoutTargets(did, ch, time.Now()),
		"qgcSeen 已超 TTL，扇出却仍把该 QGC 算进去 ⇒ 这道门形同虚设，签出方照收不误")

	// ---- 第四格（阳性对照）：qgcSeen 一新鲜，扇出立刻恢复 ----
	// 少了这一格，「门恒关」（例如把判据写成 <= 或干脆 return nil）也能全绿 —— 而那会让
	// 正常在飞的 QGC 一帧都收不到。
	e.qgcSeen = time.Now()
	require.Equal(t, []*gomavlib.Channel{ch}, m.fanoutTargets(did, ch, time.Now()),
		"qgcSeen 新鲜却仍不扇出 ⇒ 门装反了，QGC 正常在飞也收不到下行")

	// ---- 阴性对照：别的 deviceID 不得被这帧认领 ----
	require.Empty(t, m.fanoutTargets(did+1, ch, time.Now()),
		"扇出按 deviceID 过滤这一层失效：本帧会被投给无关的 QGC")
}

// TestRegistrationKeepsPairAlive 钉住配对的**保活源**之一：80005 周期登记。
//
// 配对的两条命脉都在 QGC 侧（80005 周期登记、1Hz 加密上行心跳）：lastSeen 靠它们存活、
// qgcSeen 靠它们变新鲜。本用例钉住其中之一 —— 否则「顺手把 processRegistration 里的刷新也
// 删掉」能弄成全绿，后果是配对在任务**进行中**也按 MAP_TTL 过期：QGC 还在飞、还在照常登记，
// 却再也收不到这架飞机的下行。
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
	// qgcSeen 与 lastSeen 取同一龄期：本用例只管「80005 登记能不能救活配对」，不区分两个
	// 时间戳（那个分工由 TestPX4DownlinkKeepsPairButNotFanout 钉）。
	m.pairs[aliveKey] = &pairEntry{px4Ch: ch, lastSeen: stale, qgcSeen: stale}
	m.pairs[otherKey] = &pairEntry{px4Ch: ch, lastSeen: stale, qgcSeen: stale}

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
