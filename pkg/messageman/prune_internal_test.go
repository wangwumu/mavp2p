package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// 本文件钉住 prune 对**空扇出告警节流表**的回收。内部测试（package messageman）的理由
// 与 pair_ttl_internal_test.go 同：emptyDownlinkLogged 是未导出字段、prune() 是未导出方法。
// 复用那里的 newBareManager（同包可见），不重复造夹具。

// TestPruneReclaimsEmptyDownlinkThrottle 钉住节流表**不逃出 prune**。
//
// emptyDownlinkLogged 的键是 deviceID，只在「PX4 加密下行但无配对 QGC」时写入；而那个写入
// 点位于 px4Map 命中之后 ⇒ 它记的全是**曾经活跃过**的 PX4。改前全仓没有任何删除路径
// （只有声明、初始化、写入三处），于是每见过一个 deviceID 就多一条、只增不减 —— deviceID
// 是 32 位空间，长期运行的进程里这张表没有上界。
//
// 清理条件取「不在 px4Map 里」，因为它恰好等价于「该 deviceID 的 PX4 已消失」（写入点的
// 前提就是 px4Map 命中）。两格必须同时成立，缺一不可：
//
//   - 只验阳性格 ⇒ 把整张表无条件清空也能全绿，而那会让正在空扇出的那架飞机每帧刷一条
//     日志（节流表存在的唯一理由就是压住它）；
//   - 只验阴性格 ⇒ 改前就是绿的（什么都不删），用例没有判别力。
func TestPruneReclaimsEmptyDownlinkThrottle(t *testing.T) {
	const (
		gone   = uint32(10000001) // PX4 已消失：px4Map 项超 TTL
		active = uint32(10000002) // PX4 仍活跃 —— 典型就是「正在空扇出」的那一架
	)

	m := newBareManager()
	m.MapTTL = 2 * time.Second

	ch := &gomavlib.Channel{}
	now := time.Now()
	stale := now.Add(-time.Hour)

	// 两架 PX4 都曾触发过空扇出告警（两条节流记录的时间戳刻意取同一个陈旧值：
	// 时间戳本身不参与回收判据，回收只看 px4Map 里还在不在）。
	m.emptyDownlinkLogged[gone] = stale
	m.emptyDownlinkLogged[active] = stale

	// 一架已超 TTL（本轮 prune 会把它从 px4Map 删掉），一架刚被下行刷新过（存活）。
	// ‼️ px4Map 的清理与节流表的回收在同一个 prune 里，且前者必须**先**跑 ——
	// 这一格正是把那个先后顺序钉住的：顺序反了，本轮刚该消失的 gone 还留在 px4Map 里，
	// 于是它的节流记录被漏掉，用例红。
	m.px4Map[gone] = &px4Entry{channel: ch, lastSeen: stale}
	m.px4Map[active] = &px4Entry{channel: ch, lastSeen: now}

	m.prune()

	// ---- 阳性格：PX4 已消失 ⇒ 节流记录必须一并回收 ----
	_, keptGone := m.emptyDownlinkLogged[gone]
	require.False(t, keptGone,
		"该 deviceID 在 px4Map 里已不存在，节流记录却还留着 ⇒ 这张表只增不减（改前正是如此）："+
			"每见过一个 deviceID 就多一条，没有上界")

	// ---- 阴性格：PX4 仍活跃 ⇒ 记录必须保留 ----
	_, keptActive := m.emptyDownlinkLogged[active]
	require.True(t, keptActive,
		"该 deviceID 的 PX4 仍活跃（px4Map 项就在那里，典型是正在空扇出的一架），"+
			"节流记录却被删了 ⇒ 10s 节流失效，退化成每帧一条 `no paired QGC` 日志刷屏")

	// ---- 前提自检：阳性格确实是被 prune 删掉的，而不是夹具压根没建起来 ----
	require.Len(t, m.px4Map, 1,
		"夹具前提不成立：超 TTL 的那个 px4Map 项没被 prune 删掉，阳性格就不是在验回收")
}
