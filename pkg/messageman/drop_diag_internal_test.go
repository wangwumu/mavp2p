package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// 本文件钉住**入口闸告警节流表**（dropDiag）的两端与中间：
//
//   - 写入端：`logDropThrottled` 必须记账；
//   - 主删除端：`ProcessChannelClose` 必须按通道回收；
//   - 兜底删除端：`prune` 必须按 StateIdleTimeout 回收（通道关闭事件有漏，见 manager.go
//     的 pairs 兜底注释：实测「18 开 / 17 闭」）。
//
// 只钉 prune 一端不够：删掉写入点或通道关闭的删除，整套测试仍会绿。

// TestLogDropThrottledRecordsEntry 钉住写入端：没有这一行，prune 的兜底闸永远没有对象，
// 节流也退化成每帧一条日志。
//
// 第二格钉住「原因参与节流判定」：同一通道 10s 内换了原因（不同调用点）必须重新放行。
// 否则先到的那条告警会把窗口内所有别的告警一起吞掉——端口接反时往往同时触发多种丢弃。
func TestLogDropThrottledRecordsEntry(t *testing.T) {
	m := newBareManager()
	ch := &gomavlib.Channel{}

	m.logDropThrottled(ch, "reason A from %s", ch)
	require.Contains(t, m.dropDiag, ch,
		"logDropThrottled 没有记录本次告警 ⇒ 10s 节流失效、prune 兜底也无从谈起")

	// 同一原因立即再来一次 ⇒ 被节流（时间戳与原因都不变）。
	before := m.dropDiag[ch]
	m.logDropThrottled(ch, "reason A from %s", ch)
	require.Equal(t, before, m.dropDiag[ch], "同一原因在 10s 窗口内没有被节流")

	// 换一个原因 ⇒ 必须放行并更新记录（否则新原因的首次出现被旧原因吞掉）。
	m.logDropThrottled(ch, "reason B from %s", ch)
	require.Equal(t, "reason B from %s", m.dropDiag[ch].reason,
		"同一通道换了原因仍被节流 ⇒ 端口接反时第二种丢弃的首次出现会被吞掉")
}

// TestChannelCloseReclaimsDropDiag 钉住主删除端。该表按键持有 channel 指针；漏一次关闭事件，
// 指针就永久滞留（每次 NAT 重建都是一条新 channel）。
func TestChannelCloseReclaimsDropDiag(t *testing.T) {
	m := newBareManager()
	ch := &gomavlib.Channel{}

	m.logDropThrottled(ch, "test drop from %s", ch)
	require.Contains(t, m.dropDiag, ch) // 前提自检：先证明记录真的建起来了

	m.ProcessChannelClose(&gomavlib.EventChannelClose{Channel: ch})

	require.NotContains(t, m.dropDiag, ch,
		"通道关闭后节流记录仍在 ⇒ 每条 NAT 重建的旧 channel 都会永久滞留在这张表里")
}

// TestPruneReclaimsIdleDropThrottle 钉住兜底删除端，且**阈值身份**必须是
// StateIdleTimeout（10m），不是 MapTTL（云端 30s / 默认 60s）。
//
// 三格必须同时成立：
//   - 超过 StateIdleTimeout ⇒ 删；
//   - 刚打过告警 ⇒ 保留（无条件清空会让接反端口的通道每帧刷一条日志）；
//   - 5 分钟前 ⇒ **保留**（若把判据错写成 MapTTL 或任意更短的值，这一格红）。
func TestPruneReclaimsIdleDropThrottle(t *testing.T) {
	const idle = 10 * time.Minute

	m := newBareManager()
	m.StateIdleTimeout = idle

	idleCh := &gomavlib.Channel{}
	midCh := &gomavlib.Channel{}
	freshCh := &gomavlib.Channel{}
	now := time.Now()

	// 三个通道都先走生产写入点，再按需改写时间戳（证明用例依赖的是真实记账，不是手写 map）。
	for _, ch := range []*gomavlib.Channel{idleCh, midCh, freshCh} {
		m.logDropThrottled(ch, "test drop from %s", ch)
	}
	require.Len(t, m.dropDiag, 3, "夹具前提不成立：三条节流记录没有同时建立")
	// 只改时刻、保留原因：回收判据只看时刻。
	idleEntry := m.dropDiag[idleCh]
	idleEntry.at = now.Add(-idle - time.Minute) // 11 分钟前
	m.dropDiag[idleCh] = idleEntry
	midEntry := m.dropDiag[midCh]
	midEntry.at = now.Add(-5 * time.Minute) // 5 分钟前：短于 StateIdleTimeout、长于 MapTTL
	m.dropDiag[midCh] = midEntry

	m.prune()

	_, keptIdle := m.dropDiag[idleCh]
	require.False(t, keptIdle,
		"该通道的告警记录已超过 StateIdleTimeout，却还留着 ⇒ 通道关闭事件漏一次，"+
			"这个指针就永久滞留在表里（每次 NAT 重建都是一条新 channel，只增不减）")

	_, keptMid := m.dropDiag[midCh]
	require.True(t, keptMid,
		"5 分钟前的记录被删了 ⇒ 回收阈值不是 StateIdleTimeout（可能错写成 MapTTL）："+
			"一个仍在偶发丢帧的通道会被过早忘记，首条告警的节流窗口被意外重开")

	_, keptFresh := m.dropDiag[freshCh]
	require.True(t, keptFresh,
		"刚打过告警的通道记录被删了 ⇒ 10s 节流失效，"+
			"接反端口的短命通道会每帧刷一条 `dropped …` 日志")
}
