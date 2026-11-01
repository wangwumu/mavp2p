package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// TestStandbyTaskEndClearsPairsAndWatermarks 钉住 §4.5 路径 1（任务结束）的完整状态变更。
//
// 为什么必须是内部用例：任务结束同时关会话门，而开门的第一条 QGC 上行必然重建配对
// ⇒「配对是否被删」从外部行为上永远观测不到（只能用 counter=0 之类的空断言自欺）。
// 外部用例只保留「清水位那行若被删，第二次心跳会清掉刚重建的配对」这一可观测判据。
func TestStandbyTaskEndClearsPairsAndWatermarks(t *testing.T) {
	m := newBareManager()
	now := time.Now()
	const did = uint32(10001001)

	srcCh := &gomavlib.Channel{}
	qgcCh := &gomavlib.Channel{}
	pk := pairKey{qgcCh: qgcCh, px4DeviceID: did}
	// qgcSeen 故意拨老：扇出目标为空，bare manager 没有 Node 可写。
	m.pairs[pk] = &pairEntry{
		px4Ch: srcCh, lastSeen: now, qgcSeen: now.Add(-time.Hour), lastTraffic: now,
	}
	m.px4Map[did] = &px4Entry{channel: srcCh, handshaked: true}
	m.lastTrustedDownlink[did] = trustedMark{at: now, channel: srcCh, sourceIP: "10.0.0.1"}
	nkDown := nonceKey{deviceID: did, dir: dirDownlink}
	nkUp := nonceKey{deviceID: did, dir: dirUplink}
	m.lastNonce[nkDown] = nonceEntry{counter: 9, lastSeen: now}
	m.lastNonce[nkUp] = nonceEntry{counter: 7, lastSeen: now}

	fr := verifyFrame(did, 0, 0, 9)
	m.processStandbyHeartbeat(srcCh, did, fr)

	_, hasPair := m.pairs[pk]
	require.False(t, hasPair, "任务结束必须清配对")
	_, hasPX4 := m.px4Map[did]
	require.False(t, hasPX4, "任务结束必须清已验签映射（handshaked 随条目消失）")
	_, hasDown := m.lastNonce[nkDown]
	require.False(t, hasDown, "任务结束必须清下防水位")
	_, hasUp := m.lastNonce[nkUp]
	require.False(t, hasUp, "任务结束必须清上防水位")
	g, ok := m.sessionGate[did]
	require.True(t, ok, "任务结束必须关会话门")
	require.True(t, g.endedAt.After(g.openedAt), "会话门必须处于关闭态")

	// 水位已空 ⇒ 后续同通道待命心跳只能走候选路径，不得再清任何路由状态。
	m.pairs[pk] = &pairEntry{
		px4Ch: srcCh, lastSeen: now, qgcSeen: now.Add(-time.Hour), lastTraffic: now,
	}
	m.px4Map[did] = &px4Entry{channel: srcCh}
	m.processStandbyHeartbeat(srcCh, did, fr)

	_, hasPair = m.pairs[pk]
	require.True(t, hasPair, "水位已空时第二条待命心跳不得清配对")
	_, hasPX4 = m.px4Map[did]
	require.True(t, hasPX4, "水位已空时第二条待命心跳不得清映射")
}
