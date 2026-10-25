package messageman

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// TestInitializeRejectsNegativeMaxQGCLinkedPX4 钉住入口校验。
//
// 改前：负上限在 processRegistration 里只当循环上界，是无害空转。2026-10-24 的诊断代码
// 引入 `make([]uint32, 0, num)` 之后，它变成**必崩路径**（makeslice: cap out of range）：
// 任一 QGC 发出任意 80005（含 num=0 的空登记）即打挂整个数据面。
func TestInitializeRejectsNegativeMaxQGCLinkedPX4(t *testing.T) {
	require.Error(t, (&Manager{Config: Config{MaxQGCLinkedPX4: -1}}).Initialize(),
		"负 MaxQGCLinkedPX4 被接受 ⇒ 任一 80005 都会 panic，进程直接退出")

	// 阴性对照：零值（用默认 16）与显式正值都必须能初始化，否则上面那格可以靠
	// 「Initialize 永远报错」假绿。零值这条必须真跑 —— 生产 CLI 的默认配置就是 Config{}。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	zero := &Manager{Ctx: ctx, Wg: &wg}
	require.NoError(t, zero.Initialize(), "零值配置被拒绝 ⇒ 默认启动路径起不来")
	require.Equal(t, DefaultMaxQGCLinkedPX4, zero.maxQGCLinkedPX4(),
		"零值没走默认上限 ⇒ 80005 的台数闸读到 0，任何登记都不建配对")

	m := &Manager{Ctx: ctx, Wg: &wg, Config: Config{MaxQGCLinkedPX4: 16}}
	require.NoError(t, m.Initialize(), "显式正值被拒绝 ⇒ 正常配置起不来")
	cancel()
	wg.Wait()
}

// TestRegistrationNegativeMaxDoesNotPanic 是同一风险的**第二道**：即便有人绕过
// Initialize 直接构造 Manager（测试/嵌入），负上限也不得让 processRegistration panic。
func TestRegistrationNegativeMaxDoesNotPanic(t *testing.T) {
	const (
		qgcDid = uint32(1)
		px4Did = uint32(10000001)
	)

	m := newBareManager()
	m.MaxQGCLinkedPX4 = -1
	require.Greater(t, uint64(m.gcsDeviceIDMax()), uint64(qgcDid),
		"夹具前提不成立：80005 帧头不在 GCS 段会被直接丢弃，本格空转")

	ch := &gomavlib.Channel{}
	payload := make([]byte, 1+4)
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:], px4Did)

	require.NotPanics(t, func() {
		m.processRegistration(ch, qgcDid, &message.MessageRaw{ID: 80005, Payload: payload})
	}, "负上限让 processRegistration panic ⇒ 配置写错就打挂数据面")

	_, created := m.pairs[pairKey{qgcCh: ch, px4DeviceID: px4Did}]
	require.False(t, created, "负上限下不应建任何配对（截断为 0）")
}

// TestValidAircraftDeviceIDBoundaries 钉住 Go 侧号段判据的边界值。
// QML 侧有一份同口径的实现（`OpsCommon.isValidAircraftDeviceId`）；两份必须逐值一致，
// 否则「QGC 认为合法、mavp2p 判为非法」会表现成飞机永远收不到下行且没有回执。
func TestValidAircraftDeviceIDBoundaries(t *testing.T) {
	m := newBareManager()

	for _, tc := range []struct {
		id   uint32
		want bool
		why  string
	}{
		{9999999, false, "GCS 段末尾（下界写成 <= 时这一格会红）"},
		{10000000, true, "PX4 段下界"},
		{10001048, true, "正常飞机 deviceID"},
		{0x7EFFFFFF, true, "PX4 段上界（写成 < 时这一格会红）"},
		{0x7F000000, false, "越过上界；bit24=0，只有上界判据能挡"},
		{0x80000000, false, "最高位置位；只能靠上界挡"},
		{0x01000001, false, "号段合法但 bit24（签名位）置位"},
		{0, false, "无效哨兵"},
	} {
		require.Equal(t, tc.want, m.validAircraftDeviceID(tc.id),
			"deviceID=%#x（%s）的判定与协议 §1.3/§1.4 不符", tc.id, tc.why)
	}

	// 分界可配：与 QGC 的硬编码默认值不同源，这里按**实际返回值**判，不靠记忆。
	m.GCSDeviceIDMax = 12000000
	require.False(t, m.validAircraftDeviceID(11500000),
		"自定义分界下的 GCS 段值被判合法 ⇒ 分界参数形同虚设")
	require.True(t, m.validAircraftDeviceID(12000000),
		"自定义分界的下界本身应合法（>= 语义）")
}
