package messageman

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/stretchr/testify/require"
)

// 本文件钉住 80005 诊断日志的节流判据：**当前仍在出现的**签名不会被窗口永久吞掉。
//
// 监控员视图的 2s 定向重发会逐台轮发单设备帧（`1|[A]` → `1|[B]` → …），签名几乎每帧都变；
// 5s 窗口存在的意义是把这段噪音压成"窗口过后报一次当前状态"，窗口内的**其它**中间签名
// 会被合并掉（这是本意，不是丢失）。但若实现在窗口内就把 sig 记成新值，
// 那个状态此后永远命中"签名没变"⇒ 一行都不会出现，而日志看起来一切正常。

func TestRegDiagThrottleDoesNotSwallowChangedSignature(t *testing.T) {
	m := &Manager{}
	ch := &gomavlib.Channel{}
	t0 := time.Unix(1_700_000_000, 0)

	require.True(t, m.shouldLogRegDiag(ch, "1|[A]", t0), "第一次必须打")
	require.False(t, m.shouldLogRegDiag(ch, "1|[A]", t0.Add(time.Second)), "同签名不得重复打")

	// 窗口内换成 B：先节流……
	require.False(t, m.shouldLogRegDiag(ch, "1|[B]", t0.Add(2*time.Second)), "窗口内应节流")
	// ……窗口一到，B 必须补打出来。
	require.True(t, m.shouldLogRegDiag(ch, "1|[B]", t0.Add(kRegDiagLogInterval)),
		"窗口内变过的签名被永久吞掉 ⇒ 日志里看不到这一轮实际登记了谁")

	// 打完之后，同签名再次静默。
	require.False(t, m.shouldLogRegDiag(ch, "1|[B]", t0.Add(kRegDiagLogInterval+time.Second)))
}

func TestRegDiagThrottleBackToLoggedSignatureIsSilent(t *testing.T) {
	m := &Manager{}
	ch := &gomavlib.Channel{}
	t0 := time.Unix(1_700_000_000, 0)

	require.True(t, m.shouldLogRegDiag(ch, "1|[A]", t0))
	require.False(t, m.shouldLogRegDiag(ch, "1|[B]", t0.Add(time.Second)))
	// 窗口内又变回 A（= 上次**已打过**的状态）：没有新信息，窗口过去后也不该打。
	require.False(t, m.shouldLogRegDiag(ch, "1|[A]", t0.Add(2*time.Second)))
	require.False(t, m.shouldLogRegDiag(ch, "1|[A]", t0.Add(kRegDiagLogInterval+time.Second)))
}

func TestRegDiagThrottleIsPerChannel(t *testing.T) {
	m := &Manager{}
	ch1 := &gomavlib.Channel{}
	ch2 := &gomavlib.Channel{}
	t0 := time.Unix(1_700_000_000, 0)

	require.True(t, m.shouldLogRegDiag(ch1, "1|[A]", t0))
	require.True(t, m.shouldLogRegDiag(ch2, "1|[A]", t0), "两条通道的节流互不影响")
}
