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

type countSink struct {
	n int
}

func (s *countSink) ProcessFrame(frame.Frame) {
	s.n++
}

// TestDownlinkRebuildRequiresAircraftSegment 钉住「查不到缓存就新建」的号段闸。
//
// 恢复路径本身（§3.2.4.1）必须保留：飞机在飞时 mavp2p 重启，PX4 不再发明文待命
// 心跳，只能靠加密下行的帧头 deviceID 重建。但帧头来自载具端点不等于它一定是飞机：
// QGC 被误指到载具端口、或内网任意主机发一帧，都能凭 GCS 段 deviceID 造出幻象
// PX4，并把后续真遥测判成重放、把伪造帧喂给 data_writer。
//
// 判据必须同时包含阳性对照：号段合法的帧仍要重建并进下游，否则「一律不重建」
// 也能让否定分支全绿。
func TestDownlinkRebuildRequiresAircraftSegment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		did     uint32
		counter uint64
		want    bool
	}{
		{name: "aircraft id rebuilds", did: 10001048, counter: 2, want: true},
		{name: "gcs segment id is rejected", did: 91002, counter: 2, want: false},
		{name: "zero id is rejected", did: 0, counter: 2, want: false},
		{name: "above segment upper bound is rejected", did: 0x7F016379, counter: 2, want: false},
		// 号段内、仅 bit24（签名位）命中：这一格单独把 bit24 子句钉死。
		// 上面那格同时越过上界，删掉任一条子句都不会红。
		{name: "signature bit within segment is rejected", did: 0x01000001, counter: 2, want: false},
		// counter=0 ⇒ 不写水位（nk 为 nil）。若有人把号段闸挪进 `if nk != nil` 里，
		// 这一格会先建出 px4Map 再被跳过闸门，从而红。
		{name: "invalid id with zero counter is rejected", did: 91002, counter: 0, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 号段合法的帧在 P5 后还要过验签（无映射 ⇒ needVerify），
			// 用 fake oracle 放行，才能观察到「重建 + 水位 + Sink」。
			o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
			m := newBareManager()
			m.VerifySocket = o.path()
			m.VerifyTimeout = 500 * time.Millisecond
			sink := &countSink{}
			m.Sink = sink

			// 内部用例直接构造 Channel（不走 Initialize）：本用例只关心号段闸，
			// 不依赖端点角色——角色由 ProcessFrame 决定，这里直接调用 processEncrypted。
			ch := &gomavlib.Channel{}
			payload := make([]byte, 12)
			binary.BigEndian.PutUint64(payload[:8], tc.counter) // 非零 counter 才会写水位
			fr := &frame.V2Frame{
				IncompatibilityFlag: byte(tc.did >> 24),
				CompatibilityFlag:   byte(tc.did >> 16),
				SystemID:            byte(tc.did >> 8),
				ComponentID:         byte(tc.did),
				Message:             &message.MessageRaw{ID: 33, Payload: payload},
			}

			m.processEncrypted(ch, tc.did, fr, dirDownlink)

			if tc.want {
				waitVerifyIdle(t, m)
				require.Contains(t, m.px4Map, tc.did, "合法飞机号段必须能重建缓存")
				e, ok := m.lastNonce[nonceKey{deviceID: tc.did, dir: dirDownlink}]
				require.True(t, ok, "重建后必须正常推进下行水位")
				require.Equal(t, uint64(2), e.counter, "水位写入的不是本帧 counter")
				require.Equal(t, 1, sink.n, "重建的加密下行必须进下游")
			} else {
				require.Empty(t, m.px4Map, "非法号段不得创建幻象 PX4")
				require.Empty(t, m.lastNonce, "非法号段不得推进下行水位")
				require.Zero(t, sink.n, "非法号段不得进下游")
			}
		})
	}
}
