package messageman

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClassifyIngressShapeGate 钉住 03 §4.4 的入口形状表。
//
// 这里只测纯判据：方向已由入口端点决定，判据不再读 counter 奇偶、不读 qgcOnline。
// 表里的 9/28/37/49/50/92 都是协议实测量：明文待命 9B、加密块最小 28B、QGC 加密心跳
// 37B、PX4 加密心跳 92B；T=50 落在 QGC 与 PX4 心跳之间。
func TestClassifyIngressShapeGate(t *testing.T) {
	const aircraftID uint32 = 10001048

	for _, tc := range []struct {
		name   string
		dir    direction
		msgID  uint32
		plen   int
		did    uint32
		want   ingressKind
		reason ingressDropReason
	}{
		{name: "deviceID=0 的上行加密帧", dir: dirUplink, msgID: 33, plen: 36, did: 0,
			want: ingressDrop, reason: dropZeroDeviceID},
		{name: "deviceID=0 的 80005", dir: dirUplink, msgID: msgIDQGCRegistration, plen: 5, did: 0,
			want: ingressDrop, reason: dropZeroDeviceID},
		{name: "非法方向即使长度合法也丢弃", dir: dirInvalid, msgID: 33, plen: 36, did: aircraftID,
			want: ingressDrop, reason: dropInvalidDirection},

		{name: "载具 9B 明文待命", dir: dirDownlink, msgID: 0, plen: 9, did: aircraftID,
			want: ingressStandby},
		{name: "载具 8B msgID=0", dir: dirDownlink, msgID: 0, plen: 8, did: aircraftID,
			want: ingressDrop, reason: dropMalformedStandby},
		{name: "载具 10B msgID=0", dir: dirDownlink, msgID: 0, plen: 10, did: aircraftID,
			want: ingressDrop, reason: dropMalformedStandby},
		{name: "载具 37B QGC 心跳形状", dir: dirDownlink, msgID: 0, plen: 37, did: aircraftID,
			want: ingressDrop, reason: dropMalformedStandby},
		{name: "载具 49B msgID=0", dir: dirDownlink, msgID: 0, plen: 49, did: aircraftID,
			want: ingressDrop, reason: dropMalformedStandby},
		{name: "载具 50B 达到阈值", dir: dirDownlink, msgID: 0, plen: 50, did: aircraftID,
			want: ingressEncrypted},
		{name: "载具 92B 加密心跳", dir: dirDownlink, msgID: 0, plen: 92, did: aircraftID,
			want: ingressEncrypted},

		{name: "GCS 27B msgID=0", dir: dirUplink, msgID: 0, plen: 27, did: aircraftID,
			want: ingressDrop, reason: dropMalformedHeartbeat},
		{name: "GCS 28B msgID=0", dir: dirUplink, msgID: 0, plen: 28, did: aircraftID,
			want: ingressEncrypted},
		{name: "GCS 37B QGC 心跳", dir: dirUplink, msgID: 0, plen: 37, did: aircraftID,
			want: ingressEncrypted},

		{name: "其它 msgID 27B 上行", dir: dirUplink, msgID: 33, plen: 27, did: aircraftID,
			want: ingressDrop, reason: dropShortFrame},
		{name: "其它 msgID 27B 下行", dir: dirDownlink, msgID: 33, plen: 27, did: aircraftID,
			want: ingressDrop, reason: dropShortFrame},
		{name: "其它 msgID 28B 上行", dir: dirUplink, msgID: 33, plen: 28, did: aircraftID,
			want: ingressEncrypted},
		{name: "其它 msgID 28B 下行", dir: dirDownlink, msgID: 33, plen: 28, did: aircraftID,
			want: ingressEncrypted},

		{name: "80005 上行", dir: dirUplink, msgID: msgIDQGCRegistration, plen: 5, did: 10000,
			want: ingressRegistration},
		// 方向闸仍在 ProcessFrame（80005 只收 GCS 端点），入口判据只分类、不判方向。
		{name: "80005 下行", dir: dirDownlink, msgID: msgIDQGCRegistration, plen: 5, did: 10000,
			want: ingressRegistration},

		{name: "非 MessageRaw 的 msgID=0 载具", dir: dirDownlink, msgID: 0, plen: -1, did: aircraftID,
			want: ingressDrop, reason: dropMalformedStandby},
		{name: "非 MessageRaw 的普通帧", dir: dirUplink, msgID: 33, plen: -1, did: aircraftID,
			want: ingressDrop, reason: dropShortFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, reason := classifyIngress(tc.dir, tc.msgID, tc.plen, tc.did)
			require.Equal(t, tc.want, kind)
			require.Equal(t, tc.reason, reason)
		})
	}
}

// TestHeartbeatThresholdSatisfiesProtocolLengths 把 §4.2 的安全前提写进用例：
// QGC 心跳 < T ≤ PX4 心跳（当前 37 < 50 ≤ 92）。
//
// T 是部署验收项，运行期无法检测前提是否仍成立；这条用例至少保证「改 T 的人会被
// 现有协议长度拦住」。真实抓包核对仍须在发布前做（§4.2）。
func TestHeartbeatThresholdSatisfiesProtocolLengths(t *testing.T) {
	const (
		qgcHeartbeatLen = 37
		px4HeartbeatLen = 92
	)
	require.Greater(t, heartbeatLenThreshold, qgcHeartbeatLen,
		"T 必须大于 QGC 加密心跳，否则跨方向原样重放会落进加密下行")
	require.LessOrEqual(t, heartbeatLenThreshold, px4HeartbeatLen,
		"T 必须不大于 PX4 加密心跳，否则真机心跳会被当成待命丢弃")
}
