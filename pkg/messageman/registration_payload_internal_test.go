package messageman

import (
	"testing"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// 80005 的 payload 由 `mavlink_msg_qgc_registration_pack` 零初始化后按 len 发送，
// **尾部零字节会被裁掉**——一帧一台时，被裁的就是那唯一一台 deviceID 的低字节。
// 真实 deviceID 的低字节不可控（`provision_px4_device.sh` 只校验号段/bit24），
// 低字节为 0 的飞机（如 10000384 = 0x00989800）因此会发出 len==4 的登记帧。
// mavp2p 必须按**声明的台数**解码、缺失字节按 0 补齐，否则该机永远建不出配对。

func TestRegistrationParsesTailTrimmedDeviceID(t *testing.T) {
	m := newBareManager()
	ch := &gomavlib.Channel{}

	m.processRegistration(ch, DefaultGCSDeviceID, &message.MessageRaw{
		ID:      msgIDQGCRegistration,
		Payload: []byte{1, 0x00, 0x98, 0x98}, // 10000384 的尾字节 0x00 被裁
	})

	k := pairKey{qgcCh: ch, px4DeviceID: 10000384}
	_, ok := m.pairs[k]
	require.True(t, ok,
		"尾部零被裁的合法登记被解析成 0 台 ⇒ 该机永远建不出配对、收不到下行")
}

func TestRegistrationPaddingDoesNotCreatePhantomPairs(t *testing.T) {
	m := newBareManager()
	ch := &gomavlib.Channel{}

	// 声明 2 台，第二台全零（打包后整段被裁成尾零）⇒ 只该建出第一台的配对；
	// 补出来的 0 不是合法飞机 ID，必须被 validAircraftDeviceID 挡掉。
	m.processRegistration(ch, DefaultGCSDeviceID, &message.MessageRaw{
		ID:      msgIDQGCRegistration,
		Payload: []byte{2, 0x00, 0x98, 0x98, 0x01},
	})

	require.Len(t, m.pairs, 1, "补齐的零字节被当成真实设备建了配对")
	k := pairKey{qgcCh: ch, px4DeviceID: 10000385}
	_, ok := m.pairs[k]
	require.True(t, ok, "第一台 10000385 必须照常登记")
}
