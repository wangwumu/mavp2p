package messageman

import (
	"encoding/binary"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
)

// heartbeatLenThreshold 是载具端点 msgID=0 的分流阈值 T（03 §4.4）。
//
// 载具端点上：msgID=0 且 payload<T ⇒ 待命路径（只接受 9B）；payload≥T ⇒ 加密心跳。
// GCS 端点不使用 T（QGC 加密心跳 37B，只按 ≥28B 判）。
//
// ‼️ 安全前提：QGC 加密心跳长度 < T ≤ PX4 加密心跳长度（当前 37 < 50 ≤ 92）。
// 当前值保证「截获 QGC 心跳、原样投到载具端点」会落在待命路径并被长度判据丢掉。
// 若 QGC 心跳增长到 ≥T 或 PX4 心跳缩短到 <T，必须同步调整 T——运行期无法检测这一点，
// 属部署/验收检查项（03 §4.2）。T 是常量，只在这里定义一次，不暴露为 CLI。
const heartbeatLenThreshold = 50

// ingressKind 是入口分类结果。方向已由入口端点决定，这里只回答「这帧该走哪条处理」。
type ingressKind uint8

const (
	ingressDrop ingressKind = iota
	ingressRegistration
	ingressStandby
	ingressEncrypted
)

// ingressDropReason 区分丢弃原因，仅用于日志（节流键与排障）。
type ingressDropReason uint8

const (
	dropNone ingressDropReason = iota
	dropInvalidDirection
	dropZeroDeviceID
	dropMalformedStandby
	dropMalformedHeartbeat
	dropShortFrame
)

// classifyIngress 实现 03 §4.4 的入口形状表。dir 必须已由入口端点确定。
//
// 判据只用「方向（端点）、msgID、payload 长度、帧头 deviceID」，不解密、不读 counter、
// 不看 qgcOnline 成员资格。counter=0 与下行号段闸在 ProcessFrame 里做（需要 payload/配置）。
func classifyIngress(dir direction, msgID uint32, plen int, did uint32) (ingressKind, ingressDropReason) {
	// 帧头 deviceID=0 在任意端点、任意 msgID 下都无效（保留段），80005 也不例外。
	if did == 0 {
		return ingressDrop, dropZeroDeviceID
	}
	if dir != dirUplink && dir != dirDownlink {
		return ingressDrop, dropInvalidDirection
	}
	if msgID == msgIDQGCRegistration {
		// 方向闸（仅 GCS 端点）留在 ProcessFrame：分类器不判端点身份。
		return ingressRegistration, dropNone
	}
	if msgID == msgIDHeartbeat {
		if dir == dirUplink {
			// QGC 加密心跳当前 37B；GCS 端点不存在明文待命心跳。
			if plen < encryptedPayloadMinLen {
				return ingressDrop, dropMalformedHeartbeat
			}
			return ingressEncrypted, dropNone
		}
		// 载具端点：T 以上的 msgID=0 才是加密心跳；T 以下只有 9B 待命合法。
		// 28–49B 的 msgID=0 只可能是被原样搬过来的 QGC 心跳或畸形帧，一律丢弃。
		switch {
		case plen == 9:
			return ingressStandby, dropNone
		case plen >= heartbeatLenThreshold:
			return ingressEncrypted, dropNone
		default:
			return ingressDrop, dropMalformedStandby
		}
	}
	// 其它 msgID：加密块最小 28B（counter 8 + deviceID 4 + tag 16）。
	if plen < encryptedPayloadMinLen {
		return ingressDrop, dropShortFrame
	}
	return ingressEncrypted, dropNone
}

// payloadCounter 读 MessageRaw 的明文 counter（前 8 字节）。
// ok=false 表示不是原始 payload 或不足 8B；入口只对已过 ≥28B 长度闸的帧调用。
func payloadCounter(msg message.Message) (uint64, bool) {
	raw, ok := msg.(*message.MessageRaw)
	if !ok || len(raw.Payload) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(raw.Payload[:8]), true
}

// logIngressDrop 按入口丢弃原因打一条节流告警（原因取格式串，10s 同通道同原因一次）。
func (m *Manager) logIngressDrop(srcCh *gomavlib.Channel, did uint32, msgID uint32, plen int, reason ingressDropReason) {
	switch reason {
	case dropInvalidDirection:
		m.logDropThrottled(srcCh, "frame from channel with unknown endpoint role: %s; dropping", srcCh)
	case dropZeroDeviceID:
		m.logDropThrottled(srcCh, "dropped frame with deviceID=0: msgID=%d len=%d from %s",
			msgID, plen, srcCh)
	case dropMalformedStandby:
		m.logDropThrottled(srcCh, "dropped malformed standby frame: msgID=0 len=%d on vehicle endpoint "+
			"from %s (only 9B is plaintext standby)", plen, srcCh)
	case dropMalformedHeartbeat:
		m.logDropThrottled(srcCh, "dropped malformed heartbeat: msgID=0 len=%d (<%dB) on GCS endpoint from %s",
			plen, encryptedPayloadMinLen, srcCh)
	case dropShortFrame:
		m.logDropThrottled(srcCh, "dropped short encrypted frame: msgID=%d len=%d (<%dB) from %s",
			msgID, plen, encryptedPayloadMinLen, srcCh)
	default:
		m.logDropThrottled(srcCh, "dropped frame msgID=%d len=%d (deviceID=%d) from %s",
			msgID, plen, did, srcCh)
	}
}
