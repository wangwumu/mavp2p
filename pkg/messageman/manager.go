// Package messageman contains the message manager.
// 有状态会话路由器（协议 10_deviceID与payload加密公共规范.md §3.2）：
// 按帧头 deviceID 号段 + 来源 socketID（channel）+ msgID/payload 长度路由，
// 不解密、不认证，把加密任务帧按原样透传。
package messageman

import (
	"context"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
)

// ---- 协议常量（附录 A.1，见 10_deviceID与payload加密公共规范.md）----
const (
	// GCS 段上界：deviceID < 该值 判为 QGC/地面站；≥ 该值 判为 PX4。
	DefaultGCSDeviceIDMax = 10000000
	// QGC 登记/保活心跳帧头统一使用的 GCS 段固定 deviceID。
	DefaultGCSDeviceID = 10000
	// 单 QGC 最多关联的 PX4 数量上限。
	DefaultMaxQGCLinkedPX4 = 16
	// 映射/在线/配对缓存 TTL（MAP_TTL）。
	DefaultMapTTL = 60 * time.Second

	// 加密 payload block 最小长度 = counter(8) + deviceID(4) + tag(16)（§2.3）。
	// msgID=0 且 payload 长度 < 该值是明文待命心跳，否则是加密帧。
	encryptedPayloadMinLen = 28
	msgIDHeartbeat         = 0
	msgIDQGCRegistration   = 80005
	msgIDRequestDataStream = 66
)

// Config 会话路由器配置（附录 A.1）。零值字段使用默认值。
type Config struct {
	// 是否禁用 RequestDataStream 拦截（上游功能，默认拦截）。
	StreamReqDisable bool
	// GCS 段上界（默认 DefaultGCSDeviceIDMax）。
	GCSDeviceIDMax uint32
	// 单 QGC 关联 PX4 上限（默认 DefaultMaxQGCLinkedPX4）。
	MaxQGCLinkedPX4 int
	// 状态表 TTL（默认 DefaultMapTTL）。
	MapTTL time.Duration
}

// px4Entry PX4 映射表项：deviceID → 当前可达 socketID（channel）。
type px4Entry struct {
	channel  *gomavlib.Channel
	lastSeen time.Time
}

// qgcEntry QGC 在线表项：socketID（channel）→ 最近登记心跳时间。
type qgcEntry struct {
	lastSeen time.Time
}

// pairKey 配对表键：QGC socketID × PX4 deviceID。
type pairKey struct {
	qgcCh       *gomavlib.Channel
	px4DeviceID uint32
}

// pairEntry 配对表项：三元组 (QGC socketID, PX4 deviceID, PX4 socketID)。
type pairEntry struct {
	px4Ch    *gomavlib.Channel
	lastSeen time.Time
}

// nonceKey 边缘防重放键：deviceID × 方向（上行奇数 / 下行偶数）。
type nonceKey struct {
	deviceID uint32
	odd      bool
}

// Manager 是有状态会话路由器。
type Manager struct {
	Ctx context.Context
	Wg  *sync.WaitGroup
	Config
	Node *gomavlib.Node

	mu        sync.Mutex
	px4Map    map[uint32]*px4Entry            // PX4 映射表（deviceID → socketID）
	qgcOnline map[*gomavlib.Channel]*qgcEntry // QGC 在线表（socketID → 心跳时间）
	pairs     map[pairKey]*pairEntry          // 配对表（三元组）
	lastNonce map[nonceKey]uint64             // 边缘防重放（deviceID × 方向）
}

// gcsDeviceIDMax 返回配置的号段分界（0=默认）。
func (m *Manager) gcsDeviceIDMax() uint32 {
	if m.GCSDeviceIDMax != 0 {
		return m.GCSDeviceIDMax
	}
	return DefaultGCSDeviceIDMax
}

// maxQGCLinkedPX4 返回单 QGC 关联 PX4 上限（0=默认）。
func (m *Manager) maxQGCLinkedPX4() int {
	if m.MaxQGCLinkedPX4 != 0 {
		return m.MaxQGCLinkedPX4
	}
	return DefaultMaxQGCLinkedPX4
}

// mapTTL 返回状态表 TTL（0=默认）。
func (m *Manager) mapTTL() time.Duration {
	if m.MapTTL != 0 {
		return m.MapTTL
	}
	return DefaultMapTTL
}

// Initialize 初始化 Manager。
func (m *Manager) Initialize() error {
	m.px4Map = make(map[uint32]*px4Entry)
	m.qgcOnline = make(map[*gomavlib.Channel]*qgcEntry)
	m.pairs = make(map[pairKey]*pairEntry)
	m.lastNonce = make(map[nonceKey]uint64)

	m.Wg.Add(1)
	go m.run()

	return nil
}

// run 周期清理超过 MAP_TTL 未活跃的状态。
func (m *Manager) run() {
	defer m.Wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.prune()
		case <-m.Ctx.Done():
			return
		}
	}
}

// prune 清除超过 MAP_TTL 未活跃的映射/在线/配对状态。
// lastNonce 不在此清理（每 deviceID×方向仅一条，规模有限；重启后从 unset 重开）。
func (m *Manager) prune() {
	now := time.Now()
	ttl := m.mapTTL()

	m.mu.Lock()
	defer m.mu.Unlock()

	for did, e := range m.px4Map {
		if now.Sub(e.lastSeen) >= ttl {
			log.Printf("PX4 disappeared: deviceID=%d", did)
			delete(m.px4Map, did)
		}
	}
	for ch, e := range m.qgcOnline {
		if now.Sub(e.lastSeen) >= ttl {
			log.Printf("QGC gone: %s", ch)
			delete(m.qgcOnline, ch)
		}
	}
	for k, e := range m.pairs {
		if now.Sub(e.lastSeen) >= ttl {
			delete(m.pairs, k)
		}
	}
}

// frameDeviceID 从帧头 4 字节重组 32 位 deviceID（§1.2 编码公式）。
// V2 帧：incompat<<24 | compat<<16 | sysid<<8 | compid。
func frameDeviceID(fr frame.Frame) uint32 {
	if v2, ok := fr.(*frame.V2Frame); ok {
		return uint32(v2.IncompatibilityFlag)<<24 |
			uint32(v2.CompatibilityFlag)<<16 |
			uint32(v2.SystemID)<<8 |
			uint32(v2.ComponentID)
	}
	// V1 帧无 incompat/compat，deviceID 仅低 16 位（sysid<<8 | compid）
	return uint32(fr.GetSystemID())<<8 | uint32(fr.GetComponentID())
}

// rawPayloadLen 返回原始 payload 长度；非 MessageRaw（已被 dialect 解码）返回 -1。
// dialect 为空时所有消息保持 MessageRaw，故实际恒返回真实长度。
func rawPayloadLen(msg message.Message) int {
	if raw, ok := msg.(*message.MessageRaw); ok {
		return len(raw.Payload)
	}
	return -1
}

// ProcessFrame 处理一个 EventFrame。
func (m *Manager) ProcessFrame(evt *gomavlib.EventFrame) {
	fr := evt.Frame
	srcCh := evt.Channel
	msg := evt.Message()
	msgID := msg.GetID()
	did := frameDeviceID(fr)
	plen := rawPayloadLen(msg)

	// 明文 RequestDataStream 拦截（上游功能保留）：仅明文（payload<28）拦截，
	// 加密帧（payload 含 counter/密文）不拦——由会话路由透传。
	if !m.StreamReqDisable && msgID == msgIDRequestDataStream &&
		(plen < 0 || plen < encryptedPayloadMinLen) {
		return
	}

	switch {
	case msgID == msgIDQGCRegistration:
		// QGC 明文登记/保活心跳（§3.2 步骤 2）：仅 mavp2p 消费，不转发。
		m.processRegistration(srcCh, did, msg)

	case msgID == msgIDHeartbeat && did >= m.gcsDeviceIDMax() &&
		plen >= 0 && plen < encryptedPayloadMinLen:
		// PX4 明文待命心跳（§3.2 步骤 1/10）：登记映射 + 清配对 + 扇出在线 QGC。
		m.processStandbyHeartbeat(srcCh, did, fr)

	default:
		// 加密任务帧（含加密 HEARTBEAT，msgID=0 且 payload≥28）：按会话路由定向。
		m.processEncrypted(srcCh, did, fr)
	}
}

// processRegistration 处理 80005 明文登记/保活心跳（§3.2.1 QGC 在线表 + 配对表）。
func (m *Manager) processRegistration(srcCh *gomavlib.Channel, did uint32, msg message.Message) {
	// 帧头 deviceID 必须在 GCS 段（§2.2：QGC 登记心跳用 GCS 段固定值）
	if did >= m.gcsDeviceIDMax() {
		log.Printf("dropped 80005 registration with non-GCS deviceID %d", did)
		return
	}

	raw, ok := msg.(*message.MessageRaw)
	if !ok || len(raw.Payload) < 1 {
		return
	}
	payload := raw.Payload
	num := int(payload[0])
	if num > m.maxQGCLinkedPX4() {
		num = m.maxQGCLinkedPX4()
	}
	if len(payload) < 1+num*4 {
		num = (len(payload) - 1) / 4 // 防畸形：按实际长度截断
	}

	now := time.Now()
	m.mu.Lock()
	// 核心规则：按来源方（QGC）刷新 QGC 在线表（键 = 来源 socketID）
	m.qgcOnline[srcCh] = &qgcEntry{lastSeen: now}

	// 按 payload 的 PX4 deviceID 集合刷新配对表 QGC 侧（§3.2.4 重启恢复/重定位）
	for i := 0; i < num; i++ {
		d := binary.BigEndian.Uint32(payload[1+i*4 : 5+i*4])
		k := pairKey{qgcCh: srcCh, px4DeviceID: d}
		e, ok := m.pairs[k]
		if !ok {
			e = &pairEntry{lastSeen: now}
			m.pairs[k] = e
			log.Printf("QGC %s registered: linked PX4 deviceID=%d", srcCh, d)
		} else {
			e.lastSeen = now
		}
		if px4, ok := m.px4Map[d]; ok {
			e.px4Ch = px4.channel // PX4 已在线 → 补全三元组
		}
	}
	m.mu.Unlock()

	// 80005 不转发给 PX4（§2.2/§3.2：仅 mavp2p 消费）
}

// processStandbyHeartbeat 处理 PX4 明文待命心跳（§3.2 步骤 1/10）。
func (m *Manager) processStandbyHeartbeat(srcCh *gomavlib.Channel, did uint32, fr frame.Frame) {
	now := time.Now()

	m.mu.Lock()
	// 核心规则：按来源方（PX4）刷新 PX4 映射表（deviceID → socketID）
	if e, ok := m.px4Map[did]; ok {
		if e.channel != srcCh {
			log.Printf("PX4 socketID drifted: deviceID=%d %s -> %s", did, e.channel, srcCh)
		}
		e.channel = srcCh
		e.lastSeen = now
	} else {
		m.px4Map[did] = &px4Entry{channel: srcCh, lastSeen: now}
		log.Printf("PX4 appeared: deviceID=%d channel=%s", did, srcCh)
	}
	// 回待命：清除该 PX4 的所有 QGC↔PX4 配对缓存（§3.2 步骤 10），回到可再次建链状态
	for k := range m.pairs {
		if k.px4DeviceID == did {
			delete(m.pairs, k)
		}
	}
	// 锁内收集在线 QGC 快照
	qgcs := make([]*gomavlib.Channel, 0, len(m.qgcOnline))
	for ch := range m.qgcOnline {
		qgcs = append(qgcs, ch)
	}
	m.mu.Unlock()

	// 扇出给所有在线 QGC（P2P 单发，非 IP 广播）；绝不转给其他 PX4
	for _, ch := range qgcs {
		if ch != srcCh {
			m.Node.WriteFrameTo(ch, fr) //nolint:errcheck
		}
	}
	// FIFO 副本由 fifoFilter（主循环）按 msgID 白名单独立处理
}

// processEncrypted 处理加密任务帧（§3.2 步骤 3/4/8）。
func (m *Manager) processEncrypted(srcCh *gomavlib.Channel, did uint32, fr frame.Frame) {
	// 边缘防重放（§3.1）：读 payload 明文 counter（前 8 字节），按 deviceID×方向 判重。
	// 尽力而为、不认证；权威防重放在各解密方。counter 为 0 或取不到则不判。
	var counter uint64
	odd := false
	if raw, ok := fr.GetMessage().(*message.MessageRaw); ok && len(raw.Payload) >= 8 {
		counter = binary.BigEndian.Uint64(raw.Payload[0:8])
		odd = counter&1 == 1 // 上行（QGC）奇数 / 下行（PX4/abc_vtol）偶数
	}

	m.mu.Lock()
	if counter != 0 {
		k := nonceKey{deviceID: did, odd: odd}
		if last, ok := m.lastNonce[k]; ok && counter <= last {
			m.mu.Unlock()
			log.Printf("dropped replayed frame deviceID=%d %s counter=%d", did, dirName(odd), counter)
			return
		}
		m.lastNonce[k] = counter
	}

	// 方向判定：来源 socketID ∈ QGC 在线表 → 上行；== PX4 映射表 socketID → 下行
	// （§3.2.3：加密帧帧头 deviceID 恒为该链路 PX4 的 D，不得仅凭号段判方向）
	_, isQGC := m.qgcOnline[srcCh]
	px4, isPX4 := m.px4Map[did]
	if !isPX4 || px4.channel != srcCh {
		isPX4 = false
	}

	if isQGC {
		// ---- 上行（QGC → PX4）：帧头 deviceID = 目标 PX4 的 D（§3.2.3）----
		if px4 == nil {
			// 目标 PX4 未上线（无映射缓存）→ 忽略，等 PX4 待命心跳 + QGC 80005
			// 重建配对（§3.2.4，幂等、顺序无关）
			m.mu.Unlock()
			log.Printf("received uplink for unknown PX4 deviceID=%d from %s, ignoring", did, srcCh)
			return
		}
		// 刷新/新建配对 (QGC socketID, PX4 deviceID, PX4 socketID)；
		// 按来源 socketID 建键即天然处理 QGC 侧 socketID 漂移（新 channel → 新键）
		pk := pairKey{qgcCh: srcCh, px4DeviceID: did}
		e, ok := m.pairs[pk]
		if !ok {
			m.pairs[pk] = &pairEntry{px4Ch: px4.channel, lastSeen: time.Now()}
			log.Printf("link established: QGC %s <-> PX4 deviceID=%d (%s)", srcCh, did, px4.channel)
		} else {
			e.px4Ch = px4.channel
			e.lastSeen = time.Now()
		}
		m.mu.Unlock()

		// 定向转发给目标 PX4
		if px4.channel != srcCh {
			m.Node.WriteFrameTo(px4.channel, fr) //nolint:errcheck
		}
		return
	}

	if isPX4 {
		// ---- 下行（PX4 → QGC）：只发给配对的任务 QGC（不扇出），§3.2 步骤 4 ----
		qgcs := make([]*gomavlib.Channel, 0)
		for k, e := range m.pairs {
			if k.px4DeviceID == did && e.px4Ch == srcCh {
				qgcs = append(qgcs, k.qgcCh)
			}
		}
		m.mu.Unlock()

		for _, qgc := range qgcs {
			if qgc != srcCh {
				m.Node.WriteFrameTo(qgc, fr) //nolint:errcheck
			}
		}
		// FIFO 副本由 fifoFilter（主循环）按 msgID 白名单独立处理
		return
	}

	// 来源 socketID 既不在 QGC 在线表、也不是该 deviceID 的 PX4 socketID：
	// 无法判定方向（未登记的来源）。保守丢弃（§3.2.1 核心规则：不得臆测）。
	m.mu.Unlock()
	log.Printf("received encrypted frame deviceID=%d from unknown channel %s, discarding", did, srcCh)
}

// ProcessChannelClose 处理 EventChannelClose：清理与该 channel 关联的全部状态。
func (m *Manager) ProcessChannelClose(evt *gomavlib.EventChannelClose) {
	ch := evt.Channel

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.qgcOnline[ch]; ok {
		delete(m.qgcOnline, ch)
		log.Printf("QGC gone: %s", ch)
	}
	for did, e := range m.px4Map {
		if e.channel == ch {
			log.Printf("PX4 disconnected: deviceID=%d", did)
			delete(m.px4Map, did)
		}
	}
	for k := range m.pairs {
		if k.qgcCh == ch || m.pairs[k].px4Ch == ch {
			delete(m.pairs, k)
		}
	}
}

// dirName 返回方向描述（日志用）。
func dirName(odd bool) string {
	if odd {
		return "uplink(odd)"
	}
	return "downlink(even)"
}
