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
	// 空扇出告警节流：deviceID → 上次「PX4 加密下行但无配对 QGC」告警时间。
	// PX4 持续下行而 QGC 配对已过期时会高频触发，需节流避免日志刷屏。
	emptyDownlinkLogged map[uint32]time.Time
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
	m.emptyDownlinkLogged = make(map[uint32]time.Time)

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
// lastNonce 不在此清理（每 deviceID×方向仅一条，规模有限；由 PX4 回待命心跳按
// §2.5 软重置清除，见 processStandbyHeartbeat）。
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
			log.Printf("pair expired: QGC %s <-> PX4 deviceID=%d", k.qgcCh, k.px4DeviceID)
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
		// PX4 明文待命心跳（§3.2.2 步骤 1/10）：登记映射 + 清配对 + 扇出在线 QGC。
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
		log.Printf("dropped malformed 80005 registration deviceID=%d from %s (payload < 1 byte)", did, srcCh)
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

	// 按 payload 的 PX4 deviceID 集合刷新配对表 QGC 侧（§3.2.4.1 重启恢复/重定位）
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

// processStandbyHeartbeat 处理 PX4 明文待命心跳（§3.2.2 步骤 1/10）。
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
	// 回待命：清除该 PX4 的所有 QGC↔PX4 配对缓存（§3.2.2 步骤 10），回到可再次建链状态
	for k := range m.pairs {
		if k.px4DeviceID == did {
			delete(m.pairs, k)
		}
	}
	// 同步清边缘防重放水位（§2.5：任务结束 PX4 软重置全局 lastNonce 为 unset；新任务
	// QGC 取随机 62 位奇数起点）。若 mavp2p 不清，新 QGC 随机起点若低于旧任务水位会被
	// 边缘判重误杀（≈50% 概率阻塞新任务上行，直到 counter 追平）。清后仅重开防重放
	// 窗口（协议 §2.5「重启边界」已接受的残余风险，权威防重放在解密层）。
	delete(m.lastNonce, nonceKey{deviceID: did, odd: true})
	delete(m.lastNonce, nonceKey{deviceID: did, odd: false})
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

// processEncrypted 处理加密任务帧（§3.2.2 步骤 3/4/8）。
func (m *Manager) processEncrypted(srcCh *gomavlib.Channel, did uint32, fr frame.Frame) {
	// 边缘防重放（§3.1）：读 payload 明文 counter（前 8 字节），按 deviceID×方向 判重。
	// 尽力而为、不认证；权威防重放在各解密方。counter 为 0 或取不到则不判。
	// 注意：lastNonce 的写入延后到「确认本帧会被路由」之后——被拦截/忽略的帧不推进
	// 防重放水位，避免未登记 QGC 上行（被忽略）推进奇数水位、误杀后续合法上行。
	// （下行帧即便无配对接收者，也会因刷新状态而推进水位。）
	var counter uint64
	odd := false
	if raw, ok := fr.GetMessage().(*message.MessageRaw); ok && len(raw.Payload) >= 8 {
		counter = binary.BigEndian.Uint64(raw.Payload[0:8])
		odd = counter&1 == 1 // 上行（QGC）奇数 / 下行（PX4/abc_vtol）偶数
	}

	m.mu.Lock()

	// 防重放判定（读，不写）：同方向同 deviceID 的 counter 不递增则丢弃。
	var nk *nonceKey
	if counter != 0 {
		k := nonceKey{deviceID: did, odd: odd}
		if last, ok := m.lastNonce[k]; ok && counter <= last {
			m.mu.Unlock()
			log.Printf("dropped replayed frame deviceID=%d %s counter=%d from %s", did, dirName(odd), counter, srcCh)
			return
		}
		nk = &k
	}

	// 方向判定（§3.2.3）：来源 socketID ∈ QGC 在线表 → 上行；
	// 否则 → 若 counter 为偶数/0 按 PX4 下行处理（§3.2.2 步骤 8 放宽下行判据：漂移时
	// 来源 socketID 可能与映射表记录不一致）；奇数 counter 的未登记上行先被下方 odd
	// 判据拦截（见下）。加密帧帧头 deviceID 恒为该链路 PX4 的 D——对下行帧而言它同时
	// 是发送方自身（§3.2.1 核心规则）；不得仅凭号段判方向，也不得把「来源 == PX4 映射
	// socketID」作为判下行的前提（漂移时可能不等）。
	if _, isQGC := m.qgcOnline[srcCh]; isQGC {
		// ---- 上行（QGC → PX4）：帧头 deviceID = 目标 PX4 的 D（§3.2.3）----
		// 活跃加密上行维持 QGC 在线身份（§3.2.3 保活约束：QGC 在线表靠登记/保活维护，
		// 避免活跃会话被 MAP_TTL 误清理后再落入非 QGC 分支）；先刷新，使「上行→未知
		// PX4」路径也维持在线身份
		if qe, ok := m.qgcOnline[srcCh]; ok {
			qe.lastSeen = time.Now()
		}
		px4, ok := m.px4Map[did]
		if !ok {
			// 目标 PX4 未上线（无映射缓存）→ 忽略，等 PX4 待命心跳 + QGC 80005
			// 重建配对（§3.2.4.1，幂等、顺序无关）
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
		if nk != nil {
			m.lastNonce[*nk] = counter
		}
		target := px4.channel // 锁内快照，锁外转发避免锁后读竞争
		m.mu.Unlock()

		// 定向转发给目标 PX4
		if target != srcCh {
			m.Node.WriteFrameTo(target, fr) //nolint:errcheck
		}
		return
	}

	// 非 QGC 来源 + 奇数 counter = 上行特征：未登记/登记过期的 QGC 加密上行帧
	// （帧头 deviceID = 目标 PX4 的 D）。不得按下行处理改写 PX4 映射表——否则上行命令
	// 被重定向、可能跨 QGC 泄露。忽略，等该 QGC 补发 80005（§3.2.4.1 幂等重建）。
	if odd {
		m.mu.Unlock()
		log.Printf("received uplink from unregistered channel %s for PX4 deviceID=%d, ignoring", srcCh, did)
		return
	}

	// ---- 下行（PX4 → QGC）：帧头 deviceID = 发送方自身，counter 恒为偶数/0 ----
	px4, ok := m.px4Map[did]
	if !ok {
		// 该 deviceID 无映射缓存 → 非本链路已知 PX4 的帧，丢弃
		m.mu.Unlock()
		log.Printf("received encrypted frame for unknown PX4 deviceID=%d %s from %s, discarding", did, dirName(odd), srcCh)
		return
	}
	// socketID 漂移刷新（§3.2.1 核心规则 / §3.2.2 步骤 8 / §3.2.3）：PX4 经 5G 动态地址
	// NAT 重建后 socketID 会变，每次收到 PX4 帧都比对来源 socketID，变化即以 deviceID
	// 定位原表项并更新，否则后续定向转发全部落空。
	// 安全边界：本刷新无密码学认证（与待命心跳登记一样）——任何未登记为 QGC 的来源
	// 发送一帧**偶数 counter/0**、帧头 deviceID 命中已知 PX4 的帧即可触发 socketID
	// 重定向（登记过期的合法 QGC 上行恒为奇数，已被上方 odd 判据拦截；剩余触发面即
	// 部署边界内的注入）。该风险由 §2.1.1 部署网络边界（mavp2p 不落公网、内网/VPN/IP
	// 白名单）保障，协议层不为这些状态额外设计认证（§3.2.3「无认证路由状态的安全依赖」）。
	if px4.channel != srcCh {
		log.Printf("PX4 socketID drifted: deviceID=%d %s -> %s", did, px4.channel, srcCh)
		px4.channel = srcCh
	}
	now := time.Now()
	px4.lastSeen = now
	// 刷新该 deviceID 所有配对的 PX4 侧 socketID 与活跃时间，保持三元组一致；
	// 刷新 lastSeen 使配对保活不只依赖 QGC 80005/上行——QGC 保活中断但 PX4 持续下行时
	// 配对不会因 MAP_TTL 静默过期（§3.2.3 保活约束）
	for k, e := range m.pairs {
		if k.px4DeviceID == did {
			e.px4Ch = srcCh
			e.lastSeen = now
		}
	}
	// 下行路由：只发给配对的任务 QGC（不扇出），§3.2.2 步骤 4
	qgcs := make([]*gomavlib.Channel, 0)
	for k, e := range m.pairs {
		if k.px4DeviceID == did && e.px4Ch == srcCh {
			qgcs = append(qgcs, k.qgcCh)
		}
	}
	if nk != nil {
		m.lastNonce[*nk] = counter
	}
	// 空扇出：PX4 加密下行但无配对任务 QGC（典型：QGC 配对被 MAP_TTL 剪除而 PX4 仍
	// 持续下行）。节流打日志（每 deviceID 10s 一次），避免高频遥测下刷屏。
	if len(qgcs) == 0 {
		if last, ok := m.emptyDownlinkLogged[did]; !ok || now.Sub(last) >= 10*time.Second {
			m.emptyDownlinkLogged[did] = now
			log.Printf("dropped downlink from PX4 deviceID=%d (%s): no paired QGC", did, srcCh)
		}
	}
	m.mu.Unlock()

	for _, qgc := range qgcs {
		if qgc != srcCh {
			m.Node.WriteFrameTo(qgc, fr) //nolint:errcheck
		}
	}
	// FIFO 副本由 fifoFilter（主循环）按 msgID 白名单独立处理
}

// ProcessChannelClose 处理 EventChannelClose：清理与该 channel 关联的全部状态。
// 注意：此处不清 lastNonce——PX4 断线重连（NAT 重建）counter 连续，清掉会重开防重放
// 窗口；PX4 整机重启会发回待命心跳，由 processStandbyHeartbeat 按 §2.5 清 lastNonce。
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
