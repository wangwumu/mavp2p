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
	// handshaked 该 PX4 是否已切入加密阶段（收到过它的**加密** HEARTBEAT）。置位后
	// mavp2p 拦截 QGC 发往该 PX4 的加密 GCS 心跳（§2.5）。由加密下行心跳置位、由
	// 明文待命心跳复位——两态与心跳形态一一对应：明文待命心跳恒 9B、加密心跳恒
	// ≥ encryptedPayloadMinLen，中间没有别的取值，也不与其它 msgID 撞车。mavp2p
	// 不解密、拿不到角色信息，长度是它唯一可用且无歧义的信号。
	handshaked bool
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

// FrameSink 接收「已通过会话路由全部过滤」的**下行帧副本**（FIFO / 管理出口）。
//
// 契约：只喂**过滤后**的帧——即已经过边缘防重放、已命中 px4Map、已确认为下行的
// 帧，外加 PX4 的明文待命心跳（规范 §3.2.5：FIFO 内容＝原样加密帧＋明文待命心跳）。
// QGC 上行一律不得喂入，尤其是它的加密 GCS 心跳。禁令的依据是 §3.2.5 对 FIFO 内容
// 的定义（QGC 指令不属其中），**不是**下游的某个具体机制——这一点在 2026-09-28 之后
// 尤其要照字面读：那次云端 data_writer 遥测零入库，当时的原因是该侧还有单水位判重、
// 被 1Hz 上行心跳接管后把下行饿死；**那层判重已按 §2.6「data_writer 例外」删除**
// （用户裁定），所以今天再误喂不会再重现同一个症状，而禁令依然成立。
//
// 由 main.go 注入（改前 FIFO 曾是事件循环里的平级旁路，绕过了本管理器的全部
// 过滤）；nil 表示无下游。须在首次 ProcessFrame 之前设置。
type FrameSink interface {
	ProcessFrame(fr frame.Frame)
}

// Manager 是有状态会话路由器。
type Manager struct {
	Ctx context.Context
	Wg  *sync.WaitGroup
	Config
	Node *gomavlib.Node
	// Sink 过滤后下行帧的下游出口（FIFO / 管理出口）；nil 表示不接。
	Sink FrameSink

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

// prune 清除超过 MAP_TTL 未活跃的映射/在线/配对状态，并回收随之失去意义的空扇出告警
// 节流记录（见函数末那段注释）。
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
	// 空扇出告警节流表随 px4Map 一同回收。它的写入点在 px4Map 命中**之后**（downlink 分支
	// 先查 px4Map 才可能走到扇出为空那一支）⇒ 这张表记的全是曾经活跃过的 PX4，故
	// 「不在 px4Map 里」等价于「该 deviceID 的 PX4 已消失」，留着记录没有意义。删掉还更贴合
	// 节流表本意：该 deviceID 下次出现就是新会话，第一条告警应当立刻打。
	// ‼️ 必须排在上面 px4Map 的清理**之后** —— 顺序反了的话，本轮刚该消失的 deviceID 此刻
	// 还在 px4Map 里，它的节流记录就被漏掉，表照旧只增不减。
	for did := range m.emptyDownlinkLogged {
		if _, ok := m.px4Map[did]; !ok {
			delete(m.emptyDownlinkLogged, did)
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
		// PX4 明文待命心跳（§3.2.2 步骤 1/10）：登记映射 + 扇出在线 QGC；
		// 仅当该 deviceID 有下行水位（经历过加密会话，即任务结束）才清配对。
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
		// 回待命复位 handshaked（§2.5）：明文待命心跳 ⇒ 该 PX4 已退回未握手态
		// （PX4 侧 `_tx_last_nonce_set` 为 false，此后只发明文心跳），下一次建链时
		// QGC 的加密心跳必须能重新抵达它。只置位不复位的话，任务结束后新会话的第一条
		// 上行若恰是 GCS 心跳（QGC 建链后即 1Hz 发），会被永久拦死、链路再也建不起来
		// ——即用户 2026-09-27 指出的「上行心跳被拦则加密链路建立不起来」的原形。
		e.handshaked = false
	} else {
		m.px4Map[did] = &px4Entry{channel: srcCh, lastSeen: now}
		log.Printf("PX4 appeared: deviceID=%d channel=%s", did, srcCh)
	}
	// 回待命：清除该 PX4 的所有 QGC↔PX4 配对缓存（§3.2.2 步骤 10），回到可再次建链状态。
	// 前提条件：该 deviceID 的**下行** lastNonce 存在 ⇒ 本进程曾路由过它一帧加密下行，
	// 即该 PX4 已切入过加密阶段；「开机待命」不满足。
	// 「开机待命」与「任务结束回待命」的明文心跳形态完全相同（msgID=0 + 9B payload +
	// PX4 段 deviceID；空 dialect 不解码 payload，路由器无从区分），唯此水位能判别。
	// 若无条件清除，开机待命期间每个心跳周期（PX4 待命心跳默认 20Hz，§2.5）都会清掉
	// QGC 刚经 80005 登记的配对，使其无法存活到 PX4 切入加密下行。
	// 水位缺失 ≠ 未经历过会话：上行水位由「上行且 px4Map 命中」写入、下行水位由「下行且
	// px4Map 命中」写入，二者独立。水位缺失的全部情形只有两处——从未建链，或握手期第一条
	// 加密上行已被路由、PX4 第一条加密下行尚未到达；后者的配对正是刚经 80005 建立、必须
	// 保留到切入加密下行的那一条，故此时不清同样正确。⇒ 本判据对「该不该清」是充要的。
	// 前提：PX4 不允许崩溃重启（PX4 侧状态连续，不会凭空退回待命）；mavp2p/QGC 重启则
	// 两侧水位一并清空，也不落入该窗口。
	nkUp := nonceKey{deviceID: did, odd: true}
	nkDown := nonceKey{deviceID: did, odd: false}
	if _, hadDown := m.lastNonce[nkDown]; hadDown {
		for k := range m.pairs {
			if k.px4DeviceID == did {
				delete(m.pairs, k)
			}
		}
	}
	// 水位照清，与配对是两件事（§2.5：任务结束 PX4 软重置**两个方向**的 lastNonce 为
	// unset，奇/偶各一条；新任务 QGC 取随机 62 位奇数起点）。若 mavp2p 不清，新 QGC 随机
	// 起点若低于旧任务水位会被边缘判重误杀（≈50% 概率阻塞新任务上行，直到 counter 追平）。
	// 清后仅重开防重放窗口（协议 §2.5「重启边界」已接受的残余风险）。
	// ❗ 2026-09-28 起**下游不再兜底**：data_writer 已按 §2.6「data_writer 例外」删除判重
	// （用户裁定），本组件的边缘防重放是这条链路上唯一的判重。故下面这个窗口要照字面读。
	// 注：本清除无前置条件，故在握手窗口（见上方「水位缺失」第二种情形）会连带清掉刚写入的
	// 上行水位，使该会话第一条上行可被重放一次（实测：无该次心跳时重放被拦、有则放行）。
	// 下一条上行即重新写入水位，窗口为一条上行帧的间隔。
	delete(m.lastNonce, nkUp)
	delete(m.lastNonce, nkDown)
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
	// 下游副本（规范 §3.2.5：FIFO 内容含明文待命心跳）。落点在全部过滤之后：
	// 本函数只处理 PX4 段 deviceID 的 9B 明文心跳，QGC 的心跳到不了这里。
	if m.Sink != nil {
		m.Sink.ProcessFrame(fr)
	}
}

// processEncrypted 处理加密任务帧（§3.2.2 步骤 3/4/8）。
func (m *Manager) processEncrypted(srcCh *gomavlib.Channel, did uint32, fr frame.Frame) {
	// 边缘防重放（§3.1）：读 payload 明文 counter（前 8 字节），按 deviceID×方向 判重。
	// 尽力而为、不认证；端到端的权威判重在各**解密方**（PX4 / QGC 各自维护）。
	// ❗ data_writer 不在其中：它 2026-09-28 起按 §2.6「data_writer 例外」删除了判重
	// （用户裁定）⇒ 对 FIFO 这条消费路径，本函数就是唯一的一道。counter 为 0 或取不到则不判。
	// 注意：lastNonce 的写入延后到「确认本帧会被路由」之后——被拦截/忽略的帧不推进
	// 防重放水位，避免未登记 QGC 上行（被忽略）推进奇数水位、误杀后续合法上行。
	// （下行帧即便无配对接收者，也会因刷新状态而推进水位。）
	// plen 同时用于识别加密心跳（msgID=0 且 ≥ encryptedPayloadMinLen，§2.5）。
	msg := fr.GetMessage()
	msgID := msg.GetID()
	var counter uint64
	odd := false
	plen := -1
	if raw, ok := msg.(*message.MessageRaw); ok {
		plen = len(raw.Payload)
		if plen >= 8 {
			counter = binary.BigEndian.Uint64(raw.Payload[0:8])
			odd = counter&1 == 1 // 上行（QGC）奇数 / 下行（PX4/abc_vtol）偶数
		}
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
		// 加密心跳拦截（§2.5）：该 PX4 已握手（曾发出加密心跳 ⇒ 加密链路已建成）后，
		// 不再把 QGC 的加密 GCS 心跳转给它。本位置受两条约束夹定，都不可挪：必须在
		// 上方 QGC 在线表与配对的刷新**之后**（心跳 1Hz 是配对的主要保活源，拦在刷新前
		// 会让配对因 MAP_TTL 静默过期），又必须在下方 lastNonce 写入**之前**（被拦帧
		// 不推进防重放水位，§3.1；否则心跳的 counter 会把水位抬到普通上行之上，误杀
		// 后续合法上行）。不记日志：1Hz 常态拦截会刷屏，且拦截本身是设计行为。
		if msgID == msgIDHeartbeat && plen >= encryptedPayloadMinLen && px4.handshaked {
			m.mu.Unlock()
			return
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
	// 安全边界：本分支承担**两项**无密码学认证的状态变更（与待命心跳登记一样）——
	//   ① socketID 重定向（见下）：任何未登记为 QGC 的来源，发一帧**偶数 counter/0**、
	//      帧头 deviceID 命中已知 PX4 的帧即可触发；
	//   ② 置位「已建链标记」（见下方「握手完成置位」）：该帧若同时满足 `msgID=0 且
	//      plen >= encryptedPayloadMinLen`，即把该 PX4 置为已建链，**使其 QGC 加密心跳被拦**。
	// 两项的剩余触发面都只是部署边界内的注入——登记过期的合法 QGC 上行恒为奇数，已被
	// 上方 odd 判据拦截。两项风险同由 §2.1.1 部署网络边界（mavp2p 不落公网、内网/VPN/IP
	// 白名单）保障，协议层不为这些状态额外设计认证（§3.2.3「无认证路由状态的安全依赖」）。
	if px4.channel != srcCh {
		log.Printf("PX4 socketID drifted: deviceID=%d %s -> %s", did, px4.channel, srcCh)
		px4.channel = srcCh
	}
	now := time.Now()
	px4.lastSeen = now
	// 握手完成置位（§2.5）：PX4 发来**加密** HEARTBEAT ⇒ 它的加密链路已建成，此后
	// mavp2p 拦截 QGC 发往该 deviceID 的加密 GCS 心跳。判据只用 msgID 与长度、不解密；
	// 置位点必须在本分支 px4Map 命中之后（未命中在更上方已早退），否则未知 deviceID
	// 的加密帧也能置位。
	if msgID == msgIDHeartbeat && plen >= encryptedPayloadMinLen && !px4.handshaked {
		px4.handshaked = true
		log.Printf("PX4 handshake complete: deviceID=%d (%s), GCS heartbeats blocked", did, srcCh)
	}
	// 同步该 deviceID 所有配对的 PX4 侧 socketID（保持三元组一致）。PX4 重连换 socket 后，
	// 下行路由判据 `e.px4Ch == srcCh` 靠这里跟上——**这一行不能删**，删了连在飞的飞机
	// 也会因路由失配收不到帧。
	//
	// ‼️ 这里**不刷 lastSeen**（2026-10-05 改）。原实现刷，其注释自陈是为「QGC 保活中断
	// 但 PX4 持续下行时配对不因 MAP_TTL 静默过期」——但那让配对**永不过期**：飞机在飞
	// ⇒ PX4 持续下行 ⇒ 每帧刷活 ⇒ `now.Sub(e.lastSeen) >= ttl` 永不成立，于是 MAP_TTL
	// 无论设 60s 还是 30s 都结构性失效，QGC 签出后仍无限期收帧（终端每帧一条
	// `no key for device … dropping encrypted frame`）。
	// 删掉后配对的保活只剩 QGC 侧两源——80005 周期登记与 1Hz 加密上行心跳；
	// 二者在飞时都在、签出后都不在，这正是「签出 ⇒ 配对按 MAP_TTL 过期」所需的语义。
	for k, e := range m.pairs {
		if k.px4DeviceID == did {
			e.px4Ch = srcCh
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
	// 下游副本：落点在全部过滤之后（防重放已判、px4Map 已命中、方向已确认为下行）；
	// QGC 上行在此之前的分支已各自 return，到不了这里。
	if m.Sink != nil {
		m.Sink.ProcessFrame(fr)
	}
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
