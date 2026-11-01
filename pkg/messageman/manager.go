// Package messageman contains the message manager.
// 有状态会话路由器（协议 10_deviceID与payload加密公共规范.md §3.2）：
// 按帧头 deviceID 号段 + **入口端点角色（方向）** + msgID/payload 长度路由。
// 来源 channel 只用作登记与配对表的键，不参与方向判定；counter 只做防重放比较。
// 不解密、不认证，把加密任务帧按原样透传。
package messageman

import (
	"context"
	"encoding/binary"
	"fmt"
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
	// 长期状态表（配对/防重放水位/诊断表）的空闲回收阈值。
	// 与 MAP_TTL 分工不同：MAP_TTL 管**路由存活**（秒级，靠 80005/PX4 心跳续命），
	// 本值管**卫生**（分钟级），判据是「多久没有加密流量」。⚠️ 被 §2.5 拦下的 GCS 心跳
	// **算**流量（发送端仍活跃，只是没被转发，见 pairEntry.lastTraffic）；80005 周期登记不算。
	DefaultStateIdleTimeout = 10 * time.Minute
	// 待命心跳防伪造闸的时间窗：该 deviceID 最近一次**加密下行**在此窗口内，则来自
	// 其它通道的明文待命心跳被丢弃（明文心跳无认证，见 processStandbyHeartbeat）。
	// 窗口越短，PX4 中途重启换端口后的恢复越快，但旧水位的存活时间也越短；
	// 5 分钟与 StateIdleTimeout 同量级，远大于任何一次正常任务间隔。
	DefaultStandbyGuard = 5 * time.Minute

	// 加密 payload block 最小长度 = counter(8) + deviceID(4) + tag(16)（§2.3）。
	// msgID=0 且 payload 长度 < 该值是明文待命心跳，否则是加密帧。
	encryptedPayloadMinLen = 28
	msgIDHeartbeat         = 0
	msgIDQGCRegistration   = 80005
	msgIDRequestDataStream = 66

	// 每设备候选发布（扇出/Sink）聚合窗口；< QGC 3500ms 判活（03 §4.5.5）。
	DefaultStandbyFanoutInterval = 2 * time.Second
	// 每设备候选上限（含至多 1 条保护候选）。
	DefaultMaxCandidatesPerDevice = 3
	// 候选表全局总条数上限。
	DefaultMaxCandidatesGlobal = 4096
	// 每设备新增来源 IP 的最小间隔。
	DefaultCandidateAdmitInterval = time.Second
	// 全局新候选准入速率（单位：一次「新来源 IP 入表」/s）。
	DefaultGlobalCandidateAdmitRate = 50
	// 全局候选发布预算（单位：一次聚合发布/s；任务结束不受限）。
	DefaultGlobalFanoutRate = 100
	// 上行目标集上限（含已验签通道）；必须 ≥2。
	DefaultMaxUplinkTargets = 3
	// 本地验签 Unix socket 与单请求超时。
	DefaultVerifySocket  = "/run/uavm/verify.sock"
	DefaultVerifyTimeout = 5 * time.Millisecond
	// 验签队列：每设备 in-flight=1，另可排队 8；全局排队 256。
	DefaultVerifyQueuePerDevice = 8
	DefaultVerifyQueueGlobal    = 256
)

// Config 会话路由器配置（附录 A.1）。除入口端点外，零值字段使用默认值；
// 入口端点是**必填**的（零值 Config 非法），因为它们就是方向判据。
type Config struct {
	// 是否禁用 RequestDataStream 拦截（上游功能，默认拦截）。
	StreamReqDisable bool
	// GCS 段上界（默认 DefaultGCSDeviceIDMax）。
	GCSDeviceIDMax uint32
	// 单 QGC 关联 PX4 上限（默认 DefaultMaxQGCLinkedPX4）。
	MaxQGCLinkedPX4 int
	// 状态表 TTL（默认 DefaultMapTTL）。
	MapTTL time.Duration
	// 长期状态表空闲回收阈值（默认 DefaultStateIdleTimeout）。
	StateIdleTimeout time.Duration
	// 待命心跳防伪造闸的时间窗（默认 DefaultStandbyGuard）。
	// 近期有加密下行的 deviceID，来自其它通道的明文待命心跳会被整帧丢弃 —— 明文心跳
	// 无认证，否则一帧即可改写映射、清配对/水位并复位 handshaked。
	StandbyGuard time.Duration
	// 每设备候选发布聚合窗口（默认 DefaultStandbyFanoutInterval）。
	StandbyFanoutInterval time.Duration
	// 每设备候选上限（默认 DefaultMaxCandidatesPerDevice）。
	MaxCandidatesPerDevice int
	// 候选表全局上限（默认 DefaultMaxCandidatesGlobal）。
	MaxCandidatesGlobal int
	// 每设备新增来源 IP 的最小间隔（默认 DefaultCandidateAdmitInterval）。
	CandidateAdmitInterval time.Duration
	// 全局新候选准入速率，单位 次/s（默认 DefaultGlobalCandidateAdmitRate）。
	GlobalCandidateAdmitRate int
	// 全局候选发布预算，单位 次聚合发布/s（默认 DefaultGlobalFanoutRate）。
	GlobalFanoutRate int
	// 上行目标集上限（默认 DefaultMaxUplinkTargets）；显式值必须 ≥2。
	MaxUplinkTargets int
	// 本地验签服务 socket（默认 DefaultVerifySocket）。
	VerifySocket string
	// 单次验签超时（默认 DefaultVerifyTimeout）；超时 fail-closed。
	VerifyTimeout time.Duration
	// 每设备验签排队上限（默认 DefaultVerifyQueuePerDevice）；in-flight 恒为 1。
	VerifyQueuePerDevice int
	// 全局验签排队上限（默认 DefaultVerifyQueueGlobal）。
	VerifyQueueGlobal int
	// GCSEndpoint 地面站入口端点（QGC→mavp2p 的上行），**恰好一个**。
	// ‼️ 方向由**入口端点**决定：本端点进来的帧一律按上行处理，
	// VehicleEndpoints 进来的帧一律按下行处理。不读 counter 奇偶，
	// 也不再用 qgcOnline 的成员资格判方向。
	// ‼️ 必须是传给 gomavlib.Node.Endpoints 的**同一个对象**（接口值相等）：
	// roleOf 按身份判定来源，另建一个地址相同的新端点会让所有帧判成「无角色」。
	// Initialize 会与 Node.Endpoints 交叉校验，不一致即拒绝启动。
	GCSEndpoint gomavlib.Endpoint
	// VehicleEndpoints 载具入口端点（PX4→mavp2p 的下行），至少一个。
	// 同一批对象、同一身份要求同 GCSEndpoint。
	VehicleEndpoints []gomavlib.Endpoint
}

// role 是通道的入口角色，由它所属的端点决定。
type role uint8

const (
	roleUnknown role = iota
	roleGCS
	roleVehicle
)

// direction 是帧在入口处确定的传输方向，只有两个合法值。
// ‼️ 零值 dirInvalid 表示「无法判定」（入口角色未知），不得进入路由函数；
// processEncrypted 在入口显式拒绝它，避免第三个值落进某个分支。
type direction uint8

const (
	dirInvalid direction = iota
	dirUplink
	dirDownlink
)

func (d direction) String() string {
	switch d {
	case dirUplink:
		return "uplink"
	case dirDownlink:
		return "downlink"
	default:
		return "invalid"
	}
}

// px4Entry PX4 映射表项：deviceID → 当前可达 socketID（channel）。
type px4Entry struct {
	channel  *gomavlib.Channel
	lastSeen time.Time
	// handshaked 该 PX4 是否已切入加密阶段（收到过它的**加密** HEARTBEAT）。置位后
	// mavp2p 拦截 QGC 发往该 PX4 的加密 GCS 心跳（§2.5），并且它是 D13 免验签的开关。
	// 定稿（03 §4.3.1）：只由「验签通过 ∧ msgID=0 ∧ payload≥T」置位；随 px4Map 条目
	// 删除而消失（任务结束/通道关闭/MapTTL/重启），不再由明文待命心跳复位。
	handshaked bool
}

// qgcEntry QGC 在线表项：socketID（channel）→ 最近保活时间。
// 两个来源都刷新：80005 登记心跳（周期 10s），以及该 QGC 的**加密上行**帧（§3.2.3
// 保活约束）——后者让一个正在活跃会话里的 QGC 不会因为登记抖动被 MAP_TTL 误清。
type qgcEntry struct {
	lastSeen time.Time
}

// pairKey 配对表键：QGC socketID × PX4 deviceID。
type pairKey struct {
	qgcCh       *gomavlib.Channel
	px4DeviceID uint32
}

// pairEntry 配对表项：三元组 (QGC socketID, PX4 deviceID, PX4 socketID)。
//
// 三个时间戳分工不同，**别合并**（2026-10-06 改；完整演进时间线见
// pair_ttl_internal_test.go 的 TestPX4DownlinkKeepsPairButNotFanout）：
//
//   - lastSeen：配对的**存活**。QGC 侧两源（80005 登记、加密上行心跳）与 **PX4 加密下行**
//     都刷新它。PX4 在飞就一直刷 ⇒ 配对常驻，不再有「QGC 侧一时安静 ⇒ 配对被 prune ⇒ 等
//     下一次 80005 登记（周期 10s）才重建」的空窗 —— 那段空窗期扇出为空，正是航线监控员
//     QGC 签入后界面数据冻结的成因。
//   - qgcSeen：配对的**可用性**，只由 QGC 侧两源刷新。扇出下行帧要求它新鲜（prune 另有
//     一道更宽的兜底闸）。这一格就是「PX4 保活不得延伸到 QGC」的落点：PX4 活跃只能让配对
//     活着，不能让一个已经安静的 QGC 继续收帧，否则场地操作员签出后会被无限期投喂。
type pairEntry struct {
	px4Ch    *gomavlib.Channel
	lastSeen time.Time
	qgcSeen  time.Time
	// lastTraffic：本配对最近一次承载**加密流量**的时刻，只由加密上行/加密下行刷新。
	// （准确说：上行分支的刷新点排在被 §2.5 拦下的 GCS 心跳**之前** ⇒ 被拦心跳也算流量。
	//  这是有意的——QGC 还在 1 Hz 发心跳就说明这一端仍活跃，不该按空闲回收。）
	// ‼️ 80005 登记**不刷新**它 —— 这正是「声明过、PX4 从未上线」的幽灵配对能只增不减的
	// 原因：上面两个时间戳被登记周期性刷新（默认 10s；监控员视图对无帧设备另有 2s 定向重发），
	// 两道 TTL 闸永不命中。存活判据归 lastSeen，可用性判据归 qgcSeen，本字段只管**回收**
	// （prune 的 StateIdleTimeout 闸，默认 10 分钟）。
	lastTraffic time.Time
}

// nonceKey 边缘防重放键：deviceID × 方向。
// 方向取**入口端点角色**（GCS 端点 = 上行 / 载具端点 = 下行），不读 counter 的奇偶。
// 键只由 direction 构造，不会取到 dirInvalid。
type nonceKey struct {
	deviceID uint32
	dir      direction
}

// nonceEntry 防重放水位 + 最近一次推进时间。时间戳只用于 prune 的空闲回收。
//
// 代价（2026-10-24 决策，接受）：水位被回收后，该方向的防重放窗口**重开**——不是
// 「只放行一条」。首条被重放的帧会被接受并把水位写成它自己的 counter，此后任何更大的
// 旧 counter 继续通过，直到该方向再次出现真实帧为止（协议 §2.5「重启边界」同类风险）。
type nonceEntry struct {
	counter  uint64
	lastSeen time.Time
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

// regDiagEntry 是 80005 登记观测的节流状态。
//
// 签名变化才值得看，但监控员视图会逐台轮发单设备帧（1|[A] → 1|[B] → …），签名几乎每帧
// 都变 ⇒ 再叠一道打印节流（kRegDiagLogInterval），避免每帧一条。
type regDiagEntry struct {
	sig  string
	last time.Time
}

const kRegDiagLogInterval = 5 * time.Second

// shouldLogRegDiag 决定本次 80005 诊断是否落日志，并在**决定落日志时**推进状态。
//
// 签名变了但距上次日志不足 kRegDiagLogInterval 时，本次返回 false 且**不更新 sig**：
// 该签名只要窗口过后仍然出现就会补打（不会因为窗口而永久消失）。⚠️ 窗口内出现的
// **其它**中间签名仍会被合并掉——这是节流的本意（监控员视图逐台轮发单设备帧，
// 中间态没有排障价值），别把它读成"每个签名最终都会出现"。
// 调用方须持 m.mu。
func (m *Manager) shouldLogRegDiag(srcCh *gomavlib.Channel, sig string, now time.Time) bool {
	if m.regDiag == nil {
		m.regDiag = make(map[*gomavlib.Channel]regDiagEntry)
	}
	prev, ok := m.regDiag[srcCh]
	if ok && prev.sig == sig {
		return false
	}
	if ok && now.Sub(prev.last) < kRegDiagLogInterval {
		return false
	}
	m.regDiag[srcCh] = regDiagEntry{sig: sig, last: now}
	return true
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
	qgcOnline map[*gomavlib.Channel]*qgcEntry // QGC 在线表（socketID → 最近保活时间）
	pairs     map[pairKey]*pairEntry          // 配对表（三元组）
	lastNonce map[nonceKey]nonceEntry         // 边缘防重放（deviceID × 方向；时间戳仅供 prune）
	// 待命候选与「最近可信」证据（03 §4.3.2–4.3.5）。它们只服务于待命/上行目标集，
	// 不替代 px4Map：候选永不成为权威通道。
	standbyCandidates   map[uint32][]candidate
	lastTrustedDownlink map[uint32]trustedMark
	sessionGate         map[uint32]gateState
	lastStandbyPublish  map[uint32]time.Time
	// 候选准入：每设备最近一次新增来源 IP 的时刻 + 全局令牌桶。
	lastCandidateAdmit map[uint32]time.Time
	admitBucket        tokenBucket
	// 候选发布（扇出/Sink）的全局令牌桶；任务结束路径不使用。
	fanoutBucket tokenBucket
	// 本地验签服务客户端（03 §4.2）。懒创建：内部用例直接构造 Manager、不跑 Initialize。
	// 用独立的锁，避免「入队时不持 m.mu」与主状态锁产生嵌套顺序问题。
	verifierMu sync.Mutex
	verifier   *verifier
	// 空扇出告警节流：deviceID → 上次「PX4 加密下行但无配对 QGC」告警时间。
	// PX4 持续下行而 QGC 配对已过期时会高频触发，需节流避免日志刷屏。
	emptyDownlinkLogged map[uint32]time.Time
	// ---- 路由可观测性（2026-10-24）----
	// 三张表只读状态、不参与任何路由判定；分别回答排障时最常问的三句话：
	// 「这条通道到底声明了谁」「待命心跳扇出给了几条通道」「这条下行为什么没投给某个配对」。
	// 生命周期由 prune 统一回收（regDiag 跟 qgcOnline，另两张按 StateIdleTimeout）。
	regDiag    map[*gomavlib.Channel]regDiagEntry // QGC 通道 → 上次**落日志**的签名 + 上次打印时刻
	hbDiag     map[uint32]time.Time               // deviceID → 上次待命心跳扇出日志时间（5s 节流）
	fanoutDiag map[uint32]time.Time               // deviceID → 上次下行扇出日志时间（10s 节流）
	verifyDiag map[verifyDiagKey]time.Time        // deviceID+状态 → 上次验签失败日志时间（10s 节流）
	// dropDiag 入口闸告警节流：通道 → 上次打印时刻 + 原因（即格式串）。
	// 主清理路径是通道关闭；关闭事件可能漏（见 prune 的 pairs 兜底注释），故 prune 另按
	// StateIdleTimeout 兜底回收。
	// 端口接反时两种形态都是持续高频（PX4 待命心跳 1Hz 打到 GCS 口、QGC 心跳打到载具口），
	// 逐帧打印会淹没真正有用的日志。但**原因也参与节流判定**：同一通道 10s 内换了原因
	// （不同调用点，例如先打「明文心跳打错口」再打「80005 打错口」）时，新原因的首次出现
	// 仍会打印一次——否则先到的那条会把这个窗口里所有别的告警一起吞掉。
	dropDiag map[*gomavlib.Channel]dropDiagEntry
}

// dropDiagEntry 是 logDropThrottled 的节流状态：上次打印时刻 + 原因。
type dropDiagEntry struct {
	at     time.Time
	reason string
}

// roleOf 按通道所属的端点判定入口角色。
//
// 这是**唯一**的方向判据：GCS 端点进来的帧是上行，载具端点进来的是下行。
// 不再读 counter 的奇偶（§2.3 的 nonce LSB 只是发送端的约定，不是路由依据），
// 也不再用 qgcOnline 的成员资格判方向（那张表只回答「登记过没有」）。
func (m *Manager) roleOf(ch *gomavlib.Channel) role {
	if ch == nil {
		return roleUnknown
	}
	ep := ch.Endpoint()
	if ep == nil {
		return roleUnknown
	}
	if ep == m.GCSEndpoint {
		return roleGCS
	}
	for _, v := range m.VehicleEndpoints {
		if ep == v {
			return roleVehicle
		}
	}
	return roleUnknown
}

// hasEndpoint 按接口值相等（身份）判断 eps 里有没有 ep。
// 所有 gomavlib 端点实现的动态类型都是指针，接口比较不会 panic。
func hasEndpoint(eps []gomavlib.Endpoint, ep gomavlib.Endpoint) bool {
	for _, v := range eps {
		if v == ep {
			return true
		}
	}
	return false
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

// stateIdleTimeout 返回长期状态表空闲回收阈值（0=默认）。
func (m *Manager) stateIdleTimeout() time.Duration {
	if m.StateIdleTimeout != 0 {
		return m.StateIdleTimeout
	}
	return DefaultStateIdleTimeout
}

// standbyGuard 返回待命心跳防伪造闸的时间窗（0=默认）。
func (m *Manager) standbyGuard() time.Duration {
	if m.StandbyGuard != 0 {
		return m.StandbyGuard
	}
	return DefaultStandbyGuard
}

func (m *Manager) standbyFanoutInterval() time.Duration {
	if m.StandbyFanoutInterval != 0 {
		return m.StandbyFanoutInterval
	}
	return DefaultStandbyFanoutInterval
}

func (m *Manager) maxCandidatesPerDevice() int {
	if m.MaxCandidatesPerDevice != 0 {
		return m.MaxCandidatesPerDevice
	}
	return DefaultMaxCandidatesPerDevice
}

func (m *Manager) maxCandidatesGlobal() int {
	if m.MaxCandidatesGlobal != 0 {
		return m.MaxCandidatesGlobal
	}
	return DefaultMaxCandidatesGlobal
}

func (m *Manager) candidateAdmitInterval() time.Duration {
	if m.CandidateAdmitInterval != 0 {
		return m.CandidateAdmitInterval
	}
	return DefaultCandidateAdmitInterval
}

func (m *Manager) globalCandidateAdmitRate() int {
	if m.GlobalCandidateAdmitRate != 0 {
		return m.GlobalCandidateAdmitRate
	}
	return DefaultGlobalCandidateAdmitRate
}

func (m *Manager) globalFanoutRate() int {
	if m.GlobalFanoutRate != 0 {
		return m.GlobalFanoutRate
	}
	return DefaultGlobalFanoutRate
}

func (m *Manager) maxUplinkTargets() int {
	if m.MaxUplinkTargets != 0 {
		return m.MaxUplinkTargets
	}
	return DefaultMaxUplinkTargets
}

func (m *Manager) verifySocket() string {
	if m.VerifySocket != "" {
		return m.VerifySocket
	}
	return DefaultVerifySocket
}

func (m *Manager) verifyTimeout() time.Duration {
	if m.VerifyTimeout != 0 {
		return m.VerifyTimeout
	}
	return DefaultVerifyTimeout
}

func (m *Manager) verifyQueuePerDevice() int {
	if m.VerifyQueuePerDevice != 0 {
		return m.VerifyQueuePerDevice
	}
	return DefaultVerifyQueuePerDevice
}

func (m *Manager) verifyQueueGlobal() int {
	if m.VerifyQueueGlobal != 0 {
		return m.VerifyQueueGlobal
	}
	return DefaultVerifyQueueGlobal
}

// validAircraftDeviceID 判定 80005 payload 里声明的 deviceID 是否是合法飞机 ID
// （协议 §1.3 / §1.4）：必须落在 PX4 段且 bit24（incompatFlag bit0，签名位）为 0。
//
// 这不是路由判据的替代，而是**纵深防御**：QGC 侧解析入口已经过滤（2026-10-24），但两端
// 的口径可能漂移（QGC 硬编码默认分界、本侧可配），且 mavp2p 是最后一道。
// 没有这道闸时，这样的脏 ID 会建出 `px4Ch` 恒 nil、永不扇出的幽灵配对。
// 本函数拦住来源，`StateIdleTimeout` 卫生闸负责回收历史残留。
// 跳过它不影响「QGC 先登记、PX4 后上线」的重启恢复路径——那条路用的本来就是合法 ID。
func (m *Manager) validAircraftDeviceID(d uint32) bool {
	return d >= m.gcsDeviceIDMax() && d <= 0x7EFFFFFF && d&0x01000000 == 0
}

// D12（03 §4.3.6）：每个方向的水位**只由本方向**刷新，取消双向续期。
//
// 旧实现让「任一方向有真实帧」给另一方向续期，理由是 §2.5 拦截后上行水位会因心跳被拦而
// 变旧。定稿接受这一点：水位只反映**该方向**的真实流量；空闲超过 StateIdleTimeout 就回收，
// 窗口重开是既有代价（见 nonceEntry）。双向续期反而掩盖了「上行确实很久没来」这一事实。

// validate 校验数值配置。零值表示「用默认值」（Go 零值约定）；负值一律拒绝。
//
// ‼️ 「计数与速率参数 ≥1、0 不是不限」是**用户显式配置**的口径（03 §4.11），由 CLI
// 层拒绝显式 0；这里不能把 0 当非法，否则零值 Config 与测试夹具都无法使用。
func (m *Manager) validate() error {
	// 负时长没有「用默认值」的合理解释（零值才是）：负值会让两道闸都恒真，每轮 prune
	// 把整张表清空。在入口拒绝，而不是让它在运行期静默表现成「表永远为空」。
	if m.MapTTL < 0 {
		return fmt.Errorf("messageman: MapTTL must not be negative (got %s)", m.MapTTL)
	}
	if m.StateIdleTimeout < 0 {
		return fmt.Errorf("messageman: StateIdleTimeout must not be negative (got %s)", m.StateIdleTimeout)
	}
	// 负窗口没有「关闭」的合理解释（零值才是兜底默认）：负值会让闸恒不成立，
	// 静默退化成「无防护」，配置写错必须当场暴露。
	if m.StandbyGuard < 0 {
		return fmt.Errorf("messageman: StandbyGuard must not be negative (got %s)", m.StandbyGuard)
	}
	if m.StandbyFanoutInterval < 0 {
		return fmt.Errorf("messageman: StandbyFanoutInterval must not be negative (got %s)", m.StandbyFanoutInterval)
	}
	if m.CandidateAdmitInterval < 0 {
		return fmt.Errorf("messageman: CandidateAdmitInterval must not be negative (got %s)", m.CandidateAdmitInterval)
	}
	if m.VerifyTimeout < 0 {
		return fmt.Errorf("messageman: VerifyTimeout must not be negative (got %s)", m.VerifyTimeout)
	}
	// 负上限在改前只是「循环空转」，2026-10-24 起 processRegistration 会用它作为
	// make 的 cap ⇒ 必崩。入口拒绝；processRegistration 里另有一道截断兜底。
	if m.MaxQGCLinkedPX4 < 0 {
		return fmt.Errorf("messageman: MaxQGCLinkedPX4 must not be negative (got %d)", m.MaxQGCLinkedPX4)
	}
	for _, c := range []struct {
		name string
		v    int
	}{
		{"MaxCandidatesPerDevice", m.MaxCandidatesPerDevice},
		{"MaxCandidatesGlobal", m.MaxCandidatesGlobal},
		{"GlobalCandidateAdmitRate", m.GlobalCandidateAdmitRate},
		{"GlobalFanoutRate", m.GlobalFanoutRate},
		{"VerifyQueuePerDevice", m.VerifyQueuePerDevice},
		{"VerifyQueueGlobal", m.VerifyQueueGlobal},
	} {
		if c.v < 0 {
			return fmt.Errorf("messageman: %s must not be negative (got %d)", c.name, c.v)
		}
	}
	// 上行目标集只含一个通道时，换端口的真机在晋升前收不到上行（03 §4.11）。
	if m.MaxUplinkTargets != 0 && m.MaxUplinkTargets < 2 {
		return fmt.Errorf("messageman: MaxUplinkTargets must be >= 2 (got %d)", m.MaxUplinkTargets)
	}
	return nil
}

// Initialize 初始化 Manager。
func (m *Manager) Initialize() error {
	if err := m.validate(); err != nil {
		return err
	}
	// 方向只由入口端点决定 ⇒ 两个角色都必须有且互不重叠。缺一个就拒绝启动，
	// 不允许退化成「靠 counter 奇偶猜」——那正是本次要删掉的判据。
	if m.GCSEndpoint == nil {
		return fmt.Errorf("messageman: GCSEndpoint is required (direction is decided by the ingress endpoint)")
	}
	if len(m.VehicleEndpoints) == 0 {
		return fmt.Errorf("messageman: VehicleEndpoints must not be empty")
	}
	configured := append([]gomavlib.Endpoint{m.GCSEndpoint}, m.VehicleEndpoints...)
	for _, ep := range configured {
		if ep == nil {
			return fmt.Errorf("messageman: ingress endpoints must not be nil")
		}
	}
	for _, ep := range m.VehicleEndpoints {
		if ep == m.GCSEndpoint {
			return fmt.Errorf("messageman: GCSEndpoint must not also appear in VehicleEndpoints")
		}
	}
	// 与 Node.Endpoints 交叉校验。roleOf 按接口值相等认来源，端点只要不是「同一批
	// 对象」，落到该端口的每一帧都会判成 roleUnknown 并被丢弃 —— 症状是「进程起来了、
	// 端口也监听着，但一帧都不转发」，只能从入口闸告警里看出来。装配错误必须在启动期暴露。
	if m.Node == nil {
		return fmt.Errorf("messageman: Node is required (endpoint roles are cross-checked against Node.Endpoints)")
	}
	if len(m.Node.Endpoints) != len(configured) {
		return fmt.Errorf("messageman: Node.Endpoints has %d entries, want %d "+
			"(one GCSEndpoint + %d vehicle endpoints); extra endpoints would have no role",
			len(m.Node.Endpoints), len(configured), len(m.VehicleEndpoints))
	}
	for _, want := range configured {
		if !hasEndpoint(m.Node.Endpoints, want) {
			return fmt.Errorf("messageman: configured endpoint %s is not the same object as any Node.Endpoints "+
				"entry; frames from it would be dropped as roleUnknown", want)
		}
	}
	for _, got := range m.Node.Endpoints {
		if !hasEndpoint(configured, got) {
			return fmt.Errorf("messageman: Node.Endpoints entry %s has no role; "+
				"configure it as GCSEndpoint or in VehicleEndpoints", got)
		}
	}

	m.px4Map = make(map[uint32]*px4Entry)
	m.qgcOnline = make(map[*gomavlib.Channel]*qgcEntry)
	m.pairs = make(map[pairKey]*pairEntry)
	m.lastNonce = make(map[nonceKey]nonceEntry)
	m.standbyCandidates = make(map[uint32][]candidate)
	m.lastTrustedDownlink = make(map[uint32]trustedMark)
	m.sessionGate = make(map[uint32]gateState)
	m.lastStandbyPublish = make(map[uint32]time.Time)
	m.lastCandidateAdmit = make(map[uint32]time.Time)
	m.emptyDownlinkLogged = make(map[uint32]time.Time)
	m.regDiag = make(map[*gomavlib.Channel]regDiagEntry)
	m.hbDiag = make(map[uint32]time.Time)
	m.fanoutDiag = make(map[uint32]time.Time)
	m.verifyDiag = make(map[verifyDiagKey]time.Time)
	m.dropDiag = make(map[*gomavlib.Channel]dropDiagEntry)

	m.Wg.Add(1)
	go m.run()

	return nil
}

// run 周期清理（每 10s）：先按 MAP_TTL 清路由存活表，再按 StateIdleTimeout 做卫生回收，
// 详见 prune。
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

// prune 做两档清理：
//
//  1. 路由存活（MAP_TTL）：映射/在线/配对。这些表靠 80005 与 PX4 心跳续命，秒级。
//  2. 卫生（StateIdleTimeout，默认 10 分钟）：配对、防重放水位、诊断表、最近可信通道、
//     会话门、扇出时间。
//     各表的计时源**不同**，别用一句话概括：配对按 `lastTraffic`（加密上行/下行，见下）、
//     水位按 `lastSeen`（该方向自己的加密帧，D12）、`hbDiag`/`fanoutDiag`/`dropDiag` 按各自的
//     日志写入时刻、`regDiag` 则不看时间而是看「通道是否还在 qgcOnline」、候选按
//     `lastSeen` 但用 **MapTTL**（路由存活尺度）、可信/会话门/扇出按 StateIdleTimeout。
//     80005 周期登记不算流量。
//
// lastNonce 的空闲回收是 2026-10-24 新增的：改前只有「PX4 回待命」一条清路，长期运行的
// 进程里水位表随历史 deviceID 数只增不减。代价按 2026-10-24 决策接受：设备空闲超过阈值
// 后该方向的防重放窗口**重开**，直到再次出现真实帧为止（不是「只放行一条」，见 nonceEntry
// 的注释；协议 §2.5「重启边界」同类残余风险）。D12 起每个方向只由自己的真实帧续期；
// §2.5 拦截让建链后的上行心跳不再推进上行水位，因此上行水位可能在链路仍活跃时被回收 ——
// 这是定稿接受的代价（03 §4.3.6 / §5.10），不再用双向续期掩盖。
func (m *Manager) prune() {
	now := time.Now()
	ttl := m.mapTTL()
	idle := m.stateIdleTimeout()

	// 本轮卫生回收的计数（只为打一行汇总，避免逐条刷屏；配对回收另有逐条日志）。
	var nNonces, nRegDiag, nHb, nFanout, nDrop, nVerify, nCand, nTrust, nGate, nPublish, nAdmit int

	m.mu.Lock()
	defer m.mu.Unlock()

	for did, e := range m.px4Map {
		if now.Sub(e.lastSeen) >= ttl {
			log.Printf("PX4 disappeared: deviceID=%d", did)
			delete(m.px4Map, did)
			// 03 §4.3.1：条目被 MapTTL 剪掉时，把该 deviceID 名下所有配对的 px4Ch 置空
			// （配对保留）。否则配对继续指向一个已不存在的权威通道，违反
			// 「px4Ch 只能为空或等于已验签映射」的不变量；下一次验签下行的晋升会重新补上。
			for k, p := range m.pairs {
				if k.px4DeviceID == did {
					p.px4Ch = nil
				}
			}
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
			continue
		}
		// 卫生闸（2026-10-24）：真实流量静默超过 StateIdleTimeout（默认 10 分钟）⇒ 回收。
		// ‼️ 判据必须是 lastTraffic。上面两道闸读的 lastSeen / qgcSeen 都被 80005（默认 10s，
		// 监控员视图对无帧设备另有 2s 定向重发）刷新。改前：「QGC 声明过、PX4 从未上线」的
		// 幽灵配对（px4Ch 恒 nil、永不扇出）两道闸都命中不了，只能等 QGC 通道关闭 ——
		// 这正是配对表只增不减的成因，本闸就是为它加的。
		if now.Sub(e.lastTraffic) >= idle {
			// 逐条带 deviceID：回收后到下一次 80005 登记之间的**所有**下行帧都会被丢
			// （下行分支不建配对；站点视图约 10s、监控员视图约 2s 后恢复）。没有这条日志，
			// 运维无法把一次「偶发丢帧」对上回收事件。
			log.Printf("pair idle-purged: QGC %s <-> PX4 deviceID=%d idle=%s",
				k.qgcCh, k.px4DeviceID, now.Sub(e.lastTraffic).Round(time.Second))
			delete(m.pairs, k)
			continue
		}
		// 兜底（2026-10-06）：lastSeen 现在会被 PX4 下行持续刷新，所以「QGC 侧彻底消失」这条
		// 路不能再指望上面的 TTL 清理了。主清理路径是 ProcessChannelClose（channel 关闭即清
		// 其名下配对），但它有漏——实测 18 开 / 17 闭，差的那一个会让条目跨小时累积；而
		// pairKey 含 QGC 侧 channel 指针，QGC 端口每漂移一次就多一批，只增不减。
		// 这里补一道远宽的闸：QGC 侧安静超过 10×TTL 就强制删。
		// ‼️ 判据必须是 qgcSeen 而**不是** lastSeen——后者被 PX4 刷着，永远到不了这个闸。
		// 10× 是量级选择：要远大于任何一次 80005 周期（10s）与链路抖动，才不会误杀一个刚要
		// 恢复的正常配对（那正是本次要修的场景）；又要让死通道的条目不至于长期滞留。
		if now.Sub(e.qgcSeen) >= 10*ttl {
			log.Printf("pair reaped (QGC silent): QGC %s <-> PX4 deviceID=%d", k.qgcCh, k.px4DeviceID)
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

	// ---- 卫生回收（StateIdleTimeout）----
	// 防重放水位：长期没有真实帧的设备，其水位没有保留价值（代价：窗口重开到下一条真实帧，
	// 见 nonceEntry）。D12：每个方向只由自己的真实帧刷新，两个方向互不续期。
	for k, e := range m.lastNonce {
		if now.Sub(e.lastSeen) >= idle {
			// 逐条带 deviceID + 方向：回收意味着该方向防重放窗口**重开**（见 nonceEntry），
			// 之后可能出现一条被放行的重复帧。没有这条日志，运维无法把「偶发重复」对上回收事件。
			log.Printf("nonce idle-purged: deviceID=%d direction=%s counter=%d idle=%s",
				k.deviceID, k.dir, e.counter, now.Sub(e.lastSeen).Round(time.Second))
			delete(m.lastNonce, k)
			nNonces++
		}
	}
	// regDiag 的键是 QGC 通道，没有自己的时间戳：判据取「该通道是否还在 QGC 在线表里」
	// （不按 idle 计时：一个通道只要还在线，它的登记签名就仍有比较价值）。
	// ‼️ 必须排在上面 qgcOnline 清理**之后** —— 顺序反了，本轮刚该消失的通道此刻还在
	// qgcOnline 里，它的签名会漏掉，而 map 键持有已死的 channel 指针，同样只增不减。
	for ch := range m.regDiag {
		if _, ok := m.qgcOnline[ch]; !ok {
			delete(m.regDiag, ch)
			nRegDiag++
		}
	}
	// 两张日志节流表：值本身就是上次写入时刻，直接按时间戳回收。
	for did, t := range m.hbDiag {
		if now.Sub(t) >= idle {
			delete(m.hbDiag, did)
			nHb++
		}
	}
	for did, t := range m.fanoutDiag {
		if now.Sub(t) >= idle {
			delete(m.fanoutDiag, did)
			nFanout++
		}
	}
	for k, t := range m.verifyDiag {
		if now.Sub(t) >= idle {
			delete(m.verifyDiag, k)
			nVerify++
		}
	}
	// dropDiag（入口闸告警节流）的键是通道指针，主清理路径是 ProcessChannelClose。那条路径
	// 有漏（配对表实测「18 开 / 17 闭」，见上方 pairs 的兜底注释）：漏掉的通道会把指针永久
	// 留在这里，而每次 NAT 重建都是一条新 channel ⇒ 同样只增不减。按空闲时间补一道兜底；
	// 判据取「上次打告警的时刻」，与 hbDiag/fanoutDiag 一致。
	for ch, e := range m.dropDiag {
		if now.Sub(e.at) >= idle {
			delete(m.dropDiag, ch)
			nDrop++
		}
	}
	// 候选：按 MapTTL 过期（03 §4.3.2）。主清理路径是通道关闭；这里兜底漏掉的关闭事件。
	for did, cs := range m.standbyCandidates {
		kept := cs[:0]
		for _, c := range cs {
			if now.Sub(c.lastSeen) < ttl {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			delete(m.standbyCandidates, did)
			nCand += len(cs)
		} else {
			nCand += len(cs) - len(kept)
			m.standbyCandidates[did] = kept
		}
	}
	// 最近可信通道 / 会话门 / 扇出时间：按 StateIdleTimeout 回收。
	// 信任标记**不随 MapTTL** 消失——映射 30s 被剪后，待命闸的 5m 窗口仍需要证据。
	for did, mark := range m.lastTrustedDownlink {
		if now.Sub(mark.at) >= idle {
			delete(m.lastTrustedDownlink, did)
			nTrust++
		}
	}
	for did, g := range m.sessionGate {
		ref := g.endedAt
		if g.openedAt.After(ref) {
			ref = g.openedAt
		}
		if now.Sub(ref) >= idle {
			delete(m.sessionGate, did)
			nGate++
		}
	}
	for did, t := range m.lastStandbyPublish {
		if now.Sub(t) >= idle {
			delete(m.lastStandbyPublish, did)
			nPublish++
		}
	}
	for did, t := range m.lastCandidateAdmit {
		if now.Sub(t) >= idle {
			delete(m.lastCandidateAdmit, did)
			nAdmit++
		}
	}
	if nNonces+nRegDiag+nHb+nFanout+nDrop+nVerify+nCand+nTrust+nGate+nPublish+nAdmit > 0 {
		log.Printf("state purge: nonces=%d regDiag=%d hbDiag=%d fanoutDiag=%d dropDiag=%d "+
			"verifyDiag=%d candidates=%d trust=%d gate=%d standbyPublish=%d candidateAdmit=%d",
			nNonces, nRegDiag, nHb, nFanout, nDrop, nVerify, nCand, nTrust, nGate, nPublish, nAdmit)
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
//
// 入口角色由通道所属端点决定（roleOf），这是**唯一**的上下行判据；counter 的
// 奇偶与 qgcOnline 的成员资格都不再参与方向判定。
func (m *Manager) ProcessFrame(evt *gomavlib.EventFrame) {
	fr := evt.Frame
	srcCh := evt.Channel
	msg := evt.Message()
	msgID := msg.GetID()
	did := frameDeviceID(fr)
	plen := rawPayloadLen(msg)

	r := m.roleOf(srcCh)
	var dir direction
	switch r {
	case roleGCS:
		dir = dirUplink
	case roleVehicle:
		dir = dirDownlink
	default:
		// 生产装配下不可达：Initialize 已与 Node.Endpoints 交叉校验。
		// 保留这一支只为在编程/配置错误时显式失败，而不是把帧塞进某个方向。
		m.logDropThrottled(srcCh, "frame from channel with unknown endpoint role: %s; dropping", srcCh)
		return
	}

	// 明文 RequestDataStream 拦截（上游功能保留）：与入口角色无关，明文一律拦。
	// 加密的 66（payload ≥ encryptedPayloadMinLen）不是本判据的目标。
	// ⚠️ 关闭开关（StreamReqDisable）后，明文 66 仍会被下方的短帧闸丢弃——定稿的
	// 形状闸优先于该开关（03 §4.4「其它 msgID <28B 两端丢弃」），开关不再改变结果。
	if !m.StreamReqDisable && msgID == msgIDRequestDataStream &&
		(plen < 0 || plen < encryptedPayloadMinLen) {
		return
	}

	// 入口形状闸（03 §4.4）：deviceID=0 / 短帧 / 载具 msgID=0 的 T 分流。
	kind, reason := classifyIngress(dir, msgID, plen, did)
	switch kind {
	case ingressDrop:
		m.logIngressDrop(srcCh, did, msgID, plen, reason)

	case ingressRegistration:
		// QGC 明文登记/保活心跳（§3.2 步骤 2）：仅 mavp2p 消费，不转发。
		// 只允许从地面站入口进来；出现在载具入口 = 客户端配错端口，丢弃并告警。
		if dir != dirUplink {
			m.logDropThrottled(srcCh, "dropped 80005 registration on vehicle endpoint from %s "+
				"(is the QGC still pointed at a vehicle port? it must use --gcs-endpoint)", srcCh)
			return
		}
		m.processRegistration(srcCh, did, msg)

	case ingressStandby:
		// PX4 明文待命心跳：载具端点、msgID=0、恰好 9B（长度已由入口闸钉死）。
		// 号段闸：否则一帧 9B 明文心跳（id 取 0x80016379 之类）能在 px4Map 里种出
		// 「幻象 PX4」并扇出；口径与 80005 登记、加密下行号段闸一致。
		if !m.validAircraftDeviceID(did) {
			m.logDropThrottled(srcCh, "dropped plaintext heartbeat with invalid deviceID=%d from %s: "+
				"outside aircraft segment", did, srcCh)
			return
		}
		m.processStandbyHeartbeat(srcCh, did, fr)

	case ingressEncrypted:
		// 下行帧头统一过号段闸（03 §4.4 补充）。processEncrypted 的「缓存缺失即重建」
		// 分支也有一道，这里提前拦掉，避免任何下行路径越过号段判据。
		if dir == dirDownlink && !m.validAircraftDeviceID(did) {
			m.logDropThrottled(srcCh,
				"dropped downlink for invalid deviceID=%d from %s: outside aircraft segment",
				did, srcCh)
			return
		}
		// counter=0 一律丢弃（03 §4.4）：0 不作为「不判重」的旁路。入口即丢 ⇒
		// 不写水位、不转发、不喂 Sink，也不参与任何状态变更。
		if counter, ok := payloadCounter(msg); !ok || counter == 0 {
			m.logDropThrottled(srcCh,
				"dropped encrypted frame with counter=0 or unreadable counter from %s (deviceID=%d)",
				srcCh, did)
			return
		}
		m.processEncrypted(srcCh, did, fr, dir)
	}
}

// kDropLogInterval 入口闸告警的通道级节流窗口。
// 端口接反时帧率很高（PX4 待命心跳 1Hz、QGC 心跳 1Hz），逐帧打印没有额外信息量。
const kDropLogInterval = 10 * time.Second

// logDropThrottled 打一条入口闸告警：同一通道、同一原因 10s 内最多一条。
// 原因取格式串（同一调用点恒定）；换了原因会重新放行一次，见 dropDiag 的注释。
func (m *Manager) logDropThrottled(ch *gomavlib.Channel, format string, args ...any) {
	now := time.Now()
	m.mu.Lock()
	// 内部用例直接构造 Manager（不走 Initialize），这里懒初始化，与 fanoutDiag 同口径。
	if m.dropDiag == nil {
		m.dropDiag = make(map[*gomavlib.Channel]dropDiagEntry)
	}
	if last, ok := m.dropDiag[ch]; ok && last.reason == format &&
		now.Sub(last.at) < kDropLogInterval {
		m.mu.Unlock()
		return
	}
	m.dropDiag[ch] = dropDiagEntry{at: now, reason: format}
	m.mu.Unlock()
	log.Printf(format, args...)
}

// registrationDeviceIDAt 读 80005 payload 里第 i 个 deviceID（i 从 0 起）。
//
// 超出 payload 的字节按 0 计：`mavlink_msg_qgc_registration_pack` 把 payload 零初始化后
// 按 len 发送，**尾部零字节会被裁掉**——一帧一台时，被裁的就是那唯一一台 ID 的低字节
// （如 10000384 = 0x00989800 ⇒ len 只有 4）。按 `1+i*4 : 5+i*4` 直接切片会在 len 不足时
// panic；按 `(len-1)/4` 截断台数则会把合法登记读成「0 台」⇒ 该机永远建不出配对。
func registrationDeviceIDAt(payload []byte, i int) uint32 {
	var b [4]byte
	off := 1 + i*4
	for j := range b {
		if off+j < len(payload) {
			b[j] = payload[off+j]
		}
	}
	return binary.BigEndian.Uint32(b[:])
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
	// 兜底（Initialize 已拒绝负配置）：负值在这里只该是「0 条声明」。少了这一行，
	// 下面的 `make([]uint32, 0, num)` 会 panic 打挂整个进程——配置写错不该有这种杀伤力。
	if num < 0 {
		num = 0
	}
	// ⚠️ 不按帧长截断台数：尾部零字节本来就是打包时被裁掉的（见 registrationDeviceIDAt）。
	// 声明 N 台却只有前几个字节，既可能是「末尾几台全零、整段被裁成尾零」，也可能是畸形帧；
	// 两种都按声明解码，补出来的全零 ID 由 validAircraftDeviceID 挡掉（不会建出幽灵配对）。
	// num 已被 maxQGCLinkedPX4 限幅，不会因为一个巨大的声明值把循环撑爆。

	// 诊断：先把 payload 里的声明值摊开（不受后续 pair 是否已存在影响）。
	// 同时做纵深防御的合法性判定：非法 ID 不进配对表（否则是永不扇出的幽灵配对）。
	// declaredIDs 保留**全部**声明值（与原诊断口径一致），invalidIDs 单列，进签名。
	declaredIDs := make([]uint32, 0, num)
	invalidIDs := make([]uint32, 0)
	for i := 0; i < num; i++ {
		d := registrationDeviceIDAt(payload, i)
		declaredIDs = append(declaredIDs, d)
		if !m.validAircraftDeviceID(d) {
			invalidIDs = append(invalidIDs, d)
		}
	}

	// 非法 ID 单独成行，不并入下面的签名节流：一帧一台时逐帧签名几乎每帧都变，
	// 节流只保留「与上次已落日志签名不同、且窗口已过」的那一帧 ⇒ 一轮里的第 3 台起
	// 可能永远不落日志，现场只看到「没有配对」，对不到「ID 被拒」这个根因。
	// 频率上限＝登记频率（每台每 10s 一次），异常路径值得逐条可见。
	for _, d := range invalidIDs {
		log.Printf("80005 rejected invalid deviceID=%d from %s (payloadLen=%d)", d, srcCh, len(payload))
	}

	now := time.Now()
	m.mu.Lock()
	// 核心规则：按来源方（QGC）刷新 QGC 在线表（键 = 来源 socketID）
	m.qgcOnline[srcCh] = &qgcEntry{lastSeen: now}

	// 观测（2026-10-24）：签名（num|declaredIDs|invalidIDs）变化时打一行，并叠加打印节流。
	// ⚠️ 签名是**逐帧**比较，不是按集合归一化：监控员视图的 2s 定向重发会逐台轮发单设备帧
	// （1|[A] → 1|[B] → …），签名几乎每帧都变 ⇒ 只看签名会每帧一条；5s 节流下仍能看清
	// 「某一轮声明了谁」，代价是窗口内的中间签名会被合并。
	{
		sig := fmt.Sprintf("%d|%v|%v", num, declaredIDs, invalidIDs)
		if m.shouldLogRegDiag(srcCh, sig, now) {
			log.Printf("80005 diag: %p declared=%d parsed=%d payloadLen=%d ids=%v rejected=%v",
				srcCh, int(payload[0]), num, len(payload), declaredIDs, invalidIDs)
		}
	}

	// 按 payload 的 PX4 deviceID 集合刷新配对表 QGC 侧（§3.2.4.1 重启恢复/重定位）
	for i := 0; i < num; i++ {
		d := registrationDeviceIDAt(payload, i)
		if !m.validAircraftDeviceID(d) {
			// 已在上面的 invalidIDs 里进过诊断；这里只跳过建表（纵深防御，理由见
			// validAircraftDeviceID 的注释）。
			continue
		}
		k := pairKey{qgcCh: srcCh, px4DeviceID: d}
		e, ok := m.pairs[k]
		if !ok {
			// lastTraffic 初值取登记时刻：从「第一次出现」开始计 StateIdleTimeout（默认 10 分钟），
			// 此后只有加密帧能续期（见 pairEntry 的注释）。
			e = &pairEntry{lastSeen: now, qgcSeen: now, lastTraffic: now}
			m.pairs[k] = e
			log.Printf("QGC %s registered: linked PX4 deviceID=%d", srcCh, d)
		} else {
			// lastSeen / qgcSeen 都刷：80005 是 QGC 侧**唯一**周期性的声明源，既是配对的
			// 存活源，也是唯一能让 qgcSeen 变新鲜的东西（加密上行是 1Hz，但只在有上行帧
			// 时才来）。登记周期：默认 10s；监控员视图对无帧设备另有 2s 定向重发。
			// ‼️ 刻意**不刷** lastTraffic：登记是声明、不是流量。刷了的话，一个从未上线的
			//    deviceID 会靠周期登记永远年轻，StateIdleTimeout 回收闸形同虚设。
			e.lastSeen = now
			e.qgcSeen = now
		}
		if px4, ok := m.px4Map[d]; ok {
			e.px4Ch = px4.channel // PX4 已在线 → 补全三元组
		}
	}
	m.mu.Unlock()

	// 80005 不转发给 PX4（§2.2/§3.2：仅 mavp2p 消费）
}

// processStandbyHeartbeat 处理载具端点的 9B 明文待命心跳（03 §4.5）。
//
// 动机：明文待命心跳没有签名、没有 counter、mavp2p 也不解密，任何能访问载具端点的
// 来源都能凭一个合法飞机号段 deviceID 伪造一帧。若照单全收，**一帧**就能改写映射、
// 清配对/水位、把伪造帧扇出给 QGC 并送进 Sink。定稿把它的权限压到「只有同最近可信
// 通道、且当前会话仍有下行水位时，允许做一次任务结束」；其余情况只能登记候选。
//
// 三条路径按顺序判定（03 §4.5）：
//  1. 任务结束：mark.channel == srcCh ∧ 下行水位存在；
//  2. 可疑：mark 存在 ∧ 异通道 ∧ now-mark.at < StandbyGuard ⇒ 整帧丢弃、零状态变更；
//  3. 候选：其余情况 ⇒ 只登记/刷新候选并按节流发布，不碰映射/配对/水位/handshaked。
func (m *Manager) processStandbyHeartbeat(srcCh *gomavlib.Channel, did uint32, fr frame.Frame) {
	now := time.Now()
	srcIP := channelSourceIP(srcCh)
	nkUp := nonceKey{deviceID: did, dir: dirUplink}
	nkDown := nonceKey{deviceID: did, dir: dirDownlink}

	m.mu.Lock()
	mark, hasMark := m.lastTrustedDownlink[did]
	_, hadDown := m.lastNonce[nkDown]

	// ---- 路径 1：任务结束（同最近可信通道 + 当前会话下行水位存在）----
	// 第二个条件是「一次性」的来源：任务结束清了水位后，同通道的 1Hz 待命仍会继续来，
	// 只按通道判会反复清掉 QGC 刚登记的新配对。
	if hasMark && mark.channel == srcCh && hadDown {
		// 必须先取快照再删除，否则删完配对后发布目标为空。
		targets := m.standbyFanoutTargets(did, now)
		for k := range m.pairs {
			if k.px4DeviceID == did {
				delete(m.pairs, k)
			}
		}
		delete(m.lastNonce, nkUp)
		delete(m.lastNonce, nkDown)
		delete(m.px4Map, did) // handshaked 随条目消失
		m.sessionGate[did] = gateState{endedAt: now}
		// 保护候选是强制插入：不受准入间隔/全局速率/全局上限限制。
		if !m.upsertStandbyCandidate(did, srcCh, mark.sourceIP, now, true) {
			log.Printf("standby task end: protected candidate rejected deviceID=%d", did)
		}
		// 任务结束立即发布，不受 2s 窗口与全局预算限制，但仍更新时间戳。
		m.lastStandbyPublish[did] = now
		m.mu.Unlock()

		log.Printf("standby task end: deviceID=%d channel=%s fanout=%d", did, srcCh, len(targets))
		for _, ch := range targets {
			if ch != srcCh {
				m.Node.WriteFrameTo(ch, fr) //nolint:errcheck
			}
		}
		if m.Sink != nil {
			m.Sink.ProcessFrame(fr)
		}
		return
	}

	// ---- 路径 2：可疑（异通道且仍在窗口内）----
	// 整帧丢弃：不改路由状态、不续 lastSeen、不扇出、不入 Sink、不建候选。
	if hasMark && mark.channel != srcCh && now.Sub(mark.at) < m.standbyGuard() {
		m.mu.Unlock()
		m.logDropThrottled(srcCh, "dropped standby heartbeat deviceID=%d from %s: "+
			"recent trusted downlink on another channel (guard %s)", did, srcCh, m.standbyGuard())
		return
	}

	// ---- 路径 3：候选 ----
	registered := m.upsertStandbyCandidate(did, srcCh, srcIP, now, false)
	var targets []*gomavlib.Channel
	publish := false
	if registered {
		// 先算落点再判发布：没有任何实际落点（扇出目标为空且 Sink=nil）时这一帧
		// 什么也没发出去，不得消耗 2s 窗口与全局预算（03 §4.5.3）。
		targets = m.standbyFanoutTargets(did, now)
		fanout := 0
		for _, ch := range targets {
			if ch != srcCh {
				fanout++
			}
		}
		if fanout > 0 || m.Sink != nil {
			publish = m.shouldPublishStandby(did, now)
		}
	}
	if publish {
		if m.hbDiag == nil {
			m.hbDiag = make(map[uint32]time.Time)
		}
		if last, ok := m.hbDiag[did]; !ok || now.Sub(last) >= 5*time.Second {
			m.hbDiag[did] = now
			log.Printf("standby heartbeat diag: deviceID=%d candidates=%d fanout=%d",
				did, len(m.standbyCandidates[did]), len(targets))
		}
	}
	m.mu.Unlock()

	if !publish {
		if !registered {
			m.logDropThrottled(srcCh, "dropped standby candidate deviceID=%d from %s: "+
				"candidate admission/capacity rejected", did, srcCh)
		}
		return
	}
	for _, ch := range targets {
		if ch != srcCh {
			m.Node.WriteFrameTo(ch, fr) //nolint:errcheck
		}
	}
	// 下游副本（规范 §3.2.5：FIFO 内容含明文待命心跳）。落点在全部过滤之后。
	if m.Sink != nil {
		m.Sink.ProcessFrame(fr)
	}
}

// processEncrypted 处理加密任务帧（§3.2.2 步骤 3/4/8）。
//
// 方向由入口端点决定（d 由 ProcessFrame 从 roleOf 映射而来），**不读 counter 奇偶**，
// 也不靠 qgcOnline 成员资格判方向；counter 只用于防重放水位的数值比较。
// d 必须是 dirUplink/dirDownlink 之一：dirInvalid 在这里被拒绝，不让「第三态」落进某个分支。
func (m *Manager) processEncrypted(srcCh *gomavlib.Channel, did uint32, fr frame.Frame, d direction) {
	if d != dirUplink && d != dirDownlink {
		// ProcessFrame 已在前置映射处丢弃未知角色；这里是防御性第二道，不静默选边。
		m.logDropThrottled(srcCh, "processEncrypted called with invalid direction (%d) from %s; dropping", d, srcCh)
		return
	}
	// 边缘防重放（§3.1）：读 payload 明文 counter（前 8 字节），按 deviceID×方向 判重。
	// 尽力而为、不认证；端到端的权威判重在各**解密方**（PX4 / QGC 各自维护）。
	// ❗ data_writer 不在其中：它 2026-09-28 起按 §2.6「data_writer 例外」删除了判重
	// （用户裁定）⇒ 对 FIFO 这条消费路径，本函数就是唯一的一道。counter 为 0 或取不到则不判。
	// 注意：lastNonce 的写入延后到「确认本帧会被路由」之后——被拦截/忽略的帧不推进
	// 防重放水位，避免未登记 QGC 上行（被忽略）推进上行水位、误杀后续合法上行。
	// （下行帧即便无配对接收者，也会因刷新状态而推进水位。）
	// plen 同时用于识别加密心跳（msgID=0 且 ≥ encryptedPayloadMinLen，§2.5）。
	msg := fr.GetMessage()
	msgID := msg.GetID()
	var counter uint64
	plen := -1
	if raw, ok := msg.(*message.MessageRaw); ok {
		plen = len(raw.Payload)
		if plen >= 8 {
			counter = binary.BigEndian.Uint64(raw.Payload[0:8])
		}
	}
	m.mu.Lock()

	// 防重放判定（读，不写）：同方向同 deviceID 的 counter 不递增则丢弃。
	// 方向来自入口端点 ⇒ 水位键也用方向，与 counter 的数值/奇偶无关。
	var nk *nonceKey
	if counter != 0 {
		k := nonceKey{deviceID: did, dir: d}
		if last, ok := m.lastNonce[k]; ok && counter <= last.counter {
			m.mu.Unlock()
			log.Printf("dropped replayed frame deviceID=%d %s counter=%d from %s", did, d, counter, srcCh)
			return
		}
		nk = &k
	}

	// 方向：入口端点唯一决定，GCS 端点 = 上行，载具端点 = 下行。
	if d == dirUplink {
		now := time.Now()
		// ---- 上行（QGC → PX4）：帧头 deviceID = 目标 PX4 的 D（§3.2.3）----
		// 登记是**授权/订阅**闸，不是方向判据：没发过 80005 的地面站通道不得注入上行。
		// 同一张查表顺带刷新在线身份（§3.2.3 保活约束），使「上行→未知 PX4」路径也
		// 维持在线状态；先刷新可避免活跃会话被 MAP_TTL 误清理后上行因登记闸被丢。
		qe, ok := m.qgcOnline[srcCh]
		if !ok {
			m.mu.Unlock()
			log.Printf("received uplink from unregistered GCS channel %s for PX4 deviceID=%d, ignoring", srcCh, did)
			return
		}
		qe.lastSeen = now

		// 目标集（03 §4.6）：已验签通道 + 候选，按优先级去重并裁到上限。
		// 两者都无 ⇒ 丢弃（不写水位）。
		targets := m.uplinkTargetsLocked(did)
		if len(targets) == 0 {
			m.mu.Unlock()
			log.Printf("received uplink for unknown PX4 deviceID=%d from %s, ignoring", did, srcCh)
			return
		}

		px4, hasPX4 := m.px4Map[did]
		var px4Ch *gomavlib.Channel
		if hasPX4 {
			px4Ch = px4.channel
		}
		// 刷新/新建配对 (QGC socketID, PX4 deviceID, PX4 socketID)；
		// 按来源 socketID 建键即天然处理 QGC 侧 socketID 漂移（新 channel → 新键）。
		// ‼️ px4Ch 只取已验签映射；候选永不写入（新建配对留空，等晋升补齐）。
		pk := pairKey{qgcCh: srcCh, px4DeviceID: did}
		e, ok := m.pairs[pk]
		if !ok {
			m.pairs[pk] = &pairEntry{px4Ch: px4Ch, lastSeen: now, qgcSeen: now, lastTraffic: now}
			log.Printf("link established: QGC %s <-> PX4 deviceID=%d targets=%d", srcCh, did, len(targets))
		} else {
			e.px4Ch = px4Ch
			e.lastSeen = now
			e.qgcSeen = now
			e.lastTraffic = now // 真实上行 ⇒ 本配对确实承载了转发
		}
		// 加密心跳拦截（§2.5）：仅当该 PX4 已握手**且候选为空**时拦。
		// 歧义期（有候选）不拦——QGC 心跳正是触发真机回包自证的那一帧。
		// 位置约束：必须在配对刷新之后（心跳是配对保活源），且在写水位之前
		// （被拦帧不推进防重放水位）。
		if msgID == msgIDHeartbeat && plen >= encryptedPayloadMinLen &&
			hasPX4 && px4.handshaked && len(m.standbyCandidates[did]) == 0 {
			m.mu.Unlock()
			return
		}
		if nk != nil {
			m.lastNonce[*nk] = nonceEntry{counter: counter, lastSeen: now}
		}
		// 会话门（03 §4.3.4）：任务结束后第一条被实际转发的 QGC 上行开启门。
		if g, ok := m.sessionGate[did]; ok && g.endedAt.After(g.openedAt) {
			g.openedAt = now
			m.sessionGate[did] = g
		}
		m.mu.Unlock()

		for _, target := range targets {
			if target != srcCh {
				m.Node.WriteFrameTo(target, fr) //nolint:errcheck
			}
		}
		return
	}

	// ---- 下行（PX4 → QGC）：帧头 deviceID = 发送方自身 ----
	// 走到这里一定来自载具端点（GCS 端点在上方 dirUplink 分支已 return），
	// 所以「未登记的地面站上行」不可能落到这里改写 px4Map。
	now := time.Now()
	// 号段闸（纵深防御）：ProcessFrame 已在入口拦过；这里再判一次，保证**直接调用**
	// 本函数的路径（内部用例、将来新增的调用方）同样造不出 GCS 段的幻象 PX4。
	if !m.validAircraftDeviceID(did) {
		m.mu.Unlock()
		m.logDropThrottled(srcCh,
			"dropped downlink for invalid deviceID=%d from %s: outside aircraft segment",
			did, srcCh)
		return
	}

	// 会话门（03 §4.3.4 / §4.7 第 3 步）：任务结束后的下行必须先等 QGC 主动上行开启门；
	// 否则晚到/重放的旧密文会在水位被清空后立刻重新晋升。判据在验签**之前**。
	if g, ok := m.sessionGate[did]; ok && g.endedAt.After(g.openedAt) {
		m.mu.Unlock()
		m.logDropThrottled(srcCh,
			"dropped downlink deviceID=%d from %s: session gate closed (waiting for QGC uplink)",
			did, srcCh)
		return
	}

	px4, ok := m.px4Map[did]
	// D13 免验签（03 §4.7 第 4 步）：条目存在 ∧ 已握手 ∧ 同通道。
	// 免验签天然不可能晋升（三条件里包含同通道），换通道/建链一律走验签。
	needVerify := !ok || !px4.handshaked || px4.channel != srcCh
	if needVerify {
		req := &verifyRequest{
			deviceID: did,
			dir:      dirDownlink,
			srcCh:    srcCh,
			fr:       fr,
			counter:  counter,
			msgID:    msgID,
			entry:    px4,
		}
		if ok {
			req.entryCh = px4.channel
		}
		if raw, ok := msg.(*message.MessageRaw); ok {
			req.payload = append([]byte(nil), raw.Payload...)
		}
		m.mu.Unlock()

		// 验签前只读不写：上面只取了快照，水位/映射/信任表一律等结果回来再改。
		if !m.enqueueVerify(req) {
			m.logDropThrottled(srcCh,
				"dropped downlink deviceID=%d: verify queue full", did)
		}
		return
	}

	// 免验签分支：srcCh 必等于 px4.channel，只做第 9–13 步（不晋升、不改通道）。
	qgcs := m.acceptDownlinkLocked(did, srcCh, counter, msgID, now)
	m.mu.Unlock()

	for _, qgc := range qgcs {
		if qgc != srcCh {
			m.Node.WriteFrameTo(qgc, fr) //nolint:errcheck
		}
	}
	// 下游副本：落点在全部过滤之后（防重放已判、权威映射已命中、方向已确认为下行）；
	// QGC 上行在此之前的分支已各自 return，到不了这里。
	if m.Sink != nil {
		m.Sink.ProcessFrame(fr)
	}
}

// fanoutTargets 计算本帧加密下行应当投递到的 QGC 通道（调用方须持有 m.mu）。
//
// 抽成函数是为了可测：扇出末尾的 `if qgc != srcCh` 会把「QGC 通道恰为 PX4 来源」那种情况
// 静默吃掉，而那个同指针夹具正是内部用例不造 gomavlib.Node 的前提 —— 判据想在没有 Node 的
// 情况下观测扇出，就必须把「算谁该收」与「真去写」分开。
//
// 两道判据缺一不可：
//   - 三元组齐备：配对的 deviceID 匹配，且其 px4Ch 就是本帧来源。上方循环已把该 deviceID
//     名下所有配对的 px4Ch 刷成本帧来源，故这条等价于「该配对指向的就是这台 PX4」。
//   - qgcSeen 新鲜：QGC 侧（80005 登记 / 加密上行）近期还在声明使用这台飞机。
//     ‼️ 这一道就是「PX4 保活不得延伸到 QGC」的落点 —— 没有它，PX4 持续下行会把 lastSeen
//     一直刷新，签出方永远收得到帧（2026-10-05 修的就是这个）。用严格小于：距上次 QGC 侧
//     证据恰好一个 TTL 即视为过期。
//     ⚠️ 这**不是** prune 那道闸的补集：prune 里判 `qgcSeen` 的是 **10×TTL** 的兜底闸（配对因此在
//     QGC 侧安静后还能再存活 9 个 TTL），判 `TTL` 的那句读的是 **lastSeen**（另一个字段，飞机在飞
//     时被 PX4 下行持续刷新）。两个判据刻意**不**互补，中间那 9 个 TTL 就是「配对活着但不投喂」。
//     ‼️ **别因为「prune 已经管了」就删掉这一道**：配对的存活由 `lastSeen` 决定，而它被 PX4 刷着
//     ⇒ 删掉本闸，扇出条件就退化成「配对存在」，签出方重新无限期收帧（2026-10-05 修的就是这个）。
//     ⚠️「中间 9 个 TTL」这一窗口**不受** `StateIdleTimeout` 影响：PX4 的每一帧下行都会把
//     该 deviceID 名下所有配对的 `lastTraffic` 一并刷新（见下行分支），空闲闸在这个窗口里
//     根本不会命中。窗口的上界仍由 prune 的 `qgcSeen ≥ 10×TTL` 兜底闸决定。
func (m *Manager) fanoutTargets(did uint32, srcCh *gomavlib.Channel, now time.Time) []*gomavlib.Channel {
	ttl := m.mapTTL()
	out := make([]*gomavlib.Channel, 0)
	for k, e := range m.pairs {
		if k.px4DeviceID == did && e.px4Ch == srcCh && now.Sub(e.qgcSeen) < ttl {
			out = append(out, k.qgcCh)
		}
	}
	return out
}

// ProcessChannelClose 处理 EventChannelClose：清理与该 channel 关联的全部状态。
// 注意：此处不清 lastNonce——PX4 断线重连（NAT 重建）counter 连续，清掉会重开防重放
// 窗口；PX4 整机重启会发回待命心跳，由 processStandbyHeartbeat 按 §2.5 清 lastNonce。
// 长时间没有真实流量的水位另有 prune 的 StateIdleTimeout 闸（默认 10 分钟）负责回收，
// 两条清路分工不同：这里按**通道**、那里按**空闲时长**。
func (m *Manager) ProcessChannelClose(evt *gomavlib.EventChannelClose) {
	ch := evt.Channel

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.qgcOnline[ch]; ok {
		delete(m.qgcOnline, ch)
		delete(m.regDiag, ch) // 诊断表同样按键持有 channel，随之回收
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
	// 候选按通道关闭删除（03 §4.3.2）：只删该通道那一条，同设备其它来源保留。
	for did, cs := range m.standbyCandidates {
		kept := cs[:0]
		for _, c := range cs {
			if c.channel != ch {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			delete(m.standbyCandidates, did)
		} else {
			m.standbyCandidates[did] = kept
		}
	}
	// 入口闸告警节流表按通道持有指针，通道关闭即回收；否则接反端口的短命通道会
	// 在这张表里无限累积（每次 NAT 重建都是一条新 channel）。
	delete(m.dropDiag, ch)
}
