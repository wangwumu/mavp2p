package messageman

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
)

// 本地验签服务（oracle）的线格式与状态码（03 §4.2）。
//
// 请求: [1B 版本=1][4B reqID][4B deviceID][1B 方向: 0=上行,1=下行][2B payloadLen][payload]
// 应答: [4B reqID][1B 状态]
//
// 方向只用于审计/日志，**不是密码学绑定**；oracle 也不区分「谁是发送方」，
// 区分 PX4 与 QGC 的心跳靠入口的固定阈值 T（见 entry.go）。
const (
	verifyVersion      byte = 1
	verifyRequestHdrLn      = 12
	verifyReplyLen          = 5
	verifyDirUplink    byte = 0
	verifyDirDownlink  byte = 1
)

// 验签状态码（03 §4.2）。除 verifyPass 外一律 fail-closed：整帧丢弃、零状态变更。
type verifyStatus uint8

const (
	verifyPass verifyStatus = iota
	verifyTagFail
	verifyNoKey
	verifyInternal
	verifyTimeout
	verifyDeviceMismatch
	verifyVersionUnsupported
)

func (s verifyStatus) String() string {
	switch s {
	case verifyPass:
		return "pass"
	case verifyTagFail:
		return "tag-fail"
	case verifyNoKey:
		return "no-key"
	case verifyInternal:
		return "service-error"
	case verifyTimeout:
		return "timeout"
	case verifyDeviceMismatch:
		return "device-mismatch"
	case verifyVersionUnsupported:
		return "version"
	default:
		return "unknown"
	}
}

// 连接失败后的指数退避上下界。验签服务不可用时请求立即失败（不占满 5ms 超时预算），
// 但服务恢复后的首次重试最多等这么久。
const (
	verifyBackoffMin = 100 * time.Millisecond
	verifyBackoffMax = time.Second
)

// verifyRequest 是一帧待验签的加密下行。字段大多是入队时对当前状态的**只读快照**，
// 结果返回后 Step 7 用它判断「这一帧是否仍然可晋升」（03 §4.7 第 7 步）。
type verifyRequest struct {
	reqID    uint32
	deviceID uint32
	dir      direction
	payload  []byte // 加密块副本（帧可能被主循环复用）
	srcCh    *gomavlib.Channel
	fr       frame.Frame // 验签通过后仍要转发/喂 Sink 的原帧
	counter  uint64
	msgID    uint32

	// 入队时的权威映射快照：nil 表示当时没有条目。entryCh 是当时的通道；
	// 结果返回时只要条目指针或通道任一变化，就说明期间已有别的帧改了权威映射，
	// 本帧不得按旧快照晋升（03 §4.7 第 7 步末条）。
	entry   *px4Entry
	entryCh *gomavlib.Channel
}

// kVerifyDiagInterval 是「同一 deviceID + 同一状态」验签失败日志的节流窗口。
// 服务不可用时每一帧都会失败（20Hz 遥测），不节流会淹掉晋升/上线日志。
const kVerifyDiagInterval = 10 * time.Second

// verifyDiagKey 是验签诊断节流键。状态参与键：同一设备从 tag 失败切换到超时
// 仍应立即各打一条，否则排障时会误以为只有一种失败。
type verifyDiagKey struct {
	deviceID uint32
	status   verifyStatus
}

// shouldLogVerifyLocked 决定本次验签失败是否落日志（调用方须持有 m.mu）。
func (m *Manager) shouldLogVerifyLocked(did uint32, st verifyStatus, now time.Time) bool {
	if m.verifyDiag == nil {
		m.verifyDiag = make(map[verifyDiagKey]time.Time)
	}
	k := verifyDiagKey{deviceID: did, status: st}
	if t, ok := m.verifyDiag[k]; ok && now.Sub(t) < kVerifyDiagInterval {
		return false
	}
	m.verifyDiag[k] = now
	return true
}

// verifyDeviceQueue 是单设备的验签队列。
//
// head 是已被认领的那一条（in-flight 或即将被 drain 取走），pending 只含**排队**的条目。
// 分开存是为了让「每设备队列上限」与「同一时刻 1 个 in-flight」互不污染：若把 head 留在
// pending 里，enqueue 与 drain 抢跑的那个瞬间会多占一个队位，上限变成 perDevice-1。
type verifyDeviceQueue struct {
	head    *verifyRequest
	pending []*verifyRequest
	busy    bool
}

// verifier 是 oracle 的异步客户端（03 §4.2）。
//
// 同 deviceID 保序：每个设备同一时刻最多 1 个 in-flight，队列入队顺序执行；
// 全局上限按「排队 + in-flight」计数，满时丢弃最新帧（背压）。
type verifier struct {
	m         *Manager
	socket    string
	timeout   time.Duration
	perDevice int
	global    int

	mu      sync.Mutex
	queues  map[uint32]*verifyDeviceQueue
	pending int
	seq     uint32
	retryAt time.Time
	backoff time.Duration
}

// verifierInstance 懒创建 verifier（内部用例直接构造 Manager，不走 Initialize）。
func (m *Manager) verifierInstance() *verifier {
	m.verifierMu.Lock()
	defer m.verifierMu.Unlock()
	if m.verifier == nil {
		m.verifier = &verifier{
			m:         m,
			socket:    m.verifySocket(),
			timeout:   m.verifyTimeout(),
			perDevice: m.verifyQueuePerDevice(),
			global:    m.verifyQueueGlobal(),
			queues:    make(map[uint32]*verifyDeviceQueue),
		}
	}
	return m.verifier
}

// enqueueVerify 把一帧放入验签队列。返回 false 表示队列满，调用方应丢弃该帧。
func (m *Manager) enqueueVerify(req *verifyRequest) bool {
	v := m.verifierInstance()

	v.mu.Lock()
	// ‼️ 全局容量先判：被拒的帧不得留下队列表项。表项只在 drain 把设备队列排空时
	// 删除，没有别的回收路径 —— 先建表项再拒绝，等于让「换 deviceID 撞满全局上限」
	// 的流量无界撑大 queues（每个新 deviceID 留一个空条目，永不回收）。
	if v.pending >= v.global {
		v.mu.Unlock()
		return false
	}
	dq := v.queues[req.deviceID]
	if dq == nil {
		dq = &verifyDeviceQueue{}
		v.queues[req.deviceID] = dq
	}
	start := !dq.busy
	if start {
		dq.head = req
		dq.busy = true
	} else {
		if len(dq.pending) >= v.perDevice {
			v.mu.Unlock()
			return false
		}
		dq.pending = append(dq.pending, req)
	}
	v.seq++
	req.reqID = v.seq
	v.pending++
	v.mu.Unlock()

	if start {
		go v.drain(req.deviceID)
	}
	return true
}

// drain 顺序处理某设备的队列。队列空即退出并删除表项，避免设备数把这张表撑大；
// drain 退出与新请求入队之间由 v.mu 串行化，不会出现「有活没人在干」。
func (v *verifier) drain(did uint32) {
	for {
		v.mu.Lock()
		dq := v.queues[did]
		if dq == nil {
			v.mu.Unlock()
			return
		}
		req := dq.head
		dq.head = nil
		if req == nil {
			if len(dq.pending) == 0 {
				dq.busy = false
				delete(v.queues, did)
				v.mu.Unlock()
				return
			}
			req = dq.pending[0]
			dq.pending[0] = nil
			dq.pending = dq.pending[1:]
		}
		v.mu.Unlock()

		status := v.call(req)
		targets, accepted := v.m.applyVerifyResult(req, status)
		if accepted && v.m.Node != nil {
			// 内部用例直接构造 Manager（无 Node）时只验证状态机，不实际写出。
			for _, target := range targets {
				if target != req.srcCh {
					v.m.Node.WriteFrameTo(target, req.fr) //nolint:errcheck
				}
			}
		}
		if accepted && v.m.Sink != nil {
			v.m.Sink.ProcessFrame(req.fr)
		}

		v.mu.Lock()
		v.pending--
		v.mu.Unlock()
	}
}

// pendingCount 仅供测试观察队列是否排空（排空 ⇒ 结果已应用）。
func (v *verifier) pendingCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.pending
}

// call 同步执行一次验签请求。失败一律折成状态码，调用方无需区分网络错误类型。
func (v *verifier) call(req *verifyRequest) verifyStatus {
	v.mu.Lock()
	if !v.retryAt.IsZero() && time.Now().Before(v.retryAt) {
		v.mu.Unlock()
		return verifyInternal
	}
	socket, timeout := v.socket, v.timeout
	v.mu.Unlock()

	conn, err := net.DialTimeout("unix", socket, timeout)
	if err != nil {
		v.noteFailure()
		return verifyInternal
	}
	defer conn.Close() //nolint:errcheck
	_ = conn.SetDeadline(time.Now().Add(timeout))

	buf := make([]byte, verifyRequestHdrLn+len(req.payload))
	buf[0] = verifyVersion
	binary.BigEndian.PutUint32(buf[1:5], req.reqID)
	binary.BigEndian.PutUint32(buf[5:9], req.deviceID)
	if req.dir == dirDownlink {
		buf[9] = verifyDirDownlink
	} else {
		buf[9] = verifyDirUplink
	}
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(req.payload)))
	copy(buf[verifyRequestHdrLn:], req.payload)

	if _, err := conn.Write(buf); err != nil {
		v.noteFailure()
		return verifyInternal
	}
	resp := make([]byte, verifyReplyLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return verifyTimeout
		}
		v.noteFailure()
		return verifyInternal
	}
	v.noteSuccess()

	// 应答无法解析（reqID 不符、状态越界）按 03 §4.2 视为「不支持的版本」。
	if binary.BigEndian.Uint32(resp[:4]) != req.reqID {
		return verifyVersionUnsupported
	}
	st := verifyStatus(resp[4])
	if st > verifyVersionUnsupported {
		return verifyVersionUnsupported
	}
	return st
}

func (v *verifier) noteFailure() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.backoff == 0 {
		v.backoff = verifyBackoffMin
	} else if v.backoff *= 2; v.backoff > verifyBackoffMax {
		v.backoff = verifyBackoffMax
	}
	v.retryAt = time.Now().Add(v.backoff)
}

func (v *verifier) noteSuccess() {
	v.mu.Lock()
	v.backoff = 0
	v.retryAt = time.Time{}
	v.mu.Unlock()
}

// applyVerifyResult 在验签结果返回后执行 03 §4.7 第 7–13 步。
//
// 第 7 步的四项重检全部在 m.mu 内完成：期间可能有别的帧推进了水位、任务结束关上了
// 会话门、通道已关闭、或权威映射已被替换 —— 任一发生都整帧丢弃，**不按旧快照晋升**。
// 返回 fanout 目标与「是否被接受」（接受才写水位 / 喂 Sink）。
func (m *Manager) applyVerifyResult(req *verifyRequest, status verifyStatus) ([]*gomavlib.Channel, bool) {
	m.mu.Lock()
	now := time.Now()
	// 验签失败日志：按 deviceID+状态 10s 节流（verifyDiag）。状态本身要能区分
	// tag 失败 / 无密钥 / 服务错误 / 超时 / deviceID 不匹配 / 版本（03 §8.4 第 27 条）。
	// 通过不在这里打：成功路径已有「上线/晋升」日志，逐设备 10s 一条 pass 只会淹没它。
	if status != verifyPass && m.shouldLogVerifyLocked(req.deviceID, status, now) {
		log.Printf("verify failed: deviceID=%d status=%s", req.deviceID, status)
	}
	if status != verifyPass {
		m.mu.Unlock()
		return nil, false
	}

	// 通道关闭会同时删掉 px4Map/pairs；结果迟到时不得再把已死通道写回权威映射。
	if req.srcCh == nil || req.srcCh.IsClosed() {
		m.mu.Unlock()
		return nil, false
	}
	k := nonceKey{deviceID: req.deviceID, dir: dirDownlink}
	if last, ok := m.lastNonce[k]; ok && req.counter <= last.counter {
		m.mu.Unlock()
		return nil, false
	}
	if g, ok := m.sessionGate[req.deviceID]; ok && g.endedAt.After(g.openedAt) {
		m.mu.Unlock()
		return nil, false
	}
	cur := m.px4Map[req.deviceID]
	if cur != req.entry || (cur != nil && cur.channel != req.entryCh) {
		// 条目已删、已换、或期间换了通道 ⇒ 本帧的「通过」不能套用到新状态。
		m.mu.Unlock()
		return nil, false
	}

	// 第 8 步：判定来源通道。cur == nil 或 cur.channel != srcCh 都是「晋升」：
	// 新建/替换权威映射、清空候选、把该设备**全部**配对的 px4Ch 指向新通道。
	switch {
	case cur == nil:
		cur = &px4Entry{channel: req.srcCh}
		m.px4Map[req.deviceID] = cur
		delete(m.standbyCandidates, req.deviceID)
		log.Printf("PX4 appeared (verified downlink): deviceID=%d channel=%s", req.deviceID, req.srcCh)
	case cur.channel != req.srcCh:
		log.Printf("PX4 channel promoted: deviceID=%d %s -> %s", req.deviceID, cur.channel, req.srcCh)
		cur.channel = req.srcCh
		delete(m.standbyCandidates, req.deviceID)
	}

	targets := m.acceptDownlinkLocked(req.deviceID, req.srcCh, req.counter, req.msgID, now)
	m.mu.Unlock()
	return targets, true
}

// acceptDownlinkLocked 是「验签通过」与「D13 免验签」共用的第 9–13 步
// （调用方须持有 m.mu，且 m.px4Map[did] 已指向本帧的权威通道）。
//
// 第 9 步的 handshaked 置位**只**发生在这里：入口已保证 msgID=0 的下行 payload ≥ T；
// 免验签分支到达时它本来已是 true，此处无变化。
func (m *Manager) acceptDownlinkLocked(did uint32, srcCh *gomavlib.Channel, counter uint64, msgID uint32, now time.Time) []*gomavlib.Channel {
	px4 := m.px4Map[did]
	px4.lastSeen = now
	m.lastTrustedDownlink[did] = trustedMark{
		at: now, channel: srcCh, sourceIP: channelSourceIP(srcCh),
	}
	if msgID == msgIDHeartbeat {
		px4.handshaked = true
	}
	// 第 10 步：该 deviceID 名下全部配对指向本帧来源。
	// 真实下行 ⇒ 配对承载了流量（投给谁仍由 qgcSeen 单独决定）。
	for k, e := range m.pairs {
		if k.px4DeviceID == did {
			e.px4Ch = srcCh
			e.lastSeen = now
			e.lastTraffic = now
		}
	}
	// 第 11 步：扇出目标（空扇出不是丢弃）。
	qgcs := m.fanoutTargets(did, srcCh, now)
	m.logDownlinkDiag(did, srcCh, qgcs, now)
	// 第 12 步：写下行水位。D12：只刷新本方向，不续期上行。
	m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}] = nonceEntry{counter: counter, lastSeen: now}
	return qgcs
}

// logDownlinkDiag 打印下行扇出观测 + 空扇出告警（调用方须持有 m.mu）。
//
// 10s 节流，把该 deviceID 名下每个配对的扇出判据拆开打，区分「配对不存在」
// 「px4Ch 不匹配」「qgcSeen 过期」三种被排除的原因。空扇出本身不是丢弃：
// 水位照写、Sink 照喂，否则 QGC 全部签出后 data_writer 会断流。
func (m *Manager) logDownlinkDiag(did uint32, srcCh *gomavlib.Channel, qgcs []*gomavlib.Channel, now time.Time) {
	if m.fanoutDiag == nil {
		m.fanoutDiag = make(map[uint32]time.Time)
	}
	if last, ok := m.fanoutDiag[did]; !ok || now.Sub(last) >= 10*time.Second {
		m.fanoutDiag[did] = now
		type pairDiag struct {
			qgc      string
			px4Match bool
			qgcAgeMs int64
		}
		ds := make([]pairDiag, 0, len(m.pairs))
		for k, e := range m.pairs {
			if k.px4DeviceID != did {
				continue
			}
			ds = append(ds, pairDiag{
				qgc:      fmt.Sprintf("%p", k.qgcCh),
				px4Match: e.px4Ch == srcCh,
				qgcAgeMs: now.Sub(e.qgcSeen).Milliseconds(),
			})
		}
		log.Printf("downlink diag: deviceID=%d src=%p pairs=%d fanout=%d ttl=%s detail=%v",
			did, srcCh, len(ds), len(qgcs), m.mapTTL(), ds)
	}
	if len(qgcs) == 0 {
		if m.emptyDownlinkLogged == nil {
			m.emptyDownlinkLogged = make(map[uint32]time.Time)
		}
		if last, ok := m.emptyDownlinkLogged[did]; !ok || now.Sub(last) >= 10*time.Second {
			m.emptyDownlinkLogged[did] = now
			// ‼️ 不是丢弃：空扇出仍会写水位并喂 Sink（data_writer 不能断流）。
			// 旧文案写成 dropped，运维会照着去查「遥测为什么丢了」。
			log.Printf("no QGC fanout for PX4 deviceID=%d (%s): frame still passes to watermark and Sink", did, srcCh)
		}
	}
}
