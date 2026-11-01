package messageman

import (
	"net"
	"sort"
	"time"

	"github.com/bluenviron/gomavlib/v4"
)

// candidate 是明文待命心跳登记的上行投递候选（03 §4.3.2）。
//
// 候选只用于「歧义期把 QGC 上行多播给可能的目标」，**永不**写入 px4Map 或
// pairs.px4Ch。同一 sourceIP 只保留一条：端口变化刷新 channel，不新增。
type candidate struct {
	channel   *gomavlib.Channel // 当前投递地址
	sourceIP  string            // 来源 IP（端口变化视为同源刷新）
	lastSeen  time.Time
	protected bool // 最近可信通道的来源 IP；不被普通候选挤出
}

// tokenBucket 是简单的令牌桶（容量=每秒速率，即允许 1 秒的突发）。
// 调用方须持有 m.mu。now 由调用方传入，便于测试注入。
type tokenBucket struct {
	tokens float64
	last   time.Time
}

func (b *tokenBucket) take(now time.Time, rate int) bool {
	if rate <= 0 {
		return false
	}
	capacity := float64(rate)
	if b.last.IsZero() {
		b.tokens = capacity
	} else {
		b.tokens += now.Sub(b.last).Seconds() * float64(rate)
		if b.tokens > capacity {
			b.tokens = capacity
		}
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sourceIPFromAddr 从远端地址取 IP；无端口地址退化为原字符串，nil 返回空串。
// IPv6 的 host 不带方括号（net.SplitHostPort 已剥离）。
func sourceIPFromAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

// channelSourceIP 返回通道的 sourceIP（候选按它去重）。非网络链路返回空串。
func channelSourceIP(ch *gomavlib.Channel) string {
	if ch == nil {
		return ""
	}
	return sourceIPFromAddr(ch.RemoteAddr())
}

// candidateCountLocked 返回候选表总条数（调用方须持有 m.mu）。
func (m *Manager) candidateCountLocked() int {
	n := 0
	for _, cs := range m.standbyCandidates {
		n += len(cs)
	}
	return n
}

// oldestIndex 返回 cs 中最久未刷新的指定类别下标；没有返回 -1。
func oldestIndex(cs []candidate, protected bool) int {
	idx := -1
	for i := range cs {
		if cs[i].protected != protected {
			continue
		}
		if idx < 0 || cs[i].lastSeen.Before(cs[idx].lastSeen) {
			idx = i
		}
	}
	return idx
}

// evictOldestGlobalLocked 从全表淘汰最久未刷新的指定类别候选。
// excludeDID 非 0 时跳过该设备（保护候选的兜底淘汰只动「其他设备」）。
func (m *Manager) evictOldestGlobalLocked(protected bool, excludeDID uint32) bool {
	bestDID := uint32(0)
	bestIdx := -1
	var best time.Time
	for did, cs := range m.standbyCandidates {
		if excludeDID != 0 && did == excludeDID {
			continue
		}
		idx := oldestIndex(cs, protected)
		if idx < 0 {
			continue
		}
		if bestIdx < 0 || cs[idx].lastSeen.Before(best) {
			bestDID, bestIdx, best = did, idx, cs[idx].lastSeen
		}
	}
	if bestIdx < 0 {
		return false
	}
	cs := m.standbyCandidates[bestDID]
	cs = append(cs[:bestIdx], cs[bestIdx+1:]...)
	if len(cs) == 0 {
		delete(m.standbyCandidates, bestDID)
	} else {
		m.standbyCandidates[bestDID] = cs
	}
	return true
}

// admitNewCandidate 判断是否允许某设备新增一条来源 IP。
// 同 IP 刷新、以及任务结束的保护候选不走这里（03 §4.3.2）。
// 调用方须持有 m.mu。
func (m *Manager) admitNewCandidate(did uint32, now time.Time) bool {
	if m.lastCandidateAdmit == nil {
		m.lastCandidateAdmit = make(map[uint32]time.Time)
	}
	if t, ok := m.lastCandidateAdmit[did]; ok && now.Sub(t) < m.candidateAdmitInterval() {
		return false
	}
	if !m.admitBucket.take(now, m.globalCandidateAdmitRate()) {
		return false
	}
	m.lastCandidateAdmit[did] = now
	return true
}

// upsertStandbyCandidate 登记/刷新候选（调用方须持有 m.mu）。
//
// protected=true 是任务结束的**强制插入**：不受每设备准入间隔、全局速率与全局上限
// 限制；容量不足时先淘汰最久未刷新的非保护候选，必要时淘汰其他设备的保护候选。
// 返回 false 表示被准入或容量拒绝（调用方不应再发布该帧）。
func (m *Manager) upsertStandbyCandidate(did uint32, ch *gomavlib.Channel, srcIP string, now time.Time, protected bool) bool {
	if m.standbyCandidates == nil {
		m.standbyCandidates = make(map[uint32][]candidate)
	}
	cs := m.standbyCandidates[did]

	// 同一 sourceIP：刷新 channel/lastSeen，不新增、不受准入限制。
	for i := range cs {
		if cs[i].sourceIP != srcIP {
			continue
		}
		if protected {
			for j := range cs {
				cs[j].protected = j == i
			}
		}
		cs[i].channel = ch
		cs[i].lastSeen = now
		cs[i].protected = cs[i].protected || protected
		m.standbyCandidates[did] = cs
		return true
	}

	if !protected {
		// 全局表满：拒绝新来源 IP（已有候选的同 IP 刷新已在上方返回）。
		// 先判容量再取准入令牌，避免「未入表却消耗一次准入」。
		if m.candidateCountLocked() >= m.maxCandidatesGlobal() {
			return false
		}
		if !m.admitNewCandidate(did, now) {
			return false
		}
	} else {
		// 同一设备至多 1 条保护候选：旧的先降级，容量不足时它也可被淘汰。
		for j := range cs {
			cs[j].protected = false
		}
	}

	// 每设备容量：优先淘汰最久未刷新的非保护候选。
	for len(cs) >= m.maxCandidatesPerDevice() {
		idx := oldestIndex(cs, false)
		if idx < 0 {
			if !protected {
				m.standbyCandidates[did] = cs
				return false
			}
			idx = oldestIndex(cs, true)
			if idx < 0 {
				m.standbyCandidates[did] = cs
				return false
			}
		}
		cs = append(cs[:idx], cs[idx+1:]...)
	}
	m.standbyCandidates[did] = cs

	// 全局容量：保护候选强制插入；普通候选在全局满时已被上方拒绝。
	for m.candidateCountLocked() >= m.maxCandidatesGlobal() {
		if m.evictOldestGlobalLocked(false, 0) {
			// 淘汰可能删的正是**本设备**刚写回 map 的候选：局部 cs 已过期，
			// 必须重读，否则下面的 append 会把它复活（全局上限被突破、可能重复 sourceIP）。
			cs = m.standbyCandidates[did]
			continue
		}
		if !protected || !m.evictOldestGlobalLocked(true, did) {
			return false
		}
		cs = m.standbyCandidates[did]
	}

	cs = append(cs, candidate{
		channel:   ch,
		sourceIP:  srcIP,
		lastSeen:  now,
		protected: protected,
	})
	m.standbyCandidates[did] = cs
	return true
}

// shouldPublishStandby 判断候选路径本帧是否发布（调用方须持有 m.mu）。
// 只有实际发布时才推进 lastStandbyPublish；任务结束路径不调用本函数。
func (m *Manager) shouldPublishStandby(did uint32, now time.Time) bool {
	if t, ok := m.lastStandbyPublish[did]; ok && now.Sub(t) < m.standbyFanoutInterval() {
		return false
	}
	if !m.fanoutBucket.take(now, m.globalFanoutRate()) {
		return false
	}
	if m.lastStandbyPublish == nil {
		m.lastStandbyPublish = make(map[uint32]time.Time)
	}
	m.lastStandbyPublish[did] = now
	return true
}

// uplinkTargetsLocked 计算上行目标集（调用方须持有 m.mu）：
// 已验签通道 → 保护候选 → 其余候选按 lastSeen 新到旧，去重后裁到 MaxUplinkTargets。
func (m *Manager) uplinkTargetsLocked(did uint32) []*gomavlib.Channel {
	limit := m.maxUplinkTargets()
	out := make([]*gomavlib.Channel, 0, limit)
	seen := make(map[*gomavlib.Channel]bool, limit)
	add := func(ch *gomavlib.Channel) {
		if ch == nil || seen[ch] || len(out) >= limit {
			return
		}
		seen[ch] = true
		out = append(out, ch)
	}

	if e, ok := m.px4Map[did]; ok {
		add(e.channel)
	}
	cs := append([]candidate(nil), m.standbyCandidates[did]...)
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].protected != cs[j].protected {
			return cs[i].protected
		}
		return cs[i].lastSeen.After(cs[j].lastSeen)
	})
	for _, c := range cs {
		if len(out) >= limit {
			break
		}
		add(c.channel)
	}
	return out
}

// standbyFanoutTargets 返回待命心跳的发布目标：声明过该 deviceID 且 qgcSeen 新鲜的 QGC
// （调用方须持有 m.mu）。候选路径与任务结束共用；后者必须在删除配对**之前**取快照。
func (m *Manager) standbyFanoutTargets(did uint32, now time.Time) []*gomavlib.Channel {
	ttl := m.mapTTL()
	out := make([]*gomavlib.Channel, 0)
	for k, e := range m.pairs {
		if k.px4DeviceID == did && now.Sub(e.qgcSeen) < ttl {
			out = append(out, k.qgcCh)
		}
	}
	return out
}
