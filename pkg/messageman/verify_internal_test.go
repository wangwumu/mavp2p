package messageman

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// 本文件钉住 03 §4.7 的建链期验签 / D13 免验签 / 晋升，以及 §4.2 的 oracle 契约。

// ---- fake oracle ----------------------------------------------------------

// fakeOracleRequest 是测试侧**独立**解析出来的请求（不复用实现的编码器：
// 两边共用一份编码代码时，线格式写错会一起错，用例仍是绿的）。
type fakeOracleRequest struct {
	version  byte
	reqID    uint32
	deviceID uint32
	dir      byte
	payload  []byte
}

type fakeOracleReply struct {
	delay             time.Duration
	status            byte
	raw               []byte // 非 nil 时原样回写（畸形应答用例）
	closeWithoutReply bool
}

type fakeOracle struct {
	handler  func(fakeOracleRequest) fakeOracleReply
	requests atomic.Int64

	mu   sync.Mutex
	seen []fakeOracleRequest

	ln     net.Listener
	closed atomic.Bool
	wg     sync.WaitGroup
}

func startFakeOracle(t *testing.T, handler func(fakeOracleRequest) fakeOracleReply) *fakeOracle {
	t.Helper()

	dir, err := os.MkdirTemp("", "mavp2p-oracle")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) }) //nolint:errcheck

	ln, err := net.Listen("unix", filepath.Join(dir, "verify.sock"))
	require.NoError(t, err)

	o := &fakeOracle{handler: handler, ln: ln}
	o.wg.Add(1)
	go o.serve()
	t.Cleanup(o.close)
	return o
}

func (o *fakeOracle) serve() {
	defer o.wg.Done()
	for {
		conn, err := o.ln.Accept()
		if err != nil {
			return
		}
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			o.handle(conn)
		}()
	}
}

func (o *fakeOracle) handle(c net.Conn) {
	defer c.Close() //nolint:errcheck

	hdr := make([]byte, 12) // 请求头固定 12B（版本1 + reqID4 + deviceID4 + 方向1 + 长度2）
	if _, err := io.ReadFull(c, hdr); err != nil {
		return
	}
	payload := make([]byte, int(binary.BigEndian.Uint16(hdr[10:12])))
	if _, err := io.ReadFull(c, payload); err != nil {
		return
	}
	req := fakeOracleRequest{
		version:  hdr[0],
		reqID:    binary.BigEndian.Uint32(hdr[1:5]),
		deviceID: binary.BigEndian.Uint32(hdr[5:9]),
		dir:      hdr[9],
		payload:  payload,
	}
	o.mu.Lock()
	o.seen = append(o.seen, req)
	o.mu.Unlock()
	o.requests.Add(1)

	reply := o.handler(req)
	if reply.delay > 0 {
		time.Sleep(reply.delay)
	}
	if reply.closeWithoutReply {
		return
	}
	out := reply.raw
	if out == nil {
		out = make([]byte, 5) // 应答固定 5B（reqID4 + 状态1）
		binary.BigEndian.PutUint32(out[:4], req.reqID)
		out[4] = reply.status
	}
	_, _ = c.Write(out)
}

func (o *fakeOracle) close() {
	if o.closed.CompareAndSwap(false, true) {
		o.ln.Close() //nolint:errcheck
	}
	o.wg.Wait()
}

func (o *fakeOracle) lastRequest(t *testing.T) fakeOracleRequest {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	require.NotEmpty(t, o.seen)
	return o.seen[len(o.seen)-1]
}

func passReply() fakeOracleReply { return fakeOracleReply{status: byte(verifyPass)} }

// ---- 夹具 ------------------------------------------------------------------

func newVerifyManager(t *testing.T, o *fakeOracle) *Manager {
	t.Helper()
	m := newBareManager()
	m.VerifySocket = o.path()
	m.VerifyTimeout = 500 * time.Millisecond
	m.VerifyQueuePerDevice = 8
	m.VerifyQueueGlobal = 256
	return m
}

func (o *fakeOracle) path() string {
	return o.ln.Addr().String()
}

// waitVerifyIdle 等验签队列排空（排空即结果已应用）。pending 的递减在 apply 之后，
// 因此它同时是「异步结果已经落到状态表」的同步点。
func waitVerifyIdle(t *testing.T, m *Manager) {
	t.Helper()
	v := m.verifierInstance()
	require.Eventually(t, func() bool { return v.pendingCount() == 0 },
		3*time.Second, time.Millisecond, "验签队列没有排空")
}

// verifyFrame 构造一帧加密载荷（payload 前 8B 是明文 counter）。
func verifyFrame(did uint32, msgID uint32, counter uint64, plen int) *frame.V2Frame {
	p := make([]byte, plen)
	binary.BigEndian.PutUint64(p[:8], counter)
	return &frame.V2Frame{
		IncompatibilityFlag: byte(did >> 24),
		CompatibilityFlag:   byte(did >> 16),
		SystemID:            byte(did >> 8),
		ComponentID:         byte(did),
		Message:             &message.MessageRaw{ID: msgID, Payload: p},
	}
}

type syncSink struct {
	mu sync.Mutex
	n  int
}

func (s *syncSink) ProcessFrame(frame.Frame) {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
}

func (s *syncSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// ---- 用例 ------------------------------------------------------------------

// TestVerifyWireFormatAndPromotion 钉住请求线格式 + 验签通过后的晋升：
// 新建权威映射、清空候选、刷新可信通道、写下行水位、喂 Sink。
func TestVerifyWireFormatAndPromotion(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m := newVerifyManager(t, o)
	sink := &syncSink{}
	m.Sink = sink

	ch := &gomavlib.Channel{}
	did := standbyTestDID
	other := &gomavlib.Channel{}
	m.standbyCandidates[did] = []candidate{{channel: other, sourceIP: "10.0.0.9"}}

	fr := verifyFrame(did, 33, 2, 36)
	m.processEncrypted(ch, did, fr, dirDownlink)

	waitVerifyIdle(t, m)
	require.Equal(t, int64(1), o.requests.Load())

	req := o.lastRequest(t)
	require.Equal(t, byte(1), req.version, "请求版本必须为 1")
	require.Equal(t, did, req.deviceID)
	require.Equal(t, byte(1), req.dir, "下行方向字节：0=上行,1=下行")
	require.Equal(t, fr.GetMessage().(*message.MessageRaw).Payload, req.payload,
		"请求里的加密块必须逐字节等于帧 payload")

	e, ok := m.px4Map[did]
	require.True(t, ok, "验签通过后必须建立权威映射")
	require.Same(t, ch, e.channel)
	require.False(t, e.handshaked, "非心跳帧不得置 handshaked")

	require.Empty(t, m.standbyCandidates[did], "晋升后候选必须清空，上行回到单播")
	require.Contains(t, m.lastTrustedDownlink, did)
	require.Same(t, ch, m.lastTrustedDownlink[did].channel)

	wm, ok := m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}]
	require.True(t, ok)
	require.Equal(t, uint64(2), wm.counter)
	require.Equal(t, 1, sink.count(), "验签通过的下行必须进 Sink")
}

// TestVerifyFailClosed 钉住「验签失败 ⇔ 零状态变更」：任何非 0 状态都不建映射、
// 不写水位、不刷信任、不喂 Sink。
func TestVerifyFailClosed(t *testing.T) {
	for _, st := range []verifyStatus{
		verifyTagFail, verifyNoKey, verifyInternal,
		verifyTimeout, verifyDeviceMismatch, verifyVersionUnsupported,
	} {
		t.Run(st.String(), func(t *testing.T) {
			o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
				return fakeOracleReply{status: byte(st)}
			})
			m := newVerifyManager(t, o)
			sink := &syncSink{}
			m.Sink = sink
			ch := &gomavlib.Channel{}
			did := standbyTestDID

			m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
			waitVerifyIdle(t, m)

			require.Equal(t, int64(1), o.requests.Load(), "前提自检：oracle 确实被调用")
			require.Empty(t, m.px4Map, "验签失败不得建立/改写映射")
			require.Empty(t, m.lastNonce, "验签失败不得写水位")
			require.Empty(t, m.lastTrustedDownlink, "验签失败不得刷新可信通道")
			require.Zero(t, sink.count(), "验签失败不得进 Sink")
		})
	}
}

// TestVerifyTimeoutAndMalformedReply 钉住客户端侧的两条失败路径：
// 超时按失败丢弃；应答 reqID 对不上按「不支持的版本」丢弃。
func TestVerifyTimeoutAndMalformedReply(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
			return fakeOracleReply{delay: 100 * time.Millisecond, status: byte(verifyPass)}
		})
		m := newVerifyManager(t, o)
		m.VerifyTimeout = 10 * time.Millisecond
		ch := &gomavlib.Channel{}
		did := standbyTestDID

		m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
		waitVerifyIdle(t, m)
		require.Empty(t, m.px4Map, "验签超时不得晋升")
		require.Empty(t, m.lastNonce, "验签超时不得写水位")
	})

	t.Run("reqID mismatch", func(t *testing.T) {
		o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
			raw := make([]byte, 5)
			binary.BigEndian.PutUint32(raw[:4], 0xDEADBEEF)
			raw[4] = byte(verifyPass)
			return fakeOracleReply{raw: raw}
		})
		m := newVerifyManager(t, o)
		ch := &gomavlib.Channel{}
		did := standbyTestDID

		m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
		waitVerifyIdle(t, m)
		require.Empty(t, m.px4Map, "应答无法解析必须 fail-closed")
	})
}

// TestD13SkipsOracle 钉住 D13：条目存在 ∧ 已握手 ∧ 同通道 ⇒ 不产生 IPC，
// 帧同步放行并写水位。
func TestD13SkipsOracle(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
		t.Error("D13 免验签帧不得调用 oracle")
		return passReply()
	})
	m := newVerifyManager(t, o)
	ch := &gomavlib.Channel{}
	did := standbyTestDID
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now(), handshaked: true}

	m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
	// 同步路径：无需等待。
	require.Equal(t, int64(0), o.requests.Load())
	wm, ok := m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}]
	require.True(t, ok, "免验签帧仍必须写水位")
	require.Equal(t, uint64(2), wm.counter)
}

// TestD13RequiresHandshake 钉住 D13 的开关：handshaked==false 时同通道也要逐帧验签。
// 验证通过的遥测不置位；只有验证通过的加密心跳（msgID=0 ∧ ≥T）才置位，此后免验签。
func TestD13RequiresHandshake(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m := newVerifyManager(t, o)
	ch := &gomavlib.Channel{}
	did := standbyTestDID
	m.px4Map[did] = &px4Entry{channel: ch, lastSeen: time.Now()}

	m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
	waitVerifyIdle(t, m)
	require.Equal(t, int64(1), o.requests.Load(), "未握手时同通道帧也必须验签")
	require.False(t, m.px4Map[did].handshaked, "遥测帧不得置 handshaked")
	require.Same(t, ch, m.px4Map[did].channel, "同通道不算晋升")

	m.processEncrypted(ch, did, verifyFrame(did, 0, 4, 92), dirDownlink)
	waitVerifyIdle(t, m)
	require.Equal(t, int64(2), o.requests.Load())
	require.True(t, m.px4Map[did].handshaked, "验签通过的加密心跳必须置 handshaked")

	m.processEncrypted(ch, did, verifyFrame(did, 33, 6, 36), dirDownlink)
	require.Equal(t, int64(2), o.requests.Load(), "置位后的同通道帧应免验签")
}

// TestVerifyPromotionUpdatesAllPairs 钉住晋升第 8b/8c/10 步：清候选、
// 该 deviceID 名下**全部**配对改指新通道，且晋升不改变 handshaked。
func TestVerifyPromotionUpdatesAllPairs(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m := newVerifyManager(t, o)
	did := standbyTestDID
	oldCh, newCh := &gomavlib.Channel{}, &gomavlib.Channel{}
	q1, q2 := &gomavlib.Channel{}, &gomavlib.Channel{}
	now := time.Now()

	m.px4Map[did] = &px4Entry{channel: oldCh, lastSeen: now, handshaked: true}
	m.pairs[pairKey{qgcCh: q1, px4DeviceID: did}] = &pairEntry{
		px4Ch: oldCh, lastSeen: now.Add(-time.Hour), qgcSeen: now, lastTraffic: now.Add(-time.Hour),
	}
	m.pairs[pairKey{qgcCh: q2, px4DeviceID: did}] = &pairEntry{
		px4Ch: oldCh, lastSeen: now.Add(-time.Hour), qgcSeen: now, lastTraffic: now.Add(-time.Hour),
	}
	m.standbyCandidates[did] = []candidate{{channel: newCh, sourceIP: "10.0.0.2"}}

	m.processEncrypted(newCh, did, verifyFrame(did, 33, 2, 36), dirDownlink)
	waitVerifyIdle(t, m)

	e := m.px4Map[did]
	require.Same(t, newCh, e.channel, "验签通过必须晋升到新通道")
	require.True(t, e.handshaked, "晋升不得清零 handshaked")
	require.Empty(t, m.standbyCandidates[did])
	for _, k := range []pairKey{
		{qgcCh: q1, px4DeviceID: did},
		{qgcCh: q2, px4DeviceID: did},
	} {
		p := m.pairs[k]
		require.NotNil(t, p)
		require.Same(t, newCh, p.px4Ch, "该设备**全部**配对的 px4Ch 都必须指向新通道")
		require.True(t, p.lastTraffic.After(now.Add(-time.Minute)))
	}
}

// TestVerifyResultRechecks 钉住第 7 步的四项重检（通道存活那一项见下一条用例）：
// 任一状态在排队的几毫秒里变了，迟到的「通过」都不得套用到新状态。
func TestVerifyResultRechecks(t *testing.T) {
	newReq := func(did uint32, ch *gomavlib.Channel, counter uint64) *verifyRequest {
		return &verifyRequest{
			deviceID: did, dir: dirDownlink, srcCh: ch,
			counter: counter, msgID: 33, fr: verifyFrame(did, 33, counter, 36),
		}
	}

	t.Run("watermark advanced", func(t *testing.T) {
		m := newBareManager()
		ch := &gomavlib.Channel{}
		did := standbyTestDID
		m.lastNonce[nonceKey{deviceID: did, dir: dirDownlink}] = nonceEntry{counter: 5, lastSeen: time.Now()}

		_, ok := m.applyVerifyResult(newReq(did, ch, 2), verifyPass)
		require.False(t, ok)
		require.Empty(t, m.px4Map)
	})

	t.Run("session gate closed", func(t *testing.T) {
		m := newBareManager()
		ch := &gomavlib.Channel{}
		did := standbyTestDID
		m.sessionGate[did] = gateState{endedAt: time.Now()}

		_, ok := m.applyVerifyResult(newReq(did, ch, 2), verifyPass)
		require.False(t, ok)
		require.Empty(t, m.px4Map)
	})

	t.Run("entry deleted and recreated", func(t *testing.T) {
		m := newBareManager()
		ch, other := &gomavlib.Channel{}, &gomavlib.Channel{}
		did := standbyTestDID
		old := &px4Entry{channel: ch, lastSeen: time.Now(), handshaked: true}
		m.px4Map[did] = old

		req := newReq(did, ch, 2)
		req.entry, req.entryCh = old, ch
		m.px4Map[did] = &px4Entry{channel: other, lastSeen: time.Now(), handshaked: true}

		_, ok := m.applyVerifyResult(req, verifyPass)
		require.False(t, ok, "条目已被替换时，迟到的通过不得套用到新条目")
		require.Same(t, other, m.px4Map[did].channel)
	})

	t.Run("entry channel changed", func(t *testing.T) {
		m := newBareManager()
		ch, other := &gomavlib.Channel{}, &gomavlib.Channel{}
		did := standbyTestDID
		e := &px4Entry{channel: ch, lastSeen: time.Now(), handshaked: true}
		m.px4Map[did] = e

		req := newReq(did, ch, 2)
		req.entry, req.entryCh = e, ch
		e.channel = other // 期间被别的已验签帧晋升

		_, ok := m.applyVerifyResult(req, verifyPass)
		require.False(t, ok, "通道已被别人改过时不得按旧快照晋升")
		require.Same(t, other, e.channel)
	})
}

// TestVerifyResultRejectsClosedChannel 钉住「来源通道是否仍存活」这一项。
// 通道关闭会删 px4Map/pairs；结果迟到时不得再把已死通道写回权威映射。
func TestVerifyResultRejectsClosedChannel(t *testing.T) {
	ch := closedChannel(t)
	m := newBareManager()
	did := standbyTestDID
	req := &verifyRequest{
		deviceID: did, dir: dirDownlink, srcCh: ch,
		counter: 2, msgID: 33, fr: verifyFrame(did, 33, 2, 36),
	}

	_, ok := m.applyVerifyResult(req, verifyPass)
	require.False(t, ok)
	require.Empty(t, m.px4Map)
}

// closedChannel 用真实 gomavlib 节点造一条**已关闭**的通道（IsClosed 是 gomavlib 的
// 内部状态，测试无法直接构造）。
func closedChannel(t *testing.T) *gomavlib.Channel {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	srv := &gomavlib.Node{
		Endpoints:  []gomavlib.Endpoint{&gomavlib.EndpointTCPServer{Address: addr}},
		OutVersion: gomavlib.V2, OutSystemID: 1, OutComponentID: 1,
	}
	require.NoError(t, srv.Initialize())
	t.Cleanup(srv.Close)

	cli := &gomavlib.Node{
		Endpoints:  []gomavlib.Endpoint{&gomavlib.EndpointTCPClient{Address: addr}},
		OutVersion: gomavlib.V2, OutSystemID: 1, OutComponentID: 1,
	}
	require.NoError(t, cli.Initialize())

	evt := <-srv.Events()
	open, ok := evt.(*gomavlib.EventChannelOpen)
	require.True(t, ok, "期望 EventChannelOpen，实得 %T", evt)
	ch := open.Channel
	require.False(t, ch.IsClosed())

	cli.Close()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-srv.Events():
			if _, ok := evt.(*gomavlib.EventChannelClose); ok {
				require.True(t, ch.IsClosed(), "关闭事件已到，IsClosed 必须为真")
				return ch
			}
		case <-deadline:
			t.Fatal("客户端关闭后服务端通道没有关闭事件")
			return ch
		}
	}
}

// TestVerifyQueueLimits 钉住背压：每设备队列满、全局队列满都丢弃**最新**帧。
func TestVerifyQueueLimits(t *testing.T) {
	slow := func(fakeOracleRequest) fakeOracleReply {
		return fakeOracleReply{delay: 50 * time.Millisecond, status: byte(verifyTagFail)}
	}

	newReq := func(did uint32, counter uint64) *verifyRequest {
		return &verifyRequest{
			deviceID: did, dir: dirDownlink, srcCh: &gomavlib.Channel{},
			counter: counter, msgID: 33, fr: verifyFrame(did, 33, counter, 36),
		}
	}

	t.Run("per device", func(t *testing.T) {
		o := startFakeOracle(t, slow)
		m := newVerifyManager(t, o)
		m.VerifyQueuePerDevice = 1

		require.True(t, m.enqueueVerify(newReq(standbyTestDID, 2)), "in-flight 槽位应可用")
		require.True(t, m.enqueueVerify(newReq(standbyTestDID, 4)), "队列空槽应可用")
		require.False(t, m.enqueueVerify(newReq(standbyTestDID, 6)), "队列满必须丢最新帧")
		waitVerifyIdle(t, m)
	})

	t.Run("global", func(t *testing.T) {
		o := startFakeOracle(t, slow)
		m := newVerifyManager(t, o)
		m.VerifyQueueGlobal = 1

		require.True(t, m.enqueueVerify(newReq(standbyTestDID, 2)))
		require.False(t, m.enqueueVerify(newReq(standbyTestDID+1, 2)), "全局队列满必须丢最新帧")

		// 再用一批**唯一** deviceID 撞满：全局满时必须尽早返回，不得为被拒的帧
		// 留下空队列表项 —— 那张表没有回收路径，攻击者换 deviceID 即可无界撑大内存。
		for i := uint32(2); i < 10; i++ {
			require.False(t, m.enqueueVerify(newReq(standbyTestDID+i, 2)))
		}
		v := m.verifierInstance()
		v.mu.Lock()
		rows := len(v.queues)
		v.mu.Unlock()
		require.Equal(t, 1, rows, "全局满时被拒的帧不得在 queues 留下表项")
		waitVerifyIdle(t, m)
	})
}

// TestVerifyDiagThrottlePerDeviceAndStatus 钉住 03 §8.4 第 27 条：验签失败日志按
// deviceID + 状态 10s 节流；但**不同状态必须各自立即各打一条** —— 否则排障时把
// 「tag 校验失败」误读成「超时」，会往两个完全不同的方向查。
func TestVerifyDiagThrottlePerDeviceAndStatus(t *testing.T) {
	m := newBareManager()
	now := time.Now()

	require.True(t, m.shouldLogVerifyLocked(10000001, verifyTagFail, now),
		"该设备该状态的第一条失败必须落日志")
	require.False(t, m.shouldLogVerifyLocked(10000001, verifyTagFail, now.Add(5*time.Second)),
		"同一设备同一状态 10s 内必须节流（否则 20Hz 遥测每帧一条，淹没晋升日志）")
	require.True(t, m.shouldLogVerifyLocked(10000001, verifyTagFail, now.Add(kVerifyDiagInterval)),
		"满 10s 必须再放行一条，否则一次长期故障只有开头一条日志")
	require.True(t, m.shouldLogVerifyLocked(10000001, verifyTimeout, now),
		"状态从 tag-fail 切到 timeout，必须立即各打一条")
	require.True(t, m.shouldLogVerifyLocked(10000002, verifyTagFail, now),
		"不同设备互不节流：一台失败不能压掉另一台的日志")
}

// TestSessionGateBlocksBeforeVerify 覆盖 03 §8.2 第 17 条：会话门关闭时，需要验签的
// 下行必须在**调用 oracle 之前**整帧丢弃 —— 不查表、不排空、不写水位、不喂 Sink。
// 现有「结果迟到时门已关」用例只测了竞态窗口，测不到这道前置闸。
func TestSessionGateBlocksBeforeVerify(t *testing.T) {
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply { return passReply() })
	m := newVerifyManager(t, o)
	sink := &syncSink{}
	m.Sink = sink
	ch := &gomavlib.Channel{}
	const did = standbyTestDID

	// 任务结束后的形态：会话门关着（endedAt > openedAt）。
	m.sessionGate[did] = gateState{endedAt: time.Now()}

	m.processEncrypted(ch, did, verifyFrame(did, 33, 2, 36), dirDownlink)
	waitVerifyIdle(t, m)

	require.Equal(t, int64(0), o.requests.Load(), "门关时必须整帧丢弃，不得调用 oracle")
	require.Empty(t, m.px4Map, "门关时不得建立权威映射")
	require.Empty(t, m.lastNonce, "门关时不得推进水位")
	require.Equal(t, 0, sink.count(), "门关时不得喂 Sink")
}

// TestPromotionMakesSameChannelSkipVerify 覆盖 03 §8.6 第 43/44 条的另一面：
// 换通道晋升成功后，**同一新通道**的后续下行走 D13 免验签，不再产生 oracle 请求；
// 再换一条通道则重新验签。只断言「晋升发生了」测不到这个免验签效果。
func TestPromotionMakesSameChannelSkipVerify(t *testing.T) {
	var requests atomic.Int64
	o := startFakeOracle(t, func(fakeOracleRequest) fakeOracleReply {
		requests.Add(1)
		return passReply()
	})
	m := newVerifyManager(t, o)
	m.Sink = &syncSink{}
	const did = standbyTestDID

	ch1 := &gomavlib.Channel{}
	// 首帧必须是**加密心跳**（msgID=0、payload ≥ T）：D13 的第三个条件是 handshaked，
	// 而 handshaked 只在「验签通过的加密心跳」上置位（verify.go 第 9 步）。
	m.processEncrypted(ch1, did, verifyFrame(did, 0, 2, 92), dirDownlink)
	waitVerifyIdle(t, m)
	require.Equal(t, int64(1), requests.Load(), "首帧必须验签")

	// 晋升后同通道：主循环同步路径，喂完即可断言。
	m.processEncrypted(ch1, did, verifyFrame(did, 33, 3, 36), dirDownlink)
	require.Equal(t, int64(1), requests.Load(), "晋升后同通道必须免验签（D13）")

	// 换通道：D13 不成立，必须重新验签。
	ch2 := &gomavlib.Channel{}
	m.processEncrypted(ch2, did, verifyFrame(did, 33, 4, 36), dirDownlink)
	waitVerifyIdle(t, m)
	require.Equal(t, int64(2), requests.Load(), "换通道必须重新验签")
}
