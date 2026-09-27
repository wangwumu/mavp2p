package messageman_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mavp2p/pkg/messageman"
)

// 协议参数（与 manager.go 默认一致）
const (
	gcsDeviceID    = uint32(10000)    // GCS 段固定值（QGC 登记心跳帧头 deviceID）
	px4DeviceID    = uint32(10000001) // PX4 段 deviceID（>= 1e7）
	gcsDeviceIDMax = uint32(10000000) // 号段分界
)

// peer 是一个对端（QGC 或 PX4）：node 用于接收帧（Events），ch 是 server 端为该对端分配的 channel。
type peer struct {
	node *gomavlib.Node
	ch   *gomavlib.Channel
}

// newServer 启动 server node（mavp2p 角色，空 dialect）+ 会话路由器，返回清理函数。
func newServer(t *testing.T, port string) (*gomavlib.Node, *messageman.Manager, func()) {
	t.Helper()

	node := &gomavlib.Node{
		Endpoints: []gomavlib.Endpoint{
			&gomavlib.EndpointTCPServer{Address: "127.0.0.1:" + port},
		},
		OutVersion:     gomavlib.V2,
		OutSystemID:    125,
		OutComponentID: 191,
		// 空 dialect：所有消息保持 MessageRaw 原样透传（协议定制，见 main.go generateDialect）
		Dialect: &dialect.Dialect{Version: 3},
	}
	err := node.Initialize()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	m := &messageman.Manager{
		Ctx: ctx,
		Wg:  &wg,
		Config: messageman.Config{
			GCSDeviceIDMax: gcsDeviceIDMax,
		},
		Node: node,
	}
	err = m.Initialize()
	require.NoError(t, err)

	return node, m, func() {
		cancel()
		wg.Wait()
		node.Close()
	}
}

// connectPeer 建立一个 client 对端并返回（含 server 端 channel）。
func connectPeer(t *testing.T, node *gomavlib.Node, port string) *peer {
	t.Helper()

	c := &gomavlib.Node{
		Endpoints: []gomavlib.Endpoint{
			&gomavlib.EndpointTCPClient{Address: "127.0.0.1:" + port},
		},
		OutVersion:     gomavlib.V2,
		OutSystemID:    1,
		OutComponentID: 1,
		// 无 dialect → 收 MessageRaw，不校验 CRC
	}
	err := c.Initialize()
	require.NoError(t, err)

	evt := <-node.Events()
	co, ok := evt.(*gomavlib.EventChannelOpen)
	require.True(t, ok, "expected EventChannelOpen on server, got %T", evt)

	// client 侧消费自己的连接事件
	<-c.Events()

	return &peer{node: c, ch: co.Channel}
}

// didBytes 拆分 deviceID 为帧头 4 字节（incompat/compat/sysid/compid，§1.2 编码公式）。
func didBytes(did uint32) (byte, byte, byte, byte) {
	return byte(did >> 24), byte(did >> 16), byte(did >> 8), byte(did)
}

// makeFrame 构造 V2 帧（手动指定帧头 deviceID 四字节 + MessageRaw payload）。
// Checksum 随意：messageman 不解密不校验 CRC，client 无 dialect 收 MessageRaw 也不校验。
func makeFrame(inc, com, sys, comp byte, msgID uint32, payload []byte) *frame.V2Frame {
	return &frame.V2Frame{
		IncompatibilityFlag: inc,
		CompatibilityFlag:   com,
		SequenceNumber:      0,
		SystemID:            sys,
		ComponentID:         comp,
		Message:             &message.MessageRaw{ID: msgID, Payload: payload},
		Checksum:            0,
	}
}

// feed 把帧喂给会话路由器（模拟某 channel 收到）。
func feed(m *messageman.Manager, ch *gomavlib.Channel, fr *frame.V2Frame) {
	m.ProcessFrame(&gomavlib.EventFrame{Frame: fr, Channel: ch})
}

// expectFrame 阻塞等待一个 EventFrame（带超时）。
func expectFrame(t *testing.T, p *peer, timeout time.Duration) *gomavlib.EventFrame {
	t.Helper()
	select {
	case evt := <-p.node.Events():
		ef, ok := evt.(*gomavlib.EventFrame)
		require.True(t, ok, "expected EventFrame, got %T", evt)
		return ef
	case <-time.After(timeout):
		t.Fatal("timeout waiting for frame")
		return nil
	}
}

// expectNoFrame 断言超时内没有 EventFrame（其他事件如连接事件忽略）。
func expectNoFrame(t *testing.T, p *peer, timeout time.Duration) {
	t.Helper()
	for {
		select {
		case evt := <-p.node.Events():
			if _, ok := evt.(*gomavlib.EventFrame); ok {
				t.Fatalf("unexpected frame received")
			}
		case <-time.After(timeout):
			return
		}
	}
}

// encryptedPayload 构造加密帧 payload：counter(8B 明文) + 密文。
func encryptedPayload(counter uint64) []byte {
	p := make([]byte, 8+4)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// encryptedNonHeartbeatPayload 构造**非心跳**（msgID != 0）的加密帧 payload：
// counter(8B 明文) + 密文 28B = 36B ≥ encryptedPayloadMinLen。
//
// 与 encryptedPayload（仅 12B，短于阈值）的分工：后者只能验证「短帧不受拦截判据
// 影响」，本夹具才把长度也做到达标，从而单独钉住拦截判据的 **msgID 维度**。
func encryptedNonHeartbeatPayload(counter uint64) []byte {
	p := make([]byte, 8+28)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// frameCounter 读取帧 payload 明文前 8 字节 counter（加密帧）。
//
// 读不出来一律 fail-fast，**不返回 0**：返回 0 会让「帧类型不符 / payload 太短」
// 这种夹具或实现故障伪装成「counter 就是 0」——断言照样红，但红出来的信息把排查
// 引向错误方向（去找谁发了 counter=0 的帧）。与 main_test.go 的同名 helper 同形。
func frameCounter(t *testing.T, ef *gomavlib.EventFrame) uint64 {
	t.Helper()

	raw, ok := ef.Frame.GetMessage().(*message.MessageRaw)
	require.True(t, ok, "空 dialect 下应收到 MessageRaw")
	require.GreaterOrEqual(t, len(raw.Payload), 8, "加密帧至少含 8 字节 counter")
	return binary.BigEndian.Uint64(raw.Payload[:8])
}

// registrationPayload 构造 80005 payload：deviceID_num(1B) + deviceID 集合（4B 大端）。
func registrationPayload(devices ...uint32) []byte {
	p := make([]byte, 1+len(devices)*4)
	p[0] = byte(len(devices))
	for i, d := range devices {
		binary.BigEndian.PutUint32(p[1+i*4:], d)
	}
	return p
}

// standbyHeartbeatPayload 构造明文待命心跳 payload（9 字节 HEARTBEAT，内容无关）。
func standbyHeartbeatPayload() []byte {
	return make([]byte, 9)
}

// encryptedHeartbeatPayload 构造**加密** HEARTBEAT（msgID=0）的 payload：
// counter(8B 明文) + 密文（4B deviceID + 9B 原始 HEARTBEAT payload）+ tag(16B) = 37B ≥ 28。
// 内容对路由器无意义，判据只有 msgID 与长度（§2.3/§2.5）。
func encryptedHeartbeatPayload(counter uint64) []byte {
	p := make([]byte, 37)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// TestSessionRouting 覆盖 §3.2 主流程：登记 → 待命心跳扇出 → 加密上行建链 →
// 加密下行只发配对 QGC → 回待命清配对。
func TestSessionRouting(t *testing.T) {
	node, m, stop := newServer(t, "3346")
	defer stop()

	qgc := connectPeer(t, node, "3346")
	px4 := connectPeer(t, node, "3346")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID) // PX4 帧头 deviceID 四字节
	reg := registrationPayload(px4DeviceID)

	// ---- 1. QGC 明文登记心跳（80005，帧头 GCS 段固定值）→ 仅登记，不转发给 PX4 ----
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, reg))
	expectNoFrame(t, px4, 200*time.Millisecond)

	// ---- 2. PX4 明文待命心跳（帧头 PX4 段）→ 登记 PX4 映射 + 扇出给在线 QGC ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())

	// ---- 3. QGC 加密上行（帧头 deviceID = 目标 PX4 的 D）→ 建配对 + 定向转发给 PX4 ----
	up1 := encryptedPayload(1) // 上行奇数 counter
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, up1))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())

	// ---- 4. PX4 加密下行（帧头 deviceID = 自身）→ 只发给配对的任务 QGC ----
	down1 := encryptedPayload(2) // 下行偶数 counter
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, down1))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())

	// ---- 5. 第二个 QGC（在线，但不关联该任务 PX4：空关联集合）→ 不参与配对，PX4 下行不发给它 ----
	other := connectPeer(t, node, "3346")
	feed(m, other.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload()))
	down2 := encryptedPayload(4)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, down2))
	ef = expectFrame(t, qgc, 1*time.Second) // 配对 QGC 收到
	_ = ef
	expectNoFrame(t, other, 300*time.Millisecond) // 未配对 QGC 不收

	// ---- 6. PX4 回待命心跳 → 清除该 PX4 全部配对缓存（§3.2 步骤 10）；扇出给所有在线 QGC ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second)   // qgc 收待命心跳
	expectFrame(t, other, 1*time.Second) // other 也在线，同样收到

	// ---- 7. 配对已清：PX4 加密下行不再转发 ----
	down3 := encryptedPayload(6)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, down3))
	expectNoFrame(t, qgc, 300*time.Millisecond)
	expectNoFrame(t, other, 300*time.Millisecond)
}

// TestReplayDropped 覆盖边缘防重放（§3.1）：同方向同 deviceID 的 counter 不递增则丢弃。
func TestReplayDropped(t *testing.T) {
	node, m, stop := newServer(t, "3347")
	defer stop()

	qgc := connectPeer(t, node, "3347")
	px4 := connectPeer(t, node, "3347")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳，使加密上下行可路由
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 上行 counter=1 → 通过
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second)

	// 重放 counter=1 → 防重放丢弃
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// counter=3（+2）→ 通过
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	expectFrame(t, px4, 1*time.Second)
}

// TestRestartRecovery 覆盖 §3.2.4：mavp2p 重启（新 Manager 无状态）后靠
// PX4 待命心跳 + QGC 80005 被动重建配对，无需主动握手。
func TestRestartRecovery(t *testing.T) {
	// 新 server（模拟 mavp2p 重启后，状态全空）
	node, m, stop := newServer(t, "3348")
	defer stop()

	qgc := connectPeer(t, node, "3348")
	px4 := connectPeer(t, node, "3348")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 1. PX4 持续发明文待命心跳 → 重建 PX4 映射
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 2. QGC 发明文登记心跳（payload=关联 PX4 集合）→ 重建配对三元组
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	// 3. QGC 加密上行 → 定向转发给 PX4（配对已被动重建）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second)
}

// TestPX4SocketIDDrift 覆盖 §3.2.1 核心规则 / §3.2.2 步骤 8 / §3.2.3 socketID 漂移：
// PX4 失联恢复（5G 动态地址 NAT 重建）后，来源 socketID 与 PX4 映射表记录不一致，
// mavp2p 须按帧头 deviceID（= 发送方自身）刷新映射表 + 配对表 PX4 侧 socketID，
// 仍把加密下行帧定向转发给配对的任务 QGC（不得丢弃）；上行亦须路由到刷新后的 PX4。
func TestPX4SocketIDDrift(t *testing.T) {
	node, m, stop := newServer(t, "3349")
	defer stop()

	qgc := connectPeer(t, node, "3349")
	px4 := connectPeer(t, node, "3349")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳（channel A）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// 待命心跳扇出给在线 QGC（§3.2.2 步骤 1），消费并断言
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())

	// QGC 加密上行建链（counter=1 奇数）→ 定向转发给 PX4
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())

	// 正常下行（channel A，counter=2 偶数）→ 配对 QGC 收到
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// PX4 重连：新连接（channel B）模拟 NAT 重建后 socketID 漂移。
	px4b := connectPeer(t, node, "3349")
	feed(m, px4b.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(4)))
	// §3.2.1 核心规则：按帧头 deviceID 刷新 socketID 后，仍应转发给配对 QGC。
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(4), frameCounter(t, ef))

	// 漂移后继续下行（counter=6）→ 配对表 PX4 侧已刷新，持续定向可达
	feed(m, px4b.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(6)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(6), frameCounter(t, ef))

	// 漂移后上行：px4Map.channel 已刷新到 channel B → QGC 加密上行路由到 px4b
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef = expectFrame(t, px4b, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(3), frameCounter(t, ef))

	// 防重放 × 漂移：nonceKey 按 deviceID×方向、不随 channel 变；漂移后重放 counter=4 应丢弃
	feed(m, px4b.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(4)))
	expectNoFrame(t, qgc, 300*time.Millisecond)

	// 防退化：用第三个 channel 重放曾只在旧 channel A 出现过的低 counter=2——
	// device-keyed 下 2<=6 丢弃；若实现退化为 channel-keyed（px4c 水位为空）则会转发，
	// 该断言即失败。此测试隔离验证 nonce 水位跨 channel 继承。
	px4c := connectPeer(t, node, "3349")
	feed(m, px4c.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	expectNoFrame(t, qgc, 300*time.Millisecond)
}

// TestUnregisteredQGCUplinkIgnored 覆盖 §3.2.3 / §3.2.4：未登记 QGC（80005 被 MAP_TTL
// 剪除、或未先登记就发包）的加密上行帧——帧头 deviceID = 目标 PX4 的 D、来源不在
// QGC 在线表、counter 为奇数（上行特征）。mavp2p 不得把它误判为 PX4 下行而改写
// PX4 映射表（那会导致上行命令被重定向、跨 QGC 泄露），须忽略，等该 QGC 补发 80005。
func TestUnregisteredQGCUplinkIgnored(t *testing.T) {
	node, m, stop := newServer(t, "3350")
	defer stop()

	qgc := connectPeer(t, node, "3350")
	px4 := connectPeer(t, node, "3350")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳 + 建链
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())

	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second)

	// 未登记来源（未发 80005）发加密上行（奇数 counter）→ 忽略：不转发、不改写 PX4 映射
	rogue := connectPeer(t, node, "3350")
	feed(m, rogue.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(101)))
	expectNoFrame(t, px4, 300*time.Millisecond)
	expectNoFrame(t, qgc, 300*time.Millisecond)

	// PX4 映射未被污染：真实 QGC 继续上行 → 仍路由到真实 PX4（channel A）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(3), frameCounter(t, ef))
}

// TestStandbyClearRequiresEncryptedSession 覆盖 §3.2.2 步骤 10 的前提条件。
//
// 明文待命心跳在「开机后」与「任务结束后」两个阶段都会周期出现，链路形态完全相同
// （msgID=0 + 9B payload + PX4 段 deviceID），帧内无任何可区分之处；而清除 QGC↔PX4
// 配对缓存只应在后者发生。唯一可用的判据是该 deviceID 的**下行 lastNonce 存在**
// （⇒ 该 PX4 已切入过加密阶段；水位缺失的唯一窗口是握手瞬间，见 manager.go 该函数注释）。
// 若无条件清除，开机待命期间每个心跳周期清一次（PX4 待命心跳默认 20Hz），QGC 登记/保活
// 心跳（80005）刚重建的配对无法存活到 PX4 切入加密下行。
//
// 三格：① 开机待命期间待命心跳不得清除配对；② 任务结束回退（下行水位存在）必须清除；
// ③ 清除会一并清空**下行**水位，故只发生一次——后续待命心跳不再触发。格 3 因此同时是
// `delete(m.lastNonce, nkDown)` 的守护者：该行若被移除，格 3 的待命心跳会重新满足前提、
// 清掉自己刚建的配对，末端下行断言随即超时（已用变异体实测）。
func TestStandbyClearRequiresEncryptedSession(t *testing.T) {
	node, m, stop := newServer(t, "3351")
	defer stop()

	qgc := connectPeer(t, node, "3351")
	px4 := connectPeer(t, node, "3351")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// ---- 格 1「开机待命」：PX4 上电持续发明文待命心跳，其间 QGC 登记 → 配对必须存活 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// PX4 上电在先、QGC 后启动：此刻 qgcOnline 为空，扇出无人（qgcOnline 由 80005 填充）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	// 登记后的下一个心跳周期——该 deviceID 尚无下行水位，不得清除刚建立的配对
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出给在线 QGC（消费，避免积压）
	// 配对存活的判据只能是「加密下行送达」：上行路由走 px4Map、不经配对（§3.2.2 步骤 3）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// ---- 格 2「任务结束回退」：下行水位存在 → 待命心跳必须清除配对 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second)
	// 验证帧 counter=0：manager.go 对 counter=0 不判重放也不写水位，
	// 故它不会污染格 3 的前置条件（回待命后 PX4 不再发加密下行，水位应保持为空）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(0)))
	expectNoFrame(t, qgc, 300*time.Millisecond) // 配对已清 → 不再转发

	// ---- 格 3「清除只发生一次」：水位随配对被一并清空 → 后续待命心跳不再触发清除 ----
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(6)))
	ef = expectFrame(t, qgc, 1*time.Second) // 配对仍存活 → 仍送达
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(6), frameCounter(t, ef))
}

// TestStandbyClearResetsUplinkWatermark 守 processStandbyHeartbeat 中无条件的
// `delete(m.lastNonce, nkUp)`：任务结束回待命时，**上行**水位必须与配对一并清除。
//
// 判定力来自「新任务取一个低于旧任务水位的起点」：QGC 每次建链取随机 62 位奇数起点
// （§2.5），约 50% 概率低于上一任务的末值。水位若残留，该上行会被边缘判重**静默丢弃**
// （日志有 dropped replayed frame，但链路层表现为「建链成功却控制不了飞机」）。
// 本用例中旧水位 1001、新起点 3，二者不会随机碰撞。
func TestStandbyClearResetsUplinkWatermark(t *testing.T) {
	node, m, stop := newServer(t, "3352")
	defer stop()

	qgc := connectPeer(t, node, "3352")
	px4 := connectPeer(t, node, "3352")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// ---- 任务 1 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// 先发一次心跳建 px4Map（此刻无在线 QGC，扇出无人），再登记 QGC，再发心跳产生扇出
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出

	// 上行水位 1001（远高于任务 2 的起点）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1001)))
	expectFrame(t, px4, 1*time.Second) // 转发给 PX4，消费
	// 下行水位 1000：使回待命时 hadDown 成立（否则配对本就不会被清）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1000)))
	expectFrame(t, qgc, 1*time.Second) // 送达配对 QGC，消费

	// ---- 任务结束回待命：清配对 + 清两个方向的水位 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出

	// ---- 任务 2：QGC 重新登记（幂等重建，§3.2.4.1），取低于旧水位的新起点 ----
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出

	// 上行必须被转发：水位若残留（1001 > 3）则在此超时
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(3), frameCounter(t, ef))
}

// TestStandbyClearScopedToThatPX4 守 processStandbyHeartbeat 中的
// `k.px4DeviceID == did`：一架 PX4 回待命只清**它自己**的配对，不得波及别的 PX4。
//
// 单 PX4 的用例分不出「清该架」与「清全表」（结果相同），故必须两台：PX4_A 回待命后，
// PX4_B 的加密下行仍须送达已配对的 QGC——B 的配对若被连带清掉，下行将无接收者。
func TestStandbyClearScopedToThatPX4(t *testing.T) {
	node, m, stop := newServer(t, "3353")
	defer stop()

	qgc := connectPeer(t, node, "3353")
	px4a := connectPeer(t, node, "3353")
	px4b := connectPeer(t, node, "3353")

	const px4DeviceIDB = uint32(10000002)
	aInc, aCom, aSys, aComp := didBytes(px4DeviceID)
	bInc, bCom, bSys, bComp := didBytes(px4DeviceIDB)

	// 两架 PX4 上电待命；QGC 一次登记两个 deviceID（80005 payload 为集合）
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 0, standbyHeartbeatPayload()))
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID, px4DeviceIDB)))
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出

	// 两条链路各自建链（上行建配对、下行建水位）
	feed(m, qgc.ch, makeFrame(aInc, aCom, aSys, aComp, 33, encryptedPayload(21)))
	expectFrame(t, px4a, 1*time.Second)
	feed(m, qgc.ch, makeFrame(bInc, bCom, bSys, bComp, 33, encryptedPayload(11)))
	expectFrame(t, px4b, 1*time.Second)
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 33, encryptedPayload(20)))
	expectFrame(t, qgc, 1*time.Second)
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 33, encryptedPayload(10)))
	expectFrame(t, qgc, 1*time.Second)

	// ---- PX4_A 任务结束回待命：只应清掉 A 的配对 ----
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出

	// B 的配对必须存活：其加密下行仍应送达 QGC（被连带清掉则在此超时）
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 33, encryptedPayload(12)))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint64(12), frameCounter(t, ef))
}

// TestGCSHeartbeatBlockedAfterHandshake 覆盖 §2.5 加密心跳拦截：
// PX4 一旦发出**加密** HEARTBEAT（msgID=0 且 payload≥28），即视为握手完成，
// mavp2p 此后拦截 QGC 发往该 PX4 的加密 GCS 心跳（同为 msgID=0），使其不再抵达 PX4；
// PX4 回**明文**待命心跳（退回待命态）后恢复放行。
//
// 判据之所以成立，是因为两种心跳在路由器眼里是**双向封闭**的两种形态：明文待命心跳
// 恒 9B、加密心跳恒 ≥28B，中间没有别的取值，也不与其它 msgID 撞车。mavp2p 不解密、
// 拿不到角色信息，长度是它唯一可用且无歧义的信号（2026-09-27 用户裁定）。
//
// 五格：① 未握手时加密心跳必须放行——它是 PX4 建链的唯一触发源（PX4 靠第一条解密
// 成功的奇数 counter 建链），拦了则链路永远建不起来；② PX4 加密心跳本身不拦（它正是
// 置位依据），照常送达配对 QGC；③ 置位后 QGC 加密心跳被拦；④ 拦截不得破坏路由状态
// ——该 PX4 加密下行仍须送达配对 QGC；⑤ 回待命复位后加密心跳恢复放行（否则任务结束后
// 新会话的第一条上行若恰是心跳，该会话永远建不起来）。
func TestGCSHeartbeatBlockedAfterHandshake(t *testing.T) {
	node, m, stop := newServer(t, "3354")
	defer stop()

	qgc := connectPeer(t, node, "3354")
	px4 := connectPeer(t, node, "3354")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 明文待命心跳（未握手）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	ef := expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出，消费
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())

	// ---- 格 1「未握手」：QGC 加密 GCS 心跳必须放行 ----
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(1)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(1), frameCounter(t, ef))

	// ---- 格 2「PX4 发加密心跳 ⇒ 握手完成」：该帧自身不拦，照常送达配对 QGC ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(2)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// ---- 格 3「已握手」：QGC 加密 GCS 心跳被拦截，不再抵达 PX4 ----
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(5)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// ---- 格 4「拦截不破坏路由状态」：该 PX4 加密下行仍须送达配对 QGC ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(8)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint64(8), frameCounter(t, ef))

	// ---- 格 5「回待命复位」：加密心跳恢复放行 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出，消费
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(7)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(7), frameCounter(t, ef))
}

// TestBlockedHeartbeatDoesNotAdvanceWatermark 守 processEncrypted 上行分支中拦截点的
// **位置**：必须早于 `m.lastNonce[*nk] = counter`（§3.1「被拦截/忽略的帧不推进防重放
// 水位」；2026-09-27 用户要求「避免心跳对 lastNonce 的影响」）。
//
// 判定力来自「把被拦心跳的 counter 取成区间上界」：上行水位先由一条普通上行推到 1，
// 被拦心跳 counter=5，随后一条普通上行 counter=3。若心跳推进了水位（=5），3<=5 会被
// 边缘判重**静默丢弃**（链路层表现为「建链成功却控制不了飞机」）；本实现拦截即返回，
// 水位停在 1，3 正常放行。拦截点若写在配对刷新之前（水位写入之后），本用例必红。
func TestBlockedHeartbeatDoesNotAdvanceWatermark(t *testing.T) {
	node, m, stop := newServer(t, "3355")
	defer stop()

	qgc := connectPeer(t, node, "3355")
	px4 := connectPeer(t, node, "3355")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：登记 + PX4 待命心跳 + 一条普通加密上行（写上行水位 1）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second) // 消费

	// PX4 加密心跳 ⇒ 握手完成（置位）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(2)))
	expectFrame(t, qgc, 1*time.Second) // 消费

	// 已握手：QGC 加密心跳 counter=5 被拦（不得抵达 PX4）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(5)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 水位必须仍停在 1：counter=3 放行（若被推高到 5，此处判重丢弃 → 超时）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(3), frameCounter(t, ef))
}

// TestNonHeartbeatEncryptedUplinkNotBlocked 守拦截判据的 **msgID 维度**：置位后只拦
// `msgID=0` 的加密上行心跳，长度同样达标的**非心跳**加密上行必须照常送达 PX4
// （§2.5 只拦心跳；任务/围栏/集结点等其余上行不受影响）。
//
// 判定力来自「把非心跳帧的长度也做到 ≥28B」（encryptedNonHeartbeatPayload）：若判据
// 退化成只看长度（去掉 `msgID == msgIDHeartbeat`），末帧会被误拦、断言超时。
//
// 前置中的阴性对照不可省：它以「加密心跳确实被拦」证明置位真的生效——否则末帧放行
// 也可能只是标记从未置位，用例即使绿也说明不了任何事。
func TestNonHeartbeatEncryptedUplinkNotBlocked(t *testing.T) {
	node, m, stop := newServer(t, "3356")
	defer stop()

	qgc := connectPeer(t, node, "3356")
	px4 := connectPeer(t, node, "3356")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳 + PX4 加密心跳 ⇒ 置位「已建链」。
	// 不消费 QGC 侧的下行扇出：本用例只在 PX4 队列上断言，配对与否不影响结论。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(2)))

	// 阴性对照：标记确已置位 ⇒ QGC 的加密心跳被拦
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(3)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 正题：长度同样达标、但 msgID != 0 的加密上行必须放行
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(5)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(5), frameCounter(t, ef))
}

// TestStandbyResetsHandshakeFlagWithoutDownlinkWatermark 守复位点的**无条件性**：
// 「已建链标记」的复位不得附加「该 deviceID 存在下行水位」这一前提——后者只是**清配对**
// 的前提（见 TestStandbyClearRequiresEncryptedSession），两者不是同一个条件。
//
// 依据：manager.go 写下行水位的那句是 `if nk != nil`，而 counter=0 的帧既不判重放也不
// 写水位，置位判据（msgID=0 且 plen ≥ 28）却与 counter 无关 ⇒「已置位、但无下行水位」
// 是**可达状态**，一条 counter=0 的加密心跳即可造出来。
//
// 若复位被改成与清配对同一个前提，标记在该状态下永久卡住 ⇒ 该 PX4 下一会话的首帧加密
// 上行心跳被拦、链路再也建不起来（§2.5「该复位无前提条件」）。
func TestStandbyResetsHandshakeFlagWithoutDownlinkWatermark(t *testing.T) {
	node, m, stop := newServer(t, "3357")
	defer stop()

	qgc := connectPeer(t, node, "3357")
	px4 := connectPeer(t, node, "3357")

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 明文待命心跳
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// counter=0 的加密心跳：置位「已建链」，但**不写下行水位**（counter=0 无 nonce key）；
	// 此时尚无配对，该下行帧也不扇出给任何 QGC。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(0)))

	// 阴性对照：标记确已置位 ⇒ QGC 的加密心跳被拦
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(3)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 明文待命心跳：必须复位标记（此刻并无下行水位，清配对的前提并不成立）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 复位后：QGC 的加密心跳必须恢复放行
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(7)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(7), frameCounter(t, ef))
}
