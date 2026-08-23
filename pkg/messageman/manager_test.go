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

// frameCounter 读取帧 payload 明文前 8 字节 counter（加密帧）。
func frameCounter(ef *gomavlib.EventFrame) uint64 {
	if raw, ok := ef.Frame.GetMessage().(*message.MessageRaw); ok && len(raw.Payload) >= 8 {
		return binary.BigEndian.Uint64(raw.Payload[:8])
	}
	return 0
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
	require.Equal(t, uint64(2), frameCounter(ef))

	// PX4 重连：新连接（channel B）模拟 NAT 重建后 socketID 漂移。
	px4b := connectPeer(t, node, "3349")
	feed(m, px4b.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(4)))
	// §3.2.1 核心规则：按帧头 deviceID 刷新 socketID 后，仍应转发给配对 QGC。
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(4), frameCounter(ef))

	// 漂移后继续下行（counter=6）→ 配对表 PX4 侧已刷新，持续定向可达
	feed(m, px4b.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(6)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(6), frameCounter(ef))

	// 漂移后上行：px4Map.channel 已刷新到 channel B → QGC 加密上行路由到 px4b
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef = expectFrame(t, px4b, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(3), frameCounter(ef))

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
	require.Equal(t, uint64(3), frameCounter(ef))
}
