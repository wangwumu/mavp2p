package messageman_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"strconv"
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
//
// 双端点：vehiclePort 收载具（下行入口），gcsPort 收地面站（上行入口）。
// 方向由**通道落在哪个端点**决定，两条流结构上不共享端口；测试必须走真实端点，
// 不能只造一个 channel 传角色参数，否则「端点→角色」这段映射根本没被覆盖。
//
// 端口由 freePort 现探，不再用「每个用例一个固定号段」：并行跑同一个包的多次
// `go test`（审查代理各自跑一遍就是这么撞的）会抢同一批端口，失败长成
// 「bind: address already in use」而不是断言错，看上去像代码缺陷。
func newServer(t *testing.T) (*gomavlib.Node, *messageman.Manager, string, string, func()) {
	t.Helper()

	node, m, vehiclePorts, gcsPort, stop := newServerN(t, 1)
	return node, m, vehiclePorts[0], gcsPort, stop
}

// newServerN 是 newServer 的通用形态：numVehicles 个载具端点 + 一个 GCS 端点。
// 多载具端点用来钉「每个载具端点都映射成同一个 roleVehicle，deviceID 决定投给谁」。
func newServerN(t *testing.T, numVehicles int) (*gomavlib.Node, *messageman.Manager, []string, string, func()) {
	t.Helper()

	require.GreaterOrEqual(t, numVehicles, 1)

	vehiclePorts := make([]string, numVehicles)
	vehicleEPs := make([]gomavlib.Endpoint, numVehicles)
	for i := range vehiclePorts {
		vehiclePorts[i] = freePort(t)
		vehicleEPs[i] = &gomavlib.EndpointTCPServer{Address: "127.0.0.1:" + vehiclePorts[i]}
	}
	gcsPort := freePort(t)
	gcsEP := &gomavlib.EndpointTCPServer{Address: "127.0.0.1:" + gcsPort}
	node := &gomavlib.Node{
		Endpoints:      append(append([]gomavlib.Endpoint{}, vehicleEPs...), gcsEP),
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

	// P5 起建链期下行必须过验签。本包的路由用例只关心路由，不关心密码学 ⇒ 一律放行。
	verifyPath, stopOracle := newFakeOracle(t)

	m := &messageman.Manager{
		Ctx: ctx,
		Wg:  &wg,
		Config: messageman.Config{
			GCSDeviceIDMax: gcsDeviceIDMax,
			// 端点对象与 Node.Endpoints 里的是**同一批指针**（roleOf 用指针比较认来源）。
			GCSEndpoint:      gcsEP,
			VehicleEndpoints: vehicleEPs,
			VerifySocket:     verifyPath,
			VerifyTimeout:    time.Second,
		},
		Node: node,
	}
	err = m.Initialize()
	require.NoError(t, err)

	return node, m, vehiclePorts, gcsPort, func() {
		cancel()
		wg.Wait()
		node.Close()
		stopOracle()
	}
}

// newFakeOracle 起一个最小的 Unix socket 验签服务：读 12B 请求头 + payload，
// 原样回 reqID 与状态 0（通过）。用例只关心路由，不关心密码学。
func newFakeOracle(t *testing.T) (string, func()) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "verify.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)

	var (
		once sync.Once
		wg   sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close() //nolint:errcheck
				hdr := make([]byte, 12)
				if _, err := io.ReadFull(conn, hdr); err != nil {
					return
				}
				payload := make([]byte, int(binary.BigEndian.Uint16(hdr[10:12])))
				if _, err := io.ReadFull(conn, payload); err != nil {
					return
				}
				out := make([]byte, 5)
				copy(out[:4], hdr[1:5]) // reqID
				out[4] = 0              // 通过
				_, _ = conn.Write(out)
			}()
		}
	}()

	stop := func() {
		once.Do(func() {
			ln.Close() //nolint:errcheck
			wg.Wait()
		})
	}
	t.Cleanup(stop)
	return path, stop
}

// freePort 探一个当前空闲的 TCP 端口（探完即关）。同包用例串行，竞争窗口可接受；
// 即便撞上，失败也会以 bind 错误明确指向环境，而不是变成一条看不懂的断言失败。
func freePort(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
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

	// 防串线：断言服务端为该对端分配的通道确实落在刚拨的端口上。
	// 少了这一条，把 QGC 拨到载具口、把 PX4 拨到 GCS 口也会「跑通」——
	// 那时测的就不是方向判定，而是恰好反着写的一对常量。
	serverEP, ok := co.Channel.Endpoint().(*gomavlib.EndpointTCPServer)
	require.True(t, ok, "unexpected server endpoint type %T", co.Channel.Endpoint())
	require.Equal(t, "127.0.0.1:"+port, serverEP.Address)

	return &peer{node: c, ch: co.Channel}
}

// connectQGC 连接地面站入口（上行）。
func connectQGC(t *testing.T, node *gomavlib.Node, gcsPort string) *peer {
	t.Helper()
	return connectPeer(t, node, gcsPort)
}

// connectPX4 连接载具入口（下行）。
func connectPX4(t *testing.T, node *gomavlib.Node, vehiclePort string) *peer {
	t.Helper()
	return connectPeer(t, node, vehiclePort)
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
			if ef, ok := evt.(*gomavlib.EventFrame); ok {
				t.Fatalf("unexpected frame received: msgID=%d", ef.Frame.GetMessage().GetID())
			}
		case <-time.After(timeout):
			return
		}
	}
}

// encryptedPayload 构造加密帧 payload：counter(8B 明文) + 密文 28B = 36B。
//
// 定稿入口按 encryptedPayloadMinLen=28 判加密块；12B 的旧夹具会在入口被当作
// 短帧丢弃，故统一做到 36B（03 §4.4）。
func encryptedPayload(counter uint64) []byte {
	p := make([]byte, 8+28)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// encryptedNonHeartbeatPayload 与 encryptedPayload 同形，名字强调 msgID != 0。
// 保留独立名字是为了让「只拦心跳、不拦其它 msgID」的用例读起来有判别性。
func encryptedNonHeartbeatPayload(counter uint64) []byte {
	return encryptedPayload(counter)
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

// encryptedHeartbeatPayload 构造 **QGC** 加密 HEARTBEAT（msgID=0）的 payload：
// counter(8B 明文) + 密文（4B deviceID + 9B 原始 HEARTBEAT payload）+ tag(16B) = 37B。
// 37B 落在 [28, T=50) ⇒ 只在 GCS 端点合法；原样投到载具端点会被入口当作畸形待命丢弃。
func encryptedHeartbeatPayload(counter uint64) []byte {
	p := make([]byte, 37)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// px4EncryptedHeartbeatPayload 构造 **PX4** 加密 HEARTBEAT 的 payload：92B ≥ T=50。
// 载具端点的 msgID=0 只有 ≥T 才会进入加密下行（03 §4.4），用 QGC 的 37B 夹具会被丢。
func px4EncryptedHeartbeatPayload(counter uint64) []byte {
	p := make([]byte, 92)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// TestSessionRouting 覆盖 §3.2 主流程：登记 → 待命心跳扇出 → 加密上行建链 →
// 加密下行只发配对 QGC → 回待命清配对。
func TestSessionRouting(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

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
	other := connectQGC(t, node, gcsPort)
	feed(m, other.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload()))
	down2 := encryptedPayload(4)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, down2))
	ef = expectFrame(t, qgc, 1*time.Second) // 配对 QGC 收到
	_ = ef
	expectNoFrame(t, other, 300*time.Millisecond) // 未配对 QGC 不收

	// ---- 6. PX4 回待命心跳 → 任务结束：清配对；只扇出给声明过该 deviceID 的 QGC ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second)            // qgc 声明过 → 收待命心跳
	expectNoFrame(t, other, 300*time.Millisecond) // other 空关联集合 → 不扇出（03 §4.5.1）

	// ---- 7. 配对已清：PX4 加密下行不再转发 ----
	down3 := encryptedPayload(6)
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, down3))
	expectNoFrame(t, qgc, 300*time.Millisecond)
	expectNoFrame(t, other, 300*time.Millisecond)
}

// TestReplayDropped 覆盖边缘防重放（§3.1）：同方向同 deviceID 的 counter 不递增则丢弃。
func TestReplayDropped(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 1. PX4 持续发明文待命心跳 → 重建 PX4 映射
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 2. QGC 发明文登记心跳（payload=关联 PX4 集合）→ 重建配对三元组
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	// 3. QGC 加密上行 → 定向转发给 PX4（配对已被动重建）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second)
}

// TestDownlinkRebuildsPX4EntryAfterRestart 覆盖 §3.2.4.1 的「查不到缓存就新建」：
// mavp2p 在飞机**已进入加密阶段**时重启（QGC 未退出），PX4 不会再发明文待命心跳，
// 重建只能靠加密下行的帧头 deviceID（= 发送方自身）。
// 改前该分支对 px4Map 未命中直接丢弃 ⇒ 飞机落地前整条下行无法自愈。
func TestDownlinkRebuildsPX4EntryAfterRestart(t *testing.T) {
	// 新 server（模拟 mavp2p 重启后，状态全空）
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 1. QGC 80005 先到 → 配对已建，但 px4Ch 为空（PX4 映射表尚无该项）。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	// 2. PX4 直接发**加密下行**（不发明文待命心跳 —— 它正在飞）：
	//    必须按帧头 deviceID 重建 px4Map，并补全配对的 px4Ch 后立即投递。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// 3. 重建的表项可用于上行寻址：QGC 加密上行应定向到该 PX4。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(3)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(76), ef.Frame.GetMessage().GetID())

	// 4. 后续下行持续可达（不是只放行首帧）。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(4)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint64(4), frameCounter(t, ef))
}

// TestPX4SocketIDDrift 覆盖 §3.2.1 核心规则 / §3.2.2 步骤 8 / §3.2.3 socketID 漂移：
// PX4 失联恢复（5G 动态地址 NAT 重建）后，来源 socketID 与 PX4 映射表记录不一致，
// mavp2p 须按帧头 deviceID（= 发送方自身）刷新映射表 + 配对表 PX4 侧 socketID，
// 仍把加密下行帧定向转发给配对的任务 QGC（不得丢弃）；上行亦须路由到刷新后的 PX4。
func TestPX4SocketIDDrift(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

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
	px4b := connectPX4(t, node, vehiclePort)
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
	px4c := connectPX4(t, node, vehiclePort)
	feed(m, px4c.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	expectNoFrame(t, qgc, 300*time.Millisecond)
}

// TestUnregisteredQGCUplinkIgnored 覆盖 §3.2.3 / §3.2.4：未登记 QGC（80005 被 MAP_TTL
// 剪除、或未先登记就发包）的加密上行帧——帧头 deviceID = 目标 PX4 的 D、来源是
// **GCS 端点**上不在 QGC 在线表里的通道。mavp2p 不得把它误判为 PX4 下行而改写
// PX4 映射表（那会导致上行命令被重定向、跨 QGC 泄露），须忽略，等该 QGC 补发 80005。
func TestUnregisteredQGCUplinkIgnored(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳 + 建链
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())

	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second)

	// 未登记来源（未发 80005）发加密上行（奇数 counter）→ 忽略：不转发、不改写 PX4 映射
	rogue := connectQGC(t, node, gcsPort)
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
// 若无条件清除，开机待命期间每个心跳周期清一次（PX4 待命心跳 1Hz），QGC 登记/保活
// 心跳（80005）刚重建的配对无法存活到 PX4 切入加密下行。
//
// 三格：① 开机待命期间待命心跳不得清除配对；② 任务结束回退（下行水位存在）必须清除；
// ③ 清除会一并清空**下行**水位，故只发生一次——后续待命心跳不再触发。格 3 因此同时是
// `delete(m.lastNonce, nkDown)` 的守护者：该行若被移除，格 3 的待命心跳会重新满足前提、
// 清掉自己刚建的配对，末端下行断言随即超时（已用变异体实测）。
func TestStandbyClearRequiresEncryptedSession(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// ---- 格 1「开机待命」：PX4 上电持续发明文待命心跳，其间 QGC 登记 → 配对必须存活 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// PX4 上电在先、QGC 后启动：此刻 qgcOnline 为空，扇出无人（qgcOnline 由 80005 填充）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	// 登记后的下一个心跳周期——该 deviceID 尚无下行水位，不得清除刚建立的配对。
	// 上一帧没有落点，不消耗发布窗口 ⇒ 这一帧必然发布并扇出到 QGC，先消费掉。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	hb := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), hb.Frame.GetMessage().GetID(), "先到的是待命心跳扇出")
	// 配对存活的判据只能是「加密下行送达」：上行路由走 px4Map/候选、不经配对（§3.2.2 步骤 3）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(2)))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// ---- 格 2「清除只发生一次」：水位随配对被一并清空 → 后续待命心跳不再触发清除 ----
	// 任务结束清配对/映射/水位/开门的完整状态变更**无法**从外部行为观测：任务结束同时
	// 关会话门，而开门的第一条 QGC 上行必然重建配对 ⇒「配对是否被删」在外部永远看不到。
	// 那部分由内部用例 TestStandbyTaskEndClearsPairsAndWatermarks 直接断言；
	// 这里只从外部钉住「清水位那行若被删，第二次心跳会清掉刚重建的配对」。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	// 第一条：格 1 留下的下行水位（counter=2）⇒ 走任务结束，清配对 + 清水位，并扇出。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费任务结束扇出
	// 重建配对；同一设备的发布间隔 <2s ⇒ 第二条心跳不发布、不扇出。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// 若任务结束里删 waterline 的那行被移除，水位仍在 ⇒ 第二条心跳会再次清掉刚重建的
	// 配对，末端下行不送达（已用变异体实测）。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(4)))
	_ = expectFrame(t, px4, 1*time.Second)
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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// ---- 任务 1 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// 先发一次心跳建 px4Map（此刻无在线 QGC，扇出无人），再登记 QGC，再发心跳产生扇出
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	// 候选路径可能被 2s 聚合窗口节流，不保证有扇出可消费。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

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
	// 候选路径可能被节流；上行目标集由候选提供，不依赖这帧扇出。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4a := connectPX4(t, node, vehiclePort)
	px4b := connectPX4(t, node, vehiclePort)

	const px4DeviceIDB = uint32(10000002)
	aInc, aCom, aSys, aComp := didBytes(px4DeviceID)
	bInc, bCom, bSys, bComp := didBytes(px4DeviceIDB)

	// 两架 PX4 上电待命；QGC 一次登记两个 deviceID（80005 payload 为集合）
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 0, standbyHeartbeatPayload()))
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID, px4DeviceIDB)))
	// 上电那两帧没有落点（QGC 尚未登记到配对里），不消耗发布窗口 ⇒ 这两帧必然发布
	// 并各扇出一条待命心跳，先消费掉（顺序按设备各自发送，A/B 都可能是第一条）。
	feed(m, px4a.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	feed(m, px4b.ch, makeFrame(bInc, bCom, bSys, bComp, 0, standbyHeartbeatPayload()))
	for i := 0; i < 2; i++ {
		ef := expectFrame(t, qgc, 1*time.Second)
		require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID(), "先到的是待命心跳扇出")
	}

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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

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
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, px4EncryptedHeartbeatPayload(2)))
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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：登记 + PX4 待命心跳 + 一条普通加密上行（写上行水位 1）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	expectFrame(t, qgc, 1*time.Second) // 消费扇出
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	expectFrame(t, px4, 1*time.Second) // 消费

	// PX4 加密心跳 ⇒ 握手完成（置位）
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, px4EncryptedHeartbeatPayload(2)))
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
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：QGC 登记 + PX4 待命心跳 + PX4 加密心跳 ⇒ 置位「已建链」。
	// 不消费 QGC 侧的下行扇出：本用例只在 PX4 队列上断言，配对与否不影响结论。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	// 先消费明文待命的扇出，否则下面会把两者混为一谈（msgID 都是 0）。
	ef := expectFrame(t, qgc, 1*time.Second)
	standby, ok := ef.Frame.GetMessage().(*message.MessageRaw)
	require.True(t, ok)
	require.Len(t, standby.Payload, 9, "期望明文待命心跳（9B，无 counter）")

	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, px4EncryptedHeartbeatPayload(2)))
	// P5 起建链期下行是异步验签：先消费 QGC 侧的扇出（它证明该帧已被接受、
	// handshaked 已置位、候选已清空），否则下一帧会比状态先到。
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(0), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef), "这一帧应是加密心跳下行")

	// 阴性对照：标记确已置位 ⇒ QGC 的加密心跳被拦
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 0, encryptedHeartbeatPayload(3)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 正题：长度同样达标、但 msgID != 0 的加密上行必须放行
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedNonHeartbeatPayload(5)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(5), frameCounter(t, ef))
}

// ---- 双端点方向判定的判别性用例 ----
//
// 下面四条都在钉同一件事：方向由**入口端点**决定，与 counter 奇偶无关。
//
// 判别力是**变异实测**的，不是对历史代码的断言：把本次的判据按旧逻辑改回去
// （方向退回 counter 奇偶兜底、去掉两道入口闸），四条全红（2026-10-25）。
// 各自暴露的失败形态：
//   - 载具口奇数 counter：被当「未登记上行」丢掉；
//   - GCS 口偶数 counter：被当「下行」改写 px4Map（上行命令被重定向）；
//   - 载具口 80005：照样登记，幽灵 QGC 进入 qgcOnline 收到扇出；
//   - GCS 口明文待命心跳：照样当 PX4 心跳扇出。

// TestVehiclePortOddCounterIsDownlink：载具入口的帧即使 counter 是奇数（旧判据的
// 「上行特征」），也必须按下行处理并投给配对 QGC。
func TestVehiclePortOddCounterIsDownlink(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：登记 + 待命心跳 + 上行建链
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	_ = expectFrame(t, qgc, 1*time.Second) // 待命心跳扇出
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	_ = expectFrame(t, px4, 1*time.Second) // 上行转发

	// 载具端口进来一帧**奇数** counter 的加密帧 ⇒ 仍是下行，投给配对 QGC。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(3)))
	ef := expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint64(3), frameCounter(t, ef),
		"载具入口的奇数 counter 帧被当成上行丢弃 ⇒ 方向仍在读 counter 奇偶")
}

// TestUnregisteredGCSEvenCounterStaysUplink：GCS 入口的帧即使 counter 为偶数
// （旧判据的「下行特征」），也绝不能按下行处理——不得写入/改写 px4Map，不得扇出。
// 未登记的通道一律丢弃（登记是授权闸）。
func TestUnregisteredGCSEvenCounterStaysUplink(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)
	rogue := connectQGC(t, node, gcsPort) // 从不发 80005

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：真实 QGC 登记 + PX4 待命 + 建链
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	_ = expectFrame(t, qgc, 1*time.Second)
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	_ = expectFrame(t, px4, 1*time.Second)

	// 未登记通道从 GCS 口发**偶数** counter、帧头 deviceID 命中 PX4：
	// 必须按「未登记上行」丢弃，而不是按下行改写 px4Map。
	feed(m, rogue.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(4)))
	expectNoFrame(t, px4, 300*time.Millisecond)
	expectNoFrame(t, qgc, 300*time.Millisecond)
	expectNoFrame(t, rogue, 300*time.Millisecond)

	// px4Map 未被污染：真实 QGC 的后续上行仍转发给**真实** PX4 通道。
	// （旧实现下这里会转发到 rogue，因为下行分支把 px4Map[did].channel 改写了。）
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(6)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(6), frameCounter(t, ef),
		"未登记 GCS 通道的偶数 counter 被当成了 PX4 下行 ⇒ 上行路由被改写")
	expectNoFrame(t, rogue, 300*time.Millisecond)

	// 登记通道的**偶数** counter 也必须按上行转发：上行不再依赖 counter 奇偶。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(8)))
	ef = expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint64(8), frameCounter(t, ef),
		"已登记 GCS 通道的偶数 counter 没被当成上行 ⇒ 上行仍按奇偶判定")
}

// TestRegistrationOnVehicleEndpointIgnored：80005 只在地面站入口生效。
// 从载具入口收到 80005 ⇒ 丢弃、不登记；该通道不得进入 qgcOnline，
// 因而收不到 PX4 待命心跳扇出。
func TestRegistrationOnVehicleEndpointIgnored(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	px4 := connectPX4(t, node, vehiclePort)
	intruder := connectPX4(t, node, vehiclePort) // 从载具端口伪装成 QGC 登记
	qgc := connectQGC(t, node, gcsPort)          // 正路登记，兼作扇出阳性对照

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 正路 QGC 先登记：没有它，下面的 expectNoFrame 可以靠「qgcOnline 本来就是空」假绿。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, intruder.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	// 阳性对照：扇出本身在工作 —— 正路 QGC 必须收到待命心跳。
	require.Equal(t, uint32(0), expectFrame(t, qgc, 1*time.Second).Frame.GetMessage().GetID(),
		"正路登记的地面站没收到待命心跳 ⇒ 下面的阴性断言失去判别力")
	expectNoFrame(t, intruder, 300*time.Millisecond)
}

// TestPlaintextHeartbeatOnGCSEndpointIgnored：明文待命心跳只在载具入口生效。
// 从地面站入口收到 PX4 段明文明文心跳 ⇒ 丢弃：不登记 px4Map、不扇出给在线 QGC。
func TestPlaintextHeartbeatOnGCSEndpointIgnored(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	rogue := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// qgc 先登记，使「扇出给在线 QGC」这一路径真的可达（否则本用例可被
	// 「qgcOnline 本来就是空」假绿）。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	feed(m, rogue.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))

	expectNoFrame(t, qgc, 300*time.Millisecond)
	expectNoFrame(t, rogue, 300*time.Millisecond)

	// 第二个否定面：被丢弃的明文心跳不得顺带在 px4Map 登记该 deviceID。从行为面验：
	// 已登记 QGC 发往这个「尚未在任何载具端点出现过」的 PX4 的加密上行必须被忽略。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(1)))
	expectNoFrame(t, px4, 300*time.Millisecond) // 也顺带钉住「已登记但未知 PX4 的上行被忽略」

	// 阳性对照：同一形态的明文心跳从**载具入口**进来时必须扇出给已登记 QGC。
	// 没有这一格，上面的阴性断言可以靠「扇出路径整体坏了」假绿。
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	require.Equal(t, uint32(0), expectFrame(t, qgc, 1*time.Second).Frame.GetMessage().GetID(),
		"载具入口的明文待命心跳没有扇出 ⇒ 本用例没有判别力")

	// 再验一次上行通路：此刻 PX4 已由待命心跳登记，同样的上行必须被转发。
	// 与上面那条阴性断言配对，才证明「没转发」是登记闸造成的、而不是转发坏了。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(2)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(76), ef.Frame.GetMessage().GetID())
}

// TestStandbyHeartbeatOutsideAircraftSegmentIgnored：明文待命心跳的号段闸必须与 80005
// 登记、加密下行重建**同一口径**（PX4 段且 bit24=0），不能只比对下界。
//
// 改前只判 `did >= gcsDeviceIDMax()`：向载具端点发一帧 9B 明文心跳、id 取 0x7F016379
// （超过 0x7EFFFFFF 且 bit24 置位）就能在 px4Map 里种出「幻象 PX4」，随后该 did 的加密
// 下行在缓存命中分支直接通过 —— 2026-10-26 新加的号段闸只覆盖「新建」路径，被这条登记
// 路径绕过。同时该帧还会扇出给所有在线 QGC，污染面比单纯建表更大。
//
// 三组判据缺一不可：非法心跳不扇出 / 非法心跳不建 px4Map（用后续上行是否被转发验） /
// 合法心跳与合法上行都必须仍工作（否则「不转发」可以靠转发整体坏了假绿）。
func TestStandbyHeartbeatOutsideAircraftSegmentIgnored(t *testing.T) {
	// 取「超过 0x7EFFFFFF 上界、bit24=0」的 ID。不用 bit24 置位的值（如 0x7F016379）：
	// 该位正是 MAVLink 2 的签名位，测试构造的 V2Frame 没有 Signature，一旦被转发会在
	// 重编码时 nil 解引用（那是测试夹具的坑，不是被测行为）。bit24 子句由
	// validAircraftDeviceID 的既有用例（0x01000001）覆盖，本用例只需证明待命路径用上了
	// **完整**的号段闸，而不是只比对下界。
	const invalidDID = uint32(0x80016379)

	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	px4 := connectPX4(t, node, vehiclePort)
	qgc := connectQGC(t, node, gcsPort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)
	iInc, iCom, iSys, iComp := didBytes(invalidDID)

	// 正路 QGC 先登记，使「扇出给在线 QGC」真的可达（否则下面的不扇出可被空表假绿）。
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))

	// ---- 非法号段的明文待命心跳：不得扇出 ----
	feed(m, px4.ch, makeFrame(iInc, iCom, iSys, iComp, 0, standbyHeartbeatPayload()))
	expectNoFrame(t, qgc, 300*time.Millisecond)

	// ---- 也不得建 px4Map：已登记 QGC 发往该 deviceID 的上行必须被忽略 ----
	feed(m, qgc.ch, makeFrame(iInc, iCom, iSys, iComp, 76, encryptedNonHeartbeatPayload(1)))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// ---- 阳性对照 1：合法待命心跳照常扇出 ----
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	require.Equal(t, uint32(0), expectFrame(t, qgc, 1*time.Second).Frame.GetMessage().GetID(),
		"合法载具入口的明文待命心跳没有扇出 ⇒ 上面的「不扇出」断言没有判别力")

	// ---- 阳性对照 2：合法上行照常转发 ----
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 76, encryptedNonHeartbeatPayload(2)))
	require.Equal(t, uint32(76), expectFrame(t, px4, 1*time.Second).Frame.GetMessage().GetID(),
		"合法上行没有转发 ⇒ 上面「非法 ID 上行被忽略」可以靠转发整体坏了假绿")

	// ---- 再验一次：转发已证明可用后，非法 ID 的上行仍必须被忽略 ----
	feed(m, qgc.ch, makeFrame(iInc, iCom, iSys, iComp, 76, encryptedNonHeartbeatPayload(3)))
	expectNoFrame(t, px4, 300*time.Millisecond)
}

// TestRequestDataStreamPlaintextBlockedOnBothEndpoints 钉住 RequestDataStream（msgID 66）
// 拦截的**范围**：判据只看「明文」，与入口角色无关。
//
// 这一条是撤回角色化改动的回归用例：曾把拦截面收窄成「只拦 GCS 入口」，于是载具入口的
// 明文 66 会被当成普通下行投给 QGC。协议里 66 是请求方发起的控制帧，明文形态一律不是
// 本链路的合法流量，两个入口都该拦。
func TestRequestDataStreamPlaintextBlockedOnBothEndpoints(t *testing.T) {
	node, m, vehiclePort, gcsPort, stop := newServer(t)
	defer stop()

	qgc := connectQGC(t, node, gcsPort)
	px4 := connectPX4(t, node, vehiclePort)

	pInc, pCom, pSys, pComp := didBytes(px4DeviceID)

	// 前置：登记 + 待命 + 上行建链（让两个方向的转发都真的可达）
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID)))
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 0, standbyHeartbeatPayload()))
	_ = expectFrame(t, qgc, 1*time.Second)
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 33, encryptedPayload(1)))
	_ = expectFrame(t, px4, 1*time.Second)

	// 明文 66（5B）从 GCS 入口：拦下，不转发给 PX4
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 66, []byte{0, 0, 0, 1, 0}))
	expectNoFrame(t, px4, 300*time.Millisecond)

	// 明文 66 从载具入口：同样拦下，不投给配对 QGC
	feed(m, px4.ch, makeFrame(pInc, pCom, pSys, pComp, 66, []byte{0, 0, 0, 1, 0}))
	expectNoFrame(t, qgc, 300*time.Millisecond)

	// 阳性对照：**加密**的 66（payload ≥ 28）不是本判据的目标，照常按方向转发。
	// 没有这一格，上面的阴性断言可以靠「66 一律丢」假绿。
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 66, encryptedNonHeartbeatPayload(2)))
	ef := expectFrame(t, px4, 1*time.Second)
	require.Equal(t, uint32(66), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))

	// 开关面：--streamreq-disable 打开后，明文 66 仍被入口短帧闸丢弃。
	// 03 §4.4 的通用规则「其它 msgID payload <28B 两端丢弃」优先于该开关；开关只影响
	// 上游那条专项拦截，不再改变可观察结果（明文 66 在任何配置下都进不了路由）。
	m.StreamReqDisable = true
	feed(m, qgc.ch, makeFrame(pInc, pCom, pSys, pComp, 66, []byte{0, 0, 0, 1, 0}))
	expectNoFrame(t, px4, 300*time.Millisecond)
}

// TestMultipleVehicleEndpointsRouteByDeviceID 钉住「多个载具端点」的装配：每个载具端点
// 都映射为同一个 roleVehicle，选哪台 PX4 只由帧头 deviceID 决定，与来源端口无关。
//
// 云端只用一个载具端点，但 Config 的契约是「至少一个」，且 roleOf 对
// VehicleEndpoints 是循环匹配 —— 多处端点时若某个端点没被认出来，该端口上的飞机
// 会全部表现为「上线即丢帧」。本用例用两个真实载具端口把这条路径走通。
func TestMultipleVehicleEndpointsRouteByDeviceID(t *testing.T) {
	node, m, vehiclePorts, gcsPort, stop := newServerN(t, 2)
	defer stop()

	const didB = px4DeviceID + 7

	qgc := connectQGC(t, node, gcsPort)
	px4A := connectPX4(t, node, vehiclePorts[0])
	px4B := connectPX4(t, node, vehiclePorts[1])

	aInc, aCom, aSys, aComp := didBytes(px4DeviceID)
	bInc, bCom, bSys, bComp := didBytes(didB)

	// 一台 QGC 同时声明两台飞机
	feed(m, qgc.ch, makeFrame(0, 0, 0x27, 0x10, 80005, registrationPayload(px4DeviceID, didB)))
	// 两台飞机各自从自己的载具端口上线
	feed(m, px4A.ch, makeFrame(aInc, aCom, aSys, aComp, 0, standbyHeartbeatPayload()))
	feed(m, px4B.ch, makeFrame(bInc, bCom, bSys, bComp, 0, standbyHeartbeatPayload()))
	// 两条待命心跳都要扇出到该 QGC（顺序不定）
	_ = expectFrame(t, qgc, 1*time.Second)
	_ = expectFrame(t, qgc, 1*time.Second)

	// 上行发往 A ⇒ 只投给 A（按 deviceID 选目标，而不是「随便哪个载具端点」）
	feed(m, qgc.ch, makeFrame(aInc, aCom, aSys, aComp, 33, encryptedPayload(1)))
	ef := expectFrame(t, px4A, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	expectNoFrame(t, px4B, 300*time.Millisecond)

	// B 的下行 ⇒ 投给配对的 QGC（帧头 deviceID 决定它属于哪台飞机）
	feed(m, px4B.ch, makeFrame(bInc, bCom, bSys, bComp, 33, encryptedPayload(2)))
	ef = expectFrame(t, qgc, 1*time.Second)
	require.Equal(t, uint32(33), ef.Frame.GetMessage().GetID())
	require.Equal(t, uint64(2), frameCounter(t, ef))
}
