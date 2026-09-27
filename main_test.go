package main

import (
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mavp2p/pkg/fifofilter"
)

const (
	gcsDeviceID = uint32(10000)    // GCS 段固定值（QGC 登记心跳帧头 deviceID）
	px4DeviceID = uint32(10000001) // PX4 段 deviceID（>= 1e7）
)

// newTestPeer 连到 mavp2p 的 TCP server，返回一个「空 dialect + MessageRaw 收发」的对端。
func newTestPeer(t *testing.T, addr string, sysid, compid byte) *gomavlib.Node {
	t.Helper()

	c := &gomavlib.Node{
		Endpoints: []gomavlib.Endpoint{
			&gomavlib.EndpointTCPClient{Address: addr},
		},
		OutVersion:       gomavlib.V2,
		OutSystemID:      sysid,
		OutComponentID:   compid,
		HeartbeatDisable: true,
		Dialect:          &dialect.Dialect{Version: 3}, // MessageRaw 收发
	}
	require.NoError(t, c.Initialize())
	return c
}

// testFrame 构造 V2 帧，帧头四字节直接组成 deviceID（§1.2 编码公式）。
// Checksum 留 0：mavp2p 与测试对端都用**空 dialect**，gomavlib 的 frame reader 在
// `GetMessage(id) == nil` 时跳过 CRC 校验（pkg/frame/reader.go）。
func testFrame(did, msgID uint32, payload []byte) *frame.V2Frame {
	return &frame.V2Frame{
		IncompatibilityFlag: byte(did >> 24),
		CompatibilityFlag:   byte(did >> 16),
		SystemID:            byte(did >> 8),
		ComponentID:         byte(did),
		Message:             &message.MessageRaw{ID: msgID, Payload: payload},
	}
}

// tlogMsgID 从 tlog 条目（8B 时间戳 + V2 帧）里取 msgID：帧内偏移 7，24 位小端。
func tlogMsgID(t *testing.T, data []byte) uint32 {
	t.Helper()

	require.GreaterOrEqual(t, len(data), 8+10, "tlog 条目应含完整 V2 帧头")
	require.Equal(t, byte(0xFD), data[8], "时间戳之后应是 V2 帧魔数")
	return uint32(data[15]) | uint32(data[16])<<8 | uint32(data[17])<<16
}

// TestProgramEndToEnd 端到端冒烟：程序装配正常（newProgram → node + 会话路由器接线），
// QGC 80005 登记心跳到达后被路由器消费、不转发给其他 client。
// 协议路由细节由 pkg/messageman 单测覆盖；server 端 node.Events() 由 run() 独占消费，
// 测试只从 client 端断言。
func TestProgramEndToEnd(t *testing.T) {
	p, err := newProgram([]string{"tcps:0.0.0.0:6666"})
	require.NoError(t, err)
	defer p.close()

	qgc := newTestPeer(t, "127.0.0.1:6666", 0x27, 0x10) // deviceID 10000（GCS 段）
	defer qgc.Close()
	other := newTestPeer(t, "127.0.0.1:6666", 2, 3)
	defer other.Close()

	// client 侧消费连接事件（server 侧连接由 newProgram.run() 处理）
	<-qgc.Events()
	<-other.Events()

	// QGC 发 80005 明文登记心跳（payload 关联 PX4 deviceID=10000001）
	err = qgc.WriteMessageAll(&message.MessageRaw{
		ID:      80005,
		Payload: []byte{1, 0x00, 0x98, 0x96, 0x81},
	})
	require.NoError(t, err)

	// 80005 仅 mavp2p 消费（§2.2/§3.2），不得转发给其他 client
	select {
	case evt := <-other.Events():
		t.Fatalf("80005 must not be forwarded to other client, got %T", evt)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestProgramFifoDownstreamWiring 端到端钉住 main.go 的**接线**（生产入口 newProgram）：
// FIFO / 管理出口只能收到「已通过会话路由全部过滤的下行帧」+ PX4 明文待命心跳，
// QGC 上行的加密 GCS 心跳一律不得抵达。
//
// 这一格是两层单测都盖不到的缺口：messageman 的 sink 测试用 fakeSink、fifofilter 的
// 测试直接用 Manager，**中间那一行接线**（`p.messageMan.Sink = p.fifoFilter`）只有本
// 测试覆盖。忘注入时 Sink 为 nil ⇒ FIFO 静默收不到任何东西，两层单测全绿。
//
// 改前 fifoFilter 是事件循环里的平级旁路（绕过全部过滤），QGC 上行心跳照样写进 FIFO，
// 该帧随后在 data_writer 推进其**当时的**单水位判重，把下行遥测饿死
// （2026-09-27 云端实测 table_telemetry 停在 10:38:54）。那时格 2/4 必红。
// 那层判重已于 2026-09-28 按 §2.6 删除，故格 2/4 现在钉的是**本组件自己的**过滤义务
// （§3.2.5），不再有一层下游兜底替它兜着。
func TestProgramFifoDownstreamWiring(t *testing.T) {
	tmpFolder := t.TempDir()

	// 管理出口：本地 UDP socket，落在临时目录之外，不碰用户任何路径
	mgmt, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer mgmt.Close()

	// 白名单：只放 0（HEARTBEAT）与 33（GLOBAL_POSITION_INT）
	cfgPath := filepath.Join(tmpFolder, "filter.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("- 0\n- 33\n"), 0o644))

	// 探一个空闲 TCP 端口，避免与并行/残留实例抢端口
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	p, err := newProgram([]string{
		"tcps:0.0.0.0:" + itoa(port),
		"--hb-disable", // 免得 mavp2p 自己的心跳混进对端的接收流
		"--fifo-enable",
		"--fifo-path", filepath.Join(tmpFolder, "test.fifo"),
		"--fifo-config", cfgPath,
		"--fifo-fallback-path", filepath.Join(tmpFolder, "fallback.tlog"),
		"--fifo-mgmt-endpoint", mgmt.LocalAddr().String(),
	})
	require.NoError(t, err)
	defer p.close()

	// 接线本身：Sink 必须**就是** fifoFilter（忘注入 ⇒ FIFO 静默全盲）
	sink, ok := p.messageMan.Sink.(*fifofilter.Manager)
	require.True(t, ok, "messageMan.Sink 应是 *fifofilter.Manager——main.go 未注入？")
	require.Same(t, p.fifoFilter, sink, "Sink 必须是同一个 fifoFilter 实例")

	addr := "127.0.0.1:" + itoa(port)
	qgc := newTestPeer(t, addr, 0x27, 0x10) // deviceID 10000（GCS 段）
	defer qgc.Close()
	px4 := newTestPeer(t, addr, 0x01, 0x01) // 用 testFrame 自行指定帧头 deviceID
	defer px4.Close()

	<-qgc.Events()
	<-px4.Events()

	// recvMgmt 读一帧管理出口报文；超时返回 nil（用于阴性格）。
	recvMgmt := func(timeout time.Duration) (uint32, bool) {
		_ = mgmt.SetReadDeadline(time.Now().Add(timeout))
		buf := make([]byte, 4096)
		n, _, err := mgmt.ReadFrom(buf)
		if err != nil {
			return 0, false
		}
		return tlogMsgID(t, buf[:n]), true
	}

	// ---- 格 1：登记 + PX4 明文待命心跳 ⇒ 进下游 ----
	require.NoError(t, qgc.WriteFrameAll(testFrame(gcsDeviceID, 80005,
		[]byte{1, 0x00, 0x98, 0x96, 0x81})))

	// 登记与待命心跳走两条 TCP 连接，到达顺序不确定；待命心跳先到的话扇出名单里还没有
	// QGC（真机日志实测过这个竞态）。原文用固定 `time.Sleep(150ms)` 是在赌调度——
	// 改成**重发直到 QGC 收到**：该心跳 payload 全零、重发幂等，观察条件就是断言本身。
	var standby *gomavlib.EventFrame
	for i := 0; i < 20 && standby == nil; i++ {
		require.NoError(t, px4.WriteFrameAll(testFrame(px4DeviceID, 0, make([]byte, 9))))
		standby = tryWaitFrame(qgc, 100*time.Millisecond)
	}
	require.NotNil(t, standby, "格 1：待命心跳应扇出给已登记的 QGC（登记是否生效？）")
	require.Equal(t, uint32(0), standby.Frame.GetMessage().GetID())

	id, ok := recvMgmt(2 * time.Second)
	require.True(t, ok, "格 1：PX4 明文待命心跳必须进下游（§3.2.5）")
	require.Equal(t, uint32(0), id)

	// 排空重发留下的 mgmt 副本：后面几格的阴性断言靠「400ms 内读不到帧」，缓冲区里
	// 的陈旧副本会被它们读成假红。
	for {
		if _, more := recvMgmt(100 * time.Millisecond); !more {
			break
		}
	}

	// ---- 格 2：QGC 加密 GCS 心跳（上行）⇒ **不得**进下游 ----
	//
	// 帧头 deviceID 必须用 **PX4 的**：mavp2p 的上行分支按「帧头 deviceID = 目标 PX4」
	// 取路由（§3.2；真机日志实测过反例：`received uplink for unknown PX4 deviceID=10000
	// ... ignoring`）。写成 QGC 自己的 deviceID 会让帧在更早的分支被丢弃——那样本格的
	// 绿是**空判据**。
	// ⚠️ 规范附录 A.1 另有一句「QGC 侧无独立 deviceID……其帧头 deviceID 恒为 10000」，
	// 与 §2.2「加密上行帧头用目标 PX4 的 D」互相冲突。本测试按**实测行为**（§2.2 + 真机
	// 日志）写；该冲突待与规范作者确认，不要照 A.1 单方面改这一格。
	require.NoError(t, qgc.WriteFrameAll(testFrame(px4DeviceID, 0, uplinkFrame(t, 1, true))))

	// 独立的阳性证据：该帧走的确实是上行主路径（被转发到了 PX4）
	require.Equal(t, uint64(1), frameCounter(t, waitFrame(t, px4, time.Second)),
		"格 2 阳性对照：QGC 上行心跳应被转发给 PX4")

	_, ok = recvMgmt(400 * time.Millisecond)
	require.False(t, ok,
		"格 2：QGC 上行加密心跳不得进下游（§3.2.5：FIFO 内容只含下行帧 + PX4 明文待命心跳）")

	// ---- 格 3：PX4 加密心跳（下行）⇒ 进下游，并作为格 2 的收尾对照 ----
	require.NoError(t, px4.WriteFrameAll(testFrame(px4DeviceID, 0, downlinkFrame(t, 2, true))))
	id, ok = recvMgmt(2 * time.Second)
	require.True(t, ok, "格 3：PX4 加密心跳是下行，必须进下游（同时证明格 2 的帧已被处理）")
	require.Equal(t, uint32(0), id)

	// 该下行也应送到配对的 QGC（§3.2 定向路由），同样消费掉
	require.Equal(t, uint64(2), frameCounter(t, waitFrame(t, qgc, time.Second)))

	// ---- 格 4：QGC 普通加密上行（非心跳）⇒ 同样不得进下游 ----
	require.NoError(t, qgc.WriteFrameAll(testFrame(px4DeviceID, 76, uplinkFrame(t, 3, false))))
	require.Equal(t, uint64(3), frameCounter(t, waitFrame(t, px4, time.Second)),
		"格 4 阳性对照：QGC 普通上行指令应被转发给 PX4")

	_, ok = recvMgmt(400 * time.Millisecond)
	require.False(t, ok, "格 4：QGC 普通上行指令不得进下游")

	// ---- 格 5：PX4 下行但 msgID 不在白名单 ⇒ 不得进下游 ----
	require.NoError(t, px4.WriteFrameAll(testFrame(px4DeviceID, 30, downlinkFrame(t, 4, false))))
	require.Equal(t, uint64(4), frameCounter(t, waitFrame(t, qgc, time.Second)),
		"格 5 阳性对照：白名单外的下行帧仍应转给配对 QGC（合法下行，只是不进下游）")

	_, ok = recvMgmt(400 * time.Millisecond)
	require.False(t, ok, "格 5：白名单外的下行帧不得进下游")

	// ---- 格 6：PX4 加密下行（白名单内）⇒ 进下游，并作为格 4/5 的收尾对照 ----
	require.NoError(t, px4.WriteFrameAll(testFrame(px4DeviceID, 33, downlinkFrame(t, 6, false))))
	id, ok = recvMgmt(2 * time.Second)
	require.True(t, ok, "格 6：白名单内的 PX4 加密下行必须进下游（同时证明格 4/5 已处理）")
	require.Equal(t, uint32(33), id, "解析器标定：非零 msgID 必须原样读出")
}

// TestProgramMgmtEndpointRequiresFifoEnable 守「只给 --fifo-mgmt-endpoint、不给
// --fifo-enable」这一配错形态必须**当场报错**，不得静默 no-op。
//
// 判定力：删掉 newProgram 里那段校验，本用例立即红——程序会正常建起来（err==nil），
// 而「管理出口根本没接上」这件事要等到下游一直收不到数据才暴露，中间零提示。
//
// 阴性对照在 TestProgramFifoDownstreamWiring：同样带 --fifo-mgmt-endpoint，但那里
// 同时给了 --fifo-enable，它必须 NoError。故本用例红的只可能是「缺总开关」这一条，
// 不是「见到 mgmt-endpoint 就拒」。
func TestProgramMgmtEndpointRequiresFifoEnable(t *testing.T) {
	// 地址形状合法即可：校验发生在建立任何 endpoint 之前，走不到监听那一步，
	// 所以这里不会占用端口、失败时也没有资源要清理。
	_, err := newProgram([]string{
		"tcps:0.0.0.0:6667",
		"--fifo-mgmt-endpoint", "127.0.0.1:12345",
	})
	require.Error(t, err, "--fifo-mgmt-endpoint 单独给出必须报错，不得静默降级成 no-op")
	require.Contains(t, err.Error(), "--fifo-enable",
		"错误信息必须点名它依附的那个开关，否则用户不知道该怎么改")
}

// TestProgramHighDeviceIDPassesParser 钉住「deviceID ≥ 0x02000000 的帧能穿过 gomavlib
// 的帧解析层」——这正是 mavp2p 必须编译 gomavlib deviceID4b fork 的**唯一理由**。
//
// 触发条件：帧头 incompatFlag 字节**就是** deviceID[31:24]（§1.2 编码公式）。stock
// gomavlib 的 `unmarshal()` 只放行 {0x00, 0x01}，其余值直接
// `unknown incompatibility flag` 丢帧——**无日志、无计数、无痕迹**。deviceID 落在
// [0x02000000, 0x7EFFFFFF]（bit24=0 是登记接口强制的唯一约束，§1.4）时
// incompat ∈ {0x02,…,0x7E} ⇒ 全部静默丢弃。丢的是**双向全部帧**：下行加密遥测、
// 上行指令、以及 PX4 的明文待命心跳（它也写满 4 字节帧头）——于是 QGC 看不到飞机
// 上线，data_writer 失去唯一在线状态源。
//
// ⚠️ 为什么非加这一格不可：本文件与 pkg/messageman 既有用例的 deviceID 只有 10000
// 与 10000001（= 0x00989681），两者 incompat 都恒为 **0x00**，而 0x00 是 stock
// **也放行**的值 ⇒ 「补 replace」前后整套测试都是绿的，覆盖缺口恰好落在越界区。
// pkg/messageman 的用例更是直接喂 frame struct（不经 parser），结构性测不到这一层。
//
// 两格对照（探针标定）：低号段与高号段都要通。去掉 replace 后**只有高号段那格会红**
// ——低号段仍绿正是判据有效的证据（否则无法区分「测试在测 deviceID 越界」与
// 「测试整个坏掉了」）。
func TestProgramHighDeviceIDPassesParser(t *testing.T) {
	for _, tc := range []struct {
		name string
		did  uint32
	}{
		{"低号段 10000001（incompat=0x00，stock 也放行）", 10000001},
		{"高号段 0x02000000（incompat=0x02，须 fork 才放行）", 0x02000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 探一个空闲 TCP 端口，避免与并行/残留实例抢端口
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			port := ln.Addr().(*net.TCPAddr).Port
			require.NoError(t, ln.Close())

			p, err := newProgram([]string{
				"tcps:0.0.0.0:" + itoa(port),
				"--hb-disable", // 免得 mavp2p 自己的心跳混进对端的接收流
			})
			require.NoError(t, err)
			defer p.close()

			addr := "127.0.0.1:" + itoa(port)
			qgc := newTestPeer(t, addr, 0x27, 0x10) // GCS 段（登记帧头用）
			defer qgc.Close()
			px4 := newTestPeer(t, addr, 0x01, 0x01) // 帧头 deviceID 由 testFrame 指定
			defer px4.Close()

			<-qgc.Events()
			<-px4.Events()

			// QGC 登记：80005 payload = count(1B) + 目标 PX4 deviceID(4B 大端)
			reg := make([]byte, 5)
			reg[0] = 1
			binary.BigEndian.PutUint32(reg[1:], tc.did)
			require.NoError(t, qgc.WriteFrameAll(testFrame(gcsDeviceID, 80005, reg)))

			// PX4 发**明文待命心跳**（msgID=0、payload 9B），帧头 deviceID = tc.did。
			// 登记与待命心跳走两条 TCP 连接，到达顺序不确定 ⇒ 重发直到 QGC 收到
			// （payload 全零、重发幂等，观察条件就是断言本身）。
			var standby *gomavlib.EventFrame
			for i := 0; i < 20 && standby == nil; i++ {
				require.NoError(t, px4.WriteFrameAll(testFrame(tc.did, 0, make([]byte, 9))))
				standby = tryWaitFrame(qgc, 100*time.Millisecond)
			}
			require.NotNil(t, standby,
				"deviceID=%d（incompat=0x%02X）的帧必须穿过解析层并扇出给 QGC——"+
					"此格若红，先查 go.mod 的 gomavlib replace 是否还在",
				tc.did, byte(tc.did>>24))

			// 值保真：不只「没丢」，回读的帧头四字节必须**逐位**等于发出去的那个
			// deviceID。只断言「收到了一帧」时，帧头被改写照样绿。
			v2, ok := standby.Frame.(*frame.V2Frame)
			require.True(t, ok, "空 dialect 下应是 *frame.V2Frame，实得 %T", standby.Frame)
			got := uint32(v2.IncompatibilityFlag)<<24 | uint32(v2.CompatibilityFlag)<<16 |
				uint32(v2.SystemID)<<8 | uint32(v2.ComponentID)
			require.Equal(t, tc.did, got, "帧头 deviceID 必须原样保真")
		})
	}
}

// waitFrame 阻塞等一个 EventFrame（超时即失败）。
func waitFrame(t *testing.T, n *gomavlib.Node, timeout time.Duration) *gomavlib.EventFrame {
	t.Helper()

	select {
	case evt := <-n.Events():
		ef, ok := evt.(*gomavlib.EventFrame)
		require.True(t, ok, "期望 EventFrame，实得 %T", evt)
		return ef
	case <-time.After(timeout):
		t.Fatal("等待帧超时")
		return nil
	}
}

// tryWaitFrame 在 timeout 内等一个 EventFrame，超时返回 nil（不失败）。
// 非帧事件（连接/断开）跳过继续等——waitFrame 遇到它们会 t.Fatal，不适合轮询场景。
func tryWaitFrame(n *gomavlib.Node, timeout time.Duration) *gomavlib.EventFrame {
	deadline := time.Now().Add(timeout)
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			return nil
		}
		select {
		case evt := <-n.Events():
			if ef, ok := evt.(*gomavlib.EventFrame); ok {
				return ef
			}
		case <-time.After(remain):
			return nil
		}
	}
}

// frameCounter 读取加密帧 payload 明文前 8 字节 counter（用于区分是哪一格发的帧）。
func frameCounter(t *testing.T, ef *gomavlib.EventFrame) uint64 {
	t.Helper()

	raw, ok := ef.Frame.GetMessage().(*message.MessageRaw)
	require.True(t, ok, "空 dialect 下应收到 MessageRaw")
	require.GreaterOrEqual(t, len(raw.Payload), 8, "加密帧至少含 8 字节 counter")
	return binary.BigEndian.Uint64(raw.Payload[:8])
}

// uplinkFrame / downlinkFrame 构造加密帧 payload，并**校验 counter 的奇偶与方向一致**。
//
// 方向由 counter 奇偶决定（messageman `odd = counter&1 == 1`）：奇数=上行(QGC)、
// 偶数=下行(PX4)。写反不会报错——帧会走另一条分支（PX4 发的奇数帧被当上行来源、
// 又不在 QGC 在线表里 ⇒ 静默 `ignoring`），于是「没进下游」看着是绿的，实际是空判据。
// 本文件首次写就栽在这里（格 5/6 用了奇数 counter，waitFrame 超时才暴露）。
func uplinkFrame(t *testing.T, counter uint64, heartbeat bool) []byte {
	t.Helper()
	require.Equal(t, uint64(1), counter%2, "上行帧 counter 必须是奇数（方向由奇偶判定）")
	if heartbeat {
		return encryptedHeartbeat(counter)
	}
	return encryptedPayload(counter)
}

func downlinkFrame(t *testing.T, counter uint64, heartbeat bool) []byte {
	t.Helper()
	require.Equal(t, uint64(0), counter%2, "下行帧 counter 必须是偶数（方向由奇偶判定）")
	if heartbeat {
		return encryptedHeartbeat(counter)
	}
	return encryptedPayload(counter)
}

// encryptedPayload 构造非心跳加密 payload：counter(8B 明文) + 28B 密文 = 36B ≥ 阈值。
func encryptedPayload(counter uint64) []byte {
	p := make([]byte, 8+28)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

// encryptedHeartbeat 构造加密 HEARTBEAT（msgID=0）的 payload：counter(8B 明文) + 密文
// （4B deviceID + 9B HEARTBEAT）+ tag(16B) = 37B ≥ encryptedPayloadMinLen(28)。
// 内容对路由器无意义——mavp2p 不解密，判据只有 msgID 与长度（§2.3/§2.5）。
func encryptedHeartbeat(counter uint64) []byte {
	p := make([]byte, 37)
	binary.BigEndian.PutUint64(p[:8], counter)
	return p
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
