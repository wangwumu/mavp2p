package main

import (
	"encoding/binary"
	"net"
	"os"
	"os/exec"
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

// TestMain 把配置文件的搜索路径换到一个空目录，**隔离开发机上的用户级配置**。
//
// 为什么必须隔离：newProgram 会按 configPaths 读 ~/.config/mavp2p/mavp2p.yaml，那是
// **不进版本库**的本机文件。本机只要在里面写了 fifo_mgmt_endpoint（本仓库的部署就这么
// 写），就会撞上「fifo_mgmt_endpoint 依附于 fifo_enable」那道守卫，让 TestProgramEndToEnd
// 之类的用例**在本机恒红、在 CI 恒绿** —— 红的时候错误信息还指向被测代码，实际根因在
// 被测代码之外的配置文件里。
//
// ‼️ 不能靠 `HOME=` 规避：kong.ExpandPath 走 user.Current()（读 /etc/passwd），不看 $HOME。
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mavp2p-test-config")
	if err != nil {
		panic(err)
	}
	// 指向一个**不存在**的文件：加载时静默跳过，等价于「本机没有任何配置文件」。
	// 要测配置文件本身的用例请自己临时改 configPaths，不要依赖这一份。
	configPaths = []string{filepath.Join(dir, "mavp2p.yaml")}
	code := m.Run()
	os.RemoveAll(dir) // os.Exit 不执行 defer，必须显式清理
	os.Exit(code)
}

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

// swapConfigPaths 临时把配置搜索路径换成一份测试用的 yaml。
//
// TestMain 默认把它指向一个**不存在**的文件（隔离开发机的用户级配置），所以凡是要
// 测配置文件本身的用例都必须自己换一次。它改的是**包级变量**，故只对同包内**串行**
// 执行的用例安全 —— 本文件所有用例都不调 t.Parallel()，靠的就是这一点。
func swapConfigPaths(t *testing.T, path string) {
	t.Helper()

	orig := configPaths
	configPaths = []string{path}
	t.Cleanup(func() { configPaths = orig })
}

// TestProgramConfigEndpointsMalformedStillReported 钉住「配置文件的 endpoints **校验**
// 不受命令行给没给端点影响」。
//
// 要防的形状：把 endpointsFromConfig() 整个关进 `if len(cli.Endpoints) == 0` —— 于是
// 命令行给了端点时，配置文件里写成标量（漏 `-`）的 endpoints **一个字节都不读**，
// 校验零报错。这与那个函数自己的立场（「与 Validate 同一个立场：宁可拒绝启动，也不让
// 一个写错的配置静默退化成『没配』」）直接冲突：立场被调用点的条件短路掉了。
//
// ⚠️ 别去 HEAD 里找上面说的「那种写法」——配置加载整块是本仓**未提交**的新特性，
// HEAD 的 main.go 根本不读配置文件（`git show HEAD:main.go | grep -c yamlConfiguration`
// = 0，连 endpointsFromConfig/configPaths 这两个符号都不存在）。上面描述的是本特性
// 开发中的一种写法，不是任何提交过的历史状态。
//
// 阴性对照见 TestProgramConfigEndpointsAdopted：形态正确的列表在命令行没给端点时
// 必须被采用。缺了它，本格可以靠「见配置就报错」蒙混过关。
func TestProgramConfigEndpointsMalformedStillReported(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "mavp2p.yaml")
	// 漏了 `-`：YAML 里冒号后无空格 ⇒ 解析成一个标量字符串，不是列表。
	require.NoError(t, os.WriteFile(cfg, []byte("endpoints: tcps:0.0.0.0:6666\n"), 0o644))
	swapConfigPaths(t, cfg)

	// 命令行**同时**给了端点。若校验被关进 `if len(cli.Endpoints) == 0`，本格的 err
	// 会是 nil，且进程照常监听 7777（配置文件里那个坏掉的 endpoints 无人过问）。
	_, err := newProgram([]string{"tcps:0.0.0.0:7777"})
	require.Error(t, err, "配置文件里 endpoints 形态写错必须报错，不因命令行给了端点而豁免")
	require.Contains(t, err.Error(), "必须是列表",
		"错误信息要指名形态错在哪（漏了 `-`），否则用户只知道『配置坏了』")
}

// TestProgramConfigRejectsBadKeyOrBareDuration 钉住 yamlResolver.Validate 的两半：
// key 写错、duration 写成裸数字，都必须在**启动前**拒绝，而不是静默退回默认值。
//
// 为什么走 newProgram 而不是直接调 Validate：这两半的价值全在「接没接上」。kong 的
// Resolver 接口若没人调，Validate 写得再对也拦不住任何东西 —— 把它单独拎出来测，测的是
// 它的逻辑，测不到它是否活在启动路径上（本仓正是踩过「立场被调用点的条件短路掉」）。
//
// 最后一格是阴性对照：形态正确的配置**必须能起来**。缺了它，本用例可以靠「见配置就
// 报错」蒙混过关 —— 把 Validate 改成无条件 return error，前三格照样全绿。
func TestProgramConfigRejectsBadKeyOrBareDuration(t *testing.T) {
	// 先占一个真实空闲端口：阴性对照那一格会真的把程序起起来。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	args := []string{"tcps:127.0.0.1:" + itoa(port)}

	for _, tc := range []struct {
		name    string
		yaml    string
		wantSub string // 空 ⇒ 期望**无**错（阴性对照）
	}{
		{
			name:    "连字符原样的 key 不认",
			yaml:    "map-ttl: 30s\n",
			wantSub: "无法识别的配置项",
		},
		{
			name:    "末尾的 4 是独立一段，缩写成 px4 不认",
			yaml:    "max_qgc_linked_px4: 16\n",
			wantSub: "无法识别的配置项",
		},
		{
			name:    "duration 写裸数字会被 kong 当成纳秒且不报错",
			yaml:    "map_ttl: 30\n",
			wantSub: "必须写成带单位的字符串",
		},
		{
			name: "阴性对照：两种正确写法都必须能起来",
			yaml: "map_ttl: 30s\nmapTtl: 30s\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := filepath.Join(t.TempDir(), "mavp2p.yaml")
			require.NoError(t, os.WriteFile(cfg, []byte(tc.yaml), 0o644))
			swapConfigPaths(t, cfg)

			p, err := newProgram(args)
			// ‼️ 前三格在**正确实现**下 p 必为 nil（校验先于起监听），这个 defer 是空转。
			// 但校验被架空的变异场景里，它们会真的把程序起起来并占住这个端口 —— 不关掉，
			// 第四格的阴性对照就会以 `bind: address already in use` 红，那个红与判据无关，
			// 只会掩盖「阴性对照到底有没有判别力」这个信息。
			if p != nil {
				defer p.close()
			}
			if tc.wantSub == "" {
				require.NoError(t, err, "形态正确的配置必须能起来：%s", tc.yaml)
				return
			}
			require.Error(t, err, "配置写错必须在启动前拒绝，而不是静默退回默认值：%s", tc.yaml)
			require.Contains(t, err.Error(), tc.wantSub)
		})
	}
}

// TestProgramConfigEndpointsAdopted 是上一格的阴性对照：命令行**没给**位置参数时，
// 配置文件里形态正确的列表必须被采用。这正是云端 systemd unit 的用法 —— 该 unit 的
// ExecStart 里**不含任何 ip:port**（只剩 --fifo-enable / --fifo-path / --fifo-config /
// --fifo-fallback-path 四个只管「怎么跑」的参数），端点 udps:0.0.0.0:5600 与管理出口
// fifo_mgmt_endpoint 127.0.0.1:61000 都写在 /opt/uavm/mavp2p.yaml 里，而该 unit 的
// WorkingDirectory=/opt/uavm 正是配置文件搜索路径的第一项。
// （2026-10-05 读自 39.97.235.226：/etc/systemd/system/uavm-mavp2p.service 与
// /opt/uavm/mavp2p.yaml。）
//
// ⚠️ 它**不**钉「判据读的是 args 还是 os.Args」：那条判据的两个出口是 exit / 不 exit，
// 而测试进程的 os.Args 恒非空 ⇒ 两种写法**都不**进 exit 分支，本格在两边都绿。
// 能区分它的是 TestProgramNoInputsPrintsHelp（走子进程）。
func TestProgramConfigEndpointsAdopted(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	cfg := filepath.Join(t.TempDir(), "mavp2p.yaml")
	require.NoError(t, os.WriteFile(cfg,
		[]byte("endpoints:\n  - tcps:0.0.0.0:"+itoa(port)+"\n"), 0o644))
	swapConfigPaths(t, cfg)

	// args 为空：端点只能来自配置文件。判据若误读 os.Args，这里会 PrintUsage+exit(1)，
	// 直接把测试进程带走（症状是整包 panic，不是本格红）。
	p, err := newProgram(nil)
	require.NoError(t, err, "端点只写在配置文件里时，args 为空也必须能起来")
	defer p.close()

	// 值保真：真连上去，确认监听的就是配置文件里写的那个端口（不是别的来源兜的）
	peer := newTestPeer(t, "127.0.0.1:"+itoa(port), 2, 3)
	defer peer.Close()
	<-peer.Events()
}

// noInputsEnv 让同一个测试函数在子进程里以「用户啥也没给」的身份再跑一次。
const noInputsEnv = "MAVP2P_TEST_NO_INPUTS"

// TestProgramNoInputsPrintsHelp 钉住「两个输入源都没给 ⇒ 打印用法并 exit(1)」这条
// 分支的判据读的是**入参 args**，不是 os.Args。
//
// 为什么非走子进程不可 —— 两道原因叠在一起，缺一条都还能同进程测：
//  1. 这条分支的出口是 os.Exit(1)，同进程内观察不到；
//  2. 被它拦下的前提正是「args 为空」，而主测试进程的 argv 是 `go test` 自己的（恒非空）
//     ⇒ 读 os.Args 的版本在这里**永远进不去**这个分支，同进程内无法与正确版本区分。
//
// 判据力：把 `len(args) == 0` 改回 `len(os.Args) <= 1`，子进程的 os.Args 非空 ⇒ 不 exit
// ⇒ 往下走撞上「at least one endpoint is required」，子进程那半边的 t.Fatalf 立即红。
// 正确实现下子进程 exit(1)，本格绿。
//
// ⚠️ 函数名里**刻意不含 "Usage" 这个词**，判据也刻意收紧成 `Usage:`（带冒号）。原因见
// 下面那句 require.Contains 的注释 —— 这不是洁癖，是本用例第一版真实栽过的坑。
func TestProgramNoInputsPrintsHelp(t *testing.T) {
	if os.Getenv(noInputsEnv) == "1" {
		// 子进程身份：TestMain 已把 configPaths 指向一个不存在的文件，args 也为空
		// ⇒ 两个输入源都没给 ⇒ 应当 PrintUsage 后 exit(1)，**不会**返回到这里。
		_, err := newProgram(nil)
		t.Fatalf("两个输入源都没给时应当 exit(1)，却返回了 err=%v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestProgramNoInputsPrintsHelp")
	cmd.Env = append(os.Environ(), noInputsEnv+"=1")
	out, err := cmd.CombinedOutput()

	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee, "子进程应当以非零码退出；输出：\n%s", out)

	// ‼️ 这里**刻意不**断言退出码等于 1：那一条恒绿。正确实现走 os.Exit(1)，而变异实现
	// （没拦住 ⇒ 子进程那句 t.Fatalf 生效）走的是 go test 的失败退出——**同样是 1**。
	// 两条路退出码相同 ⇒ 对目标变异零判别力。本格的判别力全在下面两条断言上。
	require.NotContains(t, string(out), "--- FAIL:",
		"子进程是 FAIL 掉了（测试没通过），不是被 usage 分支拦下的；输出：\n%s", out)

	// ‼️ 判据取 `Usage:` 而不是 `Usage`：子进程一旦失败，它的输出里**必然**含测试名
	// （`--- FAIL: <本测试函数名>`），而 go test 的失败消息又总是回显发起断言的那一行。
	// 于是只要本测试的函数名里含 `Usage` 这个子串，`out` 里就**永远**能找到它 —— 把修复
	// 变异回去，子进程清清楚楚地 FAIL 了，主进程这一格却照样绿。判据被自己的名字满足，
	// 是「红不了」的最隐蔽形态。（本用例的函数名结尾是 `…PrintsHelp`，正是不含的那种。）
	require.Contains(t, string(out), "Usage:",
		"退出前必须打印用法，否则用户不知道端点该从哪来；输出：\n%s", out)
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
