package main

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/stretchr/testify/require"
)

// TestProgramEndToEnd 端到端冒烟：程序装配正常（newProgram → node + 会话路由器接线），
// QGC 80005 登记心跳到达后被路由器消费、不转发给其他 client。
// 协议路由细节由 pkg/messageman 单测覆盖；server 端 node.Events() 由 run() 独占消费，
// 测试只从 client 端断言。
func TestProgramEndToEnd(t *testing.T) {
	p, err := newProgram([]string{"tcps:0.0.0.0:6666"})
	require.NoError(t, err)
	defer p.close()

	newPeer := func(sysid, compid byte) *gomavlib.Node {
		c := &gomavlib.Node{
			Endpoints: []gomavlib.Endpoint{
				&gomavlib.EndpointTCPClient{Address: "127.0.0.1:6666"},
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

	qgc := newPeer(0x27, 0x10) // 帧头 sysid/compid → deviceID 10000（GCS 段）
	defer qgc.Close()
	other := newPeer(2, 3)
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
