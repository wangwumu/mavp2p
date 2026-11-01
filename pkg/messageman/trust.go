package messageman

import (
	"time"

	"github.com/bluenviron/gomavlib/v4"
)

// trustedMark 是「最近一次被接受为可信的下行」的证据（03 §4.3.3）。
//
// 刷新源只有两类：①验签通过的下行；②handshaked==true 且来自当前权威通道的免验签下行。
// 它不参与路由决策，只用于待命闸的近期会话证据、同通道任务结束判定、候选保护。
// 按 StateIdleTimeout 回收，**不随 MapTTL 消失**。
type trustedMark struct {
	at       time.Time
	channel  *gomavlib.Channel
	sourceIP string
}

// gateState 是任务结束后的会话门（03 §4.3.4）。
//
// 任务结束时置 endedAt、清空 openedAt；任务结束后第一条被实际转发的 QGC 上行置 openedAt。
// 只有 openedAt >= endedAt（或没有门）时，下行才允许进入验签/晋升流程。
type gateState struct {
	endedAt  time.Time
	openedAt time.Time
}
