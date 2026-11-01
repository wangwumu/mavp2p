package fifofilter

import (
	"sync"
	"testing"
	"time"
)

// TestThrottledLogConcurrentAccess 钉住日志节流表的线程安全。
//
// 验签通过的帧由**每设备一个 drain goroutine** 直接喂 Sink（messageman/verify.go 的
// drain），D13 免验签帧与待命心跳仍在主事件循环里喂同一个 Sink ⇒ ProcessFrame 会被
// 并发调用。FIFO 队列满时多个 goroutine 会同时进入 warnKeyed，共享的 msg/at 必须同步。
func TestThrottledLogConcurrentAccess(t *testing.T) {
	var l throttledLog
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				l.warnKeyed(time.Second, "same key", "fifo queue is full (累计已丢 %d 帧)", j)
			}
		}()
	}
	wg.Wait()
}
