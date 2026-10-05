package fifofilter

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMgmtConnNotRebuiltAfterCtxCancel 钉住「Ctx 已取消后不得再重建管理出口 socket」。
//
// 场景（真竞态，不是假想）：Ctx 取消后 mgmtRun 那个 select 的两个 case 可能**同时就绪**
// —— mgmtCh 里还有积压帧、Ctx.Done() 也已关闭 —— Go 随机挑一个。挑中数据分支就走到
// mgmtWrite ⇒ mgmtConnForWrite。而此时 cleanup 很可能已经把 mgmtConn 关掉并置 nil 了，
// 于是它**重建**一个 socket 赋回 m.mgmtConn 并返回；下一轮 select 才选中 Ctx.Done 退出。
// 问题在于 cleanup 的关闭动作**已经执行过**、mgmtWg.Wait() 之后也没有第二个关闭点
// ⇒ 这个新 fd 到进程结束都没人关。日志里唯一的痕迹是「服务已停」之后仍出现一条
// `mgmt endpoint ... socket rebuilt`。
//
// 本用例不去复现那个随机时序（复现要碰运气），而是直接钉**不变量**：Ctx 取消之后，
// mgmtConnForWrite 必须给不出连接。这既比复现竞态稳，也正是修复本身的判据。
//
// 判定力：去掉函数开头那句 `if m.Ctx.Err() != nil { return nil }`，本用例立刻红 ——
// mgmtConn 为 nil、退避窗口又早已过期（mgmtRedialAt 是零值）⇒ 它会老老实实重建一个。
func TestMgmtConnNotRebuiltAfterCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &Manager{Ctx: ctx}
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)
	m.mgmtAddr = addr

	// 先确认这一路本身是通的：Ctx 有效时能建出连接来。
	// 少了这句，下面那个 Nil 断言用「本来就建不出来」也能蒙混过关。
	conn := m.mgmtConnForWrite()
	require.NotNil(t, conn, "Ctx 有效时应当能建立管理出口连接（否则本用例是空判据）")
	require.NotNil(t, m.mgmtConn, "建出来的连接应当被记住")

	// 真实的关闭时序：run() 因 Ctx.Done 退出 ⇒ defer 触发 cleanup()，cleanup 里先关
	// 连接置 nil、再 mgmtWg.Wait()。所以「Ctx 已取消」**先于**「连接被关」成立。
	cancel()
	m.mgmtConnMu.Lock()
	m.mgmtConn.Close()
	m.mgmtConn = nil
	// ‼️ 退避窗口一并清零。不清的话，紧接着的那次调用会被 mgmtRedialInterval（1s）挡下、
	// 同样返回 nil —— 于是本用例**分不清**是 Ctx 拦下的还是退避拦下的，去掉 Ctx 检查
	// 也照样绿。本用例第一版就栽在这里（探针必须先用已知取值标定）。
	m.mgmtRedialAt = time.Time{}
	m.mgmtConnMu.Unlock()

	// 此刻 mgmtRun 若还没退出，它下一步就会问这里要连接。
	require.Nil(t, m.mgmtConnForWrite(),
		"Ctx 已取消 ⇒ 不得重建 mgmt socket：cleanup 的关闭动作已经执行过，重建出来的 fd 无人关闭")
	require.Nil(t, m.mgmtConn, "被拒绝的重建不得把连接写回 m.mgmtConn")
}
