package fifofilter

import (
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsFatalSocketErrClassifiesCorrectly 守管理出口的错误分类判据（2026-10-05 裁定）：
//
//	可恢复（网络中断 / 内核缓冲不足，会自愈）→ 忽略，**不重建 socket**
//	不可恢复（socket 本身失效）           → 重建
//
// 判定力：判据写反任何一侧都会红，且症状都隐蔽。
//   - 把 ECONNREFUSED 误判为致命 ⇒ 对端每起落一次就重建 socket，丢掉既有本地绑定；
//   - 把 EBADF 误判为可恢复 ⇒ socket 已死却永远不重建，而日志只报「mgmt endpoint
//     write failed」，与「对端未起」**长得一模一样**，运维分不出这两种局面。
func TestIsFatalSocketErrClassifiesCorrectly(t *testing.T) {
	// 不可恢复：socket 层已经废了，除了重建没有出路。
	for _, errno := range []syscall.Errno{syscall.EBADF, syscall.ENOTSOCK, syscall.EINVAL} {
		require.True(t, isFatalSocketErr(errno), "%v 应判为不可恢复", errno)

		// 真实路径上的错误是包过的（net.OpError → os.SyscallError → errno），
		// 分类必须穿透这两层。只测裸 errno 会漏掉这个前提。
		wrapped := &net.OpError{
			Op:  "write",
			Net: "udp",
			Err: os.NewSyscallError("sendto", errno),
		}
		require.True(t, isFatalSocketErr(wrapped), "包装后的 %v 应判为不可恢复", errno)
	}

	// 可恢复：这些都会自愈（下一 Write 自动重试），重建 socket 反而是错的。
	recoverable := []syscall.Errno{
		syscall.ECONNREFUSED, // 对端未起：内核回 ICMP port unreachable
		syscall.EHOSTUNREACH, // 路由/主机暂时不可达
		syscall.ENETUNREACH,
		syscall.ENOBUFS, // 内核发送缓冲不足
		syscall.ENOMEM,
		syscall.EAGAIN,
	}
	for _, errno := range recoverable {
		require.False(t, isFatalSocketErr(errno),
			"%v 会自愈，必须留在可恢复一侧，不得触发重建", errno)

		wrapped := &net.OpError{
			Op:  "write",
			Net: "udp",
			Err: os.NewSyscallError("sendto", errno),
		}
		require.False(t, isFatalSocketErr(wrapped), "包装后的 %v 同样应判为可恢复", errno)
	}

	// 关闭期：cleanup 关连接来打断阻塞的 Write，那次失败由 mgmtWrite 静默吞掉，
	// 但分类函数这一层必须把它留在可恢复侧——它绝不是「socket 坏了要重建」。
	require.False(t, isFatalSocketErr(net.ErrClosed), "net.ErrClosed 不该触发重建")
	require.False(t, isFatalSocketErr(os.ErrClosed), "os.ErrClosed 不该触发重建")
}
