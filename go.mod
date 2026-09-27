module github.com/bluenviron/mavp2p

go 1.25.0

require (
	github.com/alecthomas/kong v1.16.1
	github.com/bluenviron/gomavlib/v4 v4.0.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/sys v0.45.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/transport/v2 v2.2.10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.bug.st/serial v1.8.0 // indirect
	golang.org/x/net v0.55.0 // indirect
)

// deviceID4b 改造：deviceID[31:24] 复用帧头 incompatFlag 字节，而 stock gomavlib 的
// `unmarshal()` 只放行 {0x00, 0x01}，其余值返回 `unknown incompatibility flag` 丢帧
// —— 失败是静默的（不进路由／不进 FIFO／无日志）。见
// ~/abc_common/docs/60824.0/11_deviceID与incompat_flags冲突说明.md §五：PX4 / QGC /
// mavp2p / data_writer **四处都要**去掉这一拒绝检查。fork 只改 `pkg/frame/v2_frame.go`
// 这一处；`marshalTo()` 两边都不校验，故只影响读方向。
//
// 与 data_writer 指向**同一份** fork（那边写的是 `../../gomavlib`，同一目录）。
// ⚠️ 相对路径 replace ⇒ `scripts/*.mk` 与 `.github/workflows/*` 的 Docker 构建会失败
// （build context 只含本目录，`../gomavlib` 不存在）。发布 / CI 前需改用 module-path
// replace（fork 已有 tag `v4.0.1-deviceid4b`）或 `go mod vendor`。
replace github.com/bluenviron/gomavlib/v4 => ../gomavlib
