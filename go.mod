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
//
// ‼️ 依赖已 **vendor 进本仓库**（`vendor/`，随仓库提交）。构建/测试默认走 `-mod=vendor`，
// 不读取 `../gomavlib` —— Docker 构建（build context 只含本目录）与换机器构建都能过，
// 已在 fork 目录被移走的条件下实测（`go build ./...` + `go test`）。
//
// 下面的 replace **只为** `go mod vendor` / `go mod tidy` 提供 fork 源，正常构建用不到：
//   - fork 改了代码 ⇒ 必须在本目录重新跑 `go mod vendor` 并提交 `vendor/` 的差异；
//     漏做不会报错，构建会静默沿用 vendor 里的**旧**代码。
//   - 在没有 `../gomavlib` 的机器上，`go mod tidy` / `go mod vendor` 会失败（这是预期的）。
replace github.com/bluenviron/gomavlib/v4 => ../gomavlib
