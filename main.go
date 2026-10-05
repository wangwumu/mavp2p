// main executable.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kong"
	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/bluenviron/mavp2p/pkg/dumper"
	"github.com/bluenviron/mavp2p/pkg/errorman"
	"github.com/bluenviron/mavp2p/pkg/fifofilter"
	"github.com/bluenviron/mavp2p/pkg/messageman"
	"gopkg.in/yaml.v3"
)

var version = "v0.0.0"

var (
	reArgs   = regexp.MustCompile("^([a-z]+):(.+)$")
	reSerial = regexp.MustCompile("^(.+?):([0-9]+)$")
)

// generateDialect 返回协议定制后的最小 dialect。
//
// 协议（10_deviceID与payload加密公共规范.md §2.2/§3.2）下 mavp2p 是「不解密的
// 有状态会话路由器」：加密任务帧按原样透传、明文待命心跳（msgID=0）与 QGC
// 登记心跳（msgID=80005）按帧头 deviceID 号段 + payload 长度判别。因此 **dialect
// 必须保持空**：
//
//   - 任何在 dialect 内的消息一旦以加密帧出现（任务帧全部加密，含 HEARTBEAT、
//     遥测等），gomavlib 会把它解码成结构化消息、转发时重新编码成明文而损坏；
//   - 路由决策只依赖帧头 deviceID 号段、来源 socketID（channel）、msgID 与
//     payload 长度，不依赖消息字段解码（见 pkg/messageman）。
//
// 消息一律以 message.MessageRaw 透传，frame.Reader 对未知消息不丢帧、不校验
// CRC（frame/reader.go），frame.Writer 对 MessageRaw 原样写出（frame/writer.go）。
func generateDialect() *dialect.Dialect {
	return &dialect.Dialect{Version: 3, Messages: []message.Message{}}
}

type endpointType struct {
	args string
	desc string
	make func(args string) (gomavlib.Endpoint, error)
}

var endpointTypes = map[string]endpointType{
	"serial": {
		"port:baudrate",
		"serial",
		func(args string) (gomavlib.Endpoint, error) {
			matches := reSerial.FindStringSubmatch(args)
			if matches == nil {
				return nil, fmt.Errorf("invalid address")
			}

			dev := matches[1]
			baud, _ := strconv.Atoi(matches[2])

			return &gomavlib.EndpointSerial{
				Device: dev,
				Baud:   baud,
			}, nil
		},
	},
	"udps": {
		"listen_ip:port",
		"udp, server mode",
		func(args string) (gomavlib.Endpoint, error) {
			return &gomavlib.EndpointUDPServer{Address: args}, nil
		},
	},
	"udpc": {
		"dest_ip:port",
		"udp, client mode",
		func(args string) (gomavlib.Endpoint, error) {
			return &gomavlib.EndpointUDPClient{Address: args}, nil
		},
	},
	"udpb": {
		"broadcast_ip:port",
		"udp, broadcast mode",
		func(args string) (gomavlib.Endpoint, error) {
			return &gomavlib.EndpointUDPBroadcast{BroadcastAddress: args}, nil
		},
	},
	"tcps": {
		"listen_ip:port",
		"tcp, server mode",
		func(args string) (gomavlib.Endpoint, error) {
			return &gomavlib.EndpointTCPServer{Address: args}, nil
		},
	},
	"tcpc": {
		"dest_ip:port",
		"tcp, client mode",
		func(args string) (gomavlib.Endpoint, error) {
			return &gomavlib.EndpointTCPClient{Address: args}, nil
		},
	},
}

func generateEndpointConfs(endpoints []string) ([]gomavlib.Endpoint, error) {
	if len(endpoints) < 1 {
		return nil, fmt.Errorf("at least one endpoint is required")
	}

	econfs := make([]gomavlib.Endpoint, len(endpoints))

	for i, e := range endpoints {
		matches := reArgs.FindStringSubmatch(e)
		if matches == nil {
			return nil, fmt.Errorf("invalid endpoint: %s", e)
		}
		key, args := matches[1], matches[2]

		etype, ok := endpointTypes[key]
		if !ok {
			return nil, fmt.Errorf("invalid endpoint: %s", e)
		}

		conf, err := etype.make(args)
		if err != nil {
			return nil, err
		}
		econfs[i] = conf
	}

	return econfs, nil
}

var cli struct {
	Version            bool `help:"Print version."`
	Quiet              bool `short:"q" help:"Suppress info messages."`
	Print              bool `help:"Print routed frames."`
	PrintErrors        bool
	ReadTimeout        time.Duration `help:"Timeout of read operations." default:"10s"`
	WriteTimeout       time.Duration `help:"Timeout of write operations." default:"10s"`
	IdleTimeout        time.Duration `help:"Disconnect idle connections after a timeout." default:"60s"`
	HbDisable          bool          `help:"Disable heartbeats."`
	HbVersion          int           `enum:"1,2" help:"Mavlink version of heartbeats." default:"1"`
	HbSystemid         int           `default:"125"`
	HbComponentid      int           `help:"Component ID of heartbeats." default:"191"`
	HbPeriod           int           `help:"Period of heartbeats." default:"5"`
	StreamreqDisable   bool
	StreamreqFrequency int           `help:"Stream frequency to request." default:"4"`
	Dump               bool          `help:"Dump telemetry to disk"`
	DumpPath           string        `default:"dump/2006-01-02_15-04-05.tlog"`
	DumpDuration       time.Duration `help:"Maximum duration of each dump segment" default:"1h"`
	FifoEnable         bool          `help:"Enable FIFO-based filtered message output."`
	FifoPath           string        `default:"/tmp/mavp2p-filter.fifo"`
	FifoConfig         string        `default:"../filter.yaml"`
	FifoFallbackPath   string        `default:"/tmp/mavp2p-filter-fallback.tlog"`
	FifoMgmtEndpoint   string        `help:"管理出口：写 FIFO 的同时用 UDP 把同一份报文镜像到该 ip:port。报文格式＝FIFO 那份（8B 时间戳+帧字节），是本仓库自定的约定；规范附录 A.1 的 MANAGEMENT_ENDPOINT 被标注为待定、不在协议范围。空=关闭，须同时开 --fifo-enable。"`
	GCSDeviceIDMax     uint32        `help:"GCS 段上界（protocol 附录 A.1 GCS_DEVICE_ID_MAX）" default:"10000000"`
	MaxQGCLinkedPX4    int           `help:"单 QGC 最多关联 PX4 数量上限（protocol 附录 A.1 MAX_QGC_LINKED_PX4）" default:"16"`
	MapTTL             time.Duration `help:"mavp2p 映射/在线/配对缓存 TTL（protocol 附录 A.1 MAP_TTL）" default:"60s"`
	Endpoints          []string      `arg:"" optional:""`
}

type program struct {
	ctx        context.Context
	ctxCancel  func()
	wg         sync.WaitGroup
	node       *gomavlib.Node
	errorMan   *errorman.Manager
	messageMan *messageman.Manager
	dumper     *dumper.Dumper
	fifoFilter *fifofilter.Manager
}

// ---- 配置文件支持 ----
//
// kong 的 `ConfigurationLoader` 签名是 `func(io.Reader) (kong.Resolver, error)`
// （kong v1.16.1 options.go:450）。官方 yaml 支持在那个单独的 `kong-yaml` 包里，
// 而本仓库构建时**无外网**（模块缓存里也没有它），故这里自己实现一个 ——
// 复用已有的 `gopkg.in/yaml.v3`，**零新增依赖**。
//
// ‼️ 不复用 kong 自带的 `kong.JSON`，是因为它认不出的 key 会**静默返回 nil**、
// 该参数悄悄退回默认值。本实现在 Validate 里把认不出的 key 报成错误 ——
// 与 main.go 里 `--fifo-mgmt-endpoint` 那条检查同一原则（配置写错要当场暴露，
// 不能静默降级成「没有下游」）。

// yamlConfiguration 读入一份 YAML 并当作 kong 的默认值来源。
func yamlConfiguration(r io.Reader) (kong.Resolver, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return &yamlResolver{values: values}, nil
}

type yamlResolver struct {
	values map[string]any
}

// Resolve 按 flag 名找配置值，两种写法都认：
//
//	连字符换下划线（与 `--map-ttl` 直接对应）：map_ttl
//	camelCase：mapTtl
//
// ‼️ 连字符**原样**的 `map-ttl` 不在其中 —— 与 kong 内置的 kong.JSON 保持一致
// （其 resolver.go 的 JSON 函数只查这两个 key）。写错的后果由 Validate 兜住。
func (r *yamlResolver) Resolve(_ *kong.Context, _ *kong.Path, flag *kong.Flag) (any, error) {
	for _, key := range configKeys(flag.Name) {
		if v, ok := r.values[key]; ok {
			return v, nil
		}
	}
	return nil, nil
}

// Validate 把配置文件里**认不出的 key** 与**写不对的时长值**都报成错误。
//
// key 那一半：没有它，写错 key 只会**静默退回默认值**，配错的人要等到行为不对才发现。
// 两个真实的踩坑点：① 写成连字符原样的 `map-ttl`（kong 不认）；② 把单 QGC 关联上限
// 写成 `max_qgc_linked_px4` —— 它实际是 `--max-qgc-linked-px-4`，末尾的 4 是 kong
// 切出的独立一段（见 `--help`）。
//
// 值那一半（见 durationKeys）：key 对**不代表值对**。kong 对 duration 的转换是
// 「字符串走 time.ParseDuration，数字**直接当纳秒**且不报错」，于是 `map_ttl: 30`
// （想写 30 秒）会静默变成 30ns —— 三张状态表每个 prune tick 被整表清空，症状与
// 「飞机掉线」一模一样，没人会想到是配置单位写错。这一半只能在这里拦，它没有第二个关卡。
func (r *yamlResolver) Validate(app *kong.Application) error {
	known := map[string]struct{}{}
	var walk func(n *kong.Node)
	walk = func(n *kong.Node) {
		for _, f := range n.Flags {
			for _, k := range configKeys(f.Name) {
				known[k] = struct{}{}
			}
		}
		// 位置参数（只有 `endpoints` 一个）不是 flag，但在配置文件里写它是**合法的** ——
		// kong 那一遍覆盖不到它（见 endpointsFromConfig 的说明），由我们自己消费。
		// 不列进来的话，正确写法会被这里误报成「无法识别的配置项」。
		for _, p := range n.Positional {
			for _, k := range configKeys(p.Name) {
				known[k] = struct{}{}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(app.Node)

	durKeys := durationKeys(app)

	var unknown []string
	var badDur []string
	for k, v := range r.values {
		if _, ok := known[k]; !ok {
			unknown = append(unknown, k)
			continue
		}
		if _, ok := durKeys[k]; !ok {
			continue
		}
		// 走到这里 = key 认得出、且它是 duration。值必须是带单位的字符串。
		s, isStr := v.(string)
		if !isStr {
			badDur = append(badDur, fmt.Sprintf("%s（当前值 %v）", k, v))
			continue
		}
		if _, err := time.ParseDuration(s); err != nil {
			badDur = append(badDur, fmt.Sprintf("%s（当前值 %q）", k, s))
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("配置文件里有无法识别的配置项：%s（写法见 --help 的参数名换下划线，如 map_ttl；"+
			"写错 key 会静默失效，故在此拒绝启动）", strings.Join(unknown, "、"))
	}
	if len(badDur) > 0 {
		sort.Strings(badDur)
		return fmt.Errorf("配置文件里的时长必须写成带单位的字符串（如 30s、1h），不能写裸数字 —— "+
			"裸数字会被 kong 当成纳秒且不报错（写 30 想要 30 秒，实际得到 30ns）：%s",
			strings.Join(badDur, "、"))
	}
	return nil
}

// durationKeys 返回「值是 time.Duration」的配置键集合（两种写法都收）。
//
// 之所以单独一个函数、而不是把判断塞进 known：**只有 duration 有「裸数字静默变
// 纳秒」这个坑**。int flag 写裸数字本来就是对的（`max_qgc_linked_px_4: 16`），
// 一起拦会误伤。
func durationKeys(app *kong.Application) map[string]struct{} {
	out := map[string]struct{}{}
	durType := reflect.TypeOf(time.Duration(0))
	var walk func(n *kong.Node)
	walk = func(n *kong.Node) {
		for _, f := range n.Flags {
			// ‼️ Target 可能是**零值** reflect.Value，此时 .Type() **panic**（实测：
			// `reflect: call of reflect.Value.Type on zero Value`），**不**是返回 nil。
			// 曾经这里写着「返回 nil ⇒ 自然跳过，不需要额外判断」——那是错的，而且
			// 下一行正拿它当契约用。必须显式 IsValid() 挡掉。
			// 当前 kong 建出的每个 flag 的 Target 都 IsValid，所以这条守卫跑不到；
			// 它防的是将来有人加了 Target 未填充的 flag。
			if f.Value == nil || !f.Value.Target.IsValid() || f.Value.Target.Type() != durType {
				continue
			}
			for _, k := range configKeys(f.Name) {
				out[k] = struct{}{}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(app.Node)
	return out
}

// configKeys 返回一个 flag 在配置文件里可用的 key，顺序即优先级。
func configKeys(flagName string) []string {
	return []string{
		strings.ReplaceAll(flagName, "-", "_"),
		camelKey(flagName),
	}
}

// camelKey 把 `max-qgc-linked-px-4` 转成 `maxQgcLinkedPx4`。与 kong 内置 kong.JSON
// 用的 snakeCase 等价（kong 那个函数名与产出不符，它实际产出 camelCase）。
func camelKey(name string) string {
	parts := strings.Split(name, "-")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// configPaths 是配置文件的搜索路径，按序加载、后者覆盖前者。
// ‼️ 同一份切片同时喂给 kong.Configuration 和 endpointsFromConfig —— 那个机制不覆盖
// 位置参数，endpoints 是这里单独读的一遍（见下）。若两处各写一份，一旦漂移就会出现
// 「kong 读的是一个文件、endpoints 读的是另一个」。
var configPaths = []string{
	"mavp2p.yaml",
	"~/.config/mavp2p/mavp2p.yaml",
}

// endpointsFromConfig 从配置文件里取 `endpoints`（位置参数）。
//
// ‼️ 为什么位置参数要单独读一遍，而不是像别的参数那样交给 kong：
// kong 的配置文件机制**结构性覆盖不到位置参数**。resolver 的签名是
// `Resolve(ctx, path, flag *Flag)`，而位置参数的 Flag **是 nil**
// （kong model.go:259 逐字写着 `Flag *Flag // Nil if positional argument.`），
// 且 Resolve() 只遍历 `path.Flags`（v1.16.1 context.go:620 逐字为
// `for _, flag := range path.Flags`）—— 位置参数走的是 `Path.Positional`
// （同文件 :509 逐字为 `Positional: arg`），那条路上根本没有 resolver 可调。
// 2026-10-05 实测确认：把 endpoints 写进配置文件，kong 侧完全无视它。
//
// 也不把配置文件里的 endpoints 拼进 args：`optional:""` 已经让「命令行没给位置参数」
// 不会报错，于是「命令行给没给」在 Parse 之后用 `len(cli.Endpoints)` 一望即知；
// 拼 args 反而要在 Parse 之前猜哪些 token 是位置参数（`--fifo-path /x` 的值也以
// 非 `-` 开头），猜错就会把路径当成端点。
//
// 返回空切片 = 配置文件里没写这一项，调用方据此保留命令行的值。
// 写法存在但不对（标量而非列表、列表项不是字符串）则报错返回 —— 与 Validate
// 同一个立场：宁可拒绝启动，也不让一个写错的配置**静默**退化成「没配」。
func endpointsFromConfig() ([]string, error) {
	var out []string
	for _, path := range configPaths {
		// 不存在/不可读就跳过 —— 与 kong 自己读文件的行为一致（os.IsNotExist / IsPermission）。
		// YAML 语法错误也在这里吞掉：那一遍由 kong 去报，不重复报同一个错。
		data, err := os.ReadFile(kong.ExpandPath(path))
		if err != nil {
			continue
		}
		values := map[string]any{}
		if err := yaml.Unmarshal(data, &values); err != nil {
			continue
		}
		raw, ok := values["endpoints"]
		if !ok {
			continue
		}
		// ‼️ 只认列表。写成 `endpoints: udps:0.0.0.0:5600`（漏了 `-`）是 YAML 里最常见的
		// 手误，而它的后果是「这个 key 被当成没写」—— 进程照常起来，只是端点是别的来源的，
		// 症状与配置文件没被读到一个样。所以这里出声。
		list, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("配置文件 %s 里的 endpoints 必须是列表（每行前面要有 `-`），例如：\n"+
				"  endpoints:\n    - udps:0.0.0.0:5600", path)
		}
		out = out[:0] // 后者覆盖前者（先清空，故后面的空列表也能正确把它清掉）
		for i, item := range list {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("配置文件 %s 里 endpoints 的第 %d 项不是字符串", path, i+1)
			}
			out = append(out, s)
		}
	}
	return out, nil
}

func newProgram(args []string) (*program, error) {
	parser, err := kong.New(&cli,
		kong.Description("mavp2p "+version),
		kong.UsageOnError(),
		// 配置文件只提供**默认值**，命令行给的参数优先级更高（kong context.go 的
		// Resolve()：「Flag has already been set on the command-line」即 continue）。
		// 两条路径按序打开、后者覆盖前者；文件不存在 kong 会**静默跳过**，
		// 故某台机器不放 yaml 也能正常启动（用下面的 default 值）。
		kong.Configuration(yamlConfiguration, configPaths...),
		kong.ValueFormatter(func(value *kong.Value) string {
			switch value.Name {
			case "print-errors":
				return "Print parse errors singularly, instead of printing only their quantity every 5 seconds."

			case "hb-systemid":
				return "System ID of heartbeats. It is recommended to set a different system id for each router in the network."

			case "streamreq-disable":
				return "Do not request streams to Ardupilot devices," +
					" that need an explicit request in order to emit telemetry streams." +
					" This task is usually delegated to the router," +
					" in order to avoid conflicts when multiple ground stations are active."

			case "endpoints":
				desc := "Space-separated list of endpoints. At least one endpoint is required. " +
					"Possible endpoints types are:\n\n"
				for k, etype := range endpointTypes {
					desc += fmt.Sprintf("%s:%s (%s)\n\n", k, etype.args, etype.desc)
				}
				return desc

			case "dump-path":
				return "Path of dump segments, in Golang's time.Format() format"

			case "fifo-path":
				return "Path of the FIFO named pipe for filtered message output."

			case "fifo-config":
				return "Path to JSON config file containing an array of Mavlink message IDs to filter."

			case "fifo-fallback-path":
				return "Path of the fallback file used when the FIFO is full or has no reader."

			default:
				return kong.DefaultHelpValueFormatter(value)
			}
		}))
	if err != nil {
		return nil, err
	}

	kongCtx, err := parser.Parse(args)
	if err != nil {
		return nil, err
	}

	// 显式再校验一遍。⚠️ 这**不是**唯一关卡 —— kong.Parse 内部已经调过 Validate
	// （v1.16.1 kong.go:349 逐字 `if err = ctx.Validate(); err != nil`，在 Apply 之后，
	// 且带 `exitCode: exitUsageError`）。2026-10-05 用一个「Validate 必返错」的
	// resolver 实测：Parse 确实把该错误冒了出来（`Parse 返回 err = PROBE_VALIDATE_CALLED`）。
	//
	// 留着这一行的理由不是「否则没人校验」（那是错的），而是把配置文件校验变成
	// **本函数自己的契约**：不随 kong 内部调用顺序的变动而静默失效。它是幂等的，
	// 成本只有一次 map 遍历。
	if err := kongCtx.Validate(); err != nil {
		return nil, err
	}

	if cli.Version {
		fmt.Println(version)
		os.Exit(0)
	}

	// endpoints 也允许写在配置文件里，但 kong 那一遍带不动它（见 endpointsFromConfig）。
	//
	// ‼️ 解析（=校验）与采用是**两件事**，不要合进同一个 if。配置文件里 endpoints 写成
	// 标量（漏了 `-`）要当场报错，而「命令行正好也给了端点」不该让这个错误消失 —— 那正是
	// endpointsFromConfig 自己声明要避免的「静默退化成没配」。故无条件解析一次。
	// 采用才讲优先级：命令行位置参数 > 配置文件 > 都没有（下面的 generateEndpointConfs
	// 会报「at least one endpoint is required」）。命令行给了就一个字节都不碰。
	fromConfig, err := endpointsFromConfig()
	if err != nil {
		return nil, err
	}
	if len(cli.Endpoints) == 0 {
		cli.Endpoints = fromConfig
	}

	// print usage if no args are provided
	// ‼️ 判据是「两个输入源都没给」而不是「命令行没给」：endpoints 允许只写在配置文件里，
	// 那样 ARGV 就是光秃秃一个程序名，但进程该照常起来。只留 len(args) 的话，
	// 配置文件驱动的部署（systemd unit 里不写任何端点）会被这里当成「用户啥也没给」拒掉。
	//
	// ‼️ 读的是**入参 args**，不是 os.Args：在 main() 里两者等价（传的就是 os.Args[1:]），
	// 但只有读 args 才能在测试里走通这条分支 —— 读 os.Args 时进程是 `go test` 的 argv，
	// `len(os.Args) > 1` 恒成立，分支恒不进、恒不可测。
	if len(args) == 0 && len(cli.Endpoints) == 0 {
		kongCtx.PrintUsage(false) //nolint:errcheck
		os.Exit(1)
	}

	// `--fifo-mgmt-endpoint` 依附于 `--fifo-enable` 这个总开关，单独给出它是**静默
	// no-op**：程序照常启动、不建 FIFO、不镜像、零提示，配错的人要等到下游一直收不到
	// 数据才发现。与 fifofilter.Initialize 第 7 步同一原则（配置写错要当场暴露，不能
	// 静默降级成「没有下游」），故在**建立任何资源之前**直接拒绝，省掉一路清理。
	if cli.FifoMgmtEndpoint != "" && !cli.FifoEnable {
		return nil, fmt.Errorf(
			"--fifo-mgmt-endpoint 依附于 --fifo-enable，单独给出不会生效（不建 FIFO、不镜像）；" +
				"请同时加 --fifo-enable，或去掉 --fifo-mgmt-endpoint")
	}

	endpointConfs, err := generateEndpointConfs(cli.Endpoints)
	if err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	p := &program{
		ctx:       ctx,
		ctxCancel: ctxCancel,
	}

	dialect := generateDialect()

	p.node = &gomavlib.Node{
		Endpoints: endpointConfs,
		Dialect:   dialect,
		OutVersion: func() gomavlib.Version {
			if cli.HbVersion == 2 {
				return gomavlib.V2
			}
			return gomavlib.V1
		}(),
		OutSystemID:            byte(cli.HbSystemid),
		OutComponentID:         byte(cli.HbComponentid),
		HeartbeatDisable:       cli.HbDisable,
		HeartbeatPeriod:        (time.Duration(cli.HbPeriod) * time.Second),
		StreamRequestEnable:    !cli.StreamreqDisable,
		StreamRequestFrequency: cli.StreamreqFrequency,
		ReadTimeout:            cli.ReadTimeout,
		WriteTimeout:           cli.WriteTimeout,
		IdleTimeout:            cli.IdleTimeout,
	}
	err = p.node.Initialize()
	if err != nil {
		ctxCancel()
		return nil, err
	}

	p.errorMan = &errorman.Manager{
		Ctx:               ctx,
		Wg:                &p.wg,
		PrintSingleErrors: cli.PrintErrors,
	}
	err = p.errorMan.Initialize()
	if err != nil {
		ctxCancel()
		p.wg.Wait()
		p.node.Close()
		return nil, err
	}

	p.messageMan = &messageman.Manager{
		Ctx: ctx,
		Wg:  &p.wg,
		Config: messageman.Config{
			StreamReqDisable: cli.StreamreqDisable,
			GCSDeviceIDMax:   cli.GCSDeviceIDMax,
			MaxQGCLinkedPX4:  cli.MaxQGCLinkedPX4,
			MapTTL:           cli.MapTTL,
		},
		Node: p.node,
	}
	err = p.messageMan.Initialize()
	if err != nil {
		ctxCancel()
		p.wg.Wait()
		p.node.Close()
		return nil, err
	}

	if cli.Dump {
		p.dumper = &dumper.Dumper{
			Ctx:          ctx,
			Wg:           &p.wg,
			Dialect:      dialect,
			DumpPath:     cli.DumpPath,
			DumpDuration: cli.DumpDuration,
		}
		err = p.dumper.Initialize()
		if err != nil {
			ctxCancel()
			p.wg.Wait()
			p.node.Close()
			return nil, err
		}
	}

	if cli.FifoEnable {
		p.fifoFilter = &fifofilter.Manager{
			Ctx:          ctx,
			Wg:           &p.wg,
			Dialect:      dialect,
			FifoPath:     cli.FifoPath,
			ConfigPath:   cli.FifoConfig,
			FallbackPath: cli.FifoFallbackPath,
			MgmtEndpoint: cli.FifoMgmtEndpoint,
		}
		err = p.fifoFilter.Initialize()
		if err != nil {
			ctxCancel()
			p.wg.Wait()
			p.node.Close()
			return nil, err
		}
		// 下游出口接在会话路由器**之后**：FIFO 只收「已通过防重放、已确认为下行、
		// 已命中 px4Map」的帧，外加 PX4 明文待命心跳。改前它是事件循环里的平级旁路，
		// 会连 QGC 上行的加密心跳一起写进 FIFO——那帧在 2026-09-27 曾把 data_writer
		// **当时的**单水位判重抬起来、饿死下行遥测（实测零入库）；那层判重已按 §2.6 删除，
		// 所以这条接线今天的依据是 §3.2.5 对 FIFO 内容的定义，不是「下游会拦」。
		// 注入点必须早于 `go p.run()`（事件循环首次 ProcessFrame 之前）。
		p.messageMan.Sink = p.fifoFilter
	}

	if cli.Quiet {
		log.SetOutput(io.Discard)
	}

	log.Printf("mavp2p %s", version)
	log.Printf("router started with %d %s",
		len(endpointConfs),
		func() string {
			if len(endpointConfs) == 1 {
				return "endpoint"
			}
			return "endpoints"
		}())

	p.wg.Add(1)
	go p.run()

	return p, nil
}

func (p *program) close() {
	p.ctxCancel()
	p.wg.Wait()
}

func (p *program) wait() {
	p.wg.Wait()
}

func (p *program) run() {
	defer p.wg.Done()

	defer p.node.Close()

	for {
		select {
		case e := <-p.node.Events():
			switch evt := e.(type) {
			case *gomavlib.EventChannelOpen:
				log.Printf("channel opened: %s", evt.Channel)

			case *gomavlib.EventChannelClose:
				log.Printf("channel closed: %s, %s", evt.Channel, evt.Error)
				p.messageMan.ProcessChannelClose(evt)

			case *gomavlib.EventStreamRequested:
				log.Printf("stream requested to chan=%s sid=%d cid=%d", evt.Channel,
					evt.SystemID, evt.ComponentID)

			case *gomavlib.EventFrame:
				if cli.Print {
					log.Printf("%#v, %#v\n", evt.Frame, evt.Message())
				}
				p.messageMan.ProcessFrame(evt)
				if p.dumper != nil {
					p.dumper.ProcessFrame(evt)
				}
				// fifofilter 不在这里调用：它是 messageMan 的 Sink（见上方注入点），
				// 只能收到已通过会话路由全部过滤的下行帧。

			case *gomavlib.EventParseError:
				p.errorMan.ProcessError(evt)
			}

		case <-p.ctx.Done():
			return
		}
	}
}

func main() {
	p, err := newProgram(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERR: %s\n", err)
		os.Exit(1)
	}
	defer p.close()

	p.wait()
}
