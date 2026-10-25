
# mavp2p

[![Test](https://github.com/bluenviron/mavp2p/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/bluenviron/mavp2p/actions/workflows/test.yml?query=branch%3Amain)
[![Lint](https://github.com/bluenviron/mavp2p/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/bluenviron/mavp2p/actions/workflows/lint.yml?query=branch%3Amain)
[![CodeCov](https://codecov.io/gh/bluenviron/mavp2p/branch/main/graph/badge.svg)](https://app.codecov.io/gh/bluenviron/mavp2p/tree/main)
[![Release](https://img.shields.io/github/v/release/bluenviron/mavp2p)](https://github.com/bluenviron/mavp2p/releases)
[![Docker Hub](https://img.shields.io/badge/docker-bluenviron/mavp2p-blue)](https://hub.docker.com/r/bluenviron/mavp2p)

> **⚠️ 定制 fork（云无人机管理系统 mav_gateway）**
>
> 本仓库是 `bluenviron/mavp2p` 的定制 fork，用作「云无人机管理系统」的 **mav_gateway**
>（实时数据面进程）。协议规范见 `~/abc_common/docs/61013.0/10_deviceID与payload加密公共规范.md` §3.2。
>
> 定制点：
> - **有状态会话路由器**（`pkg/messageman`）：按帧头 deviceID 号段（GCS 段 / PX4 段）+
>   **入口端点决定的上下行方向** + msgID/payload 长度路由 QGC↔PX4 会话，支撑多 QGC / 多 PX4
>   广域网（NAT/VPN）场景。**不解密**——加密任务帧按原样透传（空 dialect，全部 `MessageRaw`）。
> - **两个入口端点（必填）**：`--gcs-endpoint`（或 YAML `gcs_endpoint`）收地面站，位置参数
>   收载具。方向**只**由帧从哪个端点进来决定：GCS 端点 = 上行，载具端点 = 下行。
>   不读 counter 的奇偶。缺 `--gcs-endpoint`、或它与某个载具端点相同 ⇒ 拒绝启动。
>   ⚠️ QGC 必须连 `--gcs-endpoint` 的端口；仍是旧单端口配置时先迁 QGC 再换本进程，否则
>   QGC 的帧会被当成下行（详见 `CLAUDE.md` 的 "Dual entry endpoints (deployment)"）。
> - **80005 QGC 登记/保活心跳**：明文解析（payload = 关联 PX4 deviceID 集合），仅 mavp2p 消费、
>   不转发给 PX4。
> - **明文待命心跳**（msgID=0, PX4 段, payload<28）：登记 PX4 映射，扇出给在线 QGC。
> - **边缘防重放**：读明文 counter，按 deviceID × **入口方向**（上行/下行各一条水位）尽力判重；
>   counter 的奇偶不参与任何判定。
> - **FIFO → data_writer**：`--fifo-enable` 把下行帧经命名管道转 data_writer（消息白名单见
>   `filter.yaml`）。
>
> 上游通用功能（多端点桥接、串口/TCP/UDP、域名、重连等）仍可用。

_mavp2p_ is a flexible and efficient Mavlink proxy / bridge / router, implemented in the form of a command-line utility. It is used primarily to link UAV flight controllers, connected through a serial port, with ground stations on a network, but can be used to build any kind of routing involving serial, TCP and UDP, allowing communication across different physical layers or transport layers.

This project makes use of the [**gomavlib**](https://github.com/bluenviron/gomavlib) library, a full-featured Mavlink library.

Features:

* Link together an arbitrary number of different kinds of endpoints:
  * Serial
  * UDP (client, server or broadcast mode)
  * TCP (client or server mode)
* Support Mavlink 2.0 and 1.0, support any dialect
* Emit heartbeats
* Automatically request streams to Ardupilot devices and block stream requests from ground stations
* Route messages by target system ID / component ID
* Use domain names in place of IPs
* Reconnect to TCP/UDP servers when disconnected, remove inactive TCP/UDP clients
* Dump telemetry to disk
* Multiplatform, available for multiple operating systems (Linux, Windows) and architectures (arm6, arm7, arm64, amd64), independent from libc and compatible with lightweight distros (Alpine Linux)

> ⚠️ **上面是上游的功能列表；本 fork 有三项不成立**（原因见 `CLAUDE.md` 的 "Dialect strategy"）：
> 本 fork 生成**空 dialect**（全部 `MessageRaw` 透传），因此
> （1）**不发射节点心跳**——`--hb-*` 均不生效；
> （2）**不发起自动 stream request**——`--streamreq-frequency` 不生效，而 `--streamreq-disable`
> 的真实作用是关闭**明文 `RequestDataStream`（msgID 66, payload<28B）的入口丢弃**；
> （3）**不按 target system/component ID 路由**——路由依据是入口端点方向 + 帧头 deviceID + 配对表。

## Table of contents

* [Installation](#installation)
  * [Standalone binary](#standalone-binary)
  * [Docker image](#docker-image)
  * [OpenWrt binary](#openwrt-binary)
* [Usage](#usage)
* [Connecting popular software](#connecting-popular-software)
  * [QGroundControl](#qgroundcontrol)
  * [Mission Planner](#mission-planner)
  * [PX4 Simulator](#px4-simulator)
* [Comparison with similar software](#comparison-with-similar-software)
* [Full command-line usage](#full-command-line-usage)
* [Compile from source](#compile-from-source)
  * [Standard](#standard)
  * [OpenWrt](#openwrt)
  * [Cross compile](#cross-compile)
* [Specifications](#specifications)
* [Links](#links)

## Installation

There are several installation methods available: standalone binary, Docker image and OpenWRT binary.

### Standalone binary

Download and extract a standalone binary from the [release page](https://github.com/bluenviron/mavp2p/releases) that corresponds to your operating system and architecture.

### Docker image

There's a image available at `bluenviron/mavp2p`:

```
docker run --rm -it --network=host bluenviron/mavp2p
```

### OpenWrt binary

If the architecture of the OpenWrt device is amd64, armv6, armv7 or arm64, use the [standalone binary method](#standalone-binary) and download a Linux binary that corresponds to your architecture.

Otherwise, [compile the software from source](#openwrt).

## Usage

> **⚠️ 本 fork 的必填参数**：位置参数是**载具端点**，地面站入口另用 `--gcs-endpoint`
> （或 YAML `gcs_endpoint`）给出，两者都必须存在且互不相同。下面的示例都已按此改写。
> 典型 mav_gateway 部署：PX4 连 `udps:0.0.0.0:5600`（载具端点），QGC 连
> `udps:0.0.0.0:5601`（`--gcs-endpoint`）。

Link a serial port (vehicle) with a UDP ground station:

```
./mavp2p serial:/dev/ttyAMA0:57600 --gcs-endpoint udpc:1.2.3.4:5600
```

Link a serial port (vehicle) with a UDP server ground-station entry:

```
./mavp2p serial:/dev/ttyAMA0:57600 --gcs-endpoint udps:0.0.0.0:5601
```

Link a UDP vehicle endpoint in broadcast mode with a TCP ground-station entry:

```
./mavp2p udpb:192.168.7.255:5600 --gcs-endpoint tcps:0.0.0.0:5601
```

Create a server that links together all UDP endpoints that connect to it:
(the set of endpoints reaching a UDP server port is a **vehicle** entry; ground stations
need their own endpoint, otherwise direction cannot be told apart)

```
./mavp2p udps:0.0.0.0:5600 --gcs-endpoint udps:0.0.0.0:5601
```

Dump telemetry to disk:

```
./mavp2p udps:0.0.0.0:5600 --gcs-endpoint udps:0.0.0.0:5601 \
  --dump --dump-path="dump/2006-01-02_15-04-05.tlog"
```

Enable the FIFO → data_writer channel (protocol §3.2.5, typical mav_gateway deployment):

```
./mavp2p udps:0.0.0.0:5600 --gcs-endpoint udps:0.0.0.0:5601 --fifo-enable \
  --fifo-path /tmp/mavp2p-filter.fifo --fifo-config filter.yaml
```

Session-router parameters (protocol 附录 A.1, must match QGC/PX4 on all three sides):

| Flag | Default | Description |
|------|---------|-------------|
| `--gcs-endpoint` | — (**必填**) | 地面站入口端点（QGC 连这里）。方向判据，不得与任何载具端点相同 |
| `--gcs-device-id-max` | `10000000` | GCS 段上界：deviceID < 该值判为 QGC/地面站，≥ 判为 PX4 |
| `--max-qgc-linked-px-4` | `16` | **单帧** 80005 声明的台数上限，不是该 QGC 的关联总数（超过需分片，一轮 = `ceil(n/16)×10s`） |
| `--map-ttl` | `60s` | 路由表存活 TTL（PX4 映射 / QGC 在线 / 配对） |
| `--state-idle-timeout` | `10m` | 长期状态表空闲回收阈值：配对、防重放水位、`hbDiag`/`fanoutDiag`/`dropDiag`；`regDiag` 例外，随 `qgcOnline` 成员关系回收 |
| `--udp-send-delay` | `1µs` | 每个 UDP 数据报写出后在该通道写协程内等待的间隔，避免连续写包、给内核排空发送缓冲区留时间（`0`=关闭，负值拒绝启动）。只作用于 UDP（datagram 端点）：TCP 也会调用但立即返回（不生效）。`time.Sleep` 会向上取整：请求 1µs 在本机实测约 30–40µs。⚠️ 写在 YAML 里必须带单位：`udp_send_delay: 0s`，裸 `0` 会被当作无单位时长拒绝 |

## Connecting popular software

### QGroundControl

1. Make sure that mavp2p is started with both entry endpoints — the vehicle entry
   (PX4 connects here) and `--gcs-endpoint` (QGC connects here):

   ```
   ./mavp2p udps:0.0.0.0:5600 --gcs-endpoint udps:0.0.0.0:5601
   ```

2. Install and open QGroundControl.

3. Open _Application Settings_, click on _Comm Links_, _Add_. Fill the form with:
   * Name: some name
   * Type: UDP
   * Port: 0
   * Server Addresses: mavp2p-ip:**5601** (the `--gcs-endpoint` port, then click _Add Server_)

   In a QGC `.ini` these are `[AutoConnect] udpTargetHostPort` and
   `[LinkConfigurations] Link0\port0`. Pointing QGC at the **vehicle** port (5600) makes
   every frame it sends be treated as a downlink — see the migration note above.

4. Click _Ok_. Click _Connect_.

### Mission Planner

1. Make sure that mavp2p is started with a vehicle entry and a TCP `--gcs-endpoint`
   (Mission Planner is a ground station, so it must land on the GCS entry):

   ```
   ./mavp2p udps:0.0.0.0:5600 --gcs-endpoint tcps:0.0.0.0:5601
   ```

2. Install and open Mission Planner.

3. In the top right corner of the Window, from the select menu, change the value from "AUTO" to "TCP". Then click _Connect_. Insert the mavp2p IP and port.

### PX4 Simulator

1. Make sure that mavp2p is started with a UDP vehicle endpoint on port 14550
   (PX4 is the vehicle) plus a GCS entry for the ground station:

   ```
   ./mavp2p udps:0.0.0.0:14550 --gcs-endpoint udps:0.0.0.0:14551
   ```

2. [Install PX4 Simulator](https://docs.px4.io/main/en/simulation/).

3. If mavp2p and PX4 are on two different machines, enable Mavlink broadcasting by opening `PX4-Autopilot/ROMFS/px4fmu_common/init.d-posix/px4-rc.mavlink`, finding `mavlink start` and adding the `-p` flag:

   ```
   mavlink start -x -u $udp_gcs_port_local -r 4000000 -f -p
   ```

4. Start PX4 Simulator:

   ```
   make px4_sitl gz_x500
   ```

## Comparison with similar software

_mavp2p_ vs _mavproxy_

* Does not require python nor any interpreter
* Much lower CPU and memory usage
* Supports an arbitrary number of inputs and outputs
* Logs can be disabled, resulting in no disk I/O
* UDP clients are removed when inactive

_mavp2p_ vs _mavlink-router_

* Supports domain names
* Supports multiple TCP servers
* UDP clients are removed when inactive
* Supports automatic stream requests to Ardupilot devices

## Full command-line usage

```
Usage: mavp2p [<endpoints> ...] [flags]

mavp2p v0.0.0

Arguments:
  [<endpoints> ...]    Space-separated list of **vehicle** endpoints (PX4 →
                       mavp2p, downlink ingress). At least one is required.
                       Ground stations connect to --gcs-endpoint instead;
                       direction is decided by which endpoint a frame arrives
                       on, never by counter parity. Possible endpoints types
                       are:

                       udpb:broadcast_ip:port (udp, broadcast mode)

                       tcps:listen_ip:port (tcp, server mode)

                       tcpc:dest_ip:port (tcp, client mode)

                       serial:port:baudrate (serial)

                       udps:listen_ip:port (udp, server mode)

                       udpc:dest_ip:port (udp, client mode)

Flags:
  -h, --help                      Show context-sensitive help.
      --version                   Print version.
  -q, --quiet                     Suppress ALL log output, including warnings (not
                                  only info messages).
      --print                     Print routed frames.
      --print-errors              Print parse errors singularly, instead of
                                  printing only their quantity every 5 seconds.
      --read-timeout=10s          TCP/UDP client 端点（tcpc/udpc）建连尝试的超时；不是
                                  每次读的超时（读截止由 --idle-timeout 承担）。serial 端点两者都不适用。
      --write-timeout=10s         Timeout of write operations.
      --idle-timeout=60s          Disconnect idle connections after a timeout.
      --udp-send-delay=1µs        每个 UDP 数据报写出后，在该通道自己的写协程内等待的间隔，避免连续写包、
                                  给内核排空发送缓冲区留时间（0=关闭）。只作用于 datagram 端点；
                                  TCP 也会调用但立即返回（不生效）。
                                  注意 time.Sleep 会向上取整：请求 1µs 在 Linux 上实测约 30–40µs。
      --hb-disable                （本 fork 不生效：空 dialect 不发射节点心跳）Disable heartbeats.
      --hb-version=1              （本 fork 不生效）Mavlink version of heartbeats.
      --hb-systemid=125           （本 fork 不生效）System ID of heartbeats.
      --hb-componentid=191        （本 fork 不生效）Component ID of heartbeats.
      --hb-period=5               （本 fork 不生效）Period of heartbeats.
      --streamreq-disable         关闭明文 RequestDataStream（msgID 66、payload<28B）的入口丢弃；
                                  默认两个端点都丢。它**不是**上游「自动请求」的开关——空 dialect
                                  下自动请求本就不发生。
      --streamreq-frequency=4     （本 fork 不生效：空 dialect 不发起 stream request）
      --dump                      Dump telemetry to disk
      --dump-path="dump/2006-01-02_15-04-05.tlog"
                                  Path of dump segments, in Golang's
                                  time.Format() format
      --dump-duration=1h          Maximum duration of each dump segment
      --fifo-enable               Enable FIFO-based filtered message output.
      --fifo-path="/tmp/mavp2p-filter.fifo"
                                  Path of the FIFO named pipe for filtered
                                  message output.
      --fifo-config="../filter.yaml"
                                  Path to YAML config file containing an array
                                  of Mavlink message IDs to filter.
      --fifo-fallback-path="/tmp/mavp2p-filter-fallback.tlog"
                                  Path of the fallback file used when the FIFO
                                  is full or has no reader.
      --fifo-mgmt-endpoint=STRING
                                  管理出口：写 FIFO 的同时用 UDP 把同一份报文镜像到该
                                  ip:port。报文格式＝FIFO 那份（8B
                                  时间戳+帧字节），是本仓库自定的约定；规范附录 A.1 的
                                  MANAGEMENT_ENDPOINT 被标注为待定、不在协议范围。空=关闭，须同时开
                                  --fifo-enable。
      --gcs-device-id-max=10000000
                                  GCS 段上界（protocol 附录 A.1 GCS_DEVICE_ID_MAX）
      --max-qgc-linked-px-4=16    单帧 80005 声明的台数上限（protocol 附录 A.1
                                  MAX_QGC_LINKED_PX4）；不是该 QGC 的关联总数，超过需分片
      --map-ttl=60s               mavp2p 映射/在线/配对缓存 TTL（protocol 附录 A.1 MAP_TTL）
      --state-idle-timeout=10m    长期状态表空闲回收阈值：配对与防重放水位在加密流量静默超过该时长后回收（80005
                                  周期登记不算流量）；诊断表各有自己的判据，见 messageman.prune
      --gcs-endpoint=STRING       地面站入口端点（QGC→载具的上行入口），写法与位置参数相同，例如
                                  udps:0.0.0.0:5601。方向由入口端点决定：本端点进来的帧一律按上行，位置参数（载具端点）进来的帧一律按下行。必须配置，且不得与任何载具端点相同。
```

## Compile from source

### Standard

Install git and Go &ge; 1.25. Clone the repository, enter into the folder and start the building process:

```sh
git clone https://github.com/bluenviron/mavp2p
cd mavp2p
CGO_ENABLED=0 go build .
```

The command will produce the `mavp2p` binary.

### OpenWrt

The compilation procedure is the same as the standard one. On the OpenWrt device, install git and Go:

```sh
opkg update
opkg install golang git git-http
```

Clone the repository, enter into the folder and start the building process:

```sh
git clone https://github.com/bluenviron/mavp2p
cd mavp2p
CGO_ENABLED=0 go build .
```

The command will produce the `mavp2p` binary.

If the OpenWrt device doesn't have enough resources to compile, you can [cross compile](#cross-compile) from another machine.

### Cross compile

Cross compilation allows to build an executable for a target machine from another machine with different operating system or architecture. This is useful in case the target machine doesn't have enough resources for compilation or if you don't want to install the compilation dependencies on it.

On the machine you want to use to compile, install git and Go &ge; 1.25. Clone the repository, enter into the folder and start the building process:

```sh
git clone https://github.com/bluenviron/mavp2p
cd mavp2p
CGO_ENABLED=0 GOOS=my_os GOARCH=my_arch go build .
```

Replace `my_os` and `my_arch` with the operating system and architecture of your target machine. A list of all supported combinations can be obtained with:

```sh
go tool dist list
```

For instance:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build .
```

In case of the `arm` architecture, there's an additional flag available, `GOARM`, that allows to set the ARM version:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOARM=7 go build .
```

In case of the `mips` architecture, there's an additional flag available, `GOMIPS`, that allows to set additional parameters:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=mips GOMIPS=softfloat go build .
```

The command will produce the `mavp2p` binary.

## Specifications

* [Mavlink specifications](https://github.com/bluenviron/gomavlib#specifications)

## Links

Related projects

* [gomavlib](https://github.com/bluenviron/gomavlib)

Similar software

* [MavProxy](https://github.com/ArduPilot/MAVProxy)
* [mavlink-router](https://github.com/intel/mavlink-router)
