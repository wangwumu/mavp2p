# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**mavp2p** serves as the **mav_gateway** process in the 云端无人机管理系统 (cloud UAV management system). It is the real-time data plane — a Mavlink proxy/bridge/router CLI tool written in Go. It links UAV flight controllers (typically connected via serial) with ground stations over a network, routing Mavlink frames between endpoints: serial, UDP (server/client/broadcast), and TCP (server/client).

Core library: [gomavlib](https://github.com/bluenviron/gomavlib) (same org).

Part of a three-process architecture. See `~/abc_common/docs/uavm/00_设计要点速览.md` for the full system context. The other two processes (database write, gcs server) live in `~/uavm/`.

## Build and development commands

```sh
# Build (static binary, no libc dependency)
CGO_ENABLED=0 go build .

# Cross-compile
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build .

# Run all tests (in Docker — preferred)
make test

# Run tests without Docker (requires local Go toolchain)
make test-nodocker

# Run a single test (end-to-end smoke: TestProgramEndToEnd; routing core: TestSessionRouting)
go test -v -run TestProgramEndToEnd .
go test -v -run TestSessionRouting ./pkg/messageman/

# Run tests with coverage
go test -v -race -coverprofile=coverage-pkg.txt ./pkg/...

# Format code (containerized gofumpt)
make format

# Lint (containerized golangci-lint)
make lint

# Build release binaries for all platforms (in Docker)
make binaries
```

Go version: **&ge; 1.25**. All build and test commands run inside Docker containers for reproducibility (see `scripts/*.mk`).

## Architecture

### Entry point and wiring (`main.go`)

`main.go` is the sole entry point. It uses [alecthomas/kong](https://github.com/alecthomas/kong) for CLI parsing. The `cli` struct defines all flags and positional arguments. Endpoint strings like `tcps:0.0.0.0:5600` are parsed via regex into `gomavlib.Endpoint` implementations.

The `program` struct orchestrates initialization and lifecycle:

1. CLI args are parsed → endpoint configurations are built
2. An **empty** `dialect.Dialect` is generated — see [Dialect strategy](#dialect-strategy) below.
3. `gomavlib.Node` is created and initialized with all endpoints, dialect, heartbeats, stream request settings, and timeouts.
4. Managers are wired up in order: `errorman.Manager` → `messageman.Manager` → `dumper.Dumper` (optional, if `--dump` flag is set). When FIFO output is enabled, a `fifofilter.Manager` is built as well and injected as **`messageman.Manager.Sink`** — it is *not* a peer in the event loop.
5. The main event loop (`program.run()`) reads from `node.Events()` and dispatches events to the appropriate manager.

### Internal packages

Most packages under `pkg/` follow the same pattern: a `Manager` struct with exported `Ctx`, `Wg`, and config fields, an `Initialize()` method that spawns a background goroutine, and a processing method called from the main event loop. **`pkg/fifofilter` is the exception** — its `ProcessFrame` is never called from the event loop; it is reached only through `messageman`'s `Sink`. Calling it as a peer in the loop bypasses every session filter (that was the pre-2026-09-28 wiring, and it fed QGC's uplink heartbeat straight into the FIFO).

- **`pkg/messageman`** — **Stateful session router** for the encrypted-link protocol (云无人机管理系统 `~/abc_common/docs/61013.0/10_deviceID与payload加密公共规范.md` §3.2). It does **not decrypt or authenticate** — it routes by frame-header deviceID segments + **ingress endpoint** (UDP peer = one gomavlib channel; direction is structural, see below) + msgID/payload length. **Direction is decided at ingress**: `--gcs-endpoint` (also `gcs_endpoint` in YAML) is the ground-station entry and every frame from it is an uplink; the positional `endpoints` are vehicle entries and every frame from them is a downlink. Counter parity is **never** consulted — it is a sender-side convention (protocol §2.3 nonce LSB), not a routing signal. A missing `--gcs-endpoint`, or a GCS endpoint equal to a vehicle endpoint, refuses startup: there is no single-port fallback. Maintains three state tables (PX4 mapping `deviceID→channel`, QGC online table keyed by channel, pair table `(QGC channel, PX4 deviceID, PX4 channel)`), refreshes socketIDs per received frame (§3.2.1 core rule; vehicle-endpoint frames refresh the PX4 mapping even when the source socketID drifts after a NAT rebuild, and a **missing** PX4 entry is rebuilt from the downlink frame header under §3.2.4.1 "查不到缓存就新建" — only if that header passes `validAircraftDeviceID` (PX4 segment, bit24 clear; the same gate 80005 uses), otherwise the frame is dropped with a throttled log before any watermark/Sink — otherwise a mavp2p restart while the aircraft is already encrypted would black out all downlink until landing, since PX4 sends no plaintext standby heartbeat mid-flight; uplinks from an unregistered GCS channel are dropped rather than misclassified as PX4 downlink). Entry-point role gates **two** of the three ingress special-cases: 80005 registration is accepted **only** on the GCS endpoint, and the plaintext standby heartbeat **only** on a vehicle endpoint and only for a deviceID that passes `validAircraftDeviceID` (the same full segment gate as 80005 and the downlink rebuild) — the wrong-direction and out-of-segment forms are logged (throttled per channel, 10 s) and dropped (a single-port deployment with parity guessing used to mis-accept both). The third, plaintext `RequestDataStream` (msgID 66, payload < 28 B), is **direction-agnostic**: it is dropped on both endpoints (unless `--streamreq-disable` is set, which turns the filter off) because a plaintext 66 is never valid traffic in this protocol; an *encrypted* 66 (payload ≥ 28 B) is routed normally, so do not reintroduce a direction check on it. It also registers QGC via plaintext 80005 heartbeats (parsed from `MessageRaw` payload, never forwarded to PX4), fans out PX4 plaintext standby heartbeats (msgID=0, PX4 segment, payload<28) to online QGCs, routes encrypted uplink to the target PX4, sends encrypted downlink only to the paired task QGC, does best-effort replay filtering (per-device × ingress-direction `lastNonce` from the plaintext counter), and clears pairs on PX4 standby only when that deviceID has a **downlink** nonce watermark (the sole signal separating "task ended" from "just powered on" — the plaintext standby heartbeat is indistinguishable to the router in both phases: same msgID, same 9-byte length, same deviceID segment, and the empty dialect never decodes the payload. Clearing unconditionally would wipe the pair a QGC had just registered via 80005, once per heartbeat at 20Hz). It also **suppresses the QGC→PX4 encrypted GCS heartbeat** (§2.5 "加密心跳拦截"): §2.5 lets a QGC stop sending it once it sees PX4's encrypted traffic, but the current QGC sends it at 1 Hz with no role/link guard, and each one refreshes PX4's `heartbeat_type_gcs` — so the gateway enforces the stop. Receiving an **encrypted** heartbeat **from the PX4 itself — i.e. on the downlink branch only; a QGC uplink heartbeat carries that same target deviceID in its frame header, so direction must come from the ingress endpoint and never from the header** (`msgID=0` and payload block ≥ 28 B: the plaintext standby heartbeat is always 9 B, QGC's GCS heartbeat 37 B, PX4's EXT-carrying one exactly 92 B — with no middle values, length is the one unambiguous signal for a router that never decrypts) sets a per-device link-established flag; the flag is reset **unconditionally** by that PX4's plaintext standby heartbeat (otherwise the next session's first encrypted uplink heartbeat is dropped forever and the link never comes up). A dropped heartbeat must **not** advance the replay watermark, so the check sits after the QGC/pair table refreshes — the 1 Hz heartbeat is what keeps the pair alive against `MAP_TTL` — and before the `lastNonce` write. Expect PX4 to raise `gcs_connection_lost` a few seconds later; pair this with **both** `NAV_DLL_ACT 0` and `COM_DLL_EXCEPT 1` in the airframe (`COM_DLL_EXCEPT` bit0 Mission is the second line of defence if `NAV_DLL_ACT` is ever set back to 2) or it triggers RTL mid-cruise — see 云端mavp2p接入部署指南_PX4与QGC.md §3. Prunes stale routing entries after `--map-ttl` and reclaims long-idle state tables after `--state-idle-timeout` (default 10 m); the two are separate mechanisms — see `mavp2p.yaml`'s `map_ttl` note for which timestamp each gate reads.

- **`pkg/errorman`** — Parse error handling. By default, batches errors and prints a count every 5 seconds to avoid log spam. With `--print-errors`, prints each error individually.

- **`pkg/dumper`** — Telemetry dump to disk. Writes frames as tlog entries via `gomavlib`'s `tlog.Writer`. Supports time-based file segmentation (`--dump-duration` flag) using Go's `time.Format` in the path template (`--dump-path`). Uses a buffered channel (size 128) to decouple the disk writer goroutine from the event loop; drops frames with a warning if the channel is full (slow disk).

- **`pkg/fifofilter`** — Downstream sink for `messageman`: FIFO + fallback tlog + management endpoint. Keeps only the msgIDs listed in `--fifo-config` (a YAML list) and writes each surviving frame to `--fifo-path` as a tlog entry (8-byte µs timestamp + frame bytes, byte-identical to the fallback file); when `--fifo-mgmt-endpoint` is set it mirrors the same bytes over UDP to that `ip:port`. Frames reach it via `messageman.Manager.Sink` from two injection points, both placed after every early-return branch in `messageman`: the end of `processEncrypted` (encrypted downlink — replay, direction and `px4Map` already checked) and the end of `processStandbyHeartbeat` (PX4 plaintext standby heartbeat, msgID 0). The standby path never goes through `processEncrypted`, so for those frames the **whitelist is the only gate** — a filter config missing `0` silently costs `data_writer` its liveness source. The mgmt mirror is best-effort and runs on its **own goroutine with its own queue** (`mgmtQueueSize`), separate from the FIFO queue: `handleEntry` hands each frame over **non-blocking** (queue full ⇒ that one frame is discarded with a throttled log), and that goroutine deliberately writes with **no deadline at all**. Splitting it out is what removed the need for one — the earlier 200 ms write deadline existed only because mgmt writes shared the FIFO goroutine, where a full UDP send buffer would have blocked the FIFO with it. A slow or absent mgmt consumer therefore cannot stall the FIFO path. Write failures are classified rather than blanket-retried: recoverable ones (ECONNREFUSED, EHOSTUNREACH/ENETUNREACH, ENOBUFS/ENOMEM, EAGAIN) are logged with a 5 s throttle and the socket is **kept** — the next Write retries and self-heals — while fatal ones (EBADF, ENOTSOCK, EINVAL, i.e. the socket itself is dead) drop it **and clear the redial timestamp**, so the very next frame triggers an immediate rebuild; the 1 s backoff applies only when a rebuild attempt itself fails. A rebuild is refused outright once the context is cancelled, so cleanup's close stays the last close.

### Dialect strategy

`generateDialect()` in `main.go` returns an **empty** dialect — every message stays a `message.MessageRaw` and is forwarded byte-for-byte. Two protocol-driven reasons:

1. **Encrypted frames must not be decoded**: under the protocol all task frames are encrypted while keeping the original msgID in the header. Any message present in the dialect would be decoded by gomavlib (`frame/reader.go` decodes; `frame/writer.go:63` re-encodes from the decoded struct) and corrupted — the ciphertext would be re-encoded as plaintext. `MessageRaw` is passed through untouched with the original CRC.
2. **80005 must not be in the dialect**: gomavlib computes `CRC_EXTRA` from Go struct field names reversed to snake_case (`device_id_num`/`device_ids` → 186), which differs from the XML names `deviceID_num`/`deviceIDs` used by pymavlink (138). Putting 80005 in the dialect would make the CRC check drop every QGC registration heartbeat. mavp2p parses 80005's `MessageRaw` payload directly.

Routing therefore relies only on the frame-header deviceID (recomposed from incompat/compat/sysid/compid per §1.2), the source channel (socketID), the msgID, and the payload length — never on message-field decoding.

### Dual entry endpoints (deployment)

mavp2p requires **two distinct ingress endpoints**; direction is structural, decided at ingress:

- **GCS entry** — `--gcs-endpoint` / `gcs_endpoint` (e.g. `udps:0.0.0.0:5601`). Every frame arriving here is an uplink. QGC must point at this port (`[AutoConnect] udpTargetHostPort` and `[LinkConfigurations] Link0\port0` in its `.ini`).
- **Vehicle entries** — the positional `endpoints` (e.g. `udps:0.0.0.0:5600`). Every frame arriving here is a downlink. PX4 keeps using this port, so no PX4-side change is needed.

Startup fails if `--gcs-endpoint` is missing or duplicates a vehicle endpoint, and `Initialize` cross-checks both roles against `Node.Endpoints` by object identity — there is deliberately no single-port fallback.

**Migration order (do not get this wrong).** PX4 keeps 5600; QGC must move to 5601 (`[AutoConnect] udpTargetHostPort` and `[LinkConfigurations] Link0\port0`). Move QGC **first, or in the same change window** — never deploy the new mavp2p while QGC still points at the vehicle port. A QGC left on the vehicle port is not merely unpaired; its traffic is structurally misread as **downlink**:

- its 80005 registrations are dropped (throttled log `dropped 80005 registration on vehicle endpoint`) ⇒ it never enters `qgcOnline` and never receives standby-heartbeat fan-out;
- each encrypted frame it sends carries the **target PX4's** deviceID in the header, so it can rewrite `px4Map[did].channel` to the QGC's own channel (the real PX4's next frame corrects it back, logging `PX4 socketID drifted` every time);
- its 37-byte GCS heartbeat sets that deviceID's `handshaked` flag, so the pairing logic starts suppressing heartbeats against a link that never existed;
- its sender-side **odd** counters are written into the **downlink** replay watermark, so genuine PX4 telemetry with a lower counter is dropped as replayed — a telemetry blackout until PX4's counter climbs past them.

None of this is detectable structurally (the router never decrypts, and direction comes only from the endpoint), so it is handled by deployment discipline and this document, not by a runtime check. Rollback has the same constraint in reverse: switch QGC back to 5600 **together with** restoring the old binary, not before.

### UDP send pacing

`--udp-send-delay` (YAML `udp_send_delay`, default `1µs`) is injected into `gomavlib.Node.WriteDelay`. After a datagram is actually written, gomavlib's **per-channel `runWriter` goroutine** sleeps for that interval, so consecutive packets are not handed to the kernel back-to-back and the network stack gets a chance to drain the socket buffer. It is deliberately not placed in `WriteFrameTo`/`WriteMessageTo`, which only enqueue into `chWriteTo` and return; sleeping there would throttle the single node event loop (starving every other channel) without spacing any socket. It applies only when the channel's endpoint is a datagram (UDP); a TCP `Write` is not one packet and is left alone.

`runtime.Gosched()` is **not** a substitute here: it only returns the goroutine to the run queue and does not guarantee that any amount of wall-clock time passes, so it gives the kernel no usable window to drain anything. `time.Sleep` rounds up — a `1µs` request measures roughly 30–40 µs on this host — which sets a per-channel ceiling of tens of thousands of packets per second. `0` disables it.

Two caveats when tuning: (1) kernel UDP send is already back-pressured — `Write` blocks when the socket buffer is full — so the observable loss is usually on the receiver side or in the app-level queue, not "the kernel never flushed"; (2) gomavlib's `Channel.write` enqueues with a non-blocking `select` and **drops** when the 64-slot `chWrite` is full; each queue-full drop is counted (`Channel.DroppedFrames()`) and a 5 s-throttled `WARN: gomavlib: … write queue full …` line carries the running total **as of that log line** — after a burst ends, no second WARN is printed, so the total there can understate the burst. The final counter is printed on `channel closed: … (dropped N frames …)`. There is no metrics/HTTP endpoint for it, and drops taken while a channel is already closing (ctx done) are deliberately not counted. Pacing slows the drain, so it cannot fix queue-full loss and can make it more likely under a burst.

### Tests

- **`main_test.go`** — end-to-end smoke test: spins up the full program, connects a QGC-style peer that sends an 80005 registration heartbeat, and asserts it is consumed by the router and **not** forwarded to another peer (server-side `node.Events()` is consumed by `run()`, so assertions are client-side only).
- **`pkg/messageman/manager_test.go`** — the protocol routing core: builds a real `gomavlib.Node` (empty dialect) + peers, then feeds hand-constructed `frame.V2Frame`s (frame-header deviceID set via `IncompatibilityFlag/CompatibilityFlag/SystemID/ComponentID`) into `ProcessFrame` and asserts delivery/`WriteFrameTo` behavior. Covers the full §3.2 flow (registration → standby fan-out → encrypted uplink pairing → encrypted downlink to paired QGC only → standby clearing pairs), best-effort replay filtering, and restart recovery (passive rebuild via PX4 plaintext standby heartbeat / encrypted downlink + QGC 80005).

### CI

GitHub Actions workflows in `.github/workflows/`:
- `test.yml` — runs `make test` on push/PR to main, uploads coverage to CodeCov
- `lint.yml` — runs `make lint`

## Style notes

- Zero CGo (`CGO_ENABLED=0`) — builds are fully static, compatible with Alpine Linux and lightweight distros.
- All `make` targets run in Docker containers; the Makefile variables `BASE_IMAGE` and `LINT_IMAGE` control image versions.
- Test assertions use `stretchr/testify/require` (fail-fast).
- Error handling ignores return values from certain I/O methods (see `.golangci.yml` errcheck exclusions).
- The `version` variable in `main.go` is set at link time via `-ldflags "-X main.version=$VERSION"` during release builds.
