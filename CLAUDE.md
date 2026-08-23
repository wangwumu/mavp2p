# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**mavp2p** serves as the **mav_gateway** process in the 云端无人机管理系统 (cloud UAV management system). It is the real-time data plane — a Mavlink proxy/bridge/router CLI tool written in Go. It links UAV flight controllers (typically connected via serial) with ground stations over a network, routing Mavlink frames between endpoints: serial, UDP (server/client/broadcast), and TCP (server/client).

Core library: [gomavlib](https://github.com/bluenviron/gomavlib) (same org).

Part of a three-process architecture. See `~/CLAUDE.md` for the full system context. The other two processes (database write, gcs server) live in `~/uavm/`.

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

# Run a single test
go test -v -run TestBroadcast .

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
4. Three internal managers are wired up in order: `errorman.Manager` → `messageman.Manager` → `dumper.Dumper` (optional, if `--dump` flag is set).
5. The main event loop (`program.run()`) reads from `node.Events()` and dispatches events to the appropriate manager.

### Internal packages

Each package under `pkg/` follows the same pattern: a `Manager` struct with exported `Ctx`, `Wg`, and config fields, an `Initialize()` method that spawns a background goroutine, and a processing method called from the main event loop.

- **`pkg/messageman`** — **Stateful session router** for the encrypted-link protocol (云无人机管理系统 `~/abc_common/docs/60822.0/10_deviceID与payload加密公共规范.md` §3.2). It does **not decrypt or authenticate** — it routes by frame-header deviceID segments + source socketID (UDP peer = one gomavlib channel) + msgID/payload length. Maintains three state tables (PX4 mapping `deviceID→channel`, QGC online table keyed by channel, pair table `(QGC channel, PX4 deviceID, PX4 channel)`), refreshes socketIDs per received frame (§3.2.1 core rule; PX4 downlink frames refresh the mapping even when the source socketID drifts after a NAT rebuild, and unregistered QGC uplinks — odd-counter frames from non-QGC sources — are ignored rather than misclassified as PX4 downlink), registers QGC via plaintext 80005 heartbeats (parsed from `MessageRaw` payload, never forwarded to PX4), fans out PX4 plaintext standby heartbeats (msgID=0, PX4 segment, payload<28) to online QGCs, routes encrypted uplink to the target PX4, sends encrypted downlink only to the paired task QGC, does best-effort replay filtering (per-device × odd/even lastNonce from the plaintext counter), and clears pairs on PX4 standby (task end). Blocks plaintext `RequestDataStream` when `--streamreq-disable` is not set. Prunes stale entries after `--map-ttl`.

- **`pkg/errorman`** — Parse error handling. By default, batches errors and prints a count every 5 seconds to avoid log spam. With `--print-errors`, prints each error individually.

- **`pkg/dumper`** — Telemetry dump to disk. Writes frames as tlog entries via `gomavlib`'s `tlog.Writer`. Supports time-based file segmentation (`--dump-duration` flag) using Go's `time.Format` in the path template (`--dump-path`). Uses a buffered channel (size 128) to decouple the disk writer goroutine from the event loop; drops frames with a warning if the channel is full (slow disk).

### Dialect strategy

`generateDialect()` in `main.go` returns an **empty** dialect — every message stays a `message.MessageRaw` and is forwarded byte-for-byte. Two protocol-driven reasons:

1. **Encrypted frames must not be decoded**: under the protocol all task frames are encrypted while keeping the original msgID in the header. Any message present in the dialect would be decoded by gomavlib (`frame/reader.go` decodes; `frame/writer.go:63` re-encodes from the decoded struct) and corrupted — the ciphertext would be re-encoded as plaintext. `MessageRaw` is passed through untouched with the original CRC.
2. **80005 must not be in the dialect**: gomavlib computes `CRC_EXTRA` from Go struct field names reversed to snake_case (`device_id_num`/`device_ids` → 186), which differs from the XML names `deviceID_num`/`deviceIDs` used by pymavlink (138). Putting 80005 in the dialect would make the CRC check drop every QGC registration heartbeat. mavp2p parses 80005's `MessageRaw` payload directly.

Routing therefore relies only on the frame-header deviceID (recomposed from incompat/compat/sysid/compid per §1.2), the source channel (socketID), the msgID, and the payload length — never on message-field decoding.

### Tests

- **`main_test.go`** — end-to-end smoke test: spins up the full program, connects a QGC-style peer that sends an 80005 registration heartbeat, and asserts it is consumed by the router and **not** forwarded to another peer (server-side `node.Events()` is consumed by `run()`, so assertions are client-side only).
- **`pkg/messageman/manager_test.go`** — the protocol routing core: builds a real `gomavlib.Node` (empty dialect) + peers, then feeds hand-constructed `frame.V2Frame`s (frame-header deviceID set via `IncompatibilityFlag/CompatibilityFlag/SystemID/ComponentID`) into `ProcessFrame` and asserts delivery/`WriteFrameTo` behavior. Covers the full §3.2 flow (registration → standby fan-out → encrypted uplink pairing → encrypted downlink to paired QGC only → standby clearing pairs), best-effort replay filtering, and restart recovery (passive rebuild via PX4 heartbeat + QGC 80005).

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
