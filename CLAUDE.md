# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**mavp2p** is a Mavlink proxy/bridge/router CLI tool written in Go. It links UAV flight controllers (typically connected via serial) with ground stations over a network, routing Mavlink frames between endpoints: serial, UDP (server/client/broadcast), and TCP (server/client).

Core library: [gomavlib](https://github.com/bluenviron/gomavlib) (same org).

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
2. A minimal `dialect.Dialect` is generated — it includes only messages that have `TargetSystem`/`TargetComponent` fields (for message routing) plus `MessageHeartbeat` (if heartbeats or stream requests are enabled). This keeps the dialect small and stable.
3. `gomavlib.Node` is created and initialized with all endpoints, dialect, heartbeats, stream request settings, and timeouts.
4. Three internal managers are wired up in order: `errorman.Manager` → `messageman.Manager` → `dumper.Dumper` (optional, if `--dump` flag is set).
5. The main event loop (`program.run()`) reads from `node.Events()` and dispatches events to the appropriate manager.

### Internal packages

Each package under `pkg/` follows the same pattern: a `Manager` struct with exported `Ctx`, `Wg`, and config fields, an `Initialize()` method that spawns a background goroutine, and a processing method called from the main event loop.

- **`pkg/messageman`** — Message routing logic. Tracks remote nodes in a map keyed by `(channel, systemID, componentID)`, discovered from heartbeat frames. Routes messages with `TargetSystem`/`TargetComponent` fields to the specific node; all other messages are broadcast to every other channel. Blocks `RequestDataStream` messages from ground stations when `--streamreq-disable` is not set (the default). Periodically prunes nodes that haven't been heard from in 30 seconds.

- **`pkg/errorman`** — Parse error handling. By default, batches errors and prints a count every 5 seconds to avoid log spam. With `--print-errors`, prints each error individually.

- **`pkg/dumper`** — Telemetry dump to disk. Writes frames as tlog entries via `gomavlib`'s `tlog.Writer`. Supports time-based file segmentation (`--dump-duration` flag) using Go's `time.Format` in the path template (`--dump-path`). Uses a buffered channel (size 128) to decouple the disk writer goroutine from the event loop; drops frames with a warning if the channel is full (slow disk).

### Dialect strategy

Instead of importing a full Mavlink dialect (which changes frequently), `generateDialect()` in `main.go` builds a minimal dialect at runtime using reflection: it scans `common.Dialect.Messages` for any message type that has both `TargetSystem` and `TargetComponent` struct fields. This is sufficient for routing decisions and keeps the codebase from needing constant updates for new Mavlink message definitions.

### Tests (`main_test.go`)

Integration-style tests that spin up the full program with a `tcps` endpoint, then create additional `gomavlib.Node` instances as publishers/subscribers to verify routing behavior: broadcast routing, targeted routing by system/component ID, and behavior when a target node doesn't exist. Tests use `127.0.0.1:6666`.

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
