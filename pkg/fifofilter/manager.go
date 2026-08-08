// Package fifofilter contains a FIFO-based Mavlink message filter.
package fifofilter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/tlog"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const (
	queueSize = 128
	// FIONREAD is not exported by golang.org/x/sys/unix for all architectures.
	fionread = 0x541B
)

// Manager is a FIFO-based Mavlink message filter.
type Manager struct {
	Ctx          context.Context
	Wg           *sync.WaitGroup
	Dialect      *dialect.Dialect
	FifoPath     string
	ConfigPath   string
	FallbackPath string

	// private
	messageIDs   map[uint32]struct{}
	dialectRW    *dialect.ReadWriter
	encodeBuf    bytes.Buffer
	frameWriter  *frame.Writer
	fifoFd       *os.File
	fallbackFile *os.File
	fallbackTlog *tlog.Writer
	chEntry      chan *tlog.Entry
}

// Initialize initializes the Manager.
func (m *Manager) Initialize() error {
	// 1. Load YAML config file: list of uint32 message IDs, one per line
	data, err := os.ReadFile(m.ConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read filter config %q: %w", m.ConfigPath, err)
	}
	var ids []uint32
	if err := yaml.Unmarshal(data, &ids); err != nil {
		return fmt.Errorf("failed to parse filter config %q: %w", m.ConfigPath, err)
	}
	m.messageIDs = make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		m.messageIDs[id] = struct{}{}
	}

	// 2. Initialize dialect ReadWriter
	m.dialectRW = &dialect.ReadWriter{Dialect: m.Dialect}
	if err := m.dialectRW.Initialize(); err != nil {
		return err
	}

	// 3. Initialize frame writer backed by bytes.Buffer for marshaling
	m.frameWriter = &frame.Writer{
		ByteWriter: &m.encodeBuf,
		DialectRW:  m.dialectRW,
	}
	if err := m.frameWriter.Initialize(); err != nil {
		return err
	}

	// 4. Ensure FIFO exists
	if err := m.ensureFifo(); err != nil {
		return err
	}

	// 5. Try to open FIFO (non-blocking; succeeds only if a reader is present)
	m.fifoFd, _ = openFifoWrite(m.FifoPath)

	// 6. Open fallback file
	m.fallbackFile, err = os.Create(m.FallbackPath)
	if err != nil {
		return fmt.Errorf("failed to create fallback file %q: %w", m.FallbackPath, err)
	}
	m.fallbackTlog = &tlog.Writer{
		ByteWriter: m.fallbackFile,
		DialectRW:  m.dialectRW,
	}
	if err := m.fallbackTlog.Initialize(); err != nil {
		return fmt.Errorf("failed to init fallback tlog: %w", err)
	}

	// 7. Allocate buffered channel
	m.chEntry = make(chan *tlog.Entry, queueSize)

	// 8. Spawn background goroutine
	m.Wg.Add(1)
	go m.run()

	return nil
}

func (m *Manager) ensureFifo() error {
	info, err := os.Stat(m.FifoPath)
	if err == nil {
		if info.Mode()&os.ModeNamedPipe == 0 {
			return fmt.Errorf("%q exists but is not a FIFO", m.FifoPath)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat %q: %w", m.FifoPath, err)
	}
	if err := unix.Mkfifo(m.FifoPath, 0o666); err != nil {
		return fmt.Errorf("failed to create FIFO %q: %w", m.FifoPath, err)
	}
	return nil
}

func openFifoWrite(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func (m *Manager) run() {
	defer m.Wg.Done()
	defer m.cleanup()

	for {
		select {
		case entry := <-m.chEntry:
			if err := m.handleEntry(entry); err != nil {
				log.Printf("WARN: fifofilter: %s", err)
			}

		case <-m.Ctx.Done():
			return
		}
	}
}

func (m *Manager) handleEntry(entry *tlog.Entry) error {
	// Marshal frame to internal buffer
	m.encodeBuf.Reset()
	if err := m.frameWriter.Write(entry.Frame); err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	frameBytes := m.encodeBuf.Bytes()

	// Build tlog format: 8-byte timestamp (µs, big-endian) + frame bytes.
	// Same format as the fallback .tlog file, so FIFO and fallback are interchangeable.
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(entry.Time.UnixMicro()))
	tlogData := append(ts[:], frameBytes...)

	// Try to write to FIFO
	if err := m.writeToFifo(tlogData); err != nil {
		// FIFO unavailable or full — fall back to .tlog file.
		// The frame has already been encoded to MessageRaw by frameWriter.Write above;
		// tlog.Writer handles that correctly (re-marshals without re-encoding).
		return m.fallbackTlog.Write(entry)
	}
	return nil
}

func (m *Manager) writeToFifo(data []byte) error {
	// Reopen FIFO if not currently open (reader may have appeared)
	if m.fifoFd == nil {
		fd, err := openFifoWrite(m.FifoPath)
		if err != nil {
			return fmt.Errorf("fifo not available: %w", err)
		}
		m.fifoFd = fd
	}

	// Check FIFO buffer space before writing
	if !m.fifoHasSpace(len(data)) {
		return fmt.Errorf("FIFO buffer full (need %d bytes)", len(data))
	}

	// Write tlog-format data (8-byte timestamp + frame) to FIFO
	if _, err := m.fifoFd.Write(data); err != nil {
		// Write failed - reader may have disconnected
		m.fifoFd.Close()
		m.fifoFd = nil
		return fmt.Errorf("fifo write failed: %w", err)
	}

	return nil
}

func (m *Manager) fifoHasSpace(frameSize int) bool {
	fd := int(m.fifoFd.Fd())

	// Get total pipe capacity
	capacity, err := unix.FcntlInt(uintptr(fd), unix.F_GETPIPE_SZ, 0)
	if err != nil {
		return false
	}

	// Get current bytes in pipe via ioctl(FIONREAD)
	var unread int32
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL, uintptr(fd), fionread, uintptr(unsafe.Pointer(&unread)),
	)
	if errno != 0 {
		return false
	}

	available := capacity - int(unread)
	return available >= frameSize
}

// ProcessFrame processes a frame from the event loop.
// It checks if the message ID is in the allowed set and, if so,
// enqueues it for writing to the FIFO (or fallback file).
func (m *Manager) ProcessFrame(evt *gomavlib.EventFrame) {
	msgID := evt.Message().GetID()
	if _, ok := m.messageIDs[msgID]; !ok {
		return
	}

	select {
	case m.chEntry <- &tlog.Entry{
		Time:  time.Now(),
		Frame: evt.Frame,
	}:
	case <-m.Ctx.Done():
	default:
		log.Printf("WARN: fifofilter queue is full, discarding frame")
	}
}

func (m *Manager) cleanup() {
	if m.fifoFd != nil {
		m.fifoFd.Close()
	}
	if m.fallbackFile != nil {
		m.fallbackFile.Close()
	}
}
