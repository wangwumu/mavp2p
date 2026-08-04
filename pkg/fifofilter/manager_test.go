package fifofilter_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4"
	"github.com/bluenviron/gomavlib/v4/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/bluenviron/mavp2p/pkg/fifofilter"
)

func TestFilterFIFO(t *testing.T) {
	tmpFolder, err := os.MkdirTemp("", "mavp2p-fifofilter")
	require.NoError(t, err)
	defer os.RemoveAll(tmpFolder)

	// Write YAML config with message ID 0 (HEARTBEAT)
	configPath := filepath.Join(tmpFolder, "filter.yaml")
	err = os.WriteFile(configPath, []byte("# HEARTBEAT\n- 0\n"), 0o644)
	require.NoError(t, err)

	// Create FIFO before starting the reader
	fifoPath := filepath.Join(tmpFolder, "test.fifo")
	err = unix.Mkfifo(fifoPath, 0o666)
	require.NoError(t, err)

	// Start reader in background (blocks until writer opens)
	receivedCh := make(chan []byte, 1)
	readerReady := make(chan struct{})
	go func() {
		close(readerReady)
		f, err := os.OpenFile(fifoPath, os.O_RDONLY, 0)
		if err != nil {
			receivedCh <- nil
			return
		}
		defer f.Close()

		var buf [512]byte
		n, _ := f.Read(buf[:])
		receivedCh <- buf[:n]
	}()

	// Wait for reader goroutine to begin blocking on open
	<-readerReady
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	m := &fifofilter.Manager{
		Ctx:          ctx,
		Wg:           &wg,
		Dialect:      common.Dialect,
		FifoPath:     fifoPath,
		ConfigPath:   configPath,
		FallbackPath: filepath.Join(tmpFolder, "fallback.tlog"),
	}
	err = m.Initialize()
	require.NoError(t, err)

	// Send a matching frame (HEARTBEAT, ID=0)
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 1,
			SystemID:       1,
			ComponentID:    1,
			Message: &common.MessageHeartbeat{
				Type:           common.MAV_TYPE_GCS,
				Autopilot:      common.MAV_AUTOPILOT_INVALID,
				SystemStatus:   4,
				MavlinkVersion: 3,
			},
			Checksum: 0,
		},
	})

	// Wait for reader to receive data
	var received []byte
	select {
	case received = <-receivedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for FIFO read")
	}

	cancel()
	wg.Wait()

	require.NotEmpty(t, received, "FIFO reader should have received data")

	// Verify it's a valid Mavlink frame (starts with magic byte 0xFD for V2)
	require.Equal(t, byte(0xFD), received[0], "should be a Mavlink V2 frame")
}

func TestFilterFallback(t *testing.T) {
	tmpFolder, err := os.MkdirTemp("", "mavp2p-fifofilter")
	require.NoError(t, err)
	defer os.RemoveAll(tmpFolder)

	// Write YAML config with message ID 0 (HEARTBEAT)
	configPath := filepath.Join(tmpFolder, "filter.yaml")
	err = os.WriteFile(configPath, []byte("# HEARTBEAT\n- 0\n"), 0o644)
	require.NoError(t, err)

	// Create FIFO but don't open a reader - writer will fail to open
	fifoPath := filepath.Join(tmpFolder, "noreader.fifo")
	err = unix.Mkfifo(fifoPath, 0o666)
	require.NoError(t, err)

	fallbackPath := filepath.Join(tmpFolder, "fallback.tlog")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	m := &fifofilter.Manager{
		Ctx:          ctx,
		Wg:           &wg,
		Dialect:      common.Dialect,
		FifoPath:     fifoPath,
		ConfigPath:   configPath,
		FallbackPath: fallbackPath,
	}
	err = m.Initialize()
	require.NoError(t, err)

	// Send a matching frame (HEARTBEAT, ID=0) - should fall back to file
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 2,
			SystemID:       2,
			ComponentID:    2,
			Message: &common.MessageHeartbeat{
				Type:           common.MAV_TYPE_GCS,
				Autopilot:      common.MAV_AUTOPILOT_INVALID,
				SystemStatus:   4,
				MavlinkVersion: 3,
			},
			Checksum: 0,
		},
	})

	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	// Verify fallback file exists and has content
	info, err := os.Stat(fallbackPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0), "fallback file should have content")

	// Read back and verify it's a valid tlog entry (8-byte timestamp + magic byte)
	data, err := os.ReadFile(fallbackPath)
	require.NoError(t, err)
	require.Greater(t, len(data), 8, "tlog entry must have timestamp header")
	require.Equal(t, byte(0xFD), data[8], "should be a Mavlink V2 frame with tlog timestamp")
}

func TestFilterNoMatch(t *testing.T) {
	tmpFolder, err := os.MkdirTemp("", "mavp2p-fifofilter")
	require.NoError(t, err)
	defer os.RemoveAll(tmpFolder)

	// Write YAML config with message ID 99999 (unlikely to match)
	configPath := filepath.Join(tmpFolder, "filter.yaml")
	err = os.WriteFile(configPath, []byte("- 99999\n"), 0o644)
	require.NoError(t, err)

	fifoPath := filepath.Join(tmpFolder, "unused.fifo")
	err = unix.Mkfifo(fifoPath, 0o666)
	require.NoError(t, err)

	fallbackPath := filepath.Join(tmpFolder, "fallback.tlog")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	m := &fifofilter.Manager{
		Ctx:          ctx,
		Wg:           &wg,
		Dialect:      common.Dialect,
		FifoPath:     fifoPath,
		ConfigPath:   configPath,
		FallbackPath: fallbackPath,
	}
	err = m.Initialize()
	require.NoError(t, err)

	// Send a non-matching frame (HEARTBEAT, ID=0, not in config)
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 3,
			SystemID:       3,
			ComponentID:    3,
			Message: &common.MessageHeartbeat{
				Type:           common.MAV_TYPE_GCS,
				Autopilot:      common.MAV_AUTOPILOT_INVALID,
				SystemStatus:   4,
				MavlinkVersion: 3,
			},
			Checksum: 0,
		},
	})

	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	// Fallback file should be empty (no frames matched)
	info, err := os.Stat(fallbackPath)
	require.NoError(t, err)
	require.Equal(t, int64(0), info.Size(), "no frames matched, fallback should be empty")
}

func TestFilterMultipleMessageIDs(t *testing.T) {
	tmpFolder, err := os.MkdirTemp("", "mavp2p-fifofilter")
	require.NoError(t, err)
	defer os.RemoveAll(tmpFolder)

	// Write YAML config with multiple IDs: 0 (HEARTBEAT) and 1 (SYS_STATUS)
	configPath := filepath.Join(tmpFolder, "filter.yaml")
	err = os.WriteFile(configPath, []byte("# HEARTBEAT and SYS_STATUS\n- 0\n- 1\n"), 0o644)
	require.NoError(t, err)

	// Create FIFO
	fifoPath := filepath.Join(tmpFolder, "multi.fifo")
	err = unix.Mkfifo(fifoPath, 0o666)
	require.NoError(t, err)

	fallbackPath := filepath.Join(tmpFolder, "fallback.tlog")

	// Start FIFO reader
	receivedCh := make(chan []byte, 1)
	readerReady := make(chan struct{})
	go func() {
		close(readerReady)
		f, err := os.OpenFile(fifoPath, os.O_RDONLY, 0)
		if err != nil {
			receivedCh <- nil
			return
		}
		defer f.Close()

		// Wait briefly for all frames to be written before reading
		time.Sleep(200 * time.Millisecond)

		var buf [1024]byte
		n, _ := f.Read(buf[:])
		receivedCh <- buf[:n]
	}()

	// Wait for goroutine to start
	<-readerReady
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	m := &fifofilter.Manager{
		Ctx:          ctx,
		Wg:           &wg,
		Dialect:      common.Dialect,
		FifoPath:     fifoPath,
		ConfigPath:   configPath,
		FallbackPath: fallbackPath,
	}
	err = m.Initialize()
	require.NoError(t, err)

	// Send HEARTBEAT (ID=0) - should go to FIFO
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 10,
			SystemID:       10,
			ComponentID:    10,
			Message: &common.MessageHeartbeat{
				Type:           common.MAV_TYPE_GCS,
				Autopilot:      common.MAV_AUTOPILOT_INVALID,
				SystemStatus:   4,
				MavlinkVersion: 3,
			},
			Checksum: 0,
		},
	})

	// Send SYS_STATUS (ID=1) - should also go to FIFO
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 11,
			SystemID:       10,
			ComponentID:    10,
			Message: &common.MessageSysStatus{
				OnboardControlSensorsPresent: 1,
			},
			Checksum: 0,
		},
	})

	// Send a non-matching frame (ATTITUDE, ID=30) - should be ignored
	m.ProcessFrame(&gomavlib.EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 12,
			SystemID:       10,
			ComponentID:    10,
			Message: &common.MessageAttitude{
				Roll:  1.0,
				Pitch: 2.0,
				Yaw:   3.0,
			},
			Checksum: 0,
		},
	})

	var received []byte
	select {
	case received = <-receivedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for FIFO read")
	}

	cancel()
	wg.Wait()

	require.NotEmpty(t, received, "FIFO reader should have received data")

	// Both matching frames should be in the output (two V2 magic bytes 0xFD)
	count := bytes.Count(received, []byte{0xFD})
	require.Equal(t, 2, count, "should contain exactly two V2 frames (HEARTBEAT and SYS_STATUS)")

	// Non-matching frame should not be in fallback
	info, err := os.Stat(fallbackPath)
	require.NoError(t, err)
	require.Equal(t, int64(0), info.Size(), "no frames should fallback when FIFO works")
}
