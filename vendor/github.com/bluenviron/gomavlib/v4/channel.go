package gomavlib

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/message"
	"github.com/bluenviron/gomavlib/v4/pkg/streamwriter"
)

const (
	writeBufferSize        = 64
	datagramReadBufferSize = 512
)

type datagramReader struct {
	r   io.ReadCloser
	buf []byte
	pos int
}

func (r *datagramReader) ReadPacket() error {
	if r.buf == nil {
		r.buf = make([]byte, datagramReadBufferSize)
	}

	n, err := r.r.Read(r.buf[:datagramReadBufferSize])
	if n == 0 && err != nil {
		return err
	}

	r.buf = r.buf[:n]
	r.pos = 0

	return nil
}

func (r *datagramReader) Read(p []byte) (n int, err error) {
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	if n == 0 && r.pos >= len(r.buf) {
		return 0, fmt.Errorf("packet is too short")
	}
	return n, nil
}

type readWriter struct {
	io.Reader
	io.Writer
}

func randomByte() (byte, error) {
	var buf [1]byte
	_, err := rand.Read(buf[:])
	return buf[0], err
}

// Channel is a communication channel created by an Endpoint.
// An Endpoint can create channels.
// For instance, a TCP client endpoint creates a single channel, while a TCP
// server endpoint creates a channel for each incoming connection.
type Channel struct {
	node       *Node
	endpoint   Endpoint
	label      string
	rwc        io.ReadWriteCloser
	isDatagram bool
	// remoteAddr 是底层连接的远端地址（串口等无地址的链路为 nil）。
	// label 里虽然也拼了远端地址，但 label 还含监听地址，事后解析不可靠
	// （IPv6 与带冒号的 Label 都会歧义），故在建立通道时直接留一份。
	remoteAddr net.Addr

	datagramReader  *datagramReader
	ctx             context.Context
	ctxCancel       func()
	frameReadWriter *frame.ReadWriter
	streamWriter    *streamwriter.Writer
	running         bool

	// in
	chWrite chan any

	// write-queue observability. chWrite is deliberately non-blocking (a slow
	// consumer must not stall the node), so a full queue loses data unless it
	// is counted. Only the "queue full" branch is counted: frames discarded
	// because the channel is already closing (ctx done) are not, and writes
	// that land in chWrite after runWriter exited are not drained nor counted.
	// Counters are atomic: write() is called from the node goroutine while
	// DroppedFrames() may be read from anywhere.
	dropped     atomic.Uint64
	lastDropLog atomic.Int64 // unix nanos of the last "queue full" log

	// out
	done chan struct{}
}

func (ch *Channel) initialize() error {
	linkID, err := randomByte()
	if err != nil {
		return err
	}

	var rw io.ReadWriter
	if ch.isDatagram {
		ch.datagramReader = &datagramReader{r: ch.rwc}
		rw = &readWriter{
			Reader: ch.datagramReader,
			Writer: ch.rwc,
		}
	} else {
		rw = ch.rwc
	}

	ch.frameReadWriter = &frame.ReadWriter{
		ByteReadWriter: rw,
		DialectRW:      ch.node.dialectRW,
		InKey:          ch.node.InKey,
	}
	err = ch.frameReadWriter.Initialize()
	if err != nil {
		return err
	}

	ch.streamWriter = &streamwriter.Writer{
		FrameWriter: ch.frameReadWriter.Writer,
		SystemID:    ch.node.OutSystemID,
		Version: func() streamwriter.Version {
			if ch.node.OutVersion == V2 {
				return streamwriter.V2
			}
			return streamwriter.V1
		}(),
		ComponentID:     ch.node.OutComponentID,
		SignatureLinkID: linkID,
		Key:             ch.node.OutKey,
	}
	err = ch.streamWriter.Initialize()
	if err != nil {
		return err
	}

	ch.ctx, ch.ctxCancel = context.WithCancel(context.Background())
	ch.chWrite = make(chan any, writeBufferSize)
	ch.done = make(chan struct{})

	return nil
}

func (ch *Channel) close() {
	ch.ctxCancel()
	if !ch.running {
		ch.rwc.Close()
	}
}

func (ch *Channel) start() {
	ch.running = true
	ch.node.wg.Add(1)
	go ch.run()
}

func (ch *Channel) run() {
	defer close(ch.done)
	defer ch.node.wg.Done()

	readerDone := make(chan error)
	go func() {
		readerDone <- ch.runReader()
	}()

	writerTerminate := make(chan struct{})
	writerDone := make(chan error)
	go func() {
		writerDone <- ch.runWriter(writerTerminate)
	}()

	var err error

	select {
	case err = <-readerDone:
		ch.rwc.Close()

		close(writerTerminate)
		<-writerDone

	case err = <-writerDone:
		ch.rwc.Close()
		<-readerDone

	case <-ch.ctx.Done():
		close(writerTerminate)
		<-writerDone

		ch.rwc.Close()
		<-readerDone
	}

	ch.ctxCancel()

	ch.node.pushEvent(&EventChannelClose{
		Channel: ch,
		Error:   err,
	})
	ch.node.closeChannel(ch)
}

func (ch *Channel) runReader() error {
	// wait client here, in order to allow the writer goroutine to start
	// and allow clients to write messages before starting listening to events
	ch.node.pushEvent(&EventChannelOpen{ch})

	for {
		var fr frame.Frame

		if ch.isDatagram {
			skipped := len(ch.datagramReader.buf) - ch.datagramReader.pos
			err := ch.datagramReader.ReadPacket()
			if err != nil {
				return err
			}

			n := ch.frameReadWriter.BufByteReader.Buffered()
			skipped += n
			ch.frameReadWriter.BufByteReader.Discard(n) //nolint:errcheck

			if skipped > 0 {
				ch.node.pushEvent(&EventParseError{fmt.Errorf("skipped %d bytes", skipped), ch})
			}

			fr, err = ch.frameReadWriter.Read()
			if err != nil {
				ch.node.pushEvent(&EventParseError{err, ch})
				continue
			}
		} else {
			var err error
			fr, err = ch.frameReadWriter.Read()
			if err != nil {
				var eerr frame.ReadError
				if errors.As(err, &eerr) {
					ch.node.pushEvent(&EventParseError{err, ch})
					continue
				}
				return err
			}
		}

		evt := &EventFrame{fr, ch}

		if ch.node.nodeStreamRequest != nil {
			ch.node.nodeStreamRequest.onEventFrame(evt)
		}

		ch.node.pushEvent(evt)
	}
}

func (ch *Channel) runWriter(writerTerminate chan struct{}) error {
	for {
		select {
		case what := <-ch.chWrite:
			switch wh := what.(type) {
			case message.Message:
				err := ch.streamWriter.Write(wh)
				if err != nil {
					return err
				}
				ch.paceDatagram()

			case frame.Frame:
				err := ch.frameReadWriter.Write(wh)
				if err != nil {
					return err
				}
				ch.paceDatagram()
			}

		case <-writerTerminate:
			return nil
		}
	}
}

// paceDatagram keeps consecutive datagrams on the same channel from being
// handed to the kernel back-to-back. runtime.Gosched is not enough here: it
// does not guarantee that any time passes, so it gives the network stack no
// chance to drain the socket buffer. A message or frame write produces exactly
// one datagram on datagram-based endpoints; on stream-based endpoints there is
// no packet boundary to pace.
func (ch *Channel) paceDatagram() {
	if ch.node.WriteDelay <= 0 || !ch.isDatagram {
		return
	}
	time.Sleep(ch.node.WriteDelay)
}

// String implements fmt.Stringer.
func (ch *Channel) String() string {
	return ch.label
}

// Endpoint returns the channel Endpoint.
func (ch *Channel) Endpoint() Endpoint {
	return ch.endpoint
}

// RemoteAddr returns the peer address of the underlying connection.
// It returns nil for links without a peer address (e.g. serial ports).
func (ch *Channel) RemoteAddr() net.Addr {
	return ch.remoteAddr
}

// IsClosed reports whether the channel has stopped running; run() closes done
// right before pushing EventChannelClose. A zero-value Channel (no done) is
// reported as open, which keeps internal test fixtures usable.
func (ch *Channel) IsClosed() bool {
	if ch.done == nil {
		return false
	}
	select {
	case <-ch.done:
		return true
	default:
		return false
	}
}

// remoteAddrOf extracts the peer address from a connection when it exposes one.
// Wrappers such as timednetconn forward RemoteAddr(); endpoints that do not
// implement it (serial) yield nil instead of a fabricated address.
func remoteAddrOf(rwc io.ReadWriteCloser) net.Addr {
	if ra, ok := rwc.(interface{ RemoteAddr() net.Addr }); ok {
		return ra.RemoteAddr()
	}
	return nil
}

// dropLogInterval 是「写队列满」告警的节流窗口。遥测突发时可能每帧都丢，
// 逐帧打印只会淹没日志；计数仍然逐帧累加，可从 DroppedFrames() 读到真实值。
const dropLogInterval = 5 * time.Second

func (ch *Channel) write(what any) {
	select {
	case ch.chWrite <- what:
	case <-ch.ctx.Done():
	default: // buffer is full
		n := ch.dropped.Add(1)
		now := time.Now().UnixNano()
		last := ch.lastDropLog.Load()
		if now-last >= int64(dropLogInterval) && ch.lastDropLog.CompareAndSwap(last, now) {
			log.Printf("WARN: gomavlib: %s write queue full (cap %d), dropping frames (total dropped %d)",
				ch.label, writeBufferSize, n)
		}
	}
}

// DroppedFrames returns how many frames this channel has dropped because its
// write queue was full. The counter never decreases for the channel's lifetime.
func (ch *Channel) DroppedFrames() uint64 {
	return ch.dropped.Load()
}
