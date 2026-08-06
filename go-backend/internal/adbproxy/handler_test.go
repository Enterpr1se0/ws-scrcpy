package adbproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

func TestQueryMissingUDIDOrRemoteCloses4003(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "missing udid", query: "action=proxy-adb&remote=localabstract%3Achrome_devtools_remote"},
		{name: "missing remote", query: "action=proxy-adb&udid=device-1"},
		{name: "empty udid", query: "action=proxy-adb&udid=&remote=localabstract%3Aremote"},
		{name: "empty remote", query: "action=proxy-adb&udid=device-1&remote="},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialADBProxy(t, NewHandler(&fakeProvider{}), "/?"+tc.query)
			defer conn.Close()

			assertCloseCode(t, conn, contract.CloseInvalidParameter)
		})
	}
}

func TestQueryProviderOpenFailureCloses4005(t *testing.T) {
	provider := &fakeProvider{openErr: errors.New("adb stream unavailable")}
	conn := dialADBProxy(t, NewHandler(provider), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestBridgePreservesClientWritesAndStreamReadsAsBinary(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote&path=%2Fjson%2Fversion")
	defer conn.Close()

	clientPayload := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	if err := conn.WriteMessage(websocket.TextMessage, clientPayload); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	stream.assertWrite(t, clientPayload)

	stream.pushRead([]byte{0xff, 0x00, 0x41})
	messageType, message, err := readMessage(t, conn)
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want binary", messageType)
	}
	if string(message) != string([]byte{0xff, 0x00, 0x41}) {
		t.Fatalf("message bytes = %v", message)
	}
}

func TestStreamEOFClosesClientWith4010(t *testing.T) {
	stream := newFakeStream()
	conn := dialADBProxy(t, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	stream.closeReads()
	assertCloseCode(t, conn, contract.CloseProxyUpstreamClosed)
}

func TestStreamReadErrorClosesClientWith4011(t *testing.T) {
	stream := newFakeStream()
	conn := dialADBProxy(t, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	stream.failRead(errors.New("stream read failed"))
	assertCloseCode(t, conn, contract.CloseProxyUpstreamError)
}

func TestStreamWriteErrorClosesClientWith4011(t *testing.T) {
	stream := newFakeStream()
	stream.writeErr = errors.New("stream write failed")
	conn := dialADBProxy(t, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}

	assertCloseCode(t, conn, contract.CloseProxyUpstreamError)
}

func TestShortStreamWriteClosesClientWith4011(t *testing.T) {
	stream := newFakeStream()
	stream.writeN = 2
	conn := dialADBProxy(t, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}

	assertCloseCode(t, conn, contract.CloseProxyUpstreamError)
}

func TestParentContextCancellationClosesStream(t *testing.T) {
	stream := newFakeStream()
	ctx, cancel := context.WithCancel(context.Background())
	conn := dialADBProxyWithContext(t, ctx, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	cancel()

	stream.assertClosed(t)
}

func TestWebSocketClientCloseClosesStream(t *testing.T) {
	stream := newFakeStream()
	conn := dialADBProxy(t, NewHandler(&fakeProvider{stream: stream}), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	stream.assertClosed(t)
}

func TestInvalidEscapedShortcutValuesClose4003(t *testing.T) {
	invalidURL := &url.URL{Path: "/proxy-adb/device/%zz", RawPath: "/proxy-adb/device/%zz"}
	conn := dialADBProxyWithRequestURL(t, context.Background(), NewHandler(&fakeProvider{}), "/", invalidURL)
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseInvalidParameter)
}

func TestPathShortcutParsesEscapedUDIDRemoteAndPath(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/proxy-adb/"+url.PathEscape("device/1")+"/"+url.PathEscape("localabstract:remote socket")+"/json%20path/version")
	defer conn.Close()

	provider.assertRequest(t, Request{UDID: "device/1", Remote: "localabstract:remote socket", Path: "/json path/version"})
}

func TestPathShortcutPreservesLeadingSlashForUpstreamPath(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/proxy-adb/device/localabstract%3Aremote/json/version")
	defer conn.Close()

	provider.assertRequest(t, Request{UDID: "device", Remote: "localabstract:remote", Path: "/json/version"})
}

func TestPathShortcutPreservesQueryString(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/proxy-adb/device/localabstract%3Aremote/json/version?token=a%2Fb")
	defer conn.Close()

	provider.assertRequest(t, Request{UDID: "device", Remote: "localabstract:remote", Path: "/json/version?token=a%2Fb"})
}

func TestPathShortcutDoesNotDoubleLeadingSlashFromEscapedPath(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/proxy-adb/device/localabstract%3Aremote/%2Fjson%2Fversion")
	defer conn.Close()

	provider.assertRequest(t, Request{UDID: "device", Remote: "localabstract:remote", Path: "/json/version"})
}

func TestQueryRemoteAndUDIDOnlyIsAccepted(t *testing.T) {
	stream := newFakeStream()
	provider := &fakeProvider{stream: stream}
	conn := dialADBProxy(t, NewHandler(provider), "/?action=proxy-adb&udid=device-1&remote=localabstract%3Aremote")
	defer conn.Close()

	provider.assertRequest(t, Request{UDID: "device-1", Remote: "localabstract:remote", Path: ""})
}

type fakeProvider struct {
	stream  *fakeStream
	openErr error

	mu      sync.Mutex
	request Request
	opened  bool
}

func (p *fakeProvider) Open(ctx context.Context, request Request) (Stream, error) {
	p.mu.Lock()
	p.request = request
	p.opened = true
	p.mu.Unlock()
	if p.openErr != nil {
		return nil, p.openErr
	}
	if p.stream == nil {
		p.stream = newFakeStream()
	}
	return p.stream, nil
}

func (p *fakeProvider) assertRequest(t *testing.T, want Request) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		got := p.request
		opened := p.opened
		p.mu.Unlock()
		if opened {
			if got != want {
				t.Fatalf("request = %#v, want %#v", got, want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("provider was not opened")
}

type fakeStream struct {
	reads    chan streamRead
	writes   chan []byte
	closed   chan struct{}
	once     sync.Once
	writeN   int
	writeErr error
}

type streamRead struct {
	data []byte
	err  error
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		reads:  make(chan streamRead, 8),
		writes: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *fakeStream) Read(p []byte) (int, error) {
	select {
	case read, ok := <-s.reads:
		if !ok {
			return 0, io.EOF
		}
		if read.err != nil {
			return 0, read.err
		}
		return copy(p, read.data), nil
	case <-s.closed:
		return 0, io.EOF
	}
}

func (s *fakeStream) Write(p []byte) (int, error) {
	data := append([]byte(nil), p...)
	select {
	case s.writes <- data:
		if s.writeErr != nil {
			return 0, s.writeErr
		}
		if s.writeN > 0 {
			return s.writeN, nil
		}
		return len(p), nil
	case <-s.closed:
		return 0, io.ErrClosedPipe
	}
}

func (s *fakeStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (s *fakeStream) pushRead(data []byte) {
	s.reads <- streamRead{data: append([]byte(nil), data...)}
}

func (s *fakeStream) failRead(err error) {
	s.reads <- streamRead{err: err}
}

func (s *fakeStream) closeReads() {
	close(s.reads)
}

func (s *fakeStream) assertWrite(t *testing.T, want []byte) {
	t.Helper()
	select {
	case got := <-s.writes:
		if string(got) != string(want) {
			t.Fatalf("stream write = %v, want %v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream write")
	}
}

func (s *fakeStream) assertClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream close")
	}
}

func dialADBProxy(t *testing.T, handler *Handler, target string) *websocket.Conn {
	t.Helper()
	return dialADBProxyWithRequestURL(t, context.Background(), handler, target, nil)
}

func dialADBProxyWithContext(t *testing.T, ctx context.Context, handler *Handler, target string) *websocket.Conn {
	t.Helper()
	return dialADBProxyWithRequestURL(t, ctx, handler, target, nil)
}

func dialADBProxyWithRequestURL(t *testing.T, ctx context.Context, handler *Handler, target string, requestURL *url.URL) *websocket.Conn {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		if requestURL != nil {
			r.URL = requestURL
		}
		handler.ServeWS(ctx, conn, r)
	}))
	t.Cleanup(server.Close)

	parsed, err := url.Parse(server.URL + target)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	parsed.Scheme = "ws"
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", parsed.String(), err)
	}
	return conn
}

func readMessage(t *testing.T, conn *websocket.Conn) (int, []byte, error) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	return conn.ReadMessage()
}

func assertCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	_, _, err := readMessage(t, conn)
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("ReadMessage() error = %T %v, want websocket close", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}
