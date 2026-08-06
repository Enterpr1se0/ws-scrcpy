package adbcli_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbcli"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbproxy"
	"github.com/gorilla/websocket"
)

func TestForwardStreamProviderForwardsDialsUpstreamAndRemovesForwardOnClose(t *testing.T) {
	server, upstreamPath := newEchoWebSocketServer(t)
	port := mustPort(t, server.URL)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(
		adbcli.WithRunner(runner),
		adbcli.WithADBPath("adb-custom"),
		adbcli.WithPortAllocator(fixedPortAllocator(port)),
	)

	stream, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:scrcpy", Path: "/echo?token=a%2Fb"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer stream.Close()

	payload := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	n, err := stream.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	buf := make([]byte, len(payload))
	n, err = stream.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !reflect.DeepEqual(buf[:n], payload) {
		t.Fatalf("Read() bytes = %v, want %v", buf[:n], payload)
	}
	if got := <-upstreamPath; got != "/echo?token=a%2Fb" {
		t.Fatalf("upstream path = %q, want %q", got, "/echo?token=a%2Fb")
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	want := []runnerCall{
		{name: "adb-custom", args: []string{"-s", "device-1", "forward", "tcp:" + strconv.Itoa(port), "localabstract:scrcpy"}},
		{name: "adb-custom", args: []string{"-s", "device-1", "forward", "--remove", "tcp:" + strconv.Itoa(port)}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("adb calls = %#v, want %#v", got, want)
	}
}

func TestForwardStreamProviderReadsTextMessagesAsBytes(t *testing.T) {
	server, _ := newStaticTextWebSocketServer(t, "hello")
	port := mustPort(t, server.URL)
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(&fakeRunner{cmds: []*fakeCmd{{}, {}}}), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	stream, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer stream.Close()

	buf := make([]byte, 16)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("Read() = %q, want hello", string(buf[:n]))
	}
}

func TestForwardStreamProviderForwardErrorFailsOpen(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{{err: errors.New("forward failed")}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithPortAllocator(fixedPortAllocator(45001)))

	_, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote"})
	if err == nil {
		t.Fatal("Open() error = nil, want forward error")
	}

	want := []runnerCall{{name: "adb", args: []string{"-s", "device-1", "forward", "tcp:45001", "localabstract:remote"}}}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("adb calls = %#v, want %#v", got, want)
	}
}

func TestForwardStreamProviderDialErrorRemovesForward(t *testing.T) {
	port := unusedLocalPort(t)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	_, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote", Path: "/missing"})
	if err == nil {
		t.Fatal("Open() error = nil, want upstream dial error")
	}

	want := []runnerCall{
		{name: "adb", args: []string{"-s", "device-1", "forward", "tcp:" + strconv.Itoa(port), "localabstract:remote"}},
		{name: "adb", args: []string{"-s", "device-1", "forward", "--remove", "tcp:" + strconv.Itoa(port)}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("adb calls = %#v, want %#v", got, want)
	}
}

func TestForwardStreamProviderDialErrorRemovesForwardWithBoundedContext(t *testing.T) {
	port := unusedLocalPort(t)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	_, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote", Path: "/missing"})
	if err == nil {
		t.Fatal("Open() error = nil, want upstream dial error")
	}

	ctxs := runner.snapshotContexts()
	if len(ctxs) != 2 {
		t.Fatalf("contexts len = %d, want 2", len(ctxs))
	}
	if _, ok := ctxs[1].Deadline(); !ok {
		t.Fatal("remove context has no deadline")
	}
}

func TestForwardStreamProviderEscapesReservedCharactersInUpstreamPath(t *testing.T) {
	server, upstreamPath := newEchoWebSocketServer(t)
	port := mustPort(t, server.URL)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	stream, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote", Path: "/json/version#tab"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer stream.Close()

	select {
	case got := <-upstreamPath:
		if got != "/json/version%23tab" {
			t.Fatalf("upstream path = %q, want %q", got, "/json/version%23tab")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream request")
	}
}

func TestForwardStreamProviderPreservesEscapedSlashInUpstreamPath(t *testing.T) {
	server, upstreamPath := newEchoWebSocketServer(t)
	port := mustPort(t, server.URL)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	stream, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote", Path: "/json/a%2Fb?token=x"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer stream.Close()

	select {
	case got := <-upstreamPath:
		if got != "/json/a%2Fb?token=x" {
			t.Fatalf("upstream path = %q, want %q", got, "/json/a%2Fb?token=x")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream request")
	}
}

func TestForwardStreamProviderUsesADBHostPortOptions(t *testing.T) {
	server, _ := newStaticTextWebSocketServer(t, "ok")
	port := mustPort(t, server.URL)
	runner := &fakeRunner{cmds: []*fakeCmd{{}, {}}}
	provider := adbcli.NewForwardStreamProvider(adbcli.WithRunner(runner), adbcli.WithHostPort("adb.example", 5038), adbcli.WithPortAllocator(fixedPortAllocator(port)))

	stream, err := provider.Open(context.Background(), adbproxy.Request{UDID: "device-1", Remote: "localabstract:remote"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	_ = stream.Close()

	want := []runnerCall{
		{name: "adb", args: []string{"-H", "adb.example", "-P", "5038", "-s", "device-1", "forward", "tcp:" + strconv.Itoa(port), "localabstract:remote"}},
		{name: "adb", args: []string{"-H", "adb.example", "-P", "5038", "-s", "device-1", "forward", "--remove", "tcp:" + strconv.Itoa(port)}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("adb calls = %#v, want %#v", got, want)
	}
}

type fixedPortAllocator int

func (p fixedPortAllocator) Allocate(ctx context.Context) (int, error) {
	return int(p), nil
}

func newEchoWebSocketServer(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	paths := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return newLocalWebSocketServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.RequestURI()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(messageType, payload); err != nil {
				return
			}
		}
	})), paths
}

func newStaticTextWebSocketServer(t *testing.T, message string) (*httptest.Server, <-chan string) {
	t.Helper()
	paths := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return newLocalWebSocketServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.RequestURI()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(message))
		time.Sleep(50 * time.Millisecond)
	})), paths
}

func newLocalWebSocketServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func mustPort(t *testing.T, rawURL string) int {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", rawURL, err)
	}
	_, rawPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", parsed.Host, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("Atoi(%q) error = %v", rawPort, err)
	}
	return port
}

func unusedLocalPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().String()
	_, rawPort, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", addr, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("Atoi(%q) error = %v", rawPort, err)
	}
	return port
}

func TestFixedPortAllocatorSatisfiesInterface(t *testing.T) {
	var allocator adbcli.PortAllocator = fixedPortAllocator(1234)
	port, err := allocator.Allocate(context.Background())
	if err != nil || port != 1234 {
		t.Fatalf("Allocate() = (%d, %v), want (1234, nil)", port, err)
	}
}
