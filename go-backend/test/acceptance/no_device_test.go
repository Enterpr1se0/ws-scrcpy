package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbproxy"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/app"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/shell"
	"github.com/gorilla/websocket"
)

func TestStaticIndexServedFromTempStaticDir(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<!doctype html><title>acceptance index</title>")},
	}
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, staticFS, newFakeProvider())

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET / error = %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", response.StatusCode, body)
	}
	if !strings.Contains(string(body), "acceptance index") {
		t.Fatalf("body = %q, want temp index.html content", body)
	}
}

func TestUnknownWebSocketActionClosesUnsupportedRequest(t *testing.T) {
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), newFakeProvider())
	conn := dialWS(t, server.URL, "/?action=definitely-unknown")
	assertWSCloseCode(t, conn, contract.CloseUnsupportedRequest)
}

func TestDirectGoogDeviceListSendsEmptyDeviceList(t *testing.T) {
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), newFakeProvider())
	conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, url.Values{})
	defer conn.Close()

	var message struct {
		Type string `json:"type"`
		Data struct {
			List []devicetracker.Device `json:"list"`
		} `json:"data"`
	}
	readJSON(t, conn, &message)
	if message.Type != "devicelist" {
		t.Fatalf("type = %q, want devicelist", message.Type)
	}
	if message.Data.List == nil {
		t.Fatal("devices list is nil, want empty JSON array")
	}
	if len(message.Data.List) != 0 {
		t.Fatalf("devices = %#v, want empty", message.Data.List)
	}
}

func TestMultiplexHSTSReturnsInitialHostsMessage(t *testing.T) {
	cfg := config.Config{
		Pathname: "/",
		RemoteHosts: []config.HostItem{{
			Type:     "adb",
			Hostname: "remote.example",
			Port:     443,
			Secure:   true,
			Pathname: "/ws",
		}},
	}
	server := newAcceptanceServer(t, cfg, emptyStaticFS(), newFakeProvider())
	conn := dialAction(t, server.URL, contract.ActionMultiplex, url.Values{})
	defer conn.Close()

	writeFrame(t, conn, multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 1, Payload: []byte(contract.ChannelHSTS)})
	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData || frame.ChannelID != 1 {
		t.Fatalf("frame = {type:%d channel:%d}, want raw string data frame on channel 1", frame.Type, frame.ChannelID)
	}
	var message struct {
		Type string `json:"type"`
		Data struct {
			Local  []map[string]any  `json:"local"`
			Remote []config.HostItem `json:"remote"`
		} `json:"data"`
	}
	if err := json.Unmarshal(frame.Payload, &message); err != nil {
		t.Fatalf("Unmarshal(hosts payload) error = %v", err)
	}
	if message.Type != "hosts" {
		t.Fatalf("type = %q, want hosts", message.Type)
	}
	if len(message.Data.Local) != 1 || message.Data.Local[0]["type"] != "android" {
		t.Fatalf("local hosts = %#v, want one android host", message.Data.Local)
	}
	if len(message.Data.Remote) != 1 || message.Data.Remote[0].Hostname != "remote.example" {
		t.Fatalf("remote hosts = %#v, want configured remote host", message.Data.Remote)
	}
}

func TestMultiplexGTRCReturnsInitialDeviceListDataPayload(t *testing.T) {
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), newFakeProvider())
	conn := dialAction(t, server.URL, contract.ActionMultiplex, url.Values{})
	defer conn.Close()

	writeFrame(t, conn, multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 2, Payload: []byte(contract.ChannelGTRC)})
	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData || frame.ChannelID != 2 {
		t.Fatalf("frame = {type:%d channel:%d}, want raw string data frame on channel 2", frame.Type, frame.ChannelID)
	}
	var message struct {
		Type string `json:"type"`
		Data struct {
			List []devicetracker.Device `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(frame.Payload, &message); err != nil {
		t.Fatalf("Unmarshal(GTRC payload) error = %v", err)
	}
	if message.Type != "devicelist" {
		t.Fatalf("type = %q, want devicelist", message.Type)
	}
	if message.Data.List == nil {
		t.Fatal("devices list is nil, want empty JSON array")
	}
	if len(message.Data.List) != 0 {
		t.Fatalf("devices = %#v, want empty", message.Data.List)
	}
}


func TestDirectListFilesDisabledClosesUnsupportedRequest(t *testing.T) {
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), newFakeProvider())
	conn := dialAction(t, server.URL, contract.ActionFileListing, url.Values{})
	assertWSCloseCode(t, conn, contract.CloseUnsupportedRequest)
}

func TestProxyWSEchoRoundTripsTextAndBinary(t *testing.T) {
	upstream := newEchoWebSocketServer(t)
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), newFakeProvider())
	conn := dialAction(t, server.URL, contract.ActionProxyWS, url.Values{"ws": {"ws" + strings.TrimPrefix(upstream.URL, "http")}})
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage(text) error = %v", err)
	}
	messageType, payload := readWSMessage(t, conn)
	if messageType != websocket.TextMessage || string(payload) != "hello" {
		t.Fatalf("text echo = (%d, %q), want text hello", messageType, payload)
	}

	binary := []byte{0x00, 0x01, 0xfe, 0xff}
	if err := conn.WriteMessage(websocket.BinaryMessage, binary); err != nil {
		t.Fatalf("WriteMessage(binary) error = %v", err)
	}
	messageType, payload = readWSMessage(t, conn)
	if messageType != websocket.BinaryMessage || string(payload) != string(binary) {
		t.Fatalf("binary echo = (%d, %v), want binary %v", messageType, payload, binary)
	}
}

func TestProxyADBValidParamsBridgeBinaryStream(t *testing.T) {
	provider := newFakeProvider()
	provider.adbStream = newFakeRawStream()
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), provider)
	conn := dialAction(t, server.URL, contract.ActionProxyADB, url.Values{
		"udid":   {"device-1"},
		"remote": {"localabstract:chrome_devtools_remote"},
		"path":   {"/json/version"},
	})
	defer conn.Close()

	clientPayload := []byte{0x10, 0x20, 0x30}
	if err := conn.WriteMessage(websocket.BinaryMessage, clientPayload); err != nil {
		t.Fatalf("WriteMessage(proxy-adb) error = %v", err)
	}
	provider.adbStream.assertWrite(t, clientPayload)
	provider.assertADBRequest(t, adbproxy.Request{UDID: "device-1", Remote: "localabstract:chrome_devtools_remote", Path: "/json/version"})

	streamPayload := []byte{0xaa, 0xbb, 0xcc}
	provider.adbStream.pushRead(streamPayload)
	messageType, payload := readWSMessage(t, conn)
	if messageType != websocket.BinaryMessage || string(payload) != string(streamPayload) {
		t.Fatalf("proxy-adb read = (%d, %v), want binary %v", messageType, payload, streamPayload)
	}
}

func TestDirectShellUsesInjectedProvider(t *testing.T) {
	provider := newFakeProvider()
	provider.shellSession = newFakeShellSession()
	server := newAcceptanceServer(t, config.Config{Pathname: "/"}, emptyStaticFS(), provider)
	conn := dialAction(t, server.URL, contract.ActionShell, url.Values{"udid": {"device-1"}, "rows": {"24"}, "cols": {"80"}})
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("pwd\n")); err != nil {
		t.Fatalf("WriteMessage(shell input) error = %v", err)
	}
	provider.shellSession.assertWrite(t, []byte("pwd\n"))
	provider.assertShellRequest(t, shell.Request{UDID: "device-1", Rows: 24, Cols: 80})

	provider.shellSession.pushOutput([]byte("/tmp\n"))
	messageType, payload := readWSMessage(t, conn)
	if messageType != websocket.TextMessage || string(payload) != "/tmp\n" {
		t.Fatalf("shell output = (%d, %q), want text /tmp", messageType, payload)
	}
}

func newAcceptanceServer(t *testing.T, cfg config.Config, staticFS fs.FS, provider *fakeProvider) *httptest.Server {
	t.Helper()
	handler := app.NewHandlerWithOptions(cfg, staticFS, app.HandlerOptions{
		DeviceProvider:    provider,
		ShellProvider:     provider,
		ADBStreamProvider: provider,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func emptyStaticFS() fs.FS {
	return fstest.MapFS{}
}

func dialAction(t *testing.T, serverURL string, action string, query url.Values) *websocket.Conn {
	t.Helper()
	query.Set("action", action)
	return dialWS(t, serverURL, "/?"+query.Encode())
}

func dialWS(t *testing.T, serverURL string, requestURI string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + requestURI
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", wsURL, err)
	}
	return conn
}

func readJSON(t *testing.T, conn *websocket.Conn, target any) {
	t.Helper()
	setReadDeadline(t, conn)
	if err := conn.ReadJSON(target); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
}

func readWSMessage(t *testing.T, conn *websocket.Conn) (int, []byte) {
	t.Helper()
	setReadDeadline(t, conn)
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	return messageType, payload
}

func newEchoWebSocketServer(t *testing.T) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	t.Cleanup(server.Close)
	return server
}

func writeFrame(t *testing.T, conn *websocket.Conn, frame multiplex.Frame) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(frame)); err != nil {
		t.Fatalf("WriteMessage(frame) error = %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) multiplex.Frame {
	t.Helper()
	setReadDeadline(t, conn)
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want binary", messageType)
	}
	frame, err := multiplex.Decode(data)
	if err != nil {
		t.Fatalf("Decode(frame) error = %v", err)
	}
	return frame
}

func assertWSCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	defer conn.Close()
	setReadDeadline(t, conn)
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("ReadMessage() error = %T %v, want websocket close", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}

func setReadDeadline(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
}

type fakeProvider struct {
	mu           sync.Mutex
	adbRequest   adbproxy.Request
	adbOpened    bool
	adbStream    *fakeRawStream
	shellRequest shell.Request
	shellStarted bool
	shellSession *fakeShellSession
}

func newFakeProvider() *fakeProvider { return &fakeProvider{} }

func (p *fakeProvider) ListDevices(ctx context.Context) ([]devicetracker.Device, error) {
	return []devicetracker.Device{}, nil
}

func (p *fakeProvider) WatchDevices(ctx context.Context) (<-chan devicetracker.Event, error) {
	return make(chan devicetracker.Event), nil
}

func (p *fakeProvider) RunCommand(ctx context.Context, command devicetracker.Command) error {
	return nil
}

func (p *fakeProvider) Start(ctx context.Context, request shell.Request) (shell.Session, error) {
	p.mu.Lock()
	p.shellRequest = request
	p.shellStarted = true
	p.mu.Unlock()
	if p.shellSession == nil {
		return nil, errors.New("shell is not available in acceptance fake")
	}
	return p.shellSession, nil
}


func (p *fakeProvider) Open(ctx context.Context, request adbproxy.Request) (adbproxy.Stream, error) {
	p.mu.Lock()
	p.adbRequest = request
	p.adbOpened = true
	p.mu.Unlock()
	if p.adbStream == nil {
		return nil, errors.New("adb stream is not available in acceptance fake")
	}
	return p.adbStream, nil
}

func (p *fakeProvider) assertADBRequest(t *testing.T, want adbproxy.Request) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		got := p.adbRequest
		opened := p.adbOpened
		p.mu.Unlock()
		if opened {
			if got != want {
				t.Fatalf("adb request = %#v, want %#v", got, want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("adb stream provider was not opened")
}

func (p *fakeProvider) assertShellRequest(t *testing.T, want shell.Request) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		got := p.shellRequest
		started := p.shellStarted
		p.mu.Unlock()
		if started {
			if got != want {
				t.Fatalf("shell request = %#v, want %#v", got, want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("shell provider was not started")
}

type fakeRawStream struct {
	reads     chan streamRead
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

type streamRead struct {
	data []byte
	err  error
}

func newFakeRawStream() *fakeRawStream {
	return &fakeRawStream{reads: make(chan streamRead, 8), writes: make(chan []byte, 8), closed: make(chan struct{})}
}

func (s *fakeRawStream) Read(p []byte) (int, error) {
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

func (s *fakeRawStream) Write(p []byte) (int, error) {
	data := append([]byte(nil), p...)
	select {
	case s.writes <- data:
		return len(p), nil
	case <-s.closed:
		return 0, io.ErrClosedPipe
	}
}

func (s *fakeRawStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *fakeRawStream) pushRead(data []byte) {
	s.reads <- streamRead{data: append([]byte(nil), data...)}
}

func (s *fakeRawStream) assertWrite(t *testing.T, want []byte) {
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

type fakeShellSession struct {
	output    chan []byte
	done      chan error
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeShellSession() *fakeShellSession {
	return &fakeShellSession{output: make(chan []byte, 8), done: make(chan error, 1), writes: make(chan []byte, 8), closed: make(chan struct{})}
}

func (s *fakeShellSession) Write(ctx context.Context, data []byte) error {
	copyData := append([]byte(nil), data...)
	select {
	case s.writes <- copyData:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return io.ErrClosedPipe
	}
}

func (s *fakeShellSession) Output() <-chan []byte { return s.output }
func (s *fakeShellSession) Done() <-chan error    { return s.done }
func (s *fakeShellSession) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *fakeShellSession) pushOutput(data []byte) {
	s.output <- append([]byte(nil), data...)
}

func (s *fakeShellSession) assertWrite(t *testing.T, want []byte) {
	t.Helper()
	select {
	case got := <-s.writes:
		if string(got) != string(want) {
			t.Fatalf("shell write = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shell write")
	}
}
