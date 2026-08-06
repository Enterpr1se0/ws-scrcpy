package app

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"io/fs"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbcli"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbproxy"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/filelisting"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/shell"
	"github.com/gorilla/websocket"
)

func TestHandlerRoutesProxyWSAndFileListingDisabled(t *testing.T) {
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	proxyConn := dialAction(t, server.URL, contract.ActionProxyWS, "ws=not-a-websocket-url")
	assertCloseCode(t, proxyConn, contract.CloseInvalidParameter)

	fileConn := dialAction(t, server.URL, contract.ActionFileListing, "")
	assertCloseCode(t, fileConn, contract.CloseUnsupportedRequest)
}

func TestProxyADBRouteUsesInjectedFailingProvider(t *testing.T) {
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider, ADBStreamProvider: failingADBStreamProvider{}})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionProxyADB, "udid=device-1&remote=localabstract%3Aremote")
	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestProxyADBShortcutRouteUsesInjectedFailingProvider(t *testing.T) {
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider, ADBStreamProvider: failingADBStreamProvider{}})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialRaw(t, server.URL, "/proxy-adb/device/localabstract%3Ascrcpy/foo")
	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestDefaultProxyADBProviderConstructionDoesNotExecuteADB(t *testing.T) {
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{
		DeviceProvider:      &fakeADBProvider{},
		ShellProvider:       &fakeADBProvider{},
				FileListingProvider: &fakeADBProvider{},
		ADBProviderFactory:  newFailingADBProvider,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
}

func TestMultiplexHSTSReturnsConfiguredHosts(t *testing.T) {
	cfg := config.Config{
		Pathname: "/",
		RemoteHosts: []config.HostItem{{
			Type:     "adb",
			Secure:   true,
			Hostname: "remote.example",
			Port:     443,
			Pathname: "/ws",
			UseProxy: true,
		}},
	}
	handler := NewHandlerWithOptions(cfg, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionMultiplex, "")
	defer conn.Close()
	payload := append([]byte(contract.ChannelHSTS), []byte("ignored")...)
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 7, Payload: payload})); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData || frame.ChannelID != 7 {
		t.Fatalf("frame = {type:%d channel:%d}, want raw string frame for channel 7", frame.Type, frame.ChannelID)
	}
	var message struct {
		ID   int    `json:"id"`
		Type string `json:"type"`
		Data struct {
			Local  []map[string]any  `json:"local"`
			Remote []config.HostItem `json:"remote"`
		} `json:"data"`
	}
	if err := json.Unmarshal(frame.Payload, &message); err != nil {
		t.Fatalf("Unmarshal(hosts message) error = %v", err)
	}
	if message.Type != "hosts" || message.ID != -1 {
		t.Fatalf("message header = (%d, %q), want (-1, hosts)", message.ID, message.Type)
	}
	if len(message.Data.Local) != 1 || message.Data.Local[0]["type"] != "android" {
		t.Fatalf("local hosts = %#v, want one android host", message.Data.Local)
	}
	if len(message.Data.Remote) != 1 || message.Data.Remote[0].Hostname != "remote.example" || !message.Data.Remote[0].UseProxy {
		t.Fatalf("remote hosts = %#v, want configured remote host", message.Data.Remote)
	}
}

func TestMultiplexFSLSUsesSuppliedFileListingProvider(t *testing.T) {
	provider := &fakeADBProvider{fileListingSession: &fakeFileListingSession{entries: []filelisting.Entry{{Name: "hello.txt", Mode: 0100644, Size: 11, MTime: 1710000000}}}}
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{DeviceProvider: provider, ShellProvider: provider, FileListingProvider: provider, ADBProviderFactory: newFailingADBProvider})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionMultiplex, "")
	defer conn.Close()
	init := make([]byte, 4+4+len("device-123"))
	copy(init[0:4], contract.ChannelFSLS)
	binary.LittleEndian.PutUint32(init[4:8], uint32(len("device-123")))
	copy(init[8:], "device-123")
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 20, Payload: init})); err != nil {
		t.Fatalf("WriteMessage(create FSLS) error = %v", err)
	}
	request := make([]byte, 8+len("/sdcard"))
	copy(request[0:4], "LIST")
	binary.LittleEndian.PutUint32(request[4:8], uint32(len("/sdcard")))
	copy(request[8:], "/sdcard")
	inner := multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 1, Payload: request})
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeData, ChannelID: 20, Payload: inner})); err != nil {
		t.Fatalf("WriteMessage(list) error = %v", err)
	}

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeData || frame.ChannelID != 20 {
		t.Fatalf("outer frame = {type:%d channel:%d}, want data on channel 20", frame.Type, frame.ChannelID)
	}
	innerFrame, err := multiplex.Decode(frame.Payload)
	if err != nil {
		t.Fatalf("Decode inner frame error = %v", err)
	}
	if innerFrame.Type != contract.MessageTypeRawBinaryData || innerFrame.ChannelID != 1 || string(innerFrame.Payload[:4]) != "DENT" {
		t.Fatalf("inner frame = {type:%d channel:%d payload:%q}, want DENT", innerFrame.Type, innerFrame.ChannelID, innerFrame.Payload[:4])
	}
	if provider.openFileListingCalls != 1 || provider.fileListingUDID != "device-123" {
		t.Fatalf("file listing provider calls = %d udid %q, want 1 device-123", provider.openFileListingCalls, provider.fileListingUDID)
	}
}

func TestGoogDeviceListDirectUsesSuppliedProvider(t *testing.T) {
	provider := &fakeADBProvider{devices: []devicetracker.Device{{UDID: "device-123", State: "device", PID: 4321, Interfaces: []devicetracker.NetInterface{{Name: "wlan0", IPv4: "192.168.1.10"}}}}}
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{DeviceProvider: provider, ShellProvider: provider, FileListingProvider: provider, ADBProviderFactory: newFailingADBProvider})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, "")
	defer conn.Close()
	var message struct {
		Type string `json:"type"`
		Data struct {
			List []struct {
				UDID       string                       `json:"udid"`
				PID        int                          `json:"pid"`
				Interfaces []map[string]json.RawMessage `json:"interfaces"`
			} `json:"list"`
		} `json:"data"`
	}
	readJSON(t, conn, &message)
	if message.Type != "devicelist" {
		t.Fatalf("type = %q, want devicelist", message.Type)
	}
	if len(message.Data.List) != 1 || message.Data.List[0].UDID != "device-123" || message.Data.List[0].PID != 4321 {
		t.Fatalf("device list = %#v, want supplied provider device", message.Data.List)
	}
	got := message.Data.List[0].Interfaces
	if len(got) != 1 || string(got[0]["name"]) != `"wlan0"` || string(got[0]["ipv4"]) != `"192.168.1.10"` {
		t.Fatalf("interfaces = %#v, want supplied frontend-compatible interface", got)
	}
	if _, ok := got[0]["url"]; ok {
		t.Fatalf("interfaces = %#v, want no unsupported url field", got)
	}
	if provider.listDevicesCalls != 1 || provider.watchDevicesCalls != 1 {
		t.Fatalf("provider calls = list:%d watch:%d, want 1 each", provider.listDevicesCalls, provider.watchDevicesCalls)
	}
}


func TestDefaultADBProviderFactoryUsesHostPortEnvWithoutRealADB(t *testing.T) {
	runner := &recordingRunner{output: []byte("List of devices attached\nabc123\tdevice\n")}
	factoryCalled := false
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{
		Env: map[string]string{"ADB_HOST": "adb.example", "ADB_PORT": "5038"},
		ADBProviderFactory: func(options ...adbcli.Option) adbProvider {
			factoryCalled = true
			options = append(options, adbcli.WithRunner(runner))
			return adbcli.New(options...)
		},
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, "")
	defer conn.Close()
	var message struct {
		Data struct {
			List []devicetracker.Device `json:"list"`
		} `json:"data"`
	}
	readJSON(t, conn, &message)
	if !factoryCalled {
		t.Fatal("ADBProviderFactory was not called")
	}
	if len(message.Data.List) != 1 || message.Data.List[0].UDID != "abc123" {
		t.Fatalf("device list = %#v, want adbcli runner output", message.Data.List)
	}
	wantArgs := []string{"-H", "adb.example", "-P", "5038", "devices"}
	if runner.name != "adb" || !equalStrings(runner.args, wantArgs) {
		t.Fatalf("command = %q %#v, want adb %#v", runner.name, runner.args, wantArgs)
	}
}

func TestDefaultADBProviderFactoryUsesPortEnvWithoutHostOrRealADB(t *testing.T) {
	runner := &recordingRunner{output: []byte("List of devices attached\nabc123\tdevice\n")}
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{
		Env: map[string]string{"ADB_PORT": "5038"},
		ADBProviderFactory: func(options ...adbcli.Option) adbProvider {
			options = append(options, adbcli.WithRunner(runner))
			return adbcli.New(options...)
		},
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, "")
	defer conn.Close()
	var message struct {
		Data struct {
			List []devicetracker.Device `json:"list"`
		} `json:"data"`
	}
	readJSON(t, conn, &message)
	if len(message.Data.List) != 1 || message.Data.List[0].UDID != "abc123" {
		t.Fatalf("device list = %#v, want adbcli runner output", message.Data.List)
	}
	wantArgs := []string{"-H", "127.0.0.1", "-P", "5038", "devices"}
	if runner.name != "adb" || !equalStrings(runner.args, wantArgs) {
		t.Fatalf("command = %q %#v, want adb %#v", runner.name, runner.args, wantArgs)
	}
}

func TestUnsupportedActionReturnsRouterCloseCode(t *testing.T) {
	handler := NewHandlerWithOptions(config.Config{Pathname: "/"}, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	conn := dialAction(t, server.URL, "not-supported", "")
	assertCloseCode(t, conn, contract.CloseUnsupportedRequest)
}

func TestRunReturnsConfigLoadError(t *testing.T) {
	err := Run(context.Background(), Options{Env: map[string]string{"WS_SCRCPY_CONFIG": "missing.yaml"}, CWD: t.TempDir()})
	if err == nil {
		t.Fatal("Run() error = nil, want config load error")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Fatalf("Run() error = %q, want read config error", err.Error())
	}
}

func TestServeStartsMultipleServerEntriesAndShutsDownOnContextCancel(t *testing.T) {
	cfg := config.Config{
		Pathname: "/",
		Servers: []config.ServerItem{
			{Port: 0},
			{Port: 0},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeWithOptions(ctx, cfg, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	}()

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not exit after context cancellation")
	}
}

func TestSecureServerUsesInlineCertAndKey(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("secure index")},
	}
	cert, key := generateSelfSignedCertificate(t)
	runners, err := newHTTPServers(config.Config{
		Pathname: "/",
		Servers:  []config.ServerItem{{Secure: true, Port: 0, Options: map[string]any{"cert": cert, "key": key}}},
	}, staticFS, HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	if err != nil {
		t.Fatalf("newHTTPServers() error = %v", err)
	}
	if len(runners) != 1 {
		t.Fatalf("runners len = %d, want 1", len(runners))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runners[0].Serve(listener)
	}()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	resp, err := client.Get("https://" + listener.Addr().String() + "/")
	if err != nil {
		t.Fatalf("GET https server error = %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll(response) error = %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "secure index" {
		t.Fatalf("response = (%d, %q), want (200, secure index)", resp.StatusCode, string(body))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := runners[0].server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve() error = %v, want http.ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("secure server did not stop")
	}
}

func TestSecureServerRejectsMissingCertOrKey(t *testing.T) {
	_, err := newHTTPServers(config.Config{Pathname: "/", Servers: []config.ServerItem{{Secure: true, Port: 0, Options: map[string]any{"cert": "cert-only"}}}}, testStaticFS(t), HandlerOptions{})
	if err == nil {
		t.Fatal("newHTTPServers() error = nil, want missing key error")
	}
	if !strings.Contains(err.Error(), "secure server requires cert and key") {
		t.Fatalf("newHTTPServers() error = %q, want missing cert/key error", err.Error())
	}
}

func TestServeStartsAndShutsDownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeWithOptions(ctx, config.Config{Pathname: "/", Servers: []config.ServerItem{{Port: 0}}}, testStaticFS(t), HandlerOptions{ADBProviderFactory: newNoopADBProvider})
	}()

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not exit after context cancellation")
	}
}

func dialAction(t *testing.T, serverURL string, action string, extraQuery string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(serverURL, "http") + "/?action=" + action
	if extraQuery != "" {
		url += "&" + extraQuery
	}
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", url, err)
	}
	return conn
}

func dialRaw(t *testing.T, serverURL string, path string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(serverURL, "http") + path
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", url, err)
	}
	return conn
}

func readJSON(t *testing.T, conn *websocket.Conn, target any) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if err := conn.ReadJSON(target); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) multiplex.Frame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want binary", messageType)
	}
	frame, err := multiplex.Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	return frame
}

func assertCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("ReadMessage() error = %T %v, want websocket close", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}

type failingADBStreamProvider struct{}

func (failingADBStreamProvider) Open(ctx context.Context, request adbproxy.Request) (adbproxy.Stream, error) {
	return nil, errors.New("injected adb stream failure")
}

type fakeADBProvider struct {
	devices              []devicetracker.Device
	fileListingSession   filelisting.Session
	listDevicesCalls     int
	watchDevicesCalls    int
	openFileListingCalls int
	fileListingUDID      string
}

func (p *fakeADBProvider) ListDevices(ctx context.Context) ([]devicetracker.Device, error) {
	p.listDevicesCalls++
	return p.devices, nil
}

func (p *fakeADBProvider) WatchDevices(ctx context.Context) (<-chan devicetracker.Event, error) {
	p.watchDevicesCalls++
	events := make(chan devicetracker.Event)
	return events, nil
}

func (p *fakeADBProvider) RunCommand(ctx context.Context, command devicetracker.Command) error {
	return nil
}

func (p *fakeADBProvider) Start(ctx context.Context, request shell.Request) (shell.Session, error) {
	return nil, errors.New("shell not used in this test")
}


func (p *fakeADBProvider) OpenFileListing(ctx context.Context, udid string) (filelisting.Session, error) {
	p.openFileListingCalls++
	p.fileListingUDID = udid
	if p.fileListingSession == nil {
		p.fileListingSession = &fakeFileListingSession{}
	}
	return p.fileListingSession, nil
}

func newNoopADBProvider(options ...adbcli.Option) adbProvider {
	return &fakeADBProvider{devices: []devicetracker.Device{}}
}

type fakeFileListingSession struct {
	entries []filelisting.Entry
}

func (s *fakeFileListingSession) Stat(ctx context.Context, path string) (filelisting.Entry, error) {
	return filelisting.Entry{Name: "sdcard", Mode: 0040755, MTime: 1710000000}, nil
}

func (s *fakeFileListingSession) List(ctx context.Context, path string) ([]filelisting.Entry, error) {
	return append([]filelisting.Entry(nil), s.entries...), nil
}

func (s *fakeFileListingSession) Recv(ctx context.Context, path string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("hello")), nil
}

func (s *fakeFileListingSession) Close() error { return nil }

func newFailingADBProvider(options ...adbcli.Option) adbProvider {
	panic("default adb provider factory should not be used")
}

type recordingRunner struct {
	name   string
	args   []string
	output []byte
}

func (r *recordingRunner) Command(ctx context.Context, name string, args ...string) adbcli.Cmd {
	if r.name == "" {
		r.name = name
		r.args = append([]string(nil), args...)
	}
	return recordingCmd{output: r.output}
}

type recordingCmd struct {
	output []byte
}

func (c recordingCmd) Output() ([]byte, error)         { return c.output, nil }
func (c recordingCmd) CombinedOutput() ([]byte, error) { return c.output, nil }
func (c recordingCmd) Start() error                    { return nil }
func (c recordingCmd) Wait() error                     { return nil }
func (c recordingCmd) StdinPipe() (io.WriteCloser, error) {
	return nopWriteCloser{}, nil
}
func (c recordingCmd) StdoutPipe() (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (c recordingCmd) StderrPipe() (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (c recordingCmd) Kill() error { return nil }

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

func equalStrings(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testStaticFS(t *testing.T) fs.FS {
	t.Helper()
	return fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("test index")},
	}
}

func generateSelfSignedCertificate(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	keyDER := x509.MarshalPKCS1PrivateKey(privateKey)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	key := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
	return string(cert), string(key)
}

var _ http.Handler = NewHandlerWithOptions(config.Config{Pathname: "/"}, fstest.MapFS{}, HandlerOptions{ADBProviderFactory: newNoopADBProvider})
