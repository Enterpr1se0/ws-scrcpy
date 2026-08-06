package shell

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/wsrouter"
	"github.com/gorilla/websocket"
)

var _ wsrouter.Handler = (*Handler)(nil)

func TestDirectInvalidParamsCloses4003(t *testing.T) {
	conn := dialShell(t, NewHandler(&fakeProvider{}), "")
	defer conn.Close()

	writeJSON(t, conn, map[string]any{"type": "shell", "data": map[string]any{"type": "start"}})

	_, _, err := readMessage(t, conn)
	assertCloseCode(t, err, contract.CloseInvalidParameter)
}

func TestDirectStartFailureCloses4005(t *testing.T) {
	conn := dialShell(t, NewHandler(&fakeProvider{startErr: errors.New("start failed")}), "")
	defer conn.Close()

	writeJSON(t, conn, map[string]any{"type": "shell", "data": map[string]any{"type": "start", "udid": "device-1", "rows": 24, "cols": 80}})

	_, _, err := readMessage(t, conn)
	assertCloseCode(t, err, contract.CloseServiceStartFailed)
}

func TestDirectParentCancelBeforeInitialMessageExits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	handler := NewHandler(&fakeProvider{})
	conn := dialShell(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := handler.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer close(done)
		handler.ServeWS(ctx, ws, r)
	}), "")
	defer conn.Close()

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ServeWS did not exit after parent context cancellation")
	}
}

func TestDirectInputOutputAndSessionExitCloses4500(t *testing.T) {
	session := newFakeSession()
	provider := &fakeProvider{session: session}
	conn := dialShell(t, NewHandler(provider), "")
	defer conn.Close()

	writeJSON(t, conn, map[string]any{"type": "shell", "data": map[string]any{"type": "start", "udid": "device-1", "rows": 24, "cols": 80}})
	writeText(t, conn, "pwd\n")

	_, data, err := readMessage(t, conn)
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if string(data) != "out:pwd\n" {
		t.Fatalf("output = %q, want %q", data, "out:pwd\n")
	}
	if provider.request.UDID != "device-1" || provider.request.Rows != 24 || provider.request.Cols != 80 {
		t.Fatalf("request = %+v, want udid device-1 rows 24 cols 80", provider.request)
	}

	session.finish(nil)
	_, _, err = readMessage(t, conn)
	assertCloseCode(t, err, contract.CloseShellExit)
}

func TestDirectSessionExitCloses4500WhenOutputClosesFirst(t *testing.T) {
	session := newFakeSession()
	conn := dialShell(t, NewHandler(&fakeProvider{session: session}), "")
	defer conn.Close()

	writeJSON(t, conn, map[string]any{"type": "shell", "data": map[string]any{"type": "start", "udid": "device-1", "rows": 24, "cols": 80}})

	close(session.output)
	time.Sleep(25 * time.Millisecond)
	session.finish(nil)

	_, _, err := readMessage(t, conn)
	assertCloseCode(t, err, contract.CloseShellExit)
}

func TestSHELFactoryWaitsForFrontendStartMessageAndStreamsProviderOutput(t *testing.T) {
	session := newFakeSession()
	provider := &fakeProvider{session: session}
	factory := NewSHELFactory(provider)

	channel, initial, err := factory(nil)
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	async, ok := channel.(multiplex.AsyncChannel)
	if !ok {
		t.Fatal("channel does not implement AsyncChannel")
	}
	if len(initial) != 0 {
		t.Fatalf("initial responses = %d, want 0", len(initial))
	}
	if provider.request.UDID != "" {
		t.Fatalf("provider started before shell start message: %+v", provider.request)
	}

	responses, err := channel.OnData([]byte(`{"type":"shell","data":{"type":"start","udid":"device-1","rows":24,"cols":80}}`))
	if err != nil {
		t.Fatalf("OnData(start) returned error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("start responses = %q, want none", responses)
	}
	if provider.request.UDID != "device-1" || provider.request.Rows != 24 || provider.request.Cols != 80 {
		t.Fatalf("request = %+v, want udid device-1 rows 24 cols 80", provider.request)
	}

	responses, err = channel.OnData([]byte("ls\n"))
	if err != nil {
		t.Fatalf("OnData(input) returned error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("input responses = %q, want async output only", responses)
	}
	select {
	case output := <-async.Messages():
		if string(output) != "out:ls\n" {
			t.Fatalf("output = %q, want out:ls", output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async shell output")
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !session.closed {
		t.Fatal("session was not closed")
	}
}

func dialShell(t *testing.T, handler http.Handler, rawQuery string) *websocket.Conn {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.RawQuery = rawQuery
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	return conn
}

func writeJSON(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	if err := conn.WriteJSON(value); err != nil {
		t.Fatalf("WriteJSON returned error: %v", err)
	}
}

func writeText(t *testing.T, conn *websocket.Conn, value string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(value)); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
}

func readMessage(t *testing.T, conn *websocket.Conn) (int, []byte, error) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return conn.ReadMessage()
}

func assertCloseCode(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("ReadMessage error = nil, want close code %d", want)
	}
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("error = %T %v, want *websocket.CloseError", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}

type fakeProvider struct {
	session  *fakeSession
	startErr error
	request  Request
}

func (p *fakeProvider) Start(ctx context.Context, request Request) (Session, error) {
	p.request = request
	if p.startErr != nil {
		return nil, p.startErr
	}
	if p.session == nil {
		p.session = newFakeSession()
	}
	return p.session, nil
}

type fakeSession struct {
	output chan []byte
	done   chan error
	closed bool
}

func newFakeSession() *fakeSession {
	return &fakeSession{output: make(chan []byte, 8), done: make(chan error, 1)}
}

func (s *fakeSession) Write(ctx context.Context, data []byte) error {
	s.output <- append([]byte("out:"), data...)
	return nil
}

func (s *fakeSession) Output() <-chan []byte { return s.output }
func (s *fakeSession) Done() <-chan error    { return s.done }
func (s *fakeSession) Close() error          { s.closed = true; return nil }
func (s *fakeSession) finish(err error)      { s.done <- err }

var _ multiplex.Channel = (*shellChannel)(nil)
var _ multiplex.AsyncChannel = (*shellChannel)(nil)
