package wsrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

type recordingHandler struct {
	called bool
	action string
}

func (h *recordingHandler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	h.called = true
	h.action = r.URL.Query().Get("action")
	_ = conn.WriteMessage(websocket.TextMessage, []byte("handled:"+h.action))
	_ = conn.Close()
}

func TestDispatchesByActionQuery(t *testing.T) {
	handler := &recordingHandler{}
	server := httptest.NewServer(New(map[string]Handler{contract.ActionProxyWS: handler}))
	defer server.Close()
	conn := dialWS(t, server.URL+"/?action=proxy-ws")
	defer conn.Close()
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if string(message) != "handled:proxy-ws" {
		t.Fatalf("message = %q", message)
	}
	if !handler.called {
		t.Fatal("handler was not called")
	}
}

func TestUnsupportedActionClosesWith4002(t *testing.T) {
	server := httptest.NewServer(New(map[string]Handler{}))
	defer server.Close()
	conn := dialWS(t, server.URL+"/?action=missing")
	defer conn.Close()
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("error = %T %v, want *websocket.CloseError", err, err)
	}
	if closeErr.Code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", closeErr.Code, contract.CloseUnsupportedRequest)
	}
}

func TestMissingActionClosesWith4002(t *testing.T) {
	server := httptest.NewServer(New(map[string]Handler{}))
	defer server.Close()
	conn := dialWS(t, server.URL+"/")
	defer conn.Close()
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("error = %T %v, want *websocket.CloseError", err, err)
	}
	if closeErr.Code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", closeErr.Code, contract.CloseUnsupportedRequest)
	}
}

func dialWS(t *testing.T, rawURL string) *websocket.Conn {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	parsed.Scheme = "ws"
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	return conn
}
