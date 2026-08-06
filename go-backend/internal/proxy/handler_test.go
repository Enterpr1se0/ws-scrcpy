package proxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

func TestMissingWSParamClosesWith4003(t *testing.T) {
	server := newProxyServer(t)
	defer server.Close()

	conn := dialWS(t, server.URL+"/?action=proxy-ws")
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseInvalidParameter)
}

func TestEmptyWSParamClosesWith4003(t *testing.T) {
	server := newProxyServer(t)
	defer server.Close()

	conn := dialWS(t, server.URL+"/?action=proxy-ws&ws=")
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseInvalidParameter)
}

func TestInvalidWSParamClosesWith4003(t *testing.T) {
	tests := []struct {
		name string
		ws   string
	}{
		{name: "syntactically invalid", ws: "://bad"},
		{name: "non websocket scheme", ws: "http://example.com/ws"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newProxyServer(t)
			defer server.Close()

			conn := dialWS(t, server.URL+"/?action=proxy-ws&ws="+url.QueryEscape(tc.ws))
			defer conn.Close()

			assertCloseCode(t, conn, contract.CloseInvalidParameter)
		})
	}
}

func TestUnreachableUpstreamClosesWith4005(t *testing.T) {
	server := newProxyServer(t)
	defer server.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	conn := dialWS(t, server.URL+"/?action=proxy-ws&ws="+url.QueryEscape("ws://"+addr+"/unreachable"))
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestTextRoundTrip(t *testing.T) {
	upstream := newEchoUpstream(t)
	defer upstream.Close()
	server := newProxyServer(t)
	defer server.Close()

	conn := dialProxy(t, server.URL, upstream.URL)
	defer conn.Close()

	payload := []byte("hello through proxy")
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
	messageType, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.TextMessage)
	}
	if string(message) != string(payload) {
		t.Fatalf("message = %q, want %q", message, payload)
	}
}

func TestBinaryRoundTrip(t *testing.T) {
	upstream := newEchoUpstream(t)
	defer upstream.Close()
	server := newProxyServer(t)
	defer server.Close()

	conn := dialProxy(t, server.URL, upstream.URL)
	defer conn.Close()

	payload := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
	messageType, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.BinaryMessage)
	}
	if string(message) != string(payload) {
		t.Fatalf("message bytes = %v, want %v", message, payload)
	}
}

func TestUpstreamClosePropagatesWith4010(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second))
		_ = conn.Close()
	}))
	defer upstream.Close()
	server := newProxyServer(t)
	defer server.Close()

	conn := dialProxy(t, server.URL, upstream.URL)
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseProxyUpstreamClosed)
}

func TestAbnormalUpstreamClosePropagatesWith4011(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.UnderlyingConn().Close()
	}))
	defer upstream.Close()
	server := newProxyServer(t)
	defer server.Close()

	conn := dialProxy(t, server.URL, upstream.URL)
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseProxyUpstreamError)
}

func newProxyServer(t *testing.T) *httptest.Server {
	t.Helper()
	handler := NewHandler()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Upgrade returned error: %v", err)
		}
		handler.ServeWS(context.Background(), conn, r)
	}))
}

func newEchoUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(messageType, message); err != nil {
				return
			}
		}
	}))
}

func dialProxy(t *testing.T, proxyURL string, upstreamHTTPURL string) *websocket.Conn {
	t.Helper()
	upstreamURL := httpURLToWS(t, upstreamHTTPURL)
	return dialWS(t, proxyURL+"/?action=proxy-ws&ws="+url.QueryEscape(upstreamURL))
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

func httpURLToWS(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	parsed.Scheme = "ws"
	return parsed.String()
}

func assertCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("error = %T %v, want *websocket.CloseError", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}
