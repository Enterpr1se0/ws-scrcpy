package proxy

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

type Handler struct {
	dialer *websocket.Dialer
}

func NewHandler() *Handler {
	return &Handler{dialer: websocket.DefaultDialer}
}

func (h *Handler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	defer conn.Close()

	upstreamURL, ok := validUpstreamURL(r.URL.Query().Get("ws"))
	if !ok {
		applog.Warnf("proxy invalid ws parameter remote=%s raw=%q", r.RemoteAddr, r.URL.Query().Get("ws"))
		closeClient(conn, contract.CloseInvalidParameter, "invalid ws parameter")
		return
	}

	upstream, _, err := h.dialer.DialContext(ctx, upstreamURL, nil)
	if err != nil {
		applog.Errorf("proxy upstream dial failed remote=%s url=%s err=%v", r.RemoteAddr, upstreamURL, err)
		closeClient(conn, contract.CloseServiceStartFailed, "failed to connect upstream websocket")
		return
	}
	defer upstream.Close()

	clientWrites := &sync.Mutex{}
	done := make(chan int, 2)

	go copyClientToUpstream(conn, upstream, clientWrites, done)
	go copyUpstreamToClient(upstream, conn, clientWrites, done)

	code := <-done
	if code != 0 {
		applog.Warnf("proxy session closed remote=%s code=%d", r.RemoteAddr, code)
		closeClientLocked(conn, clientWrites, code, "proxy upstream closed")
	}
}

func validUpstreamURL(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	if parsed.Host == "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
		return "", false
	}
	return parsed.String(), true
}

func copyClientToUpstream(client *websocket.Conn, upstream *websocket.Conn, clientWrites *sync.Mutex, done chan<- int) {
	for {
		messageType, payload, err := client.ReadMessage()
		if err != nil {
			done <- 0
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		if err := upstream.WriteMessage(messageType, payload); err != nil {
			applog.Warnf("proxy write upstream failed: %v", err)
			closeClientLocked(client, clientWrites, contract.CloseProxyUpstreamError, "proxy upstream error")
			done <- contract.CloseProxyUpstreamError
			return
		}
	}
}

func copyUpstreamToClient(upstream *websocket.Conn, client *websocket.Conn, clientWrites *sync.Mutex, done chan<- int) {
	for {
		messageType, payload, err := upstream.ReadMessage()
		if err != nil {
			code := closeCodeForUpstreamRead(err)
			if code == contract.CloseProxyUpstreamError {
				applog.Warnf("proxy upstream read failed: %v", err)
			} else {
				applog.Debugf("proxy upstream closed: %v", err)
			}
			done <- code
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		clientWrites.Lock()
		err = client.WriteMessage(messageType, payload)
		clientWrites.Unlock()
		if err != nil {
			done <- 0
			return
		}
	}
}

func closeCodeForUpstreamRead(err error) int {
	if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return contract.CloseProxyUpstreamClosed
	}
	return contract.CloseProxyUpstreamError
}

func closeClient(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
}

func closeClientLocked(conn *websocket.Conn, mu *sync.Mutex, code int, reason string) {
	mu.Lock()
	defer mu.Unlock()
	closeClient(conn, code, reason)
}
