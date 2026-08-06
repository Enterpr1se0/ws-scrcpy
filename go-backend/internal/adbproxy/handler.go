package adbproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

// Request describes the ADB-backed websocket stream requested by a client.
type Request struct {
	UDID   string
	Remote string
	Path   string
}

// Stream is the raw bidirectional byte stream bridged to a websocket.
type Stream interface {
	io.ReadWriteCloser
}

// StreamProvider opens raw ADB-backed byte streams.
type StreamProvider interface {
	Open(ctx context.Context, request Request) (Stream, error)
}

// Handler adapts websocket clients to raw ADB-backed streams.
type Handler struct {
	provider StreamProvider
}

func NewHandler(provider StreamProvider) *Handler {
	return &Handler{provider: provider}
}

func (h *Handler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	defer conn.Close()

	request, ok := parseRequest(r.URL)
	if !ok {
		applog.Warnf("adbproxy invalid parameters remote=%s path=%s query=%s", r.RemoteAddr, r.URL.Path, r.URL.RawQuery)
		closeClient(conn, contract.CloseInvalidParameter, "invalid proxy-adb parameters")
		return
	}
	if h.provider == nil {
		applog.Errorf("adbproxy provider not configured remote=%s", r.RemoteAddr)
		closeClient(conn, contract.CloseServiceStartFailed, "adb stream provider is not configured")
		return
	}
	stream, err := h.provider.Open(ctx, request)
	if err != nil {
		applog.Errorf("adbproxy open failed udid=%s remote=%s path=%s err=%v", request.UDID, request.Remote, request.Path, err)
		closeClient(conn, contract.CloseServiceStartFailed, "failed to open adb stream")
		return
	}
	defer stream.Close()

	clientWrites := &sync.Mutex{}
	done := make(chan int, 2)
	go copyClientToStream(conn, stream, done)
	go copyStreamToClient(stream, conn, clientWrites, done)

	select {
	case <-ctx.Done():
		_ = stream.Close()
	case code := <-done:
		_ = stream.Close()
		if code != 0 {
			applog.Warnf("adbproxy stream closed udid=%s code=%d", request.UDID, code)
			closeClientLocked(conn, clientWrites, code, "adb proxy stream closed")
		}
	}
}

func parseRequest(u *url.URL) (Request, bool) {
	if request, ok := parseShortcutPath(u); ok {
		return request, validRequest(request)
	}
	query := u.Query()
	request := Request{
		UDID:   query.Get("udid"),
		Remote: query.Get("remote"),
		Path:   query.Get("path"),
	}
	return request, validRequest(request)
}

func parseShortcutPath(u *url.URL) (Request, bool) {
	escaped := u.EscapedPath()
	if u.RawPath != "" {
		escaped = u.RawPath
	}
	if escaped == "" {
		return Request{}, false
	}
	segments := strings.Split(escaped, "/")
	if len(segments) < 4 || segments[0] != "" || segments[1] != contract.ActionProxyADB {
		return Request{}, false
	}
	udid, err := url.PathUnescape(segments[2])
	if err != nil {
		return Request{}, true
	}
	remote, err := url.PathUnescape(segments[3])
	if err != nil {
		return Request{}, true
	}
	path := ""
	if len(segments) > 4 {
		path, err = url.PathUnescape(strings.Join(segments[4:], "/"))
		if err != nil {
			return Request{}, true
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		if u.RawQuery != "" {
			path += "?" + u.RawQuery
		}
	}
	return Request{UDID: udid, Remote: remote, Path: path}, true
}

func validRequest(request Request) bool {
	return request.UDID != "" && request.Remote != ""
}

func copyClientToStream(client *websocket.Conn, stream Stream, done chan<- int) {
	for {
		messageType, payload, err := client.ReadMessage()
		if err != nil {
			done <- 0
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		if n, err := stream.Write(payload); err != nil || n != len(payload) {
			if err != nil {
				applog.Warnf("adbproxy stream write failed: %v", err)
			} else {
				applog.Warnf("adbproxy stream short write n=%d want=%d", n, len(payload))
			}
			done <- contract.CloseProxyUpstreamError
			return
		}
	}
}

func copyStreamToClient(stream Stream, client *websocket.Conn, clientWrites *sync.Mutex, done chan<- int) {
	buf := make([]byte, 32*1024)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			clientWrites.Lock()
			writeErr := client.WriteMessage(websocket.BinaryMessage, chunk)
			clientWrites.Unlock()
			if writeErr != nil {
				done <- 0
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				done <- contract.CloseProxyUpstreamClosed
				return
			}
			applog.Warnf("adbproxy stream read failed: %v", err)
			done <- contract.CloseProxyUpstreamError
			return
		}
	}
}

func closeClient(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, safeCloseReason(reason)), time.Now().Add(time.Second))
}

func closeClientLocked(conn *websocket.Conn, mu *sync.Mutex, code int, reason string) {
	mu.Lock()
	defer mu.Unlock()
	closeClient(conn, code, reason)
}

func safeCloseReason(reason string) string {
	const maxCloseReasonBytes = 123
	if len(reason) <= maxCloseReasonBytes {
		return reason
	}
	bytes := 0
	for i, r := range reason {
		runeBytes := utf8.RuneLen(r)
		if bytes+runeBytes > maxCloseReasonBytes {
			return reason[:i]
		}
		bytes += runeBytes
	}
	return reason
}
