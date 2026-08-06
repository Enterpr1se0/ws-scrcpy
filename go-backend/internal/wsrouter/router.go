package wsrouter

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/gorilla/websocket"
)

type Handler interface {
	ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request)
}

type Router struct {
	handlers map[string]Handler
	upgrader websocket.Upgrader
}

func New(handlers map[string]Handler) *Router {
	copied := make(map[string]Handler, len(handlers))
	for action, handler := range handlers {
		copied[action] = handler
	}
	return &Router{
		handlers: copied,
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	remote := req.RemoteAddr
	action := req.URL.Query().Get("action")
	path := req.URL.EscapedPath()
	applog.Infof("ws upgrade remote=%s path=%s action=%q", remote, path, action)
	conn, err := r.upgrader.Upgrade(w, req, nil)
	if err != nil {
		applog.Warnf("ws upgrade failed remote=%s path=%s err=%v", remote, path, err)
		return
	}
	handler := r.handlers[action]
	if handler == nil && isProxyADBShortcut(path) {
		handler = r.handlers[contract.ActionProxyADB]
		applog.Debugf("ws using proxy-adb shortcut remote=%s path=%s", remote, path)
	}
	if handler == nil {
		reason := fmt.Sprintf("[WebSocket Server {%s}] Unsupported request", req.Host)
		applog.Warnf("ws unsupported remote=%s path=%s action=%q", remote, path, action)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(contract.CloseUnsupportedRequest, reason), websocketDeadline())
		_ = conn.Close()
		return
	}
	applog.Debugf("ws handler start remote=%s action=%q", remote, action)
	handler.ServeWS(req.Context(), conn, req)
	applog.Debugf("ws handler end remote=%s action=%q", remote, action)
}

func isProxyADBShortcut(escapedPath string) bool {
	return escapedPath == "/"+contract.ActionProxyADB || strings.HasPrefix(escapedPath, "/"+contract.ActionProxyADB+"/")
}
