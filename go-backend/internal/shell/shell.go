package shell

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/gorilla/websocket"
)

var ErrInvalidRequest = errors.New("invalid shell request")

type Request struct {
	UDID string `json:"udid"`
	Rows int    `json:"rows,omitempty"`
	Cols int    `json:"cols,omitempty"`
}

type Provider interface {
	Start(ctx context.Context, request Request) (Session, error)
}

type Session interface {
	Write(ctx context.Context, data []byte) error
	Output() <-chan []byte
	Done() <-chan error
	Close() error
}

type Handler struct {
	provider Provider
	upgrader websocket.Upgrader
}

func NewHandler(provider Provider) *Handler {
	return &Handler{provider: provider, upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		applog.Warnf("shell upgrade failed remote=%s err=%v", r.RemoteAddr, err)
		return
	}
	h.ServeWS(r.Context(), conn, r)
}

func (h *Handler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	applog.Infof("shell session open remote=%s query=%s", r.RemoteAddr, r.URL.RawQuery)
	defer applog.Infof("shell session close remote=%s", r.RemoteAddr)
	defer conn.Close()
	request, err := initialRequestFromQuery(r)
	if err != nil {
		applog.Warnf("shell invalid query remote=%s err=%v", r.RemoteAddr, err)
		writeClose(conn, contract.CloseInvalidParameter, err.Error())
		return
	}
	if request.UDID == "" {
		stopClosingOnCancel := closeConnOnContextDone(ctx, conn)
		defer stopClosingOnCancel()
		_, data, err := conn.ReadMessage()
		if err != nil {
			applog.Debugf("shell initial read failed remote=%s err=%v", r.RemoteAddr, err)
			return
		}
		request, err = parseInitialMessage(data)
		if err != nil {
			applog.Warnf("shell invalid initial message remote=%s err=%v", r.RemoteAddr, err)
			writeClose(conn, contract.CloseInvalidParameter, err.Error())
			return
		}
	}

	session, err := h.provider.Start(ctx, request)
	if err != nil {
		applog.Errorf("shell start failed udid=%s remote=%s err=%v", request.UDID, r.RemoteAddr, err)
		writeClose(conn, contract.CloseServiceStartFailed, err.Error())
		return
	}
	defer session.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writeMu sync.Mutex
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-session.Output():
				if !ok {
					return
				}
				writeMu.Lock()
				err := conn.WriteMessage(websocket.TextMessage, data)
				writeMu.Unlock()
				if err != nil {
					applog.Debugf("shell output write failed udid=%s err=%v", request.UDID, err)
					cancel()
					return
				}
			}
		}
	}()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				cancel()
				return
			}
			if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
				continue
			}
			if err := session.Write(ctx, data); err != nil {
				applog.Warnf("shell input write failed udid=%s err=%v", request.UDID, err)
				cancel()
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-readDone:
	case <-outputDone:
		select {
		case <-ctx.Done():
		case <-readDone:
		case <-session.Done():
			writeMu.Lock()
			writeClose(conn, contract.CloseShellExit, "shell session exited")
			writeMu.Unlock()
		}
	case <-session.Done():
		writeMu.Lock()
		writeClose(conn, contract.CloseShellExit, "shell session exited")
		writeMu.Unlock()
	}
}

func NewSHELFactory(provider Provider) multiplex.ChannelFactory {
	return func(initialPayload []byte) (multiplex.Channel, [][]byte, error) {
		ctx, cancel := context.WithCancel(context.Background())
		channel := &shellChannel{
			ctx:      ctx,
			cancel:   cancel,
			provider: provider,
			messages: make(chan []byte, 16),
		}
		if len(initialPayload) > 0 {
			if err := channel.start(initialPayload); err != nil {
				cancel()
				return nil, nil, err
			}
		}
		return channel, nil, nil
	}
}

type shellChannel struct {
	ctx      context.Context
	cancel   context.CancelFunc
	provider Provider

	mu      sync.Mutex
	session Session

	messages      chan []byte
	closeMessages sync.Once
}

func (c *shellChannel) OnData(payload []byte) ([][]byte, error) {
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session == nil {
		if err := c.start(payload); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err := session.Write(c.ctx, payload); err != nil {
		return nil, err
	}
	return nil, nil
}

func (c *shellChannel) Messages() <-chan []byte {
	return c.messages
}

func (c *shellChannel) Close() error {
	c.cancel()
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session == nil {
		c.closeMessages.Do(func() { close(c.messages) })
		return nil
	}
	return session.Close()
}

func (c *shellChannel) start(payload []byte) error {
	request, err := parseInitialMessage(payload)
	if err != nil {
		return err
	}
	session, err := c.provider.Start(c.ctx, request)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.session != nil {
		c.mu.Unlock()
		return session.Close()
	}
	c.session = session
	c.mu.Unlock()
	go c.forwardOutput(session)
	return nil
}

func (c *shellChannel) forwardOutput(session Session) {
	defer c.closeMessages.Do(func() { close(c.messages) })
	for {
		select {
		case <-c.ctx.Done():
			return
		case data, ok := <-session.Output():
			if !ok {
				return
			}
			select {
			case c.messages <- data:
			case <-c.ctx.Done():
				return
			}
		case <-session.Done():
			return
		}
	}
}

func initialRequestFromQuery(r *http.Request) (Request, error) {
	query := r.URL.Query()
	request := Request{UDID: query.Get("udid")}
	var err error
	if query.Get("rows") != "" {
		request.Rows, err = strconv.Atoi(query.Get("rows"))
		if err != nil {
			return Request{}, ErrInvalidRequest
		}
	}
	if query.Get("cols") != "" {
		request.Cols, err = strconv.Atoi(query.Get("cols"))
		if err != nil {
			return Request{}, ErrInvalidRequest
		}
	}
	if request.UDID == "" {
		return request, nil
	}
	return validateRequest(request)
}

func parseInitialMessage(data []byte) (Request, error) {
	var wrapper struct {
		Type string `json:"type"`
		Data struct {
			Type string `json:"type"`
			UDID string `json:"udid"`
			Rows int    `json:"rows"`
			Cols int    `json:"cols"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return Request{}, err
	}
	if wrapper.Type != contract.ActionShell || wrapper.Data.Type != "start" {
		return Request{}, ErrInvalidRequest
	}
	return validateRequest(Request{UDID: wrapper.Data.UDID, Rows: wrapper.Data.Rows, Cols: wrapper.Data.Cols})
}

func parseMultiplexRequest(data []byte) (Request, error) {
	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		return Request{}, err
	}
	return validateRequest(request)
}

func validateRequest(request Request) (Request, error) {
	if request.UDID == "" || request.Rows < 0 || request.Cols < 0 {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

func closeConnOnContextDone(ctx context.Context, conn *websocket.Conn) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func writeClose(conn *websocket.Conn, code int, reason string) {
	message := websocket.FormatCloseMessage(code, safeCloseReason(reason))
	if err := conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second)); err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	}
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
