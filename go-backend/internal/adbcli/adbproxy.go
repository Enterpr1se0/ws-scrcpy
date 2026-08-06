package adbcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbproxy"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/gorilla/websocket"
)

var ErrADBStreamUnsupported = errors.New("adb raw stream proxy is not implemented for adbcli provider")

var _ adbproxy.StreamProvider = UnsupportedStreamProvider{}
var _ adbproxy.StreamProvider = (*Provider)(nil)

type PortAllocator interface {
	Allocate(ctx context.Context) (int, error)
}

// UnsupportedStreamProvider is retained for tests that need a provider which always fails.
type UnsupportedStreamProvider struct{}

func NewUnsupportedStreamProvider() UnsupportedStreamProvider {
	return UnsupportedStreamProvider{}
}

func (UnsupportedStreamProvider) Open(ctx context.Context, request adbproxy.Request) (adbproxy.Stream, error) {
	return nil, ErrADBStreamUnsupported
}

func NewForwardStreamProvider(options ...Option) *Provider {
	return New(options...)
}

func (p *Provider) Open(ctx context.Context, request adbproxy.Request) (adbproxy.Stream, error) {
	allocator := p.portAllocator
	if allocator == nil {
		allocator = tcpPortAllocator{}
	}
	localPort, err := allocator.Allocate(ctx)
	if err != nil {
		applog.Errorf("adbproxy allocate port failed udid=%s err=%v", request.UDID, err)
		return nil, err
	}
	localTarget := "tcp:" + strconv.Itoa(localPort)
	if _, err := p.command(ctx, "-s", request.UDID, "forward", localTarget, request.Remote).CombinedOutput(); err != nil {
		applog.Errorf("adbproxy forward failed udid=%s local=%s remote=%s err=%v", request.UDID, localTarget, request.Remote, err)
		return nil, err
	}
	upstreamURL := websocketURL(localPort, request.Path)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, upstreamURL, nil)
	if err != nil {
		if removeErr := p.removeForwardWithTimeout(request.UDID, localTarget); removeErr != nil {
			applog.Warnf("adbproxy cleanup forward failed udid=%s local=%s err=%v", request.UDID, localTarget, removeErr)
		}
		applog.Errorf("adbproxy dial upstream failed udid=%s url=%s err=%v", request.UDID, upstreamURL, err)
		return nil, err
	}
	return &forwardStream{provider: p, conn: conn, udid: request.UDID, localTarget: localTarget}, nil
}

func (p *Provider) removeForward(ctx context.Context, udid string, localTarget string) error {
	_, err := p.command(ctx, "-s", udid, "forward", "--remove", localTarget).CombinedOutput()
	return err
}

func (p *Provider) removeForwardWithTimeout(udid string, localTarget string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return p.removeForward(ctx, udid, localTarget)
}

func websocketURL(localPort int, requestPath string) string {
	u := url.URL{Scheme: "ws", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(localPort))}
	if requestPath != "" {
		rawPath, rawQuery, _ := strings.Cut(requestPath, "?")
		path, err := url.PathUnescape(rawPath)
		if err != nil {
			path = rawPath
		}
		u.Path = path
		u.RawPath = rawPath
		u.RawQuery = rawQuery
	}
	return u.String()
}

type tcpPortAllocator struct{}

func (tcpPortAllocator) Allocate(ctx context.Context) (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("allocated address %q is not TCP", listener.Addr().String())
	}
	return addr.Port, nil
}

type forwardStream struct {
	provider    *Provider
	conn        *websocket.Conn
	udid        string
	localTarget string

	mu       sync.Mutex
	pending  []byte
	once     sync.Once
	closeErr error
}

func (s *forwardStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		return n, nil
	}
	for {
		messageType, payload, err := s.conn.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) || errors.Is(err, net.ErrClosed) {
				return 0, io.EOF
			}
			return 0, err
		}
		if messageType != websocket.BinaryMessage && messageType != websocket.TextMessage {
			continue
		}
		n := copy(p, payload)
		if n < len(payload) {
			s.pending = append(s.pending[:0], payload[n:]...)
		}
		return n, nil
	}
}

func (s *forwardStream) Write(p []byte) (int, error) {
	if err := s.conn.WriteMessage(websocket.BinaryMessage, append([]byte(nil), p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *forwardStream) Close() error {
	s.once.Do(func() {
		connErr := s.conn.Close()
		removeErr := s.provider.removeForwardWithTimeout(s.udid, s.localTarget)
		if connErr != nil {
			s.closeErr = connErr
			return
		}
		s.closeErr = removeErr
	})
	return s.closeErr
}
