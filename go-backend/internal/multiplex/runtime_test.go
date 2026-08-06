package multiplex

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/hosttracker"
	"github.com/gorilla/websocket"
)

func TestHSTSCreateReturnsInitialHostsDataFrame(t *testing.T) {
	conn := dialMultiplex(t, NewHandler(testRegistry()))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 7, Payload: []byte(contract.ChannelHSTS)})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeRawStringData)
	}
	if frame.ChannelID != 7 {
		t.Fatalf("channel id = %d, want 7", frame.ChannelID)
	}
	want := `{"id":-1,"type":"hosts","data":{"local":[],"remote":[]}}`
	if string(frame.Payload) != want {
		t.Fatalf("payload = %s, want %s", frame.Payload, want)
	}
}

func TestUnsupportedChannelCreateReturnsCloseChannel4002(t *testing.T) {
	conn := dialMultiplex(t, NewHandler(testRegistry()))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 8, Payload: []byte(contract.ChannelSHEL)})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 8 {
		t.Fatalf("channel id = %d, want 8", frame.ChannelID)
	}
	code, reason, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unsupported") || !strings.Contains(reason, contract.ChannelSHEL) {
		t.Fatalf("reason = %q, want unsupported channel/code mention", reason)
	}
}

func TestMalformedCreatePayloadReturnsCloseChannel4003(t *testing.T) {
	conn := dialMultiplex(t, NewHandler(testRegistry()))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 9, Payload: []byte("H")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 9 {
		t.Fatalf("channel id = %d, want 9", frame.ChannelID)
	}
	code, _, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseInvalidParameter {
		t.Fatalf("close code = %d, want %d", code, contract.CloseInvalidParameter)
	}
}

func TestHSTSDataMessageReturnsUnsupportedMessageDataFrame(t *testing.T) {
	conn := dialMultiplex(t, NewHandler(testRegistry()))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 10, Payload: []byte(contract.ChannelHSTS)})
	_ = readFrame(t, conn)
	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 10, Payload: []byte("hello")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeRawStringData)
	}
	if frame.ChannelID != 10 {
		t.Fatalf("channel id = %d, want 10", frame.ChannelID)
	}
	want := `{"id":-1,"type":"error","data":"Unsupported message: \"hello\""}`
	if string(frame.Payload) != want {
		t.Fatalf("payload = %s, want %s", frame.Payload, want)
	}
}

func TestClientCloseChannelRemovesChannelAndLaterDataReturns4002(t *testing.T) {
	conn := dialMultiplex(t, NewHandler(testRegistry()))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 11, Payload: []byte(contract.ChannelHSTS)})
	_ = readFrame(t, conn)
	writeFrame(t, conn, Frame{Type: contract.MessageTypeCloseChannel, ChannelID: 11})
	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 11, Payload: []byte("hello")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 11 {
		t.Fatalf("channel id = %d, want 11", frame.ChannelID)
	}
	code, reason, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unknown") {
		t.Fatalf("reason = %q, want unknown channel mention", reason)
	}
}

func TestServerWebsocketCloseClosesRegisteredChannels(t *testing.T) {
	channel := newTestChannel()
	conn := dialMultiplex(t, NewHandler(singleChannelRegistry("TEST", channel)))

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 21, Payload: []byte("TEST")})
	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	channel.waitClosed(t)
}

func TestInvalidFrameClosesRegisteredChannels(t *testing.T) {
	channel := newTestChannel()
	conn := dialMultiplex(t, NewHandler(singleChannelRegistry("TEST", channel)))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 22, Payload: []byte("TEST")})
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte{contract.MessageTypeData}); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}

	channel.waitClosed(t)
}

func TestDuplicateCreateChannelClosesAndRemovesExistingChannel(t *testing.T) {
	channel := newTestChannel()
	var factoryCalls atomic.Int32
	registry := NewRegistry()
	registry.Register("TEST", func(initialPayload []byte) (Channel, [][]byte, error) {
		factoryCalls.Add(1)
		return channel, nil, nil
	})
	conn := dialMultiplex(t, NewHandler(registry))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 23, Payload: []byte("TEST")})
	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 23, Payload: []byte("TEST")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 23 {
		t.Fatalf("channel id = %d, want 23", frame.ChannelID)
	}
	code, reason, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "duplicate") {
		t.Fatalf("reason = %q, want duplicate channel mention", reason)
	}
	channel.waitClosed(t)
	if got := factoryCalls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}

	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 23, Payload: []byte("after duplicate")})
	frame = readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	code, reason, ok = DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unknown") {
		t.Fatalf("reason = %q, want unknown channel mention", reason)
	}
}

func TestUnsupportedMessageTypeClosesAndRemovesExistingChannel(t *testing.T) {
	channel := newTestChannel()
	conn := dialMultiplex(t, NewHandler(singleChannelRegistry("TEST", channel)))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 26, Payload: []byte("TEST")})
	writeFrame(t, conn, Frame{Type: 0xff, ChannelID: 26, Payload: []byte("unsupported")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 26 {
		t.Fatalf("channel id = %d, want 26", frame.ChannelID)
	}
	code, reason, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unsupported message type") {
		t.Fatalf("reason = %q, want unsupported message type mention", reason)
	}
	channel.waitClosed(t)

	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 26, Payload: []byte("after unsupported type")})
	frame = readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	code, reason, ok = DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unknown") {
		t.Fatalf("reason = %q, want unknown channel mention", reason)
	}
}

func TestOnDataErrorClosesAndRemovesChannel(t *testing.T) {
	channel := newTestChannel()
	channel.dataErr = errors.New("data failed")
	conn := dialMultiplex(t, NewHandler(singleChannelRegistry("TEST", channel)))
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 24, Payload: []byte("TEST")})
	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 24, Payload: []byte("fail")})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 24 {
		t.Fatalf("channel id = %d, want 24", frame.ChannelID)
	}
	code, reason, ok := DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(reason, "data failed") {
		t.Fatalf("reason = %q, want data error mention", reason)
	}
	channel.waitClosed(t)

	writeFrame(t, conn, Frame{Type: contract.MessageTypeData, ChannelID: 24, Payload: []byte("after error")})
	frame = readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	code, reason, ok = DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
	if !strings.Contains(strings.ToLower(reason), "unknown") {
		t.Fatalf("reason = %q, want unknown channel mention", reason)
	}
}

func testRegistry() *Registry {
	registry := NewRegistry()
	registry.Register(contract.ChannelHSTS, NewHostTrackerFactory(hosttracker.New(nil, nil)))
	return registry
}

func singleChannelRegistry(code string, channel Channel) *Registry {
	registry := NewRegistry()
	registry.Register(code, func(initialPayload []byte) (Channel, [][]byte, error) {
		return channel, nil, nil
	})
	return registry
}

type testChannel struct {
	dataErr error

	once   sync.Once
	closed chan struct{}
}

func newTestChannel() *testChannel {
	return &testChannel{closed: make(chan struct{})}
}

func (c *testChannel) OnData(payload []byte) ([][]byte, error) {
	if c.dataErr != nil {
		return nil, c.dataErr
	}
	return nil, nil
}

func (c *testChannel) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *testChannel) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed")
	}
}

func dialMultiplex(t *testing.T, handler http.Handler) *websocket.Conn {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	parsed, err := url.Parse(server.URL)
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

func writeFrame(t *testing.T, conn *websocket.Conn, frame Frame) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, Encode(frame)); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.BinaryMessage)
	}
	frame, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	return frame
}

func TestHandlerExitsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	channel := newTestChannel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		NewHandler(singleChannelRegistry("TEST", channel)).ServeWS(ctx, conn, r)
	}))
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	parsed.Scheme = "ws"
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	defer conn.Close()

	writeFrame(t, conn, Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 25, Payload: []byte("TEST")})

	cancel()
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("ReadMessage returned nil error after context cancellation")
	}
	channel.waitClosed(t)
}
