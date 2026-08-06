package filelisting

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/wsrouter"
	"github.com/gorilla/websocket"
)

var _ wsrouter.Handler = (*Handler)(nil)

func TestDirectDisabledCloses4002(t *testing.T) {
	conn := dialWebsocket(t, NewHandler(WithEnabled(false)), "")
	defer conn.Close()

	_, _, err := readMessage(t, conn)
	assertCloseCode(t, err, contract.CloseUnsupportedRequest)
}

func TestFSLSDisabledFactoryReturnsCloseChannel4002ViaMultiplexRuntime(t *testing.T) {
	registry := multiplex.NewRegistry()
	registry.Register(contract.ChannelFSLS, NewFSLSFactory(WithEnabled(false)))
	conn := dialWebsocket(t, multiplex.NewHandler(registry), "")
	defer conn.Close()

	payload := append([]byte(contract.ChannelFSLS), []byte("ignored")...)
	writeFrame(t, conn, multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 42, Payload: payload})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeCloseChannel {
		t.Fatalf("frame type = %d, want %d", frame.Type, contract.MessageTypeCloseChannel)
	}
	if frame.ChannelID != 42 {
		t.Fatalf("channel id = %d, want 42", frame.ChannelID)
	}
	code, _, ok := multiplex.DecodeClosePayload(frame.Payload)
	if !ok {
		t.Fatalf("close payload did not decode: %v", frame.Payload)
	}
	if code != contract.CloseUnsupportedRequest {
		t.Fatalf("close code = %d, want %d", code, contract.CloseUnsupportedRequest)
	}
}

func TestFSLSFactoryParsesInitialSerial(t *testing.T) {
	provider := &fakeFileProvider{session: &fakeFileSession{}}
	factory := NewFSLSFactory(WithEnabled(true), WithProvider(provider))

	channel, initial, err := factory(fslsInitPayload("device-1"))
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	defer channel.Close()
	if len(initial) != 0 {
		t.Fatalf("initial responses = %d, want none", len(initial))
	}
	if provider.udid != "device-1" {
		t.Fatalf("provider udid = %q, want device-1", provider.udid)
	}
}

func TestFSLSStatReturnsAdbkitCompatibleSTAT(t *testing.T) {
	channel := newTestFSLSChannel(t, &fakeFileSession{stat: Entry{Name: "hello.txt", Mode: 0100644, Size: 5, MTime: 1710000000}})
	defer channel.Close()

	responses, err := channel.OnData(nestedCreateFrame(7, requestPayload("STAT", "/sdcard/hello.txt")))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	payloads := nestedPayloads(t, responses, 7)
	if len(payloads) < 1 {
		t.Fatalf("payload count = %d, want at least 1", len(payloads))
	}
	got := payloads[0]
	if string(got[:4]) != "STAT" {
		t.Fatalf("reply = %q, want STAT", got[:4])
	}
	if mode := binary.LittleEndian.Uint32(got[4:8]); mode != 0100644 {
		t.Fatalf("mode = %#o, want 0100644", mode)
	}
	if size := binary.LittleEndian.Uint32(got[8:12]); size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}
	if mtime := binary.LittleEndian.Uint32(got[12:16]); mtime != 1710000000 {
		t.Fatalf("mtime = %d, want 1710000000", mtime)
	}
}

func TestFSLSListReturnsDENTsAndDONE(t *testing.T) {
	channel := newTestFSLSChannel(t, &fakeFileSession{list: []Entry{
		{Name: ".", Mode: 0040755, MTime: 1710000000},
		{Name: "hello.txt", Mode: 0100644, Size: 5, MTime: 1710000001},
	}})
	defer channel.Close()

	responses, err := channel.OnData(nestedCreateFrame(8, requestPayload("LIST", "/sdcard")))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	payloads := nestedPayloads(t, responses, 8)
	if len(payloads) < 3 {
		t.Fatalf("payload count = %d, want DENT, DENT, DONE", len(payloads))
	}
	if string(payloads[0][:4]) != "DENT" || string(payloads[1][:4]) != "DENT" || string(payloads[2][:4]) != "DONE" {
		t.Fatalf("replies = %q %q %q, want DENT DENT DONE", payloads[0][:4], payloads[1][:4], payloads[2][:4])
	}
	nameLen := binary.LittleEndian.Uint32(payloads[1][16:20])
	name := string(payloads[1][20 : 20+nameLen])
	if name != "hello.txt" {
		t.Fatalf("DENT name = %q, want hello.txt", name)
	}
}

func TestFSLSRecvReturnsDATAChunksAndDONE(t *testing.T) {
	channel := newTestFSLSChannel(t, &fakeFileSession{recv: "hello world"})
	defer channel.Close()

	responses, err := channel.OnData(nestedCreateFrame(9, requestPayload("RECV", "/sdcard/hello.txt")))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	payloads := nestedPayloads(t, responses, 9)
	if len(payloads) < 2 {
		t.Fatalf("payload count = %d, want DATA and DONE", len(payloads))
	}
	if string(payloads[0][:4]) != "DATA" {
		t.Fatalf("first reply = %q, want DATA", payloads[0][:4])
	}
	if string(payloads[0][4:]) != "hello world" {
		t.Fatalf("DATA = %q, want hello world", payloads[0][4:])
	}
	if string(payloads[1][:4]) != "DONE" {
		t.Fatalf("second reply = %q, want DONE", payloads[1][:4])
	}
}

func TestFSLSSendReturnsFAILUnsupported(t *testing.T) {
	channel := newTestFSLSChannel(t, &fakeFileSession{})
	defer channel.Close()

	responses, err := channel.OnData(nestedCreateFrame(10, []byte("SEND")))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	payloads := nestedPayloads(t, responses, 10)
	if len(payloads) == 0 || string(payloads[0][:4]) != "FAIL" {
		t.Fatalf("payloads = %#v, want first FAIL", payloads)
	}
	messageLen := binary.LittleEndian.Uint32(payloads[0][4:8])
	message := string(payloads[0][8 : 8+messageLen])
	if !strings.Contains(message, "SEND") || !strings.Contains(message, "not supported") {
		t.Fatalf("FAIL message = %q, want SEND not supported", message)
	}
}

func TestFSLSInvalidPayloadReturnsFAIL(t *testing.T) {
	channel := newTestFSLSChannel(t, &fakeFileSession{})
	defer channel.Close()

	responses, err := channel.OnData(nestedCreateFrame(11, []byte("BAD")))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	payloads := nestedPayloads(t, responses, 11)
	if len(payloads) == 0 || string(payloads[0][:4]) != "FAIL" {
		t.Fatalf("payloads = %#v, want first FAIL", payloads)
	}
}

func dialWebsocket(t *testing.T, handler http.Handler, rawQuery string) *websocket.Conn {
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

func writeFrame(t *testing.T, conn *websocket.Conn, frame multiplex.Frame) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(frame)); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) multiplex.Frame {
	t.Helper()
	messageType, data, err := readMessage(t, conn)
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.BinaryMessage)
	}
	frame, err := multiplex.Decode(data)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	return frame
}

func newTestFSLSChannel(t *testing.T, session *fakeFileSession) multiplex.Channel {
	t.Helper()
	provider := &fakeFileProvider{session: session}
	factory := NewFSLSFactory(WithEnabled(true), WithProvider(provider))
	channel, _, err := factory(fslsInitPayload("device-1"))
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	return channel
}

func fslsInitPayload(serial string) []byte {
	payload := make([]byte, 4+len(serial))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(serial)))
	copy(payload[4:], serial)
	return payload
}

func requestPayload(command string, path string) []byte {
	payload := make([]byte, 8+len(path))
	copy(payload[0:4], command)
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(path)))
	copy(payload[8:], path)
	return payload
}

func nestedCreateFrame(channelID uint32, payload []byte) []byte {
	return multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: channelID, Payload: payload})
}

func nestedPayloads(t *testing.T, responses [][]byte, channelID uint32) [][]byte {
	t.Helper()
	var payloads [][]byte
	for _, response := range responses {
		frame, err := multiplex.Decode(response)
		if err != nil {
			t.Fatalf("Decode nested frame returned error: %v", err)
		}
		if frame.ChannelID != channelID {
			t.Fatalf("nested channel id = %d, want %d", frame.ChannelID, channelID)
		}
		if frame.Type == contract.MessageTypeRawBinaryData {
			payloads = append(payloads, frame.Payload)
		}
	}
	return payloads
}

type fakeFileProvider struct {
	udid    string
	session *fakeFileSession
}

func (p *fakeFileProvider) OpenFileListing(ctx context.Context, udid string) (Session, error) {
	p.udid = udid
	if p.session == nil {
		p.session = &fakeFileSession{}
	}
	return p.session, nil
}

type fakeFileSession struct {
	stat   Entry
	list   []Entry
	recv   string
	closed bool
}

func (s *fakeFileSession) Stat(ctx context.Context, path string) (Entry, error) { return s.stat, nil }
func (s *fakeFileSession) List(ctx context.Context, path string) ([]Entry, error) {
	return append([]Entry(nil), s.list...), nil
}
func (s *fakeFileSession) Recv(ctx context.Context, path string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBufferString(s.recv)), nil
}
func (s *fakeFileSession) Close() error { s.closed = true; return nil }
