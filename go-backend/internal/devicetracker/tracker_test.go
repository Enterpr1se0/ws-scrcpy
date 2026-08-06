package devicetracker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/gorilla/websocket"
)

func TestDirectWebSocketSendsInitialDeviceListSchema(t *testing.T) {
	provider := newFakeProvider()
	provider.devices = []Device{{UDID: "abc123", State: "device", PID: 1234, Interfaces: []NetInterface{{Name: "wlan0", IPv4: "192.168.1.10"}}, LastUpdateTimestamp: 1712345678}}
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()

	message := readJSONMessage(t, conn)
	if message.ID != -1 {
		t.Fatalf("message id = %d, want -1", message.ID)
	}
	if message.Type != "devicelist" {
		t.Fatalf("message type = %q, want devicelist", message.Type)
	}
	var data struct {
		Name string   `json:"name"`
		ID   string   `json:"id"`
		List []Device `json:"list"`
	}
	unmarshalData(t, message.Data, &data)
	if len(data.List) != 1 {
		t.Fatalf("list length = %d, want 1", len(data.List))
	}
	device := data.List[0]
	if device.UDID != "abc123" || device.State != "device" || device.PID != 1234 {
		t.Fatalf("device = %+v, want udid/state/pid populated", device)
	}
	if len(device.Interfaces) != 1 || device.Interfaces[0].Name != "wlan0" || device.Interfaces[0].IPv4 != "192.168.1.10" {
		t.Fatalf("interfaces = %+v, want wlan0 IPv4", device.Interfaces)
	}
	if device.LastUpdateTimestamp != 1712345678 {
		t.Fatalf("last update timestamp = %d, want 1712345678", device.LastUpdateTimestamp)
	}
}

func TestDirectWebSocketInitialDeviceListOmitsNetInterfaceURL(t *testing.T) {
	provider := newFakeProvider()
	provider.devices = []Device{{UDID: "abc123", State: "device", PID: 1234, Interfaces: []NetInterface{{Name: "wlan0", IPv4: "192.168.1.10"}}, LastUpdateTimestamp: 1712345678}}
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()

	message := readJSONMessage(t, conn)
	var data struct {
		List []struct {
			Interfaces []json.RawMessage `json:"interfaces"`
		} `json:"list"`
	}
	unmarshalData(t, message.Data, &data)
	if len(data.List) != 1 || len(data.List[0].Interfaces) != 1 {
		t.Fatalf("data = %+v, want one device with one interface", data)
	}
	assertInterfaceContractJSON(t, data.List[0].Interfaces[0])
}

func TestDirectWebSocketInitialDeviceListSerializesNilInterfacesAsEmptyArrayAndZeroTimestamp(t *testing.T) {
	provider := newFakeProvider()
	provider.devices = []Device{{UDID: "nil-iface", State: "device", PID: 12}}
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()

	message := readJSONMessage(t, conn)
	var data struct {
		List []json.RawMessage `json:"list"`
	}
	unmarshalData(t, message.Data, &data)
	if len(data.List) != 1 {
		t.Fatalf("list length = %d, want 1", len(data.List))
	}
	assertDeviceContractJSON(t, data.List[0])
}

func TestDirectWebSocketDeviceEventSerializesNilInterfacesAsEmptyArrayAndZeroTimestamp(t *testing.T) {
	provider := newFakeProvider()
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()
	_ = readJSONMessage(t, conn)
	provider.waitWatching(t)

	provider.events <- Event{Device: Device{UDID: "nil-event", State: "offline", PID: 34}}

	message := readJSONMessage(t, conn)
	var data struct {
		Device json.RawMessage `json:"device"`
	}
	unmarshalData(t, message.Data, &data)
	assertDeviceContractJSON(t, data.Device)
}

func TestDirectWebSocketStreamsDeviceUpdateEvents(t *testing.T) {
	provider := newFakeProvider()
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()
	_ = readJSONMessage(t, conn)
	provider.waitWatching(t)

	provider.events <- Event{Device: Device{UDID: "updated", State: "offline", PID: 88, Interfaces: []NetInterface{{Name: "rmnet0", IPv4: "10.0.0.2"}}, LastUpdateTimestamp: 99}}

	message := readJSONMessage(t, conn)
	if message.ID != -1 || message.Type != "device" {
		t.Fatalf("message = %+v, want id -1 type device", message)
	}
	var data struct {
		Name   string `json:"name"`
		ID     string `json:"id"`
		Device Device `json:"device"`
	}
	unmarshalData(t, message.Data, &data)
	if data.Device.UDID != "updated" || data.Device.State != "offline" || data.Device.PID != 88 {
		t.Fatalf("device = %+v, want streamed update", data.Device)
	}
}

func TestDirectWebSocketDispatchesControlCenterCommands(t *testing.T) {
	provider := newFakeProvider()
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()
	_ = readJSONMessage(t, conn)

	commands := []string{
		`{"id":1,"type":"kill_server","data":{"udid":"abc123","pid":4321}}`,
		`{"id":2,"type":"start_server","data":{"udid":"abc123"}}`,
		`{"id":3,"type":"update_interfaces","data":{"udid":"abc123"}}`,
	}
	for _, command := range commands {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
			t.Fatalf("WriteMessage returned error: %v", err)
		}
	}

	got := provider.waitCommands(t, 3)
	wantTypes := []string{"kill_server", "start_server", "update_interfaces"}
	for i, want := range wantTypes {
		if got[i].Type != want {
			t.Fatalf("command %d type = %q, want %q", i, got[i].Type, want)
		}
		if got[i].UDID != "abc123" {
			t.Fatalf("command %d udid = %q, want abc123", i, got[i].UDID)
		}
	}
	if got[0].PID != 4321 {
		t.Fatalf("kill_server pid = %d, want 4321", got[0].PID)
	}
}

func TestDirectWebSocketListDevicesErrorClosesWith4005(t *testing.T) {
	provider := newFakeProvider()
	provider.listErr = errors.New("adb unavailable")
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestDirectWebSocketListDevicesLongErrorClosesWith4005(t *testing.T) {
	provider := newFakeProvider()
	provider.listErr = errors.New(strings.Repeat("adb unavailable ", 32))
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	defer conn.Close()

	assertCloseCode(t, conn, contract.CloseServiceStartFailed)
}

func TestMultiplexGTRCCreateReturnsInitialDeviceListDataPayload(t *testing.T) {
	provider := newFakeProvider()
	provider.devices = []Device{{UDID: "mux-device", State: "device", PID: 7}}
	factory := NewGTRCFactory(provider)

	channel, payloads, err := factory(nil)
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	defer channel.Close()
	if len(payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(payloads))
	}
	message := decodeWrappedMessage(t, payloads[0])
	if message.ID != -1 || message.Type != "devicelist" {
		t.Fatalf("message = %+v, want initial devicelist", message)
	}
	var data struct {
		List []json.RawMessage `json:"list"`
	}
	unmarshalData(t, message.Data, &data)
	if len(data.List) != 1 {
		t.Fatalf("list length = %d, want 1", len(data.List))
	}
	var device Device
	if err := json.Unmarshal(data.List[0], &device); err != nil {
		t.Fatalf("Unmarshal device %s returned error: %v", data.List[0], err)
	}
	if device.UDID != "mux-device" || device.State != "device" || device.PID != 7 {
		t.Fatalf("device = %+v, want mux-device state device pid 7", device)
	}
	assertDeviceContractJSON(t, data.List[0])
}

func TestMultiplexGTRCDataDispatchesCommand(t *testing.T) {
	provider := newFakeProvider()
	channel, _, err := NewGTRCFactory(provider)(nil)
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	defer channel.Close()

	responses, err := channel.OnData([]byte(`{"id":9,"type":"start_server","data":{"udid":"mux"}}`))
	if err != nil {
		t.Fatalf("OnData returned error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %d, want 0", len(responses))
	}
	got := provider.waitCommands(t, 1)
	if got[0].Type != "start_server" || got[0].UDID != "mux" {
		t.Fatalf("command = %+v, want start_server for mux", got[0])
	}
}

func TestDirectWebSocketCloseCancelsDeviceWatch(t *testing.T) {
	provider := newFakeProvider()
	conn := dialDeviceTracker(t, NewHandler(provider), context.Background())
	_ = readJSONMessage(t, conn)
	provider.waitWatching(t)

	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	provider.waitWatchCanceled(t)
}

func TestDirectWebSocketParentContextCancelClosesConnection(t *testing.T) {
	provider := newFakeProvider()
	ctx, cancel := context.WithCancel(context.Background())
	conn, handlerDone := dialDeviceTrackerWithDone(t, NewHandler(provider), ctx)
	defer conn.Close()
	_ = readJSONMessage(t, conn)
	provider.waitWatching(t)

	cancel()
	provider.waitWatchCanceled(t)

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		_ = conn.Close()
		t.Fatal("ServeWS did not exit after parent context cancellation")
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("ReadMessage returned nil error, want closed websocket after parent context cancellation")
	}
}

func TestGTRCFactoryIsRegisteredUnderContractChannelCode(t *testing.T) {
	provider := newFakeProvider()
	registry := multiplex.NewRegistry()
	registry.Register(contract.ChannelGTRC, NewGTRCFactory(provider))
	conn := dialMultiplex(t, multiplex.NewHandler(registry))
	defer conn.Close()

	writeFrame(t, conn, multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 5, Payload: []byte(contract.ChannelGTRC)})

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData || frame.ChannelID != 5 {
		t.Fatalf("frame = %+v, want raw string data for channel 5", frame)
	}
	message := decodeWrappedMessage(t, frame.Payload)
	if message.Type != "devicelist" {
		t.Fatalf("message type = %q, want devicelist", message.Type)
	}
}

func TestMultiplexGTRCStreamsWatchDevicesEventsAsDataFrames(t *testing.T) {
	provider := newFakeProvider()
	registry := multiplex.NewRegistry()
	registry.Register(contract.ChannelGTRC, NewGTRCFactory(provider))
	conn := dialMultiplex(t, multiplex.NewHandler(registry))
	defer conn.Close()

	writeFrame(t, conn, multiplex.Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 6, Payload: []byte(contract.ChannelGTRC)})
	_ = readFrame(t, conn)
	provider.waitWatching(t)

	provider.events <- Event{Device: Device{UDID: "mux-updated", State: "device", PID: 99, Interfaces: []NetInterface{{Name: "wlan0", IPv4: "192.168.1.20"}}}}

	frame := readFrame(t, conn)
	if frame.Type != contract.MessageTypeRawStringData || frame.ChannelID != 6 {
		t.Fatalf("frame = %+v, want raw string data for channel 6", frame)
	}
	message := decodeWrappedMessage(t, frame.Payload)
	if message.ID != -1 || message.Type != "device" {
		t.Fatalf("message = %+v, want device event", message)
	}
	var data struct {
		Device Device `json:"device"`
	}
	unmarshalData(t, message.Data, &data)
	if data.Device.UDID != "mux-updated" || data.Device.PID != 99 {
		t.Fatalf("device = %+v, want streamed mux-updated event", data.Device)
	}
}

type wrappedMessage struct {
	ID   int             `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type fakeProvider struct {
	devices []Device
	listErr error
	events  chan Event

	mu          sync.Mutex
	commands    []Command
	commandCond *sync.Cond

	watchStarted chan struct{}
	watchDone    chan struct{}
	watchOnce    sync.Once
	doneOnce     sync.Once
}

func newFakeProvider() *fakeProvider {
	p := &fakeProvider{
		events:       make(chan Event, 8),
		watchStarted: make(chan struct{}),
		watchDone:    make(chan struct{}),
	}
	p.commandCond = sync.NewCond(&p.mu)
	return p
}

func (p *fakeProvider) ListDevices(ctx context.Context) ([]Device, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	devices := make([]Device, len(p.devices))
	copy(devices, p.devices)
	return devices, nil
}

func (p *fakeProvider) WatchDevices(ctx context.Context) (<-chan Event, error) {
	p.watchOnce.Do(func() { close(p.watchStarted) })
	go func() {
		<-ctx.Done()
		p.doneOnce.Do(func() { close(p.watchDone) })
	}()
	return p.events, nil
}

func (p *fakeProvider) RunCommand(ctx context.Context, command Command) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands = append(p.commands, command)
	p.commandCond.Broadcast()
	return nil
}

func (p *fakeProvider) waitWatching(t *testing.T) {
	t.Helper()
	select {
	case <-p.watchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchDevices was not called")
	}
}

func (p *fakeProvider) waitWatchCanceled(t *testing.T) {
	t.Helper()
	select {
	case <-p.watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watch context was not canceled")
	}
}

func (p *fakeProvider) waitCommands(t *testing.T, count int) []Command {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	timer := time.AfterFunc(time.Until(deadline), func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.commandCond.Broadcast()
	})
	defer timer.Stop()

	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.commands) < count {
		p.commandCond.Wait()
		if time.Now().After(deadline) && len(p.commands) < count {
			t.Fatalf("commands = %d, want %d", len(p.commands), count)
		}
	}
	commands := make([]Command, len(p.commands))
	copy(commands, p.commands)
	return commands
}

func dialDeviceTracker(t *testing.T, handler *Handler, ctx context.Context) *websocket.Conn {
	t.Helper()
	conn, _ := dialDeviceTrackerWithDone(t, handler, ctx)
	return conn
}

func dialDeviceTrackerWithDone(t *testing.T, handler *Handler, ctx context.Context) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer close(done)
		handler.ServeWS(ctx, conn, r)
	}))
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
	return conn, done
}

func assertCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("ReadMessage returned nil error, want websocket close")
	}
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("ReadMessage error = %T %v, want *websocket.CloseError", err, err)
	}
	if closeErr.Code != want {
		t.Fatalf("close code = %d, want %d", closeErr.Code, want)
	}
}

func readJSONMessage(t *testing.T, conn *websocket.Conn) wrappedMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage returned error: %v", err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.TextMessage)
	}
	return decodeWrappedMessage(t, data)
}

func decodeWrappedMessage(t *testing.T, data []byte) wrappedMessage {
	t.Helper()
	var message wrappedMessage
	if err := json.Unmarshal(data, &message); err != nil {
		t.Fatalf("Unmarshal message %s returned error: %v", data, err)
	}
	return message
}

func unmarshalData(t *testing.T, raw json.RawMessage, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("Unmarshal data %s returned error: %v", raw, err)
	}
}

func assertDeviceContractJSON(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("Unmarshal device %s returned error: %v", raw, err)
	}
	interfaces, ok := fields["interfaces"]
	if !ok {
		t.Fatalf("device JSON %s is missing interfaces field", raw)
	}
	if string(interfaces) != "[]" {
		t.Fatalf("interfaces JSON = %s, want []", interfaces)
	}
	timestamp, ok := fields["last.update.timestamp"]
	if !ok {
		t.Fatalf("device JSON %s is missing last.update.timestamp field", raw)
	}
	if string(timestamp) != "0" {
		t.Fatalf("last.update.timestamp JSON = %s, want 0", timestamp)
	}
	requiredProps := []string{
		"ro.build.version.release",
		"ro.build.version.sdk",
		"ro.product.cpu.abi",
		"ro.product.manufacturer",
		"ro.product.model",
		"wifi.interface",
	}
	for _, key := range requiredProps {
		value, ok := fields[key]
		if !ok {
			t.Fatalf("device JSON %s is missing %s field", raw, key)
		}
		if string(value) != `""` {
			t.Fatalf("%s JSON = %s, want empty string", key, value)
		}
	}
}

func assertInterfaceContractJSON(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("Unmarshal interface %s returned error: %v", raw, err)
	}
	if _, ok := fields["name"]; !ok {
		t.Fatalf("interface JSON %s is missing name field", raw)
	}
	if _, ok := fields["ipv4"]; !ok {
		t.Fatalf("interface JSON %s is missing ipv4 field", raw)
	}
	if _, ok := fields["url"]; ok {
		t.Fatalf("interface JSON %s includes unsupported url field", raw)
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

func writeFrame(t *testing.T, conn *websocket.Conn, frame multiplex.Frame) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, multiplex.Encode(frame)); err != nil {
		t.Fatalf("WriteMessage returned error: %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) multiplex.Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	messageType, data, err := conn.ReadMessage()
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
