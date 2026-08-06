package devicetracker

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/gorilla/websocket"
)

const (
	CommandKillServer       = "kill_server"
	CommandStartServer      = "start_server"
	CommandUpdateInterfaces = "update_interfaces"
)

type Provider interface {
	ListDevices(ctx context.Context) ([]Device, error)
	WatchDevices(ctx context.Context) (<-chan Event, error)
	RunCommand(ctx context.Context, command Command) error
}

type Device struct {
	UDID                string         `json:"udid"`
	State               string         `json:"state"`
	PID                 int            `json:"pid"`
	Interfaces          []NetInterface `json:"interfaces"`
	LastUpdateTimestamp int64          `json:"last.update.timestamp"`

	// Frontend GoogDeviceDescriptor property fields.
	BuildVersionRelease string `json:"ro.build.version.release"`
	BuildVersionSDK     string `json:"ro.build.version.sdk"`
	ProductCPUABI       string `json:"ro.product.cpu.abi"`
	ProductManufacturer string `json:"ro.product.manufacturer"`
	ProductModel        string `json:"ro.product.model"`
	WifiInterface       string `json:"wifi.interface"`
}

type NetInterface struct {
	Name string `json:"name"`
	IPv4 string `json:"ipv4"`
}

type Event struct {
	Device Device
}

type Command struct {
	ID   int             `json:"id"`
	Type string          `json:"type"`
	UDID string          `json:"-"`
	PID  int             `json:"-"`
	Data json.RawMessage `json:"data"`
}

type Handler struct {
	provider Provider
	tracker  trackerIdentity
}

type trackerIdentity struct {
	Name string
	ID   string
}

type wrapperMessage struct {
	ID   int         `json:"id"`
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type deviceListData struct {
	Name string   `json:"name"`
	ID   string   `json:"id"`
	List []Device `json:"list"`
}

type deviceData struct {
	Name   string `json:"name"`
	ID     string `json:"id"`
	Device Device `json:"device"`
}

func trackerHostName() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "localhost"
	}
	return name
}

func NewHandler(provider Provider) *Handler {
	return &Handler{
		provider: provider,
		tracker: trackerIdentity{
			// Display name for the frontend host group (Linux/Windows hostname).
			Name: trackerHostName(),
			ID:   "go",
		},
	}
}

func (h *Handler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	devices, err := h.provider.ListDevices(ctx)
	if err != nil {
		applog.Errorf("device list failed: %v", err)
		writeClose(conn, contract.CloseServiceStartFailed, err.Error())
		return
	}
	applog.Infof("device list sent count=%d", len(devices))
	events, err := h.provider.WatchDevices(ctx)
	if err != nil {
		applog.Errorf("device watch failed: %v", err)
		writeClose(conn, contract.CloseServiceStartFailed, err.Error())
		return
	}

	var writeMu sync.Mutex
	if err := writeWrappedJSON(conn, &writeMu, h.deviceListMessage(devices)); err != nil {
		applog.Warnf("device list write failed: %v", err)
		return
	}

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				if err := writeWrappedJSON(conn, &writeMu, h.deviceMessage(event.Device)); err != nil {
					applog.Debugf("device event write failed udid=%s: %v", event.Device.UDID, err)
					cancel()
					_ = conn.Close()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-watchDone
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		command, err := ParseCommand(data)
		if err != nil {
			applog.Warnf("device command parse failed: %v", err)
			continue
		}
		if err := h.provider.RunCommand(ctx, command); err != nil {
			applog.Errorf("device command failed type=%s udid=%s pid=%d err=%v", command.Type, command.UDID, command.PID, err)
		}
	}
}

func (h *Handler) deviceListMessage(devices []Device) wrapperMessage {
	return wrapperMessage{ID: -1, Type: "devicelist", Data: deviceListData{Name: h.tracker.Name, ID: h.tracker.ID, List: normalizeDevices(devices)}}
}

func (h *Handler) deviceMessage(device Device) wrapperMessage {
	return wrapperMessage{ID: -1, Type: "device", Data: deviceData{Name: h.tracker.Name, ID: h.tracker.ID, Device: normalizeDevice(device)}}
}

func NewGTRCFactory(provider Provider) multiplex.ChannelFactory {
	return func(initialPayload []byte) (multiplex.Channel, [][]byte, error) {
		ctx, cancel := context.WithCancel(context.Background())
		channel := &gtrcChannel{provider: provider, ctx: ctx, cancel: cancel, tracker: trackerIdentity{Name: trackerHostName(), ID: "go"}, messages: make(chan []byte, 16), watchDone: make(chan struct{})}
		devices, err := provider.ListDevices(ctx)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		events, err := provider.WatchDevices(ctx)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		payload, err := json.Marshal(channel.deviceListMessage(devices))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		go channel.forwardEvents(events)
		return channel, [][]byte{payload}, nil
	}
}

type gtrcChannel struct {
	provider  Provider
	ctx       context.Context
	cancel    context.CancelFunc
	tracker   trackerIdentity
	messages  chan []byte
	watchDone chan struct{}
}

func (c *gtrcChannel) OnData(payload []byte) ([][]byte, error) {
	command, err := ParseCommand(payload)
	if err != nil {
		return nil, err
	}
	if err := c.provider.RunCommand(c.ctx, command); err != nil {
		return nil, err
	}
	return nil, nil
}

func (c *gtrcChannel) Messages() <-chan []byte {
	return c.messages
}

func (c *gtrcChannel) Close() error {
	c.cancel()
	<-c.watchDone
	return nil
}

func (c *gtrcChannel) forwardEvents(events <-chan Event) {
	defer close(c.watchDone)
	defer close(c.messages)
	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(c.deviceMessage(event.Device))
			if err != nil {
				applog.Warnf("device event marshal failed udid=%s: %v", event.Device.UDID, err)
				continue
			}
			select {
			case <-c.ctx.Done():
				return
			case c.messages <- payload:
			}
		}
	}
}

func (c *gtrcChannel) deviceListMessage(devices []Device) wrapperMessage {
	return wrapperMessage{ID: -1, Type: "devicelist", Data: deviceListData{Name: c.tracker.Name, ID: c.tracker.ID, List: normalizeDevices(devices)}}
}

func (c *gtrcChannel) deviceMessage(device Device) wrapperMessage {
	return wrapperMessage{ID: -1, Type: "device", Data: deviceData{Name: c.tracker.Name, ID: c.tracker.ID, Device: normalizeDevice(device)}}
}

func normalizeDevices(devices []Device) []Device {
	normalized := make([]Device, len(devices))
	for i, device := range devices {
		normalized[i] = normalizeDevice(device)
	}
	return normalized
}

func normalizeDevice(device Device) Device {
	if device.Interfaces == nil {
		device.Interfaces = []NetInterface{}
	}
	return device
}

func ParseCommand(data []byte) (Command, error) {
	var raw struct {
		ID   int             `json:"id"`
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Command{}, err
	}
	command := Command{ID: raw.ID, Type: raw.Type, Data: raw.Data}
	if len(raw.Data) > 0 {
		var body struct {
			UDID string `json:"udid"`
			PID  int    `json:"pid"`
		}
		if err := json.Unmarshal(raw.Data, &body); err != nil {
			return Command{}, err
		}
		command.UDID = body.UDID
		command.PID = body.PID
	}
	return command, nil
}

func writeWrappedJSON(conn *websocket.Conn, writeMu *sync.Mutex, message wrapperMessage) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	return conn.WriteJSON(message)
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
