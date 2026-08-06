package multiplex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/hosttracker"
	"github.com/gorilla/websocket"
)

type Channel interface {
	OnData(payload []byte) ([][]byte, error)
	Close() error
}

type AsyncChannel interface {
	Messages() <-chan []byte
}

type ResponseTypeChannel interface {
	ResponseMessageType() byte
}

type ChannelFactory func(initialPayload []byte) (Channel, [][]byte, error)

type Registry struct {
	mu        sync.RWMutex
	factories map[string]ChannelFactory
}

func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]ChannelFactory)}
}

func (r *Registry) Register(code string, factory ChannelFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[code] = factory
}

func (r *Registry) factory(code string) ChannelFactory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.factories[code]
}

type Handler struct {
	registry *Registry
	upgrader websocket.Upgrader
}

func NewHandler(registry *Registry) *Handler {
	if registry == nil {
		registry = NewRegistry()
	}
	return &Handler{
		registry: registry,
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		applog.Warnf("multiplex upgrade failed remote=%s err=%v", r.RemoteAddr, err)
		return
	}
	h.ServeWS(r.Context(), conn, r)
}

func (h *Handler) ServeWS(ctx context.Context, conn *websocket.Conn, r *http.Request) {
	remote := ""
	if r != nil {
		remote = r.RemoteAddr
	}
	applog.Infof("multiplex session open remote=%s", remote)
	defer func() {
		applog.Infof("multiplex session close remote=%s", remote)
		_ = conn.Close()
	}()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	channels := make(map[uint32]Channel)
	defer closeChannels(channels)
	var writeMu sync.Mutex
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			applog.Debugf("multiplex read end remote=%s err=%v", remote, err)
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		frame, err := Decode(data)
		if err != nil {
			applog.Warnf("multiplex decode failed remote=%s err=%v", remote, err)
			return
		}
		if !h.handleFrame(conn, &writeMu, channels, frame) {
			applog.Debugf("multiplex frame handler stopped remote=%s type=%d channel=%d", remote, frame.Type, frame.ChannelID)
			return
		}
	}
}

func (h *Handler) handleFrame(conn *websocket.Conn, writeMu *sync.Mutex, channels map[uint32]Channel, frame Frame) bool {
	switch frame.Type {
	case contract.MessageTypeCreateChannel:
		return h.handleCreateChannel(conn, writeMu, channels, frame)
	case contract.MessageTypeRawStringData, contract.MessageTypeRawBinaryData, contract.MessageTypeData:
		channel := channels[frame.ChannelID]
		if channel == nil {
			return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, fmt.Sprintf("unknown channel %d", frame.ChannelID))
		}
		responses, err := channel.OnData(frame.Payload)
		if err != nil {
			applog.Warnf("multiplex channel OnData failed id=%d err=%v", frame.ChannelID, err)
			if closeErr := channel.Close(); closeErr != nil {
				applog.Debugf("multiplex channel close after OnData error id=%d err=%v", frame.ChannelID, closeErr)
			}
			delete(channels, frame.ChannelID)
			return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, err.Error())
		}
		responseType := contract.MessageTypeRawStringData
		if typed, ok := channel.(ResponseTypeChannel); ok {
			responseType = typed.ResponseMessageType()
		}
		for _, payload := range responses {
			if !writeDataFrameWithType(conn, writeMu, frame.ChannelID, responseType, payload) {
				return false
			}
		}
		return true
	case contract.MessageTypeCloseChannel:
		if channel := channels[frame.ChannelID]; channel != nil {
			applog.Debugf("multiplex channel closed id=%d", frame.ChannelID)
			_ = channel.Close()
			delete(channels, frame.ChannelID)
		}
		return true
	default:
		if channel := channels[frame.ChannelID]; channel != nil {
			_ = channel.Close()
			delete(channels, frame.ChannelID)
		}
		return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, fmt.Sprintf("unsupported message type %d", frame.Type))
	}
}

func (h *Handler) handleCreateChannel(conn *websocket.Conn, writeMu *sync.Mutex, channels map[uint32]Channel, frame Frame) bool {
	if existing := channels[frame.ChannelID]; existing != nil {
		_ = existing.Close()
		delete(channels, frame.ChannelID)
		return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, fmt.Sprintf("duplicate channel %d", frame.ChannelID))
	}
	if len(frame.Payload) < 4 {
		return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseInvalidParameter, "create channel payload must include 4-byte channel code")
	}
	code := string(frame.Payload[:4])
	factory := h.registry.factory(code)
	if factory == nil {
		applog.Warnf("multiplex unsupported channel code=%s id=%d", code, frame.ChannelID)
		return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, fmt.Sprintf("unsupported channel code %s", code))
	}
	channel, responses, err := factory(frame.Payload[4:])
	if err != nil {
		applog.Warnf("multiplex create channel failed id=%d code=%s err=%v", frame.ChannelID, code, err)
		return writeCloseFrame(conn, writeMu, frame.ChannelID, contract.CloseUnsupportedRequest, err.Error())
	}
	applog.Infof("multiplex channel created id=%d code=%s", frame.ChannelID, code)
	channels[frame.ChannelID] = channel
	for _, payload := range responses {
		if !writeDataFrame(conn, writeMu, frame.ChannelID, payload) {
			return false
		}
	}
	if async, ok := channel.(AsyncChannel); ok {
		go forwardAsyncMessages(conn, writeMu, frame.ChannelID, async.Messages())
	}
	return true
}

func forwardAsyncMessages(conn *websocket.Conn, writeMu *sync.Mutex, channelID uint32, messages <-chan []byte) {
	for payload := range messages {
		if !writeDataFrame(conn, writeMu, channelID, payload) {
			return
		}
	}
}

func writeDataFrame(conn *websocket.Conn, writeMu *sync.Mutex, channelID uint32, payload []byte) bool {
	return writeDataFrameWithType(conn, writeMu, channelID, contract.MessageTypeRawStringData, payload)
}

func writeDataFrameWithType(conn *websocket.Conn, writeMu *sync.Mutex, channelID uint32, messageType byte, payload []byte) bool {
	writeMu.Lock()
	defer writeMu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, Encode(Frame{Type: messageType, ChannelID: channelID, Payload: payload})) == nil
}

func writeCloseFrame(conn *websocket.Conn, writeMu *sync.Mutex, channelID uint32, code int, reason string) bool {
	payload := EncodeClosePayload(uint16(code), reason)
	writeMu.Lock()
	defer writeMu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, Encode(Frame{Type: contract.MessageTypeCloseChannel, ChannelID: channelID, Payload: payload})) == nil
}

func closeChannels(channels map[uint32]Channel) {
	for id, channel := range channels {
		_ = channel.Close()
		delete(channels, id)
	}
}

type hostTrackerChannel struct {
	tracker *hosttracker.Tracker
}

func NewHostTrackerFactory(tracker *hosttracker.Tracker) ChannelFactory {
	if tracker == nil {
		tracker = hosttracker.New(nil, nil)
	}
	return func(initialPayload []byte) (Channel, [][]byte, error) {
		payload, err := json.Marshal(tracker.InitialMessage())
		if err != nil {
			return nil, nil, err
		}
		return &hostTrackerChannel{tracker: tracker}, [][]byte{payload}, nil
	}
}

func (c *hostTrackerChannel) OnData(payload []byte) ([][]byte, error) {
	response, err := json.Marshal(c.tracker.UnsupportedMessage(string(payload)))
	if err != nil {
		return nil, err
	}
	return [][]byte{response}, nil
}

func (c *hostTrackerChannel) Close() error {
	return nil
}

func websocketDeadline() time.Time {
	return time.Now().Add(time.Second)
}
