package filelisting

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/gorilla/websocket"
)

const recvChunkSize = 64 * 1024

var ErrDisabled = errors.New("file listing is disabled")

type Entry struct {
	Name  string
	Mode  uint32
	Size  uint32
	MTime uint32
}

type Provider interface {
	OpenFileListing(ctx context.Context, udid string) (Session, error)
}

type Session interface {
	Stat(ctx context.Context, path string) (Entry, error)
	List(ctx context.Context, path string) ([]Entry, error)
	Recv(ctx context.Context, path string) (io.ReadCloser, error)
	Close() error
}

type Options struct {
	Enabled  bool
	Provider Provider
}

type Option func(*Options)

func WithEnabled(enabled bool) Option {
	return func(options *Options) { options.Enabled = enabled }
}

func WithProvider(provider Provider) Option {
	return func(options *Options) { options.Provider = provider }
}

type Handler struct {
	options  Options
	upgrader websocket.Upgrader
}

func NewHandler(options ...Option) *Handler {
	return &Handler{options: applyOptions(options), upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		applog.Warnf("filelisting upgrade failed remote=%s err=%v", r.RemoteAddr, err)
		return
	}
	h.ServeWS(r.Context(), conn, r)
}

func (h *Handler) ServeWS(_ context.Context, conn *websocket.Conn, _ *http.Request) {
	defer conn.Close()
	if !h.options.Enabled {
		writeClose(conn, contract.CloseUnsupportedRequest, ErrDisabled.Error())
		return
	}
	writeClose(conn, contract.CloseUnsupportedRequest, "direct file listing websocket is not supported; use multiplex FSLS")
}

func NewFSLSFactory(options ...Option) multiplex.ChannelFactory {
	configured := applyOptions(options)
	return func(initialPayload []byte) (multiplex.Channel, [][]byte, error) {
		if !configured.Enabled {
			return nil, nil, ErrDisabled
		}
		if configured.Provider == nil {
			return nil, nil, errors.New("file listing provider is not configured")
		}
		serial, err := parseInitialPayload(initialPayload)
		if err != nil {
			return nil, nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		session, err := configured.Provider.OpenFileListing(ctx, serial)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		return &fslsChannel{ctx: ctx, cancel: cancel, session: session}, nil, nil
	}
}

func applyOptions(options []Option) Options {
	configured := Options{Enabled: false}
	for _, option := range options {
		option(&configured)
	}
	return configured
}

type fslsChannel struct {
	ctx     context.Context
	cancel  context.CancelFunc
	session Session
}

func (c *fslsChannel) ResponseMessageType() byte {
	return contract.MessageTypeData
}

func (c *fslsChannel) OnData(payload []byte) ([][]byte, error) {
	frame, err := multiplex.Decode(payload)
	if err != nil {
		return [][]byte{nestedDataFrame(0, failReply("invalid file listing request"))}, nil
	}
	switch frame.Type {
	case contract.MessageTypeCreateChannel:
		return c.handleRequest(frame.ChannelID, frame.Payload), nil
	case contract.MessageTypeCloseChannel:
		return nil, nil
	default:
		return [][]byte{nestedDataFrame(frame.ChannelID, failReply(fmt.Sprintf("unsupported file listing frame type %d", frame.Type)))}, nil
	}
}

func (c *fslsChannel) Close() error {
	c.cancel()
	if c.session == nil {
		return nil
	}
	return c.session.Close()
}

func (c *fslsChannel) handleRequest(channelID uint32, payload []byte) [][]byte {
	command, path, err := parseRequest(payload)
	if err != nil {
		return [][]byte{nestedDataFrame(channelID, failReply(err.Error()))}
	}
	applog.Debugf("filelisting request channel=%d command=%s path=%q", channelID, command, path)
	switch command {
	case "STAT":
		entry, err := c.session.Stat(c.ctx, path)
		if err != nil {
			applog.Warnf("filelisting STAT path=%q err=%v", path, err)
			return [][]byte{nestedDataFrame(channelID, failReply(err.Error()))}
		}
		applog.Debugf("filelisting STAT path=%q mode=%#o size=%d", path, entry.Mode, entry.Size)
		return [][]byte{nestedDataFrame(channelID, statReply(entry))}
	case "LIST":
		entries, err := c.session.List(c.ctx, path)
		if err != nil {
			applog.Warnf("filelisting LIST path=%q err=%v", path, err)
			return [][]byte{nestedDataFrame(channelID, failReply(err.Error()))}
		}
		applog.Infof("filelisting LIST path=%q entries=%d", path, len(entries))
		responses := make([][]byte, 0, len(entries)+1)
		for _, entry := range entries {
			responses = append(responses, nestedDataFrame(channelID, dentReply(entry)))
		}
		responses = append(responses, nestedDataFrame(channelID, []byte("DONE")))
		return responses
	case "RECV":
		reader, err := c.session.Recv(c.ctx, path)
		if err != nil {
			applog.Warnf("filelisting RECV path=%q err=%v", path, err)
			return [][]byte{nestedDataFrame(channelID, failReply(err.Error()))}
		}
		applog.Infof("filelisting RECV path=%q", path)
		defer reader.Close()
		return c.recvResponses(channelID, reader)
	case "SEND":
		applog.Warnf("filelisting SEND unsupported path=%q", path)
		return [][]byte{nestedDataFrame(channelID, failReply("SEND is not supported"))}
	default:
		return [][]byte{nestedDataFrame(channelID, failReply("unsupported file listing command "+command))}
	}
}

func (c *fslsChannel) recvResponses(channelID uint32, reader io.Reader) [][]byte {
	responses := make([][]byte, 0, 2)
	buffer := make([]byte, recvChunkSize)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			chunk := make([]byte, 4+n)
			copy(chunk[0:4], "DATA")
			copy(chunk[4:], buffer[:n])
			responses = append(responses, nestedDataFrame(channelID, chunk))
		}
		if err == io.EOF {
			responses = append(responses, nestedDataFrame(channelID, []byte("DONE")))
			return responses
		}
		if err != nil {
			responses = append(responses, nestedDataFrame(channelID, failReply(err.Error())))
			return responses
		}
	}
}

func parseInitialPayload(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", errors.New("FSLS payload must include serial length")
	}
	length := int(binary.LittleEndian.Uint32(payload[0:4]))
	if length <= 0 || 4+length > len(payload) {
		return "", errors.New("FSLS payload has invalid serial length")
	}
	serial := string(payload[4 : 4+length])
	if serial == "" {
		return "", errors.New("FSLS serial is empty")
	}
	return serial, nil
}

func parseRequest(payload []byte) (string, string, error) {
	if len(payload) < 4 {
		return "", "", errors.New("invalid file listing request")
	}
	command := string(payload[0:4])
	if command == "SEND" {
		return command, "", nil
	}
	if len(payload) < 8 {
		return "", "", errors.New("invalid file listing request")
	}
	length := int(binary.LittleEndian.Uint32(payload[4:8]))
	if length <= 0 || 8+length > len(payload) {
		return "", "", errors.New("invalid file listing path length")
	}
	return command, string(payload[8 : 8+length]), nil
}

func statReply(entry Entry) []byte {
	out := make([]byte, 16)
	copy(out[0:4], "STAT")
	binary.LittleEndian.PutUint32(out[4:8], entry.Mode)
	binary.LittleEndian.PutUint32(out[8:12], entry.Size)
	binary.LittleEndian.PutUint32(out[12:16], entry.MTime)
	return out
}

func dentReply(entry Entry) []byte {
	name := []byte(entry.Name)
	out := make([]byte, 20+len(name))
	copy(out[0:4], "DENT")
	binary.LittleEndian.PutUint32(out[4:8], entry.Mode)
	binary.LittleEndian.PutUint32(out[8:12], entry.Size)
	binary.LittleEndian.PutUint32(out[12:16], entry.MTime)
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(name)))
	copy(out[20:], name)
	return out
}

func failReply(message string) []byte {
	data := []byte(message)
	out := make([]byte, 8+len(data))
	copy(out[0:4], "FAIL")
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(data)))
	copy(out[8:], data)
	return out
}

func nestedDataFrame(channelID uint32, payload []byte) []byte {
	return multiplex.Encode(multiplex.Frame{Type: contract.MessageTypeRawBinaryData, ChannelID: channelID, Payload: payload})
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
