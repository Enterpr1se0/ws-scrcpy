package multiplex

import (
	"encoding/binary"
	"errors"
)

var ErrFrameTooShort = errors.New("multiplexer frame must be at least 5 bytes")

type Frame struct {
	Type      byte
	ChannelID uint32
	Payload   []byte
}

func Encode(frame Frame) []byte {
	out := make([]byte, 5+len(frame.Payload))
	out[0] = frame.Type
	binary.LittleEndian.PutUint32(out[1:5], frame.ChannelID)
	copy(out[5:], frame.Payload)
	return out
}

func Decode(data []byte) (Frame, error) {
	if len(data) < 5 {
		return Frame{}, ErrFrameTooShort
	}
	payload := make([]byte, len(data)-5)
	copy(payload, data[5:])
	return Frame{
		Type:      data[0],
		ChannelID: binary.LittleEndian.Uint32(data[1:5]),
		Payload:   payload,
	}, nil
}

func EncodeClosePayload(code uint16, reason string) []byte {
	reasonBytes := []byte(reason)
	if len(reasonBytes) == 0 {
		out := make([]byte, 2)
		binary.LittleEndian.PutUint16(out[0:2], code)
		return out
	}
	out := make([]byte, 6+len(reasonBytes))
	binary.LittleEndian.PutUint16(out[0:2], code)
	binary.LittleEndian.PutUint32(out[2:6], uint32(len(reasonBytes)))
	copy(out[6:], reasonBytes)
	return out
}

func DecodeClosePayload(data []byte) (uint16, string, bool) {
	if len(data) < 2 {
		return 0, "", false
	}
	code := binary.LittleEndian.Uint16(data[0:2])
	if len(data) == 2 {
		return code, "", true
	}
	if len(data) < 6 {
		return 0, "", false
	}
	length := binary.LittleEndian.Uint32(data[2:6])
	end := 6 + int(length)
	if end > len(data) {
		return 0, "", false
	}
	return code, string(data[6:end]), true
}
