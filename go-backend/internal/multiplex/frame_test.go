package multiplex

import (
	"bytes"
	"testing"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
)

func TestDecodeRejectsShortFrame(t *testing.T) {
	_, err := Decode([]byte{contract.MessageTypeData, 1, 2, 3})
	if err == nil {
		t.Fatal("Decode returned nil error for a 4-byte frame")
	}
}

func TestEncodeDecodeRoundTripUsesLittleEndianChannelID(t *testing.T) {
	payload := []byte("HSTS")
	encoded := Encode(Frame{Type: contract.MessageTypeCreateChannel, ChannelID: 0x01020304, Payload: payload})
	wantHeader := []byte{contract.MessageTypeCreateChannel, 0x04, 0x03, 0x02, 0x01}
	if !bytes.Equal(encoded[:5], wantHeader) {
		t.Fatalf("header = %v, want %v", encoded[:5], wantHeader)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if decoded.Type != contract.MessageTypeCreateChannel {
		t.Fatalf("Type = %d, want %d", decoded.Type, contract.MessageTypeCreateChannel)
	}
	if decoded.ChannelID != 0x01020304 {
		t.Fatalf("ChannelID = %#x, want %#x", decoded.ChannelID, uint32(0x01020304))
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("Payload = %q, want %q", decoded.Payload, payload)
	}
}

func TestDecodeAcceptsEmptyPayload(t *testing.T) {
	decoded, err := Decode([]byte{contract.MessageTypeData, 7, 0, 0, 0})
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if decoded.ChannelID != 7 {
		t.Fatalf("ChannelID = %d, want 7", decoded.ChannelID)
	}
	if len(decoded.Payload) != 0 {
		t.Fatalf("Payload length = %d, want 0", len(decoded.Payload))
	}
}

func TestClosePayloadRoundTripWithReason(t *testing.T) {
	payload := EncodeClosePayload(4002, "[WebsocketMultiplexer] Unsupported request")
	code, reason, ok := DecodeClosePayload(payload)
	if !ok {
		t.Fatal("DecodeClosePayload returned ok=false")
	}
	if code != 4002 {
		t.Fatalf("code = %d, want 4002", code)
	}
	if reason != "[WebsocketMultiplexer] Unsupported request" {
		t.Fatalf("reason = %q", reason)
	}
}

func TestClosePayloadWithoutReason(t *testing.T) {
	payload := EncodeClosePayload(1000, "")
	code, reason, ok := DecodeClosePayload(payload)
	if !ok {
		t.Fatal("DecodeClosePayload returned ok=false")
	}
	if code != 1000 || reason != "" {
		t.Fatalf("got code=%d reason=%q, want code=1000 reason empty", code, reason)
	}
}

func TestDecodeClosePayloadRejectsDeclaredReasonLengthLongerThanData(t *testing.T) {
	payload := []byte{0xa2, 0x0f, 0x05, 0x00, 0x00, 0x00, 'h', 'e'}

	code, reason, ok := DecodeClosePayload(payload)

	if ok {
		t.Fatalf("DecodeClosePayload = code=%d reason=%q ok=true, want ok=false", code, reason)
	}
}

func TestDecodeClosePayloadRejectsTruncatedReasonLengthHeader(t *testing.T) {
	for length := 3; length <= 5; length++ {
		payload := make([]byte, length)
		payload[0] = 0xa2
		payload[1] = 0x0f
		code, reason, ok := DecodeClosePayload(payload)
		if ok {
			t.Fatalf("DecodeClosePayload(%d bytes) = code=%d reason=%q ok=true, want ok=false", length, code, reason)
		}
	}
}
