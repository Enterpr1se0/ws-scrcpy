package hosttracker

import (
	"encoding/json"
	"testing"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
)

func TestInitialMessageIncludesLocalAndRemoteHosts(t *testing.T) {
	tracker := New([]string{"android"}, []config.HostItem{{Type: "android", Secure: true, Hostname: "second.example.com", Port: 8443, Pathname: "/scrcpy", UseProxy: true}})
	message := tracker.InitialMessage()
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	want := `{"id":-1,"type":"hosts","data":{"local":[{"type":"android"}],"remote":[{"type":"android","secure":true,"hostname":"second.example.com","port":8443,"pathname":"/scrcpy","useProxy":true}]}}`
	if string(data) != want {
		t.Fatalf("message JSON = %s, want %s", data, want)
	}
}

func TestUnsupportedMessageShape(t *testing.T) {
	tracker := New(nil, nil)
	message := tracker.UnsupportedMessage("hello")
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	want := `{"id":-1,"type":"error","data":"Unsupported message: \"hello\""}`
	if string(data) != want {
		t.Fatalf("message JSON = %s, want %s", data, want)
	}
}

func TestInitialMessageWithNilRemoteSerializesEmptyArrays(t *testing.T) {
	tracker := New(nil, nil)
	message := tracker.InitialMessage()
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	want := `{"id":-1,"type":"hosts","data":{"local":[],"remote":[]}}`
	if string(data) != want {
		t.Fatalf("message JSON = %s, want %s", data, want)
	}
}
