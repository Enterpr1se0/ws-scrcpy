package acceptance

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/app"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/gorilla/websocket"
)

const realDeviceUDIDEnv = "WS_SCRCPY_REAL_DEVICE_UDID"
const realDeviceStartScrcpyEnv = "WS_SCRCPY_REAL_DEVICE_START_SCRCPY"

func TestRealDeviceSmoke(t *testing.T) {
	udid := os.Getenv(realDeviceUDIDEnv)
	if udid == "" {
		t.Skipf("set %s to run real Android device smoke acceptance", realDeviceUDIDEnv)
	}
	server := newRealDeviceServer(t)

	t.Run("device list contains configured device", func(t *testing.T) {
		conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, url.Values{})
		defer conn.Close()
		var message struct {
			Type string `json:"type"`
			Data struct {
				List []devicetracker.Device `json:"list"`
			} `json:"data"`
		}
		readJSON(t, conn, &message)
		if message.Type != "devicelist" {
			t.Fatalf("type = %q, want devicelist", message.Type)
		}
		device := findDevice(message.Data.List, udid)
		if device == nil {
			t.Fatalf("device list = %#v, want udid %q", message.Data.List, udid)
		}
		if device.State != "device" {
			t.Fatalf("device state = %q, want device", device.State)
		}
	})


	t.Run("shell echoes marker", func(t *testing.T) {
		conn := dialAction(t, server.URL, contract.ActionShell, url.Values{"udid": {udid}, "rows": {"24"}, "cols": {"80"}})
		defer conn.Close()
		marker := "ws-scrcpy-go-smoke"
		if err := conn.WriteMessage(websocket.TextMessage, []byte("echo "+marker+"\n")); err != nil {
			t.Fatalf("WriteMessage(shell echo) error = %v", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			setReadDeadline(t, conn)
			_, payload, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage(shell output) error = %v", err)
			}
			if strings.Contains(string(payload), marker) {
				return
			}
		}
		t.Fatalf("shell output did not contain marker %q", marker)
	})

	t.Run("scrcpy server start command starts process", func(t *testing.T) {
		if os.Getenv(realDeviceStartScrcpyEnv) != "1" {
			t.Skipf("set %s=1 with %s to run scrcpy start smoke", realDeviceStartScrcpyEnv, realDeviceUDIDEnv)
		}
		conn := dialAction(t, server.URL, contract.ActionGoogDeviceList, url.Values{})
		defer conn.Close()
		var initial json.RawMessage
		readJSON(t, conn, &initial)
		command := map[string]any{
			"id":   1,
			"type": devicetracker.CommandStartServer,
			"data": map[string]any{"udid": udid},
		}
		if err := conn.WriteJSON(command); err != nil {
			t.Fatalf("WriteJSON(start_server) error = %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if pid := currentScrcpyPID(t, server.URL, udid); pid > 0 {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatal("scrcpy PID did not appear after start_server command")
	})
}

func newRealDeviceServer(t *testing.T) *httptest.Server {
	t.Helper()
	handler := app.NewHandler(config.Config{Pathname: "/"}, fstest.MapFS{})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func currentScrcpyPID(t *testing.T, serverURL string, udid string) int {
	t.Helper()
	conn := dialAction(t, serverURL, contract.ActionGoogDeviceList, url.Values{})
	defer conn.Close()
	var message struct {
		Data struct {
			List []devicetracker.Device `json:"list"`
		} `json:"data"`
	}
	readJSON(t, conn, &message)
	device := findDevice(message.Data.List, udid)
	if device == nil {
		return -1
	}
	return device.PID
}

func findDevice(devices []devicetracker.Device, udid string) *devicetracker.Device {
	for i := range devices {
		if devices[i].UDID == udid {
			return &devices[i]
		}
	}
	return nil
}
