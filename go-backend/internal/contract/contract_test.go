package contract

import "testing"

func TestActionsMatchTypeScriptContract(t *testing.T) {
	tests := map[string]string{
		"LIST_HOSTS":       ActionListHosts,
		"GOOG_DEVICE_LIST": ActionGoogDeviceList,
		"MULTIPLEX":        ActionMultiplex,
		"SHELL":            ActionShell,
		"PROXY_WS":         ActionProxyWS,
		"PROXY_ADB":        ActionProxyADB,
		"STREAM_SCRCPY":    ActionStream,
		"FILE_LISTING":     ActionFileListing,
	}
	want := map[string]string{
		"LIST_HOSTS":       "list-hosts",
		"GOOG_DEVICE_LIST": "goog-device-list",
		"MULTIPLEX":        "multiplex",
		"SHELL":            "shell",
		"PROXY_WS":         "proxy-ws",
		"PROXY_ADB":        "proxy-adb",
		"STREAM_SCRCPY":    "stream",
		"FILE_LISTING":     "list-files",
	}
	for name, got := range tests {
		if got != want[name] {
			t.Fatalf("%s = %q, want %q", name, got, want[name])
		}
	}
}

func TestChannelCodesMatchTypeScriptContract(t *testing.T) {
	tests := map[string]string{
		"FSLS": ChannelFSLS,
		"HSTS": ChannelHSTS,
		"SHEL": ChannelSHEL,
		"GTRC": ChannelGTRC,
	}
	want := map[string]string{
		"FSLS": "FSLS",
		"HSTS": "HSTS",
		"SHEL": "SHEL",
		"GTRC": "GTRC",
	}
	for name, got := range tests {
		if got != want[name] {
			t.Fatalf("%s = %q, want %q", name, got, want[name])
		}
	}
}

func TestMessageTypesMatchTypeScriptContract(t *testing.T) {
	tests := map[string]byte{
		"CreateChannel": MessageTypeCreateChannel,
		"CloseChannel":  MessageTypeCloseChannel,
		"RawBinaryData": MessageTypeRawBinaryData,
		"RawStringData": MessageTypeRawStringData,
		"Data":          MessageTypeData,
	}
	want := map[string]byte{
		"CreateChannel": 4,
		"CloseChannel":  8,
		"RawBinaryData": 16,
		"RawStringData": 32,
		"Data":          64,
	}
	for name, got := range tests {
		if got != want[name] {
			t.Fatalf("%s = %d, want %d", name, got, want[name])
		}
	}
}

func TestCloseCodesMatchCurrentBackendContract(t *testing.T) {
	tests := map[string]int{
		"invalid URL":          CloseInvalidURL,
		"unsupported request":  CloseUnsupportedRequest,
		"invalid parameter":    CloseInvalidParameter,
		"service start failed": CloseServiceStartFailed,
		"upstream closed":      CloseProxyUpstreamClosed,
		"upstream error":       CloseProxyUpstreamError,
		"shell exit":           CloseShellExit,
	}
	want := map[string]int{
		"invalid URL":          4001,
		"unsupported request":  4002,
		"invalid parameter":    4003,
		"service start failed": 4005,
		"upstream closed":      4010,
		"upstream error":       4011,
		"shell exit":           4500,
	}
	for name, got := range tests {
		if got != want[name] {
			t.Fatalf("%s close code = %d, want %d", name, got, want[name])
		}
	}
}
