package adbcli_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbcli"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/shell"
)

type runnerCall struct {
	name string
	args []string
}

type fakeRunner struct {
	mu    sync.Mutex
	cmds  []*fakeCmd
	calls []runnerCall
	ctxs  []context.Context
}

func (r *fakeRunner) Command(ctx context.Context, name string, args ...string) adbcli.Cmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	callArgs := append([]string(nil), args...)
	r.calls = append(r.calls, runnerCall{name: name, args: callArgs})
	r.ctxs = append(r.ctxs, ctx)
	if len(r.cmds) == 0 {
		return &fakeCmd{}
	}
	cmd := r.cmds[0]
	r.cmds = r.cmds[1:]
	return cmd
}

func (r *fakeRunner) snapshotCalls() []runnerCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := make([]runnerCall, len(r.calls))
	copy(calls, r.calls)
	return calls
}

func (r *fakeRunner) snapshotContexts() []context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	ctxs := make([]context.Context, len(r.ctxs))
	copy(ctxs, r.ctxs)
	return ctxs
}

type fakeCmd struct {
	output         []byte
	combinedOutput []byte
	err            error
	startErr       error
	waitErr        error
	stdin          *recordingWriteCloser
	stdout         io.ReadCloser
	stderr         io.ReadCloser
	started        bool
	killed         bool
}

func (c *fakeCmd) Output() ([]byte, error) { return c.output, c.err }
func (c *fakeCmd) CombinedOutput() ([]byte, error) {
	if c.combinedOutput != nil {
		return c.combinedOutput, c.err
	}
	return c.output, c.err
}
func (c *fakeCmd) Start() error { c.started = true; return c.startErr }
func (c *fakeCmd) Wait() error  { return c.waitErr }
func (c *fakeCmd) StdinPipe() (io.WriteCloser, error) {
	if c.stdin == nil {
		c.stdin = &recordingWriteCloser{}
	}
	return c.stdin, nil
}
func (c *fakeCmd) StdoutPipe() (io.ReadCloser, error) {
	if c.stdout == nil {
		c.stdout = io.NopCloser(strings.NewReader(""))
	}
	return c.stdout, nil
}
func (c *fakeCmd) StderrPipe() (io.ReadCloser, error) {
	if c.stderr == nil {
		c.stderr = io.NopCloser(strings.NewReader(""))
	}
	return c.stderr, nil
}
func (c *fakeCmd) Kill() error { c.killed = true; return nil }

func scrcpyCmdline() []byte {
	return []byte("app_process\x00/\x00com.genymobile.scrcpy.Server\x001.19-ws7\x00web\x00")
}

type recordingWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (w *recordingWriteCloser) Close() error { w.closed = true; return nil }

type closeAwareInfiniteReader struct {
	permits chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newCloseAwareInfiniteReader() *closeAwareInfiniteReader {
	return &closeAwareInfiniteReader{permits: make(chan struct{}, 64), closed: make(chan struct{})}
}

func (r *closeAwareInfiniteReader) allowReads(n int) {
	for i := 0; i < n; i++ {
		r.permits <- struct{}{}
	}
}

func (r *closeAwareInfiniteReader) Read(p []byte) (int, error) {
	select {
	case <-r.closed:
		return 0, io.EOF
	case <-r.permits:
	}
	p[0] = 'x'
	return 1, nil
}

func (r *closeAwareInfiniteReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

type trackingReadCloser struct {
	reader io.Reader
	closed chan struct{}
	once   sync.Once
	reads  int
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	r.reads++
	return r.reader.Read(p)
}

func (r *trackingReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func TestListDevicesParsesADBDevicesAndUsesConfiguredCommand(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\n\nemulator-5554\tdevice\nabc\toffline\nmalformed\n")},
		{output: []byte("ro.product.cpu.abi:x86_64\nro.product.manufacturer:Google\nro.product.model:sdk_gphone64_x86_64\nro.build.version.release:14\nro.build.version.sdk:34\nwifi.interface:wlan0\n")},
		{output: []byte("2: wlan0: <BROADCAST,MULTICAST,UP> mtu 1500\n    inet 192.168.1.10/24 brd 192.168.1.255 scope global wlan0\n")},
		{output: []byte("4321\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithADBPath("adb-custom"), adbcli.WithHostPort("127.0.0.1", 5037))

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices returned error: %v", err)
	}

	wantCalls := []runnerCall{
		{name: "adb-custom", args: []string{"-H", "127.0.0.1", "-P", "5037", "devices"}},
		{name: "adb-custom", args: []string{"-H", "127.0.0.1", "-P", "5037", "-s", "emulator-5554", "shell", "echo ro.product.cpu.abi:$(getprop ro.product.cpu.abi);echo ro.product.manufacturer:$(getprop ro.product.manufacturer);echo ro.product.model:$(getprop ro.product.model);echo ro.build.version.release:$(getprop ro.build.version.release);echo ro.build.version.sdk:$(getprop ro.build.version.sdk);echo wifi.interface:$(getprop wifi.interface)"}},
		{name: "adb-custom", args: []string{"-H", "127.0.0.1", "-P", "5037", "-s", "emulator-5554", "shell", "ip", "-f", "inet", "addr", "show"}},
		{name: "adb-custom", args: []string{"-H", "127.0.0.1", "-P", "5037", "-s", "emulator-5554", "shell", "pidof", "app_process"}},
		{name: "adb-custom", args: []string{"-H", "127.0.0.1", "-P", "5037", "-s", "emulator-5554", "shell", "cat", "/proc/4321/cmdline"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
	if len(devices) != 2 {
		t.Fatalf("len(devices) = %d, want 2", len(devices))
	}
	if devices[0].UDID != "emulator-5554" || devices[0].State != "device" || devices[0].PID != 4321 {
		t.Fatalf("first device = %#v", devices[0])
	}
	if devices[0].ProductManufacturer != "Google" || devices[0].ProductModel != "sdk_gphone64_x86_64" {
		t.Fatalf("first product fields = manufacturer:%q model:%q", devices[0].ProductManufacturer, devices[0].ProductModel)
	}
	if devices[0].BuildVersionRelease != "14" || devices[0].BuildVersionSDK != "34" || devices[0].ProductCPUABI != "x86_64" || devices[0].WifiInterface != "wlan0" {
		t.Fatalf("first property fields = %#v", devices[0])
	}
	if got := devices[0].Interfaces; len(got) != 1 || got[0].Name != "wlan0" || got[0].IPv4 != "192.168.1.10" {
		t.Fatalf("first interfaces = %#v, want wlan0 IPv4", got)
	}
	if devices[1].UDID != "abc" || devices[1].State != "offline" || devices[1].PID != -1 {
		t.Fatalf("second device = %#v", devices[1])
	}
	for _, device := range devices {
		if device.Interfaces == nil {
			t.Fatalf("device %#v has nil Interfaces", device)
		}
		if device.LastUpdateTimestamp == 0 {
			t.Fatalf("device %#v has zero LastUpdateTimestamp", device)
		}
	}
}

func TestListDevicesUsesVerifiedScrcpyPIDWhenPidofReturnsMultipleAppProcessIDs(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("")},
		{output: []byte("111 222\n")},
		{output: []byte("zygote64\x00/system/bin/app_process64\x00unrelated.package\x00")},
		{output: []byte("app_process\x00/\x00com.genymobile.scrcpy.Server\x001.19-ws7\x00web\x00")},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices returned error: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("len(devices) = %d, want 1", len(devices))
	}
	if devices[0].PID != 222 {
		t.Fatalf("pid = %d, want verified scrcpy pid 222", devices[0].PID)
	}
	wantCalls := []runnerCall{
		{name: "adb", args: []string{"devices"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "echo ro.product.cpu.abi:$(getprop ro.product.cpu.abi);echo ro.product.manufacturer:$(getprop ro.product.manufacturer);echo ro.product.model:$(getprop ro.product.model);echo ro.build.version.release:$(getprop ro.build.version.release);echo ro.build.version.sdk:$(getprop ro.build.version.sdk);echo wifi.interface:$(getprop wifi.interface)"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "ip", "-f", "inet", "addr", "show"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/111/cmdline"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/222/cmdline"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
}

func TestRunCommandDefaultStartServerDoesNotSkipWhenPidofCmdlineIsMissingOrWrongVersion(t *testing.T) {
	startCmd := &fakeCmd{}
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("333\n")},
		{output: []byte("app_process\x00/\x00com.genymobile.scrcpy.Server\x001.18\x00web\x00")},
		{},
		startCmd,
		{output: []byte("333\n")},
		{err: errors.New("cmdline missing")},
	}}
	provider := adbcli.New(
		adbcli.WithRunner(runner),
		adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"),
		adbcli.WithScrcpyStartPollAttempts(1),
		adbcli.WithScrcpyStartPollInterval(0),
	)

	err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"})
	if err == nil || !strings.Contains(err.Error(), "scrcpy server pid did not appear") {
		t.Fatalf("error = %v, want pid did not appear", err)
	}
	if !startCmd.started {
		t.Fatalf("scrcpy shell command was not started")
	}
	wantShellCommand := "CLASSPATH=/data/local/tmp/scrcpy-server.jar nohup app_process / com.genymobile.scrcpy.Server 1.19-ws7 web ERROR 8886 true 2>&1 > /dev/null"
	wantCalls := []runnerCall{
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/333/cmdline"}},
		{name: "adb", args: []string{"-s", "abc", "push", "C:\\tmp\\scrcpy-server.jar", "/data/local/tmp/scrcpy-server.jar"}},
		{name: "adb", args: []string{"-s", "abc", "shell", wantShellCommand}},
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/333/cmdline"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
}

func TestRunCommandDefaultStartServerSucceedsAfterPollingVerifiedCmdline(t *testing.T) {
	startCmd := &fakeCmd{}
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("")},
		{},
		startCmd,
		{output: []byte("444\n")},
		{output: []byte("app_process\x00/\x00com.genymobile.scrcpy.Server\x001.18\x00web\x00")},
		{output: []byte("555\n")},
		{output: []byte("app_process\x00/\x00com.genymobile.scrcpy.Server\x001.19-ws7\x00web\x00")},
	}}
	provider := adbcli.New(
		adbcli.WithRunner(runner),
		adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"),
		adbcli.WithScrcpyStartPollAttempts(2),
		adbcli.WithScrcpyStartPollInterval(0),
	)

	if err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"}); err != nil {
		t.Fatalf("start_server error: %v", err)
	}
	if !startCmd.started {
		t.Fatalf("scrcpy shell command was not started")
	}
}

func TestListDevicesUsesSafeDefaultsWhenScrcpyMetadataLookupFails(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\ndevice/serial\tdevice\n")},
		{err: errors.New("getprop unavailable")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("")},
		{err: errors.New("ip unavailable")},
		{err: errors.New("pidof unavailable")},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices returned error: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("len(devices) = %d, want 1", len(devices))
	}
	if devices[0].PID != -1 {
		t.Fatalf("pid = %d, want -1 when pid lookup fails", devices[0].PID)
	}
	if devices[0].Interfaces == nil || len(devices[0].Interfaces) != 0 {
		t.Fatalf("interfaces = %#v, want empty slice when lookup fails", devices[0].Interfaces)
	}
	if devices[0].ProductManufacturer != "" || devices[0].ProductModel != "" {
		t.Fatalf("product fields = manufacturer:%q model:%q, want empty defaults", devices[0].ProductManufacturer, devices[0].ProductModel)
	}
}

func TestListDevicesReturnsFrontendCompatibleInterfaceFields(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\ndevice/serial\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("3: wlan0: <UP> mtu 1500\n    inet 10.0.0.2/24 scope global wlan0\n4: rmnet_data0: <UP> mtu 1500\n    inet 100.64.1.2/30 scope global rmnet_data0\n")},
		{output: []byte("987\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices returned error: %v", err)
	}
	got := devices[0].Interfaces
	want := []devicetracker.NetInterface{
		{Name: "wlan0", IPv4: "10.0.0.2"},
		{Name: "rmnet_data0", IPv4: "100.64.1.2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("interfaces = %#v, want %#v", got, want)
	}
}

func TestListDevicesFiltersLoopbackAndNonGlobalScopedInterfaces(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("1: lo: <LOOPBACK,UP> mtu 65536\n    inet 127.0.0.1/8 scope host lo\n2: wlan0: <BROADCAST,MULTICAST,UP> mtu 1500\n    inet 192.168.1.10/24 brd 192.168.1.255 scope global wlan0\n3: rmnet0: <UP> mtu 1500\n    inet 10.1.2.3/24 scope link rmnet0\n4: legacy0: <UP> mtu 1500\n    inet 172.16.0.2/24 legacy0\n")},
		{output: []byte("123\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices returned error: %v", err)
	}
	want := []devicetracker.NetInterface{{Name: "wlan0", IPv4: "192.168.1.10"}, {Name: "legacy0", IPv4: "172.16.0.2"}}
	if got := devices[0].Interfaces; !reflect.DeepEqual(got, want) {
		t.Fatalf("interfaces = %#v, want %#v", got, want)
	}
}

func TestWatchDevicesPollsAndEmitsChangedDevices(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("List of devices attached\nabc\toffline\n")},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := provider.WatchDevices(ctx)
	if err != nil {
		t.Fatalf("WatchDevices returned error: %v", err)
	}
	first := receiveEvent(t, events)
	if first.Device.UDID != "abc" || first.Device.State != "offline" {
		t.Fatalf("first event = %#v, want changed device offline", first)
	}
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatalf("events channel still open after cancellation")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("events channel did not close after cancellation")
	}
}

func TestWatchDevicesEmitsWhenPIDChanges(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("2: wlan0: <UP> mtu 1500\n    inet 192.168.1.10/24 scope global wlan0\n")},
		{output: []byte("111\n")},
		{output: scrcpyCmdline()},
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("2: wlan0: <UP> mtu 1500\n    inet 192.168.1.10/24 scope global wlan0\n")},
		{output: []byte("222\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := provider.WatchDevices(ctx)
	if err != nil {
		t.Fatalf("WatchDevices returned error: %v", err)
	}
	first := receiveEvent(t, events)
	if first.Device.UDID != "abc" || first.Device.State != "device" || first.Device.PID != 222 {
		t.Fatalf("first event = %#v, want same device with PID 222", first)
	}
}

func TestWatchDevicesEmitsWhenInterfacesChange(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("2: wlan0: <UP> mtu 1500\n    inet 192.168.1.10/24 scope global wlan0\n")},
		{output: []byte("111\n")},
		{output: scrcpyCmdline()},
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("2: wlan0: <UP> mtu 1500\n    inet 192.168.1.20/24 scope global wlan0\n")},
		{output: []byte("111\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := provider.WatchDevices(ctx)
	if err != nil {
		t.Fatalf("WatchDevices returned error: %v", err)
	}
	first := receiveEvent(t, events)
	if got := first.Device.Interfaces; len(got) != 1 || got[0].Name != "wlan0" || got[0].IPv4 != "192.168.1.20" {
		t.Fatalf("first interfaces = %#v, want changed wlan0 192.168.1.20", got)
	}
}

func receiveEvent(t *testing.T, events <-chan devicetracker.Event) devicetracker.Event {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatalf("events channel closed")
		}
		return event
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for event")
		return devicetracker.Event{}
	}
}

func TestWatchDevicesEmitsDisconnectedEventWhenDeviceDisappears(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("")},
		{output: []byte("")},
		{output: []byte("List of devices attached\n")},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := provider.WatchDevices(ctx)
	if err != nil {
		t.Fatalf("WatchDevices returned error: %v", err)
	}
	removed := receiveEvent(t, events)
	if removed.Device.UDID != "abc" {
		t.Fatalf("removed UDID = %q, want abc", removed.Device.UDID)
	}
	if removed.Device.State != "disconnected" {
		t.Fatalf("removed State = %q, want disconnected", removed.Device.State)
	}
	if removed.Device.Interfaces == nil {
		t.Fatalf("removed Interfaces is nil")
	}
	if removed.Device.LastUpdateTimestamp == 0 {
		t.Fatalf("removed LastUpdateTimestamp is zero")
	}
}

func TestRunCommandDispatchesScrcpyCommands(t *testing.T) {
	manager := &fakeScrcpyManager{}
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("List of devices attached\nabc\tdevice\n")},
		{output: []byte("ro.product.cpu.abi:arm64-v8a\nro.product.manufacturer:ACME\nro.product.model:Phone\nro.build.version.release:13\nro.build.version.sdk:33\nwifi.interface:wlan0\n")},
		{output: []byte("")},
		{output: []byte("")},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithScrcpyManager(manager))
	ctx := context.Background()

	if err := provider.RunCommand(ctx, devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"}); err != nil {
		t.Fatalf("start_server error: %v", err)
	}
	if err := provider.RunCommand(ctx, devicetracker.Command{Type: devicetracker.CommandKillServer, UDID: "abc", PID: 4321}); err != nil {
		t.Fatalf("kill_server error: %v", err)
	}
	if err := provider.RunCommand(ctx, devicetracker.Command{Type: devicetracker.CommandUpdateInterfaces, UDID: "abc"}); err != nil {
		t.Fatalf("update_interfaces error: %v", err)
	}

	if !reflect.DeepEqual(manager.starts, []string{"abc"}) {
		t.Fatalf("manager starts = %#v, want abc", manager.starts)
	}
	if !reflect.DeepEqual(manager.kills, []killCall{{udid: "abc", pid: 4321}}) {
		t.Fatalf("manager kills = %#v, want abc pid 4321", manager.kills)
	}
	wantCalls := []runnerCall{
		{name: "adb", args: []string{"devices"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "echo ro.product.cpu.abi:$(getprop ro.product.cpu.abi);echo ro.product.manufacturer:$(getprop ro.product.manufacturer);echo ro.product.model:$(getprop ro.product.model);echo ro.build.version.release:$(getprop ro.build.version.release);echo ro.build.version.sdk:$(getprop ro.build.version.sdk);echo wifi.interface:$(getprop wifi.interface)"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "ip", "-f", "inet", "addr", "show"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
}

func TestRunCommandDefaultStartServerSkipsPushWhenAlreadyRunning(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("4321\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"))

	if err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"}); err != nil {
		t.Fatalf("start_server error: %v", err)
	}
	wantCalls := []runnerCall{
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/4321/cmdline"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
}

func TestRunCommandDefaultStartServerPushesJarStartsShellAndPollsPid(t *testing.T) {
	startCmd := &fakeCmd{}
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("")},
		{},
		startCmd,
		{output: []byte("777\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(
		adbcli.WithRunner(runner),
		adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"),
		adbcli.WithScrcpyStartPollAttempts(1),
		adbcli.WithScrcpyStartPollInterval(0),
	)

	if err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"}); err != nil {
		t.Fatalf("start_server error: %v", err)
	}
	wantShellCommand := "CLASSPATH=/data/local/tmp/scrcpy-server.jar nohup app_process / com.genymobile.scrcpy.Server 1.19-ws7 web ERROR 8886 true 2>&1 > /dev/null"
	wantCalls := []runnerCall{
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "push", "C:\\tmp\\scrcpy-server.jar", "/data/local/tmp/scrcpy-server.jar"}},
		{name: "adb", args: []string{"-s", "abc", "shell", wantShellCommand}},
		{name: "adb", args: []string{"-s", "abc", "shell", "pidof", "app_process"}},
		{name: "adb", args: []string{"-s", "abc", "shell", "cat", "/proc/777/cmdline"}},
	}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
	if !startCmd.started {
		t.Fatalf("scrcpy shell command was not started")
	}
}

func TestRunCommandDefaultStartServerReturnsErrorWhenPidNeverAppears(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("")},
		{},
		{},
		{output: []byte("")},
		{output: []byte("")},
	}}
	provider := adbcli.New(
		adbcli.WithRunner(runner),
		adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"),
		adbcli.WithScrcpyStartPollAttempts(2),
		adbcli.WithScrcpyStartPollInterval(0),
	)

	err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"})
	if err == nil || !strings.Contains(err.Error(), "scrcpy server pid did not appear") {
		t.Fatalf("error = %v, want pid did not appear", err)
	}
}

func TestRunCommandDefaultStartServerRejectsEmptyUDID(t *testing.T) {
	runner := &fakeRunner{}
	provider := adbcli.New(adbcli.WithRunner(runner), adbcli.WithScrcpyServerJar("C:\\tmp\\scrcpy-server.jar"))

	err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer})
	if err == nil || !strings.Contains(err.Error(), "udid is required") {
		t.Fatalf("error = %v, want udid is required", err)
	}
	if got := runner.snapshotCalls(); len(got) != 0 {
		t.Fatalf("calls = %#v, want none", got)
	}
}

func TestRunCommandDefaultStartServerUsesInjectedJarPath(t *testing.T) {
	runner := &fakeRunner{cmds: []*fakeCmd{
		{output: []byte("")},
		{},
		{},
		{output: []byte("999\n")},
		{output: scrcpyCmdline()},
	}}
	provider := adbcli.New(
		adbcli.WithRunner(runner),
		adbcli.WithScrcpyServerJar("D:\\vendor\\custom-scrcpy-server.jar"),
		adbcli.WithScrcpyStartPollAttempts(1),
		adbcli.WithScrcpyStartPollInterval(0),
	)

	if err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandStartServer, UDID: "abc"}); err != nil {
		t.Fatalf("start_server error: %v", err)
	}
	calls := runner.snapshotCalls()
	if len(calls) < 2 {
		t.Fatalf("calls = %#v, want push call", calls)
	}
	wantPushArgs := []string{"-s", "abc", "push", "D:\\vendor\\custom-scrcpy-server.jar", "/data/local/tmp/scrcpy-server.jar"}
	if !reflect.DeepEqual(calls[1].args, wantPushArgs) {
		t.Fatalf("push args = %#v, want %#v", calls[1].args, wantPushArgs)
	}
}

func TestRunCommandDefaultKillServerRunsADBShellKillAndPropagatesError(t *testing.T) {
	killErr := errors.New("kill failed")
	runner := &fakeRunner{cmds: []*fakeCmd{{err: killErr}}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	err := provider.RunCommand(context.Background(), devicetracker.Command{Type: devicetracker.CommandKillServer, UDID: "abc", PID: 4321})
	if !errors.Is(err, killErr) {
		t.Fatalf("error = %v, want kill failed", err)
	}
	wantCalls := []runnerCall{{name: "adb", args: []string{"-s", "abc", "shell", "kill", "4321"}}}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
}

type fakeScrcpyManager struct {
	starts []string
	kills  []killCall
}

type killCall struct {
	udid string
	pid  int
}

func (m *fakeScrcpyManager) Start(ctx context.Context, udid string) error {
	m.starts = append(m.starts, udid)
	return nil
}

func (m *fakeScrcpyManager) Kill(ctx context.Context, udid string, pid int) error {
	m.kills = append(m.kills, killCall{udid: udid, pid: pid})
	return nil
}

func TestShellStartRunsADBShellAndSessionStreamsIO(t *testing.T) {
	stdin := &recordingWriteCloser{}
	cmd := &fakeCmd{stdin: stdin, stdout: io.NopCloser(strings.NewReader("hello\n")), waitErr: errors.New("shell exited")}
	runner := &fakeRunner{cmds: []*fakeCmd{cmd}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	session, err := provider.Start(context.Background(), shell.Request{UDID: "abc"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	wantCalls := []runnerCall{{name: "adb", args: []string{"-s", "abc", "shell", "-tt"}}}
	if got := runner.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", got, wantCalls)
	}
	if !cmd.started {
		t.Fatalf("command was not started")
	}
	if err := session.Write(context.Background(), []byte("input\r")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if got := stdin.String(); got != "input\n" {
		t.Fatalf("stdin = %q, want input with CR translated to LF", got)
	}
	select {
	case got := <-session.Output():
		if string(got) != "hello\n" {
			t.Fatalf("output = %q", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for output")
	}
	select {
	case err := <-session.Done():
		if err == nil || err.Error() != "shell exited" {
			t.Fatalf("Done error = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for Done")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !cmd.killed {
		t.Fatalf("command was not killed on Close")
	}
	if !stdin.closed {
		t.Fatalf("stdin was not closed on Close")
	}
}

func TestShellCloseStopsOutputForwardingWhenConsumerStopsReading(t *testing.T) {
	stdout := newCloseAwareInfiniteReader()
	cmd := &fakeCmd{stdout: stdout}
	runner := &fakeRunner{cmds: []*fakeCmd{cmd}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	session, err := provider.Start(context.Background(), shell.Request{UDID: "abc"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	stdout.allowReads(17)
	deadline := time.After(200 * time.Millisecond)
	for len(session.Output()) < 16 {
		select {
		case <-deadline:
			t.Fatalf("output channel len = %d, want full", len(session.Output()))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	closed := make(chan struct{})
	go func() {
		for range session.Output() {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("output channel did not close after Close")
	}
}

func TestShellStartDrainsAndClosesStderr(t *testing.T) {
	stderr := &trackingReadCloser{reader: strings.NewReader("warning\n"), closed: make(chan struct{})}
	cmd := &fakeCmd{stderr: stderr}
	runner := &fakeRunner{cmds: []*fakeCmd{cmd}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	session, err := provider.Start(context.Background(), shell.Request{UDID: "abc"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer session.Close()

	select {
	case <-stderr.closed:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("stderr was not drained and closed")
	}
	if stderr.reads == 0 {
		t.Fatalf("stderr was closed without being read")
	}
}

func TestShellDoneClosesWhenWaitReturnsErrNotFound(t *testing.T) {
	cmd := &fakeCmd{waitErr: exec.ErrNotFound}
	runner := &fakeRunner{cmds: []*fakeCmd{cmd}}
	provider := adbcli.New(adbcli.WithRunner(runner))

	session, err := provider.Start(context.Background(), shell.Request{UDID: "abc"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case got := <-session.Done():
		if !errors.Is(got, exec.ErrNotFound) {
			t.Fatalf("Done error = %v, want exec.ErrNotFound", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for Done")
	}
	select {
	case _, ok := <-session.Done():
		if ok {
			t.Fatalf("Done channel is still open")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for Done to close")
	}
}


