package adbcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/filelisting"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/shell"
)

const (
	defaultADBPath = "adb"

	ScrcpyServerPackage = "com.genymobile.scrcpy.Server"
	ScrcpyServerPort    = 8886
	ScrcpyServerVersion = "1.19-ws7"
	ScrcpyServerType    = "web"
	ScrcpyLogLevel      = "ERROR"
	// Must stay in sync with SCRCPY_LISTENS_ON_ALL_INTERFACES in frontend/src/common/Constants.ts.
	// The web UI offers direct ws://<device-ip>:8886 links when its flag is true;
	// those only work if the on-device server is started with `true` here.
	ScrcpyListensOnAllInterfaces   = true
	ScrcpyServerProcessName        = "app_process"
	ScrcpyServerRemoteJarPath      = "/data/local/tmp/scrcpy-server.jar"
	ScrcpyRemoteTarget             = "tcp:8886"
	defaultScrcpyStartPollAttempts = 5
	defaultScrcpyStartPollInterval = 500 * time.Millisecond
	scrcpyServerJarRelativePath    = "vendor/Genymobile/scrcpy/scrcpy-server.jar"
)

var ErrScrcpyServerUnsupported = errors.New("scrcpy server jar is not configured or could not be found")

var _ devicetracker.Provider = (*Provider)(nil)
var _ shell.Provider = (*Provider)(nil)
var _ filelisting.Provider = (*Provider)(nil)

// CommandRunner creates commands. Tests provide fakes so adb is never executed there.
type CommandRunner interface {
	Command(ctx context.Context, name string, args ...string) Cmd
}

// Cmd is the subset of exec.Cmd behavior needed by adbcli providers.
type Cmd interface {
	Output() ([]byte, error)
	CombinedOutput() ([]byte, error)
	Start() error
	Wait() error
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
	Kill() error
}

type Option func(*Provider)

type ScrcpyManager interface {
	Start(ctx context.Context, udid string) error
	Kill(ctx context.Context, udid string, pid int) error
}

type Provider struct {
	runner                  CommandRunner
	adbPath                 string
	host                    string
	port                    int
	pollInterval            time.Duration
	scrcpyManager           ScrcpyManager
	scrcpyServerJar         string
	scrcpyStartPollAttempts int
	scrcpyStartPollInterval time.Duration
	portAllocator           PortAllocator
	dialer                  Dialer
}

func New(options ...Option) *Provider {
	provider := &Provider{
		runner:                  execRunner{},
		adbPath:                 defaultADBPath,
		pollInterval:            2 * time.Second,
		scrcpyStartPollAttempts: defaultScrcpyStartPollAttempts,
		scrcpyStartPollInterval: defaultScrcpyStartPollInterval,
	}
	for _, option := range options {
		option(provider)
	}
	if provider.runner == nil {
		provider.runner = execRunner{}
	}
	if provider.adbPath == "" {
		provider.adbPath = defaultADBPath
	}
	if provider.pollInterval <= 0 {
		provider.pollInterval = 2 * time.Second
	}
	if provider.scrcpyStartPollAttempts <= 0 {
		provider.scrcpyStartPollAttempts = defaultScrcpyStartPollAttempts
	}
	return provider
}

func WithRunner(runner CommandRunner) Option {
	return func(provider *Provider) {
		provider.runner = runner
	}
}

func WithADBPath(path string) Option {
	return func(provider *Provider) {
		provider.adbPath = path
	}
}

func WithHostPort(host string, port int) Option {
	return func(provider *Provider) {
		provider.host = host
		provider.port = port
	}
}

func WithPollInterval(interval time.Duration) Option {
	return func(provider *Provider) {
		provider.pollInterval = interval
	}
}

func WithScrcpyServerJar(path string) Option {
	return func(provider *Provider) {
		provider.scrcpyServerJar = path
	}
}

func WithScrcpyStartPollAttempts(attempts int) Option {
	return func(provider *Provider) {
		provider.scrcpyStartPollAttempts = attempts
	}
}

func WithScrcpyStartPollInterval(interval time.Duration) Option {
	return func(provider *Provider) {
		provider.scrcpyStartPollInterval = interval
	}
}

func WithScrcpyManager(manager ScrcpyManager) Option {
	return func(provider *Provider) {
		provider.scrcpyManager = manager
	}
}

func WithPortAllocator(allocator PortAllocator) Option {
	return func(provider *Provider) {
		provider.portAllocator = allocator
	}
}

func WithDialer(dialer Dialer) Option {
	return func(provider *Provider) {
		provider.dialer = dialer
	}
}

func (p *Provider) ListDevices(ctx context.Context) ([]devicetracker.Device, error) {
	output, err := p.command(ctx, "devices").Output()
	if err != nil {
		applog.Errorf("adb devices failed: %v", err)
		return nil, err
	}
	devices := parseDevices(output, time.Now().UnixMilli())
	applog.Debugf("adb devices raw count=%d", len(devices))
	for i := range devices {
		devices[i].PID = -1
		if devices[i].State != "device" {
			applog.Debugf("adb device udid=%s state=%s skipped metadata", devices[i].UDID, devices[i].State)
			continue
		}
		p.fillDeviceProperties(ctx, &devices[i])
		devices[i].Interfaces = p.listInterfaces(ctx, devices[i].UDID)
		devices[i].PID = p.scrcpyPID(ctx, devices[i].UDID)
		// WatchDevices polls ListDevices every few seconds; keep per-scan details at debug.
		applog.Debugf("adb device udid=%s state=%s model=%s manufacturer=%s pid=%d ifaces=%d",
			devices[i].UDID, devices[i].State, devices[i].ProductModel, devices[i].ProductManufacturer, devices[i].PID, len(devices[i].Interfaces))
	}
	return devices, nil
}

func (p *Provider) WatchDevices(ctx context.Context) (<-chan devicetracker.Event, error) {
	events := make(chan devicetracker.Event)
	go func() {
		defer close(events)
		previous := map[string]devicetracker.Device{}
		initialized := false
		poll := func() bool {
			devices, err := p.ListDevices(ctx)
			if err != nil {
				return true
			}
			current := make(map[string]devicetracker.Device, len(devices))
			for _, device := range devices {
				current[device.UDID] = device
				previousDevice, existed := previous[device.UDID]
				if initialized && (!existed || deviceDescriptorChanged(previousDevice, device)) {
					if !existed {
						applog.Infof("adb device added udid=%s state=%s", device.UDID, device.State)
					} else {
						applog.Infof("adb device changed udid=%s state=%s pid=%d", device.UDID, device.State, device.PID)
					}
					select {
					case <-ctx.Done():
						return false
					case events <- devicetracker.Event{Device: device}:
					}
				}
			}
			for udid := range previous {
				if _, ok := current[udid]; ok {
					continue
				}
				device := devicetracker.Device{
					UDID:                udid,
					State:               "disconnected",
					Interfaces:          []devicetracker.NetInterface{},
					LastUpdateTimestamp: time.Now().UnixMilli(),
				}
				applog.Infof("adb device removed udid=%s", udid)
				select {
				case <-ctx.Done():
					return false
				case events <- devicetracker.Event{Device: device}:
				}
			}
			previous = current
			initialized = true
			return true
		}
		if !poll() {
			return
		}
		ticker := time.NewTicker(p.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !poll() {
					return
				}
			}
		}
	}()
	return events, nil
}

func deviceDescriptorChanged(previous devicetracker.Device, current devicetracker.Device) bool {
	if previous.UDID != current.UDID || previous.State != current.State || previous.PID != current.PID {
		return true
	}
	if previous.BuildVersionRelease != current.BuildVersionRelease ||
		previous.BuildVersionSDK != current.BuildVersionSDK ||
		previous.ProductCPUABI != current.ProductCPUABI ||
		previous.ProductManufacturer != current.ProductManufacturer ||
		previous.ProductModel != current.ProductModel ||
		previous.WifiInterface != current.WifiInterface {
		return true
	}
	if len(previous.Interfaces) != len(current.Interfaces) {
		return true
	}
	for i := range previous.Interfaces {
		if previous.Interfaces[i].Name != current.Interfaces[i].Name || previous.Interfaces[i].IPv4 != current.Interfaces[i].IPv4 {
			return true
		}
	}
	return false
}

func (p *Provider) RunCommand(ctx context.Context, command devicetracker.Command) error {
	applog.Infof("adb command type=%s udid=%s pid=%d", command.Type, command.UDID, command.PID)
	switch command.Type {
	case devicetracker.CommandKillServer:
		return p.scrcpy().Kill(ctx, command.UDID, command.PID)
	case devicetracker.CommandStartServer:
		return p.scrcpy().Start(ctx, command.UDID)
	case devicetracker.CommandUpdateInterfaces:
		_, err := p.ListDevices(ctx)
		return err
	default:
		return fmt.Errorf("unsupported device tracker command: %s", command.Type)
	}
}

func (p *Provider) Start(ctx context.Context, request shell.Request) (shell.Session, error) {
	cmd := p.command(ctx, "-s", request.UDID, "shell", "-tt")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	session := &adbShellSession{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		closed: make(chan struct{}),
		output: make(chan []byte, 16),
		done:   make(chan error, 1),
	}
	go session.copyOutput()
	go session.drainStderr()
	go session.wait()
	return session, nil
}

func (p *Provider) command(ctx context.Context, args ...string) Cmd {
	allArgs := p.adbArgs(args...)
	return p.runner.Command(ctx, p.adbPath, allArgs...)
}

func (p *Provider) adbArgs(args ...string) []string {
	allArgs := make([]string, 0, len(args)+4)
	if p.host != "" {
		allArgs = append(allArgs, "-H", p.host)
		if p.port > 0 {
			allArgs = append(allArgs, "-P", strconv.Itoa(p.port))
		}
	}
	allArgs = append(allArgs, args...)
	return allArgs
}

func parseDevices(output []byte, timestamp int64) []devicetracker.Device {
	lines := strings.Split(string(output), "\n")
	devices := make([]devicetracker.Device, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "List of devices attached") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		devices = append(devices, devicetracker.Device{
			UDID:                fields[0],
			State:               fields[1],
			PID:                 -1,
			Interfaces:          []devicetracker.NetInterface{},
			LastUpdateTimestamp: timestamp,
		})
	}
	return devices
}

func (p *Provider) fillDeviceProperties(ctx context.Context, device *devicetracker.Device) {
	props := p.getDeviceProperties(ctx, device.UDID)
	device.BuildVersionRelease = props["ro.build.version.release"]
	device.BuildVersionSDK = props["ro.build.version.sdk"]
	device.ProductCPUABI = props["ro.product.cpu.abi"]
	device.ProductManufacturer = props["ro.product.manufacturer"]
	device.ProductModel = props["ro.product.model"]
	device.WifiInterface = props["wifi.interface"]
}

// devicePropertyNames matches the Node backend Properties list expected by the frontend.
var devicePropertyNames = []string{
	"ro.product.cpu.abi",
	"ro.product.manufacturer",
	"ro.product.model",
	"ro.build.version.release",
	"ro.build.version.sdk",
	"wifi.interface",
}

func (p *Provider) getDeviceProperties(ctx context.Context, udid string) map[string]string {
	// Prefer one shell call with multiple getprop invocations. Fall back to per-key
	// getprop only if the batch command fails entirely.
	parts := make([]string, 0, len(devicePropertyNames))
	for _, name := range devicePropertyNames {
		parts = append(parts, "echo "+name+":$(getprop "+name+")")
	}
	script := strings.Join(parts, ";")
	output, err := p.command(ctx, "-s", udid, "shell", script).Output()
	props := map[string]string{}
	if err == nil {
		for _, line := range strings.Split(string(output), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			props[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
		return props
	}
	for _, name := range devicePropertyNames {
		value, propErr := p.command(ctx, "-s", udid, "shell", "getprop", name).Output()
		if propErr != nil {
			props[name] = ""
			continue
		}
		props[name] = strings.TrimSpace(string(value))
	}
	return props
}

func (p *Provider) listInterfaces(ctx context.Context, udid string) []devicetracker.NetInterface {
	output, err := p.command(ctx, "-s", udid, "shell", "ip", "-f", "inet", "addr", "show").Output()
	if err != nil {
		return []devicetracker.NetInterface{}
	}
	return parseInterfaces(output)
}

func parseInterfaces(output []byte) []devicetracker.NetInterface {
	lines := strings.Split(string(output), "\n")
	interfaces := make([]devicetracker.NetInterface, 0)
	currentName := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if fields := strings.Fields(trimmed); len(fields) >= 2 && strings.HasSuffix(fields[0], ":") && strings.HasSuffix(fields[1], ":") {
			currentName = strings.TrimSuffix(fields[1], ":")
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 2 || fields[0] != "inet" || currentName == "" {
			continue
		}
		ipv4 := strings.SplitN(fields[1], "/", 2)[0]
		if isLoopbackIPv4(ipv4) || !hasUsableScope(fields) {
			continue
		}
		interfaces = append(interfaces, devicetracker.NetInterface{Name: currentName, IPv4: ipv4})
	}
	return interfaces
}

func isLoopbackIPv4(ipv4 string) bool {
	return ipv4 == "127.0.0.1" || strings.HasPrefix(ipv4, "127.")
}

func hasUsableScope(fields []string) bool {
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "scope" {
			return fields[i+1] == "global"
		}
	}
	return true
}

func (p *Provider) scrcpyPID(ctx context.Context, udid string) int {
	output, err := p.command(ctx, "-s", udid, "shell", "pidof", ScrcpyServerProcessName).Output()
	if err != nil {
		return -1
	}
	for _, field := range strings.Fields(string(output)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			continue
		}
		if p.isScrcpyServerPID(ctx, udid, pid) {
			return pid
		}
	}
	return -1
}

func (p *Provider) isScrcpyServerPID(ctx context.Context, udid string, pid int) bool {
	output, err := p.command(ctx, "-s", udid, "shell", "cat", fmt.Sprintf("/proc/%d/cmdline", pid)).Output()
	if err != nil {
		return false
	}
	args := strings.Split(strings.TrimRight(string(output), "\x00"), "\x00")
	for i, arg := range args {
		if arg != ScrcpyServerPackage {
			continue
		}
		return i+1 < len(args) && args[i+1] == ScrcpyServerVersion
	}
	return false
}

func (p *Provider) scrcpy() ScrcpyManager {
	if p.scrcpyManager != nil {
		return p.scrcpyManager
	}
	return adbScrcpyManager{provider: p}
}

type adbScrcpyManager struct {
	provider *Provider
}

func (m adbScrcpyManager) Start(ctx context.Context, udid string) error {
	if udid == "" {
		return errors.New("udid is required to start scrcpy server")
	}
	if pid := m.provider.scrcpyPID(ctx, udid); pid > 0 {
		return nil
	}
	jarPath, err := m.provider.scrcpyServerJarPath()
	if err != nil {
		return err
	}
	if _, err := m.provider.command(ctx, "-s", udid, "push", jarPath, ScrcpyServerRemoteJarPath).CombinedOutput(); err != nil {
		return fmt.Errorf("push scrcpy server jar: %w", err)
	}
	cmd := m.provider.command(ctx, "-s", udid, "shell", scrcpyStartShellCommand())
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start scrcpy server: %w", err)
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			applog.Debugf("scrcpy start shell exited udid=%s err=%v", udid, err)
		}
	}()
	for attempt := 0; attempt < m.provider.scrcpyStartPollAttempts; attempt++ {
		if attempt > 0 && m.provider.scrcpyStartPollInterval > 0 {
			timer := time.NewTimer(m.provider.scrcpyStartPollInterval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			case <-timer.C:
			}
		}
		if pid := m.provider.scrcpyPID(ctx, udid); pid > 0 {
			return nil
		}
	}
	return fmt.Errorf("scrcpy server pid did not appear after %d poll attempts", m.provider.scrcpyStartPollAttempts)
}

func scrcpyStartShellCommand() string {
	return fmt.Sprintf(
		"CLASSPATH=%s nohup %s / %s %s %s %s %d %t 2>&1 > /dev/null",
		ScrcpyServerRemoteJarPath,
		ScrcpyServerProcessName,
		ScrcpyServerPackage,
		ScrcpyServerVersion,
		ScrcpyServerType,
		ScrcpyLogLevel,
		ScrcpyServerPort,
		ScrcpyListensOnAllInterfaces,
	)
}

func (p *Provider) scrcpyServerJarPath() (string, error) {
	if p.scrcpyServerJar != "" {
		return p.scrcpyServerJar, nil
	}
	// Prefer a jar placed next to the executable (release bundle layout),
	// then fall back to walking up from the working directory (repo layout).
	var candidates []string
	if executable, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(executable), "scrcpy-server.jar"))
	}
	workingDir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve scrcpy server jar: %w", err)
	}
	for dir := workingDir; ; dir = filepath.Dir(dir) {
		candidates = append(candidates, filepath.Join(dir, scrcpyServerJarRelativePath))
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", ErrScrcpyServerUnsupported
}

func (m adbScrcpyManager) Kill(ctx context.Context, udid string, pid int) error {
	if udid == "" || pid <= 0 {
		return nil
	}
	_, err := m.provider.command(ctx, "-s", udid, "shell", "kill", strconv.Itoa(pid)).CombinedOutput()
	return err
}

type adbShellSession struct {
	cmd    Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	output chan []byte
	done   chan error
	closed chan struct{}
	once   sync.Once
}

func (s *adbShellSession) Write(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err := s.stdin.Write(normalizeShellInput(data))
	return err
}

func normalizeShellInput(data []byte) []byte {
	for _, b := range data {
		if b == '\r' {
			converted := make([]byte, len(data))
			copy(converted, data)
			for i, value := range converted {
				if value == '\r' {
					converted[i] = '\n'
				}
			}
			return converted
		}
	}
	return data
}

func (s *adbShellSession) Output() <-chan []byte {
	return s.output
}

func (s *adbShellSession) Done() <-chan error {
	return s.done
}

func (s *adbShellSession) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		stdinErr := s.stdin.Close()
		stdoutErr := s.stdout.Close()
		stderrErr := s.stderr.Close()
		killErr := s.cmd.Kill()
		for _, closeErr := range []error{stdinErr, stdoutErr, stderrErr, killErr} {
			if closeErr != nil {
				err = closeErr
				break
			}
		}
	})
	return err
}

func (s *adbShellSession) copyOutput() {
	defer close(s.output)
	defer s.stdout.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := s.stdout.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			select {
			case <-s.closed:
				return
			case s.output <- chunk:
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *adbShellSession) drainStderr() {
	defer s.stderr.Close()
	buf := make([]byte, 4*1024)
	for {
		n, err := s.stderr.Read(buf)
		if n > 0 {
			applog.Debugf("adb shell stderr: %s", strings.TrimSpace(string(buf[:n])))
		}
		if err != nil {
			return
		}
	}
}

func (s *adbShellSession) wait() {
	defer close(s.done)
	s.done <- s.cmd.Wait()
}

type execRunner struct{}

func (execRunner) Command(ctx context.Context, name string, args ...string) Cmd {
	return &execCmd{cmd: exec.CommandContext(ctx, name, args...)}
}

type execCmd struct {
	cmd *exec.Cmd
}

func (c *execCmd) Output() ([]byte, error)            { return c.cmd.Output() }
func (c *execCmd) CombinedOutput() ([]byte, error)    { return c.cmd.CombinedOutput() }
func (c *execCmd) Start() error                       { return c.cmd.Start() }
func (c *execCmd) Wait() error                        { return c.cmd.Wait() }
func (c *execCmd) StdinPipe() (io.WriteCloser, error) { return c.cmd.StdinPipe() }
func (c *execCmd) StdoutPipe() (io.ReadCloser, error) { return c.cmd.StdoutPipe() }
func (c *execCmd) StderrPipe() (io.ReadCloser, error) { return c.cmd.StderrPipe() }
func (c *execCmd) Kill() error {
	if c.cmd.Process == nil {
		return nil
	}
	return c.cmd.Process.Kill()
}
