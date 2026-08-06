package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbcli"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbproxy"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/contract"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/devicetracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/filelisting"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/hosttracker"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/httpserver"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/multiplex"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/proxy"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/shell"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/wsrouter"
	"github.com/Genymobile/ws-scrcpy/go-backend/web"
)

const shutdownTimeout = 5 * time.Second

type Options struct {
	Env map[string]string
	CWD string

	HandlerOptions HandlerOptions
}

type HandlerOptions struct {
	Env map[string]string

	ADBProviderFactory  adbProviderFactory
	DeviceProvider      devicetracker.Provider
	ShellProvider       shell.Provider
	FileListingProvider filelisting.Provider
	ADBStreamProvider   adbproxy.StreamProvider
}

type adbProvider interface {
	devicetracker.Provider
	shell.Provider
	filelisting.Provider
}

type adbProviderFactory func(options ...adbcli.Option) adbProvider

func Run(ctx context.Context, options Options) error {
	cwd := options.CWD
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	cfg, err := config.Load(options.Env, cwd)
	if err != nil {
		return err
	}
	applog.Infof("config loaded pathname=%q servers=%d remote_hosts=%d", cfg.Pathname, len(cfg.Servers), len(cfg.RemoteHosts))
	staticFS, err := fs.Sub(web.FS, "public")
	if err != nil {
		return fmt.Errorf("load embedded frontend: %w", err)
	}
	handlerOptions := options.HandlerOptions
	if handlerOptions.Env == nil {
		handlerOptions.Env = options.Env
	}
	return ServeWithOptions(ctx, cfg, staticFS, handlerOptions)
}

func Serve(ctx context.Context, cfg config.Config, staticFS fs.FS) error {
	return ServeWithOptions(ctx, cfg, staticFS, HandlerOptions{})
}

func ServeWithOptions(ctx context.Context, cfg config.Config, staticFS fs.FS, handlerOptions HandlerOptions) error {
	runners, err := newHTTPServers(cfg, staticFS, handlerOptions)
	if err != nil {
		return err
	}
	for _, runner := range runners {
		scheme := "http"
		if runner.secure {
			scheme = "https"
		}
		applog.Infof("listening %s://0.0.0.0%s pathname=%q", scheme, runner.server.Addr, cfg.Pathname)
	}
	errCh := make(chan error, len(runners))
	for _, runner := range runners {
		runner := runner
		go func() {
			errCh <- runner.ListenAndServe()
		}()
	}
	select {
	case <-ctx.Done():
		applog.Infof("shutdown requested: %v", ctx.Err())
		if err := shutdownServers(runners); err != nil {
			return err
		}
		if err := collectServerErrors(errCh, len(runners)); err != nil {
			return err
		}
		applog.Infof("server stopped cleanly")
		return ctx.Err()
	case err := <-errCh:
		if shutdownErr := shutdownServers(runners); shutdownErr != nil {
			applog.Warnf("shutdown after server error failed: %v", shutdownErr)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		applog.Errorf("server exited with error: %v", err)
		return err
	}
}

func NewHandler(cfg config.Config, staticFS fs.FS) http.Handler {
	return NewHandlerWithOptions(cfg, staticFS, HandlerOptions{})
}

func NewHandlerWithOptions(cfg config.Config, staticFS fs.FS, options HandlerOptions) http.Handler {
	return httpserver.NewHandler(cfg, staticFS, NewRouterWithOptions(cfg, options))
}

func NewRouter(cfg config.Config) *wsrouter.Router {
	return NewRouterWithOptions(cfg, HandlerOptions{})
}

func NewRouterWithOptions(cfg config.Config, options HandlerOptions) *wsrouter.Router {
	providers := newProviders(options)
	registry := newMultiplexRegistry(cfg, providers)
	return wsrouter.New(map[string]wsrouter.Handler{
		contract.ActionProxyWS:        proxy.NewHandler(),
		contract.ActionProxyADB:       adbproxy.NewHandler(providers.adbStream),
		contract.ActionMultiplex:      multiplex.NewHandler(registry),
		contract.ActionGoogDeviceList: devicetracker.NewHandler(providers.device),
		contract.ActionShell:          shell.NewHandler(providers.shell),
		contract.ActionFileListing:    filelisting.NewHandler(),
	})
}

func NewMultiplexRegistry(cfg config.Config) *multiplex.Registry {
	return newMultiplexRegistry(cfg, newProviders(HandlerOptions{}))
}

type providers struct {
	device      devicetracker.Provider
	shell       shell.Provider
	fileListing filelisting.Provider
	adbStream   adbproxy.StreamProvider
}

func newProviders(options HandlerOptions) providers {
	var adb adbProvider
	getADB := func() adbProvider {
		if adb != nil {
			return adb
		}
		factory := options.ADBProviderFactory
		if factory == nil {
			factory = func(providerOptions ...adbcli.Option) adbProvider {
				return adbcli.New(providerOptions...)
			}
		}
		adb = factory(adbOptionsFromEnv(options.Env)...)
		return adb
	}
	deviceProvider := options.DeviceProvider
	if deviceProvider == nil {
		deviceProvider = getADB()
	}
	shellProvider := options.ShellProvider
	if shellProvider == nil {
		shellProvider = getADB()
	}
	fileListingProvider := options.FileListingProvider
	if fileListingProvider == nil {
		fileListingProvider = getADB()
	}
	adbStreamProvider := options.ADBStreamProvider
	if adbStreamProvider == nil {
		adbStreamProvider = adbcli.NewForwardStreamProvider(adbOptionsFromEnv(options.Env)...)
	}
	return providers{device: deviceProvider, shell: shellProvider, fileListing: fileListingProvider, adbStream: adbStreamProvider}
}

func newMultiplexRegistry(cfg config.Config, providers providers) *multiplex.Registry {
	registry := multiplex.NewRegistry()
	registry.Register(contract.ChannelHSTS, multiplex.NewHostTrackerFactory(hosttracker.New([]string{"android"}, cfg.RemoteHosts)))
	registry.Register(contract.ChannelGTRC, devicetracker.NewGTRCFactory(providers.device))
	registry.Register(contract.ChannelSHEL, shell.NewSHELFactory(providers.shell))
	registry.Register(contract.ChannelFSLS, filelisting.NewFSLSFactory(filelisting.WithEnabled(true), filelisting.WithProvider(providers.fileListing)))
	return registry
}

func adbOptionsFromEnv(env map[string]string) []adbcli.Option {
	host := envValue(env, "ADB_HOST")
	port := 0
	if rawPort := envValue(env, "ADB_PORT"); rawPort != "" {
		parsed, err := strconv.Atoi(rawPort)
		if err == nil {
			port = parsed
		}
	}
	if host == "" {
		if port == 0 {
			return nil
		}
		host = "127.0.0.1"
	}
	return []adbcli.Option{adbcli.WithHostPort(host, port)}
}

func envValue(env map[string]string, key string) string {
	if env != nil {
		return env[key]
	}
	return os.Getenv(key)
}

type httpServerRunner struct {
	server *http.Server
	secure bool
}

func (r httpServerRunner) ListenAndServe() error {
	listener, err := net.Listen("tcp", r.server.Addr)
	if err != nil {
		applog.Errorf("listen %s failed: %v", r.server.Addr, err)
		return err
	}
	applog.Debugf("accepted listener on %s", listener.Addr())
	return r.Serve(listener)
}

func (r httpServerRunner) Serve(listener net.Listener) error {
	if r.secure {
		listener = tls.NewListener(listener, r.server.TLSConfig)
	}
	return r.server.Serve(listener)
}

func newHTTPServer(cfg config.Config, staticFS fs.FS, handlerOptions HandlerOptions) (*http.Server, error) {
	runners, err := newHTTPServers(cfg, staticFS, handlerOptions)
	if err != nil {
		return nil, err
	}
	if len(runners) != 1 {
		return nil, errors.New("newHTTPServer requires exactly one server configuration")
	}
	return runners[0].server, nil
}

func newHTTPServers(cfg config.Config, staticFS fs.FS, handlerOptions HandlerOptions) ([]httpServerRunner, error) {
	if len(cfg.Servers) == 0 {
		return nil, errors.New("no server configuration")
	}
	handler := NewHandlerWithOptions(cfg, staticFS, handlerOptions)
	runners := make([]httpServerRunner, 0, len(cfg.Servers))
	for _, serverCfg := range cfg.Servers {
		server := &http.Server{
			Addr:              fmt.Sprintf(":%d", serverCfg.Port),
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		}
		runner := httpServerRunner{server: server, secure: serverCfg.Secure}
		if serverCfg.Secure {
			tlsConfig, err := tlsConfigFromOptions(serverCfg.Options)
			if err != nil {
				return nil, err
			}
			server.TLSConfig = tlsConfig
		}
		runners = append(runners, runner)
	}
	return runners, nil
}

func tlsConfigFromOptions(options map[string]any) (*tls.Config, error) {
	certPEM, certOK := options["cert"].(string)
	keyPEM, keyOK := options["key"].(string)
	if !certOK || certPEM == "" || !keyOK || keyPEM == "" {
		return nil, errors.New("secure server requires cert and key options")
	}
	certificate, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}, nil
}

func shutdownServers(runners []httpServerRunner) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, runner := range runners {
		if err := runner.server.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
}

func collectServerErrors(errCh <-chan error, count int) error {
	for i := 0; i < count; i++ {
		err := <-errCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}
