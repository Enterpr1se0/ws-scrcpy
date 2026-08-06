package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultPort = 8099

type Config struct {
	Pathname            string
	Servers             []ServerItem
	RunGoogTracker      bool
	AnnounceGoogTracker bool
	RemoteHosts         []HostItem
}

type ServerItem struct {
	Secure           bool
	Port             int
	Options          map[string]any
	RedirectToSecure RedirectToSecure
}

type RedirectToSecure struct {
	Enabled bool
	Host    string
	Port    int
}

type HostItem struct {
	Type     string `json:"type" yaml:"type"`
	Secure   bool   `json:"secure" yaml:"secure"`
	Hostname string `json:"hostname" yaml:"hostname"`
	Port     int    `json:"port" yaml:"port"`
	Pathname string `json:"pathname,omitempty" yaml:"pathname,omitempty"`
	UseProxy bool   `json:"useProxy" yaml:"useProxy"`
}

type rawConfig struct {
	Server              []rawServerItem `json:"server" yaml:"server"`
	RunGoogTracker      *bool           `json:"runGoogTracker" yaml:"runGoogTracker"`
	AnnounceGoogTracker *bool           `json:"announceGoogTracker" yaml:"announceGoogTracker"`
	RemoteHostList      []rawHostItem   `json:"remoteHostList" yaml:"remoteHostList"`
}

type rawServerItem struct {
	Secure           bool           `json:"secure" yaml:"secure"`
	Port             int            `json:"port" yaml:"port"`
	Options          map[string]any `json:"options" yaml:"options"`
	RedirectToSecure any            `json:"redirectToSecure" yaml:"redirectToSecure"`
}

type rawHostItem struct {
	Type     any    `json:"type" yaml:"type"`
	Secure   bool   `json:"secure" yaml:"secure"`
	Hostname string `json:"hostname" yaml:"hostname"`
	Port     int    `json:"port" yaml:"port"`
	Pathname string `json:"pathname" yaml:"pathname"`
	UseProxy bool   `json:"useProxy" yaml:"useProxy"`
}

func Load(env map[string]string, cwd string) (Config, error) {
	raw := rawConfig{}
	if configPath := env["WS_SCRCPY_CONFIG"]; configPath != "" {
		loaded, err := readConfig(configPath, cwd)
		if err != nil {
			return Config{}, err
		}
		raw = loaded
	}
	return normalize(raw, env, cwd)
}

func readConfig(pathString string, cwd string) (rawConfig, error) {
	absolutePath := pathString
	if !filepath.IsAbs(pathString) {
		absolutePath = filepath.Join(cwd, pathString)
	}
	data, err := os.ReadFile(absolutePath)
	if err != nil {
		return rawConfig{}, fmt.Errorf("read config %q: %w", absolutePath, err)
	}
	var raw rawConfig
	ext := strings.ToLower(filepath.Ext(absolutePath))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return rawConfig{}, err
		}
	case ".json", ".js":
		if err := json.Unmarshal(data, &raw); err != nil {
			return rawConfig{}, err
		}
	default:
		return rawConfig{}, fmt.Errorf("unknown file type: %s", pathString)
	}
	return raw, nil
}

func normalize(raw rawConfig, env map[string]string, cwd string) (Config, error) {
	pathname := env["WS_SCRCPY_PATHNAME"]
	if pathname == "" {
		pathname = env["__PATHNAME__"]
	}
	if pathname == "" {
		pathname = "/"
	}
	runGoogTracker := true
	if raw.RunGoogTracker != nil {
		runGoogTracker = *raw.RunGoogTracker
	}
	announceGoogTracker := runGoogTracker
	servers := raw.Server
	if len(servers) == 0 {
		servers = []rawServerItem{{Secure: false, Port: defaultPort}}
	}
	normalizedServers := make([]ServerItem, 0, len(servers))
	for _, item := range servers {
		server, err := normalizeServer(item, cwd)
		if err != nil {
			return Config{}, err
		}
		normalizedServers = append(normalizedServers, server)
	}
	remoteHosts, err := normalizeHosts(raw.RemoteHostList)
	if err != nil {
		return Config{}, err
	}
	return Config{
		Pathname:            pathname,
		Servers:             normalizedServers,
		RunGoogTracker:      runGoogTracker,
		AnnounceGoogTracker: announceGoogTracker,
		RemoteHosts:         remoteHosts,
	}, nil
}

func normalizeServer(raw rawServerItem, cwd string) (ServerItem, error) {
	port := raw.Port
	if port == 0 {
		if raw.Secure {
			port = 443
		} else {
			port = 80
		}
	}
	if raw.Secure && raw.Options == nil {
		return ServerItem{}, errors.New("must provide options for secure server configuration")
	}
	options, err := normalizeOptions(raw.Options, cwd)
	if err != nil {
		return ServerItem{}, err
	}
	redirect := normalizeRedirect(raw.RedirectToSecure)
	return ServerItem{Secure: raw.Secure, Port: port, Options: options, RedirectToSecure: redirect}, nil
}

func normalizeOptions(options map[string]any, cwd string) (map[string]any, error) {
	if options == nil {
		return nil, nil
	}
	if certPath, ok := options["certPath"].(string); ok && certPath != "" {
		if _, exists := options["cert"]; exists {
			return nil, errors.New("can't use cert and certPath together")
		}
		cert, err := readTextFile(certPath, cwd)
		if err != nil {
			return nil, err
		}
		options["cert"] = cert
	}
	if keyPath, ok := options["keyPath"].(string); ok && keyPath != "" {
		if _, exists := options["key"]; exists {
			return nil, errors.New("can't use key and keyPath together")
		}
		key, err := readTextFile(keyPath, cwd)
		if err != nil {
			return nil, err
		}
		options["key"] = key
	}
	return options, nil
}

func readTextFile(pathString string, cwd string) (string, error) {
	absolutePath := pathString
	if !filepath.IsAbs(pathString) {
		absolutePath = filepath.Join(cwd, pathString)
	}
	data, err := os.ReadFile(absolutePath)
	if err != nil {
		return "", fmt.Errorf("read file %q: %w", absolutePath, err)
	}
	return string(data), nil
}

func normalizeRedirect(value any) RedirectToSecure {
	if enabled, ok := value.(bool); ok {
		return RedirectToSecure{Enabled: enabled, Port: 443}
	}
	m, ok := value.(map[string]any)
	if !ok {
		return RedirectToSecure{}
	}
	redirect := RedirectToSecure{Enabled: true, Port: 443}
	if host, ok := m["host"].(string); ok {
		redirect.Host = host
	}
	if port, ok := numberAsInt(m["port"]); ok {
		redirect.Port = port
	}
	return redirect
}

func normalizeHosts(rawHosts []rawHostItem) ([]HostItem, error) {
	hosts := make([]HostItem, 0, len(rawHosts))
	for _, raw := range rawHosts {
		types, err := hostTypes(raw.Type)
		if err != nil {
			return nil, err
		}
		for _, typ := range types {
			hosts = append(hosts, HostItem{Type: typ, Secure: raw.Secure, Hostname: raw.Hostname, Port: raw.Port, Pathname: raw.Pathname, UseProxy: raw.UseProxy})
		}
	}
	return hosts, nil
}

func hostTypes(value any) ([]string, error) {
	switch typed := value.(type) {
	case string:
		return []string{typed}, nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("host type item must be string")
			}
			out = append(out, text)
		}
		return out, nil
	case []string:
		return typed, nil
	default:
		return nil, fmt.Errorf("host type must be string or string array")
	}
}

func numberAsInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}
