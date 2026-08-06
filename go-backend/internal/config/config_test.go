package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(map[string]string{}, t.TempDir())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.Servers) != 1 {
		t.Fatalf("len(Servers) = %d, want 1", len(cfg.Servers))
	}
	if cfg.Servers[0].Secure {
		t.Fatal("default server Secure = true, want false")
	}
	if cfg.Servers[0].Port != 8099 {
		t.Fatalf("default port = %d, want 8099", cfg.Servers[0].Port)
	}
	if cfg.Pathname != "/" {
		t.Fatalf("Pathname = %q, want /", cfg.Pathname)
	}
	if !cfg.RunGoogTracker {
		t.Fatal("RunGoogTracker = false, want true for default INCLUDE_GOOG-compatible behavior")
	}
	if !cfg.AnnounceGoogTracker {
		t.Fatal("AnnounceGoogTracker = false, want true for default INCLUDE_GOOG-compatible behavior")
	}
}

func TestPathnameUsesWSScrcpyPathnameBeforeDefinePathname(t *testing.T) {
	cfg, err := Load(map[string]string{"WS_SCRCPY_PATHNAME": "/runtime", "__PATHNAME__": "/built"}, t.TempDir())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Pathname != "/runtime" {
		t.Fatalf("Pathname = %q, want /runtime", cfg.Pathname)
	}
}

func TestPathnameFallsBackToDefinePathname(t *testing.T) {
	cfg, err := Load(map[string]string{"__PATHNAME__": "/built"}, t.TempDir())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Pathname != "/built" {
		t.Fatalf("Pathname = %q, want /built", cfg.Pathname)
	}
}

func TestLoadYAMLConfigAndFlattenRemoteHostTypes(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte("runGoogTracker: false\nannounceGoogTracker: false\nserver:\n  - secure: false\n    port: 9001\nremoteHostList:\n  - useProxy: true\n    type: [android]\n    secure: true\n    hostname: second.example.com\n    port: 8443\n")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	cfg, err := Load(map[string]string{"WS_SCRCPY_CONFIG": configPath}, dir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.RunGoogTracker {
		t.Fatal("RunGoogTracker = true, want false from YAML")
	}
	if cfg.AnnounceGoogTracker {
		t.Fatal("AnnounceGoogTracker = true, want false from YAML")
	}
	if cfg.Servers[0].Port != 9001 {
		t.Fatalf("server port = %d, want 9001", cfg.Servers[0].Port)
	}
	if len(cfg.RemoteHosts) != 1 {
		t.Fatalf("len(RemoteHosts) = %d, want 1", len(cfg.RemoteHosts))
	}
	host := cfg.RemoteHosts[0]
	if host.Type != "android" || host.Hostname != "second.example.com" || host.Port != 8443 || !host.Secure || !host.UseProxy {
		t.Fatalf("host = %+v", host)
	}
}

func TestHostItemJSONUsesFrontendFieldNames(t *testing.T) {
	data, err := json.Marshal(HostItem{Type: "android", Secure: false, Hostname: "device.example.com", Port: 8000, UseProxy: false})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	jsonText := string(data)
	for _, required := range []string{`"type":"android"`, `"secure":false`, `"hostname":"device.example.com"`, `"port":8000`, `"useProxy":false`} {
		if !strings.Contains(jsonText, required) {
			t.Fatalf("marshaled HostItem = %s, missing %s", jsonText, required)
		}
	}
	if strings.Contains(jsonText, "Type") || strings.Contains(jsonText, "UseProxy") || strings.Contains(jsonText, "Pathname") {
		t.Fatalf("marshaled HostItem uses Go field names: %s", jsonText)
	}
}

func TestSecureServerRequiresOptions(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"server":[{"secure":true,"port":8443}]}`), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	_, err := Load(map[string]string{"WS_SCRCPY_CONFIG": configPath}, dir)
	if err == nil {
		t.Fatal("Load returned nil error for secure server without options")
	}
}

func TestSecureServerReadsCertPathAndKeyPathIntoOptions(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), []byte("certificate contents"), 0o600); err != nil {
		t.Fatalf("WriteFile cert returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "key.pem"), []byte("key contents"), 0o600); err != nil {
		t.Fatalf("WriteFile key returned error: %v", err)
	}
	configPath := filepath.Join(dir, "config.json")
	configJSON := `{"server":[{"secure":true,"port":8443,"options":{"certPath":"cert.pem","keyPath":"key.pem"}}]}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatalf("WriteFile config returned error: %v", err)
	}

	cfg, err := Load(map[string]string{"WS_SCRCPY_CONFIG": configPath}, dir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	options := cfg.Servers[0].Options
	if options["cert"] != "certificate contents" {
		t.Fatalf("cert option = %q, want certificate contents", options["cert"])
	}
	if options["key"] != "key contents" {
		t.Fatalf("key option = %q, want key contents", options["key"])
	}
}

func TestSecureServerRejectsCertPathWithCertAndKeyPathWithKey(t *testing.T) {
	tests := []struct {
		name       string
		configJSON string
	}{
		{name: "cert conflict", configJSON: `{"server":[{"secure":true,"options":{"cert":"inline","certPath":"cert.pem"}}]}`},
		{name: "key conflict", configJSON: `{"server":[{"secure":true,"options":{"key":"inline","keyPath":"key.pem"}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.json")
			if err := os.WriteFile(configPath, []byte(tt.configJSON), 0o600); err != nil {
				t.Fatalf("WriteFile returned error: %v", err)
			}
			_, err := Load(map[string]string{"WS_SCRCPY_CONFIG": configPath}, dir)
			if err == nil {
				t.Fatal("Load returned nil error for conflicting path and inline TLS option")
			}
		})
	}
}

func TestAnnounceGoogTrackerMatchesRunGoogTrackerForNodeCompatibility(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte("runGoogTracker: true\nannounceGoogTracker: false\n")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	cfg, err := Load(map[string]string{"WS_SCRCPY_CONFIG": configPath}, dir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !cfg.AnnounceGoogTracker {
		t.Fatal("AnnounceGoogTracker = false, want true to match Node announceLocalGoogTracker getter")
	}
}
