package main

import (
	"context"
	"strings"
	"testing"
)

func TestRunReturnsConfigLoadError(t *testing.T) {
	err := run(context.Background(), map[string]string{"WS_SCRCPY_CONFIG": "missing.yaml"}, t.TempDir())
	if err == nil {
		t.Fatal("run() error = nil, want config load error")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Fatalf("run() error = %q, want read config error", err.Error())
	}
}
