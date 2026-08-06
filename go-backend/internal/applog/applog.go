package applog

import (
	"log"
	"os"
	"strings"
	"sync"
)

// Simple process-wide logger with level control via WS_SCRCPY_LOG_LEVEL
// (debug|info|warn|error). Default: info.

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	mu       sync.RWMutex
	minLevel = LevelInfo
	logger   = log.New(os.Stdout, "", log.LstdFlags|log.Lmsgprefix)
)

func init() {
	SetLevelFromEnv(os.Getenv("WS_SCRCPY_LOG_LEVEL"))
}

func SetLevelFromEnv(raw string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug", "trace":
		SetLevel(LevelDebug)
	case "warn", "warning":
		SetLevel(LevelWarn)
	case "error":
		SetLevel(LevelError)
	case "", "info":
		SetLevel(LevelInfo)
	default:
		SetLevel(LevelInfo)
	}
}

func SetLevel(level Level) {
	mu.Lock()
	defer mu.Unlock()
	minLevel = level
}

func enabled(level Level) bool {
	mu.RLock()
	defer mu.RUnlock()
	return level >= minLevel
}

func logf(level Level, prefix string, format string, args ...any) {
	if !enabled(level) {
		return
	}
	logger.SetPrefix(prefix)
	logger.Printf(format, args...)
}

func Debugf(format string, args ...any) { logf(LevelDebug, "[DEBUG] ", format, args...) }
func Infof(format string, args ...any)  { logf(LevelInfo, "[INFO]  ", format, args...) }
func Warnf(format string, args ...any)  { logf(LevelWarn, "[WARN]  ", format, args...) }
func Errorf(format string, args ...any) { logf(LevelError, "[ERROR] ", format, args...) }
