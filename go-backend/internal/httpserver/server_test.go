package httpserver

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
)

func TestServesStaticFileAtRootPathname(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<html>ws-scrcpy</html>")},
	}
	handler := NewHandler(config.Config{Pathname: "/"}, staticFS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("websocket handler should not receive normal static request")
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/index.html", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if recorder.Body.String() != "<html>ws-scrcpy</html>" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestServesIndexForDirectoryRoot(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("root-index")},
	}
	handler := NewHandler(config.Config{Pathname: "/"}, staticFS, http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if recorder.Body.String() != "root-index" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestPathnamePrefixesStaticFiles(t *testing.T) {
	staticDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(staticDir, "index.html"), []byte("prefixed"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	handler := NewHandler(config.Config{Pathname: "/scrcpy"}, os.DirFS(staticDir), http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/scrcpy/index.html", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if recorder.Body.String() != "prefixed" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestRequestOutsidePathnameReturnsNotFound(t *testing.T) {
	handler := NewHandler(config.Config{Pathname: "/scrcpy"}, fstest.MapFS{}, http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/index.html", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}

func TestPathnameDirectoryWithoutTrailingSlashRedirects(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("prefixed")},
	}
	handler := NewHandler(config.Config{Pathname: "/scrcpy"}, staticFS, http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/scrcpy", nil))
	if recorder.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", recorder.Code)
	}
	if location := recorder.Header().Get("Location"); location != "/scrcpy/" {
		t.Fatalf("Location = %q, want /scrcpy/", location)
	}
}

func TestRedirectToSecureReturnsHTTPSLocation(t *testing.T) {
	handler := NewHandler(config.Config{Pathname: "/", Servers: []config.ServerItem{{RedirectToSecure: config.RedirectToSecure{Enabled: true, Host: "secure.example.com", Port: 8443}}}}, fstest.MapFS{}, http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://plain.example.com/scrcpy/?a=1", nil))
	if recorder.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", recorder.Code)
	}
	want := "https://secure.example.com:8443/scrcpy/?a=1"
	if location := recorder.Header().Get("Location"); location != want {
		t.Fatalf("Location = %q, want %q", location, want)
	}
}

func TestStaticPathTraversalIsRejected(t *testing.T) {
	// DirFS rooted at static/ cannot open ../secret.txt as a valid child path.
	// Also reject raw ".." and backslash request paths before Open.
	staticDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(staticDir, "index.html"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	staticFS := os.DirFS(staticDir)

	tests := []struct {
		name string
		path string
	}{
		{name: "dot dot", path: "/../secret.txt"},
		{name: "encoded dot dot", path: "/%2e%2e/secret.txt"},
		{name: "windows backslash", path: "/..\\secret.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(config.Config{Pathname: "/"}, staticFS, http.NotFoundHandler())
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", recorder.Code)
			}
			if recorder.Body.String() == "secret" {
				t.Fatal("served file outside static directory")
			}
		})
	}
}

func TestWebSocketRequestIsDelegated(t *testing.T) {
	called := false
	ws := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	})
	handler := NewHandler(config.Config{Pathname: "/"}, fstest.MapFS{}, ws)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/?action=multiplex", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	handler.ServeHTTP(recorder, request)
	if !called {
		t.Fatal("websocket handler was not called")
	}
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", recorder.Code)
	}
}

func TestStaticCacheHeadersAndConditionalGet(t *testing.T) {
	staticFS := fstest.MapFS{
		"index.html":           &fstest.MapFile{Data: []byte("root-index")},
		"assets/app-abc123.js": &fstest.MapFile{Data: []byte("console.log(1)")},
	}
	handler := NewHandler(config.Config{Pathname: "/"}, staticFS, http.NotFoundHandler())

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("index Cache-Control = %q, want no-cache", got)
	}
	etag := recorder.Header().Get("ETag")
	if etag == "" {
		t.Fatal("index response has no ETag")
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/app-abc123.js", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("asset Cache-Control = %q, want immutable", got)
	}
	if recorder.Header().Get("ETag") == "" {
		t.Fatal("asset response has no ETag")
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("If-None-Match", etag)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotModified {
		t.Fatalf("conditional GET status = %d, want 304", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("304 response has body: %q", recorder.Body.String())
	}
}

// silence unused import if helpers change
var _ fs.FS = fstest.MapFS{}
