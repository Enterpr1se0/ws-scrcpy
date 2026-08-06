package httpserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
)

func NewHandler(cfg config.Config, staticFS fs.FS, ws http.Handler) http.Handler {
	pathname := normalizePathname(cfg.Pathname)
	redirectToSecure := firstRedirectToSecure(cfg)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if redirectToSecure.Enabled && r.TLS == nil {
			applog.Infof("http redirect-to-secure remote=%s path=%s", r.RemoteAddr, r.URL.RequestURI())
			redirectSecure(w, r, redirectToSecure)
			return
		}
		if isWebSocketRequest(r) {
			ws.ServeHTTP(w, r)
			return
		}
		applog.Debugf("http %s %s remote=%s", r.Method, r.URL.RequestURI(), r.RemoteAddr)
		requestPath := r.URL.Path
		if pathname != "/" {
			if requestPath != pathname && !strings.HasPrefix(requestPath, pathname+"/") {
				http.NotFound(w, r)
				return
			}
			requestPath = strings.TrimPrefix(requestPath, pathname)
			if requestPath == "" {
				requestPath = "/"
			}
			if requestPath == "/" && r.URL.Path == pathname {
				http.Redirect(w, r, pathname+"/", http.StatusMovedPermanently)
				return
			}
		}
		if staticFS == nil {
			http.NotFound(w, r)
			return
		}
		serveStatic(w, r, staticFS, requestPath)
	})
}

func serveStatic(w http.ResponseWriter, r *http.Request, staticFS fs.FS, requestPath string) {
	if strings.Contains(requestPath, "\\") || strings.Contains(r.URL.EscapedPath(), "\\") || hasParentPathSegment(requestPath) {
		http.NotFound(w, r)
		return
	}

	cleanPath := path.Clean("/" + strings.TrimPrefix(requestPath, "/"))
	name := strings.TrimPrefix(cleanPath, "/")
	if name == "" || name == "." {
		name = "index.html"
	}

	file, info, err := openStatic(staticFS, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if info.IsDir() {
		file.Close()
		indexName := path.Join(name, "index.html")
		if indexName == "index.html" || strings.HasPrefix(indexName, name+"/") {
			file, info, err = openStatic(staticFS, indexName)
			if err != nil || info.IsDir() {
				if file != nil {
					file.Close()
				}
				http.NotFound(w, r)
				return
			}
			name = indexName
		} else {
			http.NotFound(w, r)
			return
		}
	}
	defer file.Close()
	w.Header().Set("Cache-Control", cacheControlFor(name))
	serveFile(w, r, info.Name(), info.ModTime(), file)
}

func openStatic(staticFS fs.FS, name string) (fs.File, fs.FileInfo, error) {
	if name != path.Clean(name) || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
		return nil, nil, fs.ErrNotExist
	}
	file, err := staticFS.Open(name)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, info, nil
}

// cacheControlFor: Vite 产物 assets/ 下的文件名带内容哈希，可永久缓存；
// 其余（index.html、avc.wasm 等未哈希文件）要求每次协商，配合 ETag 命中时返回 304。
func cacheControlFor(name string) string {
	if strings.HasPrefix(name, "assets/") {
		return "public, max-age=31536000, immutable"
	}
	return "no-cache"
}

func serveFile(w http.ResponseWriter, r *http.Request, name string, modTime time.Time, file fs.File) {
	data, err := io.ReadAll(file)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// go:embed 的文件 ModTime 为零值，不发 Last-Modified；用内容哈希 ETag 保证缓存可协商
	sum := sha256.Sum256(data)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	http.ServeContent(w, r, name, modTime, bytes.NewReader(data))
}

func firstRedirectToSecure(cfg config.Config) config.RedirectToSecure {
	for _, server := range cfg.Servers {
		if server.RedirectToSecure.Enabled {
			return server.RedirectToSecure
		}
	}
	return config.RedirectToSecure{}
}

func redirectSecure(w http.ResponseWriter, r *http.Request, redirect config.RedirectToSecure) {
	host := redirect.Host
	if host == "" {
		host = r.Host
	}
	if redirect.Port != 0 && redirect.Port != 443 && !hostHasPort(host) {
		host = fmt.Sprintf("%s:%d", host, redirect.Port)
	}
	target := "https://" + host + r.URL.RequestURI()
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}

func hostHasPort(host string) bool {
	_, _, err := net.SplitHostPort(host)
	if err == nil {
		return true
	}
	return strings.Count(host, ":") == 1
}

func hasParentPathSegment(requestPath string) bool {
	for _, segment := range strings.Split(requestPath, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

func isWebSocketRequest(r *http.Request) bool {
	return headerContains(r.Header.Get("Connection"), "upgrade") && strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func headerContains(value string, token string) bool {
	for _, part := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func normalizePathname(value string) string {
	if value == "" {
		return "/"
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	value = path.Clean(value)
	if value == "." {
		return "/"
	}
	return value
}
