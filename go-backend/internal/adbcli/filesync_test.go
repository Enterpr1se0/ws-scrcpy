package adbcli_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/adbcli"
)

func TestFileSyncOpenSendsTransportAndSyncServices(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		service := readADBService(t, conn)
		if service != "host:transport:device-1" {
			t.Fatalf("first service = %q, want host:transport:device-1", service)
		}
		writeADBOKAY(t, conn)
		service = readADBService(t, conn)
		if service != "sync:" {
			t.Fatalf("second service = %q, want sync:", service)
		}
		writeADBOKAY(t, conn)
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestFileSyncStatEncodesRequestAndParsesResponse(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		acceptSync(t, conn)
		id, path := readSyncRequest(t, conn)
		if id != "STAT" || path != "/sdcard/hello.txt" {
			t.Fatalf("request = %s %q, want STAT /sdcard/hello.txt", id, path)
		}
		writeSyncStat(t, conn, 0100644, 5, 1710000000)
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer session.Close()
	entry, err := session.Stat(context.Background(), "/sdcard/hello.txt")
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	if entry.Mode != 0100644 || entry.Size != 5 || entry.MTime != 1710000000 {
		t.Fatalf("entry = %#v, want mode 0100644 size 5 mtime 1710000000", entry)
	}
}

func TestFileSyncStatFollowsDirectorySymlinkWithTrailingSlash(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		acceptSync(t, conn)
		id, path := readSyncRequest(t, conn)
		if id != "STAT" || path != "/sdcard" {
			t.Fatalf("request = %s %q, want STAT /sdcard", id, path)
		}
		// adb reports /sdcard as a symlink.
		writeSyncStat(t, conn, 0120644, 21, 1230768000)

		id, path = readSyncRequest(t, conn)
		if id != "STAT" || path != "/sdcard/" {
			t.Fatalf("request = %s %q, want STAT /sdcard/", id, path)
		}
		// Trailing slash resolves one hop to a directory.
		writeSyncStat(t, conn, 0040770, 4096, 1710000000)
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer session.Close()
	entry, err := session.Stat(context.Background(), "/sdcard")
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	if entry.Mode != 0040770 || entry.Size != 4096 || entry.MTime != 1710000000 {
		t.Fatalf("entry = %#v, want resolved directory metadata", entry)
	}
}

func TestFileSyncStatStopsAfterMaxSymlinkDepth(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		acceptSync(t, conn)
		// Initial path plus maxSymlinkDepth trailing-slash hops.
		for i := 0; i <= 5; i++ {
			id, path := readSyncRequest(t, conn)
			if id != "STAT" {
				t.Fatalf("request id = %s, want STAT", id)
			}
			if i == 0 && path != "/loop" {
				t.Fatalf("path = %q, want /loop", path)
			}
			if i > 0 && path != "/loop/" {
				t.Fatalf("path = %q, want /loop/", path)
			}
			writeSyncStat(t, conn, 0120777, 1, 100+uint32(i))
		}
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer session.Close()
	entry, err := session.Stat(context.Background(), "/loop")
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	if entry.Mode != 0120777 {
		t.Fatalf("mode = %#o, want symlink mode after depth limit", entry.Mode)
	}
}

func TestFileSyncListParsesDENTUntilDONE(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		acceptSync(t, conn)
		id, path := readSyncRequest(t, conn)
		if id != "LIST" || path != "/sdcard" {
			t.Fatalf("request = %s %q, want LIST /sdcard", id, path)
		}
		writeSyncDent(t, conn, 0040755, 0, 1710000000, ".")
		writeSyncDent(t, conn, 0100644, 11, 1710000001, "hello.txt")
		writeSyncListDone(t, conn)
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer session.Close()
	entries, err := session.List(context.Background(), "/sdcard")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	want := []string{".", "hello.txt"}
	got := []string{entries[0].Name, entries[1].Name}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("names = %#v, want %#v", got, want)
	}
	if entries[1].Mode != 0100644 || entries[1].Size != 11 || entries[1].MTime != 1710000001 {
		t.Fatalf("second entry = %#v", entries[1])
	}
}

func TestFileSyncRecvReadsDATAUntilDONE(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		acceptSync(t, conn)
		id, path := readSyncRequest(t, conn)
		if id != "RECV" || path != "/sdcard/hello.txt" {
			t.Fatalf("request = %s %q, want RECV /sdcard/hello.txt", id, path)
		}
		writeSyncData(t, conn, []byte("hello "))
		writeSyncData(t, conn, []byte("world"))
		writeSyncDone(t, conn)
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	session, err := provider.OpenFileListing(context.Background(), "device-1")
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer session.Close()
	reader, err := session.Recv(context.Background(), "/sdcard/hello.txt")
	if err != nil {
		t.Fatalf("Recv returned error: %v", err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if string(content) != "hello world" {
		t.Fatalf("content = %q, want hello world", content)
	}
}

func TestFileSyncADBFailIncludesReason(t *testing.T) {
	server := newFakeADBServer(t, func(conn net.Conn) {
		_ = readADBService(t, conn)
		writeADBFAIL(t, conn, "device not found")
	})
	provider := adbcli.New(adbcli.WithDialer(server))

	_, err := provider.OpenFileListing(context.Background(), "missing")
	if err == nil || !contains(err.Error(), "device not found") {
		t.Fatalf("Open error = %v, want device not found", err)
	}
}

type fakeADBServer struct {
	addr string
	done chan struct{}
}

func newFakeADBServer(t *testing.T, handler func(net.Conn)) *fakeADBServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	server := &fakeADBServer{addr: listener.Addr().String(), done: make(chan struct{})}
	go func() {
		defer close(server.done)
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()
	t.Cleanup(func() {
		listener.Close()
		select {
		case <-server.done:
		case <-time.After(2 * time.Second):
			t.Fatal("fake adb server did not finish")
		}
	})
	return server
}

func (s *fakeADBServer) DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", s.addr)
}

func acceptSync(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = readADBService(t, conn)
	writeADBOKAY(t, conn)
	_ = readADBService(t, conn)
	writeADBOKAY(t, conn)
}

func readADBService(t *testing.T, conn net.Conn) string {
	t.Helper()
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		t.Fatalf("ReadFull length returned error: %v", err)
	}
	var length int
	if _, err := fmt.Sscanf(string(lengthBytes), "%04x", &length); err != nil {
		t.Fatalf("length %q did not parse: %v", lengthBytes, err)
	}
	service := make([]byte, length)
	if _, err := io.ReadFull(conn, service); err != nil {
		t.Fatalf("ReadFull service returned error: %v", err)
	}
	return string(service)
}

func writeADBOKAY(t *testing.T, conn net.Conn) { t.Helper(); _, _ = conn.Write([]byte("OKAY")) }

func writeADBFAIL(t *testing.T, conn net.Conn, message string) {
	t.Helper()
	_, _ = conn.Write([]byte("FAIL"))
	_, _ = conn.Write([]byte(fmt.Sprintf("%04x", len(message))))
	_, _ = conn.Write([]byte(message))
}

func readSyncRequest(t *testing.T, conn net.Conn) (string, string) {
	t.Helper()
	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("ReadFull sync header returned error: %v", err)
	}
	pathLen := binary.LittleEndian.Uint32(header[4:8])
	path := make([]byte, pathLen)
	if _, err := io.ReadFull(conn, path); err != nil {
		t.Fatalf("ReadFull path returned error: %v", err)
	}
	return string(header[0:4]), string(path)
}

func writeSyncStat(t *testing.T, conn net.Conn, mode uint32, size uint32, mtime uint32) {
	t.Helper()
	out := make([]byte, 16)
	copy(out[0:4], "STAT")
	binary.LittleEndian.PutUint32(out[4:8], mode)
	binary.LittleEndian.PutUint32(out[8:12], size)
	binary.LittleEndian.PutUint32(out[12:16], mtime)
	_, _ = conn.Write(out)
}

func writeSyncDent(t *testing.T, conn net.Conn, mode uint32, size uint32, mtime uint32, name string) {
	t.Helper()
	out := make([]byte, 20+len(name))
	copy(out[0:4], "DENT")
	binary.LittleEndian.PutUint32(out[4:8], mode)
	binary.LittleEndian.PutUint32(out[8:12], size)
	binary.LittleEndian.PutUint32(out[12:16], mtime)
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(name)))
	copy(out[20:], name)
	_, _ = conn.Write(out)
}

func writeSyncData(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	out := make([]byte, 8+len(data))
	copy(out[0:4], "DATA")
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(data)))
	copy(out[8:], data)
	_, _ = conn.Write(out)
}

func writeSyncDone(t *testing.T, conn net.Conn) {
	t.Helper()
	out := make([]byte, 8)
	copy(out[0:4], "DONE")
	_, _ = conn.Write(out)
}

func writeSyncListDone(t *testing.T, conn net.Conn) {
	t.Helper()
	out := make([]byte, 20)
	copy(out[0:4], "DONE")
	_, _ = conn.Write(out)
}

func contains(s string, substr string) bool { return strings.Contains(s, substr) }
