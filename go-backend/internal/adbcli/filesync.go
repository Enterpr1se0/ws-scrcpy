package adbcli

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/filelisting"
)

type Dialer interface {
	DialContext(ctx context.Context, network string, address string) (net.Conn, error)
}

type netDialer struct{}

func (netDialer) DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, network, address)
}

type fileSyncSession struct {
	conn net.Conn
}

func (p *Provider) OpenFileListing(ctx context.Context, udid string) (filelisting.Session, error) {
	applog.Debugf("adb sync open udid=%s", udid)
	conn, err := p.dialADB(ctx)
	if err != nil {
		applog.Errorf("adb dial failed udid=%s err=%v", udid, err)
		return nil, err
	}
	if err := sendADBService(ctx, conn, "host:transport:"+udid); err != nil {
		applog.Errorf("adb transport failed udid=%s err=%v", udid, err)
		conn.Close()
		return nil, err
	}
	if err := sendADBService(ctx, conn, "sync:"); err != nil {
		applog.Errorf("adb sync service failed udid=%s err=%v", udid, err)
		conn.Close()
		return nil, err
	}
	return &fileSyncSession{conn: conn}, nil
}



func (p *Provider) dialADB(ctx context.Context) (net.Conn, error) {
	dialer := p.dialer
	if dialer == nil {
		dialer = netDialer{}
	}
	host := p.host
	if host == "" {
		host = "127.0.0.1"
	}
	port := p.port
	if port == 0 {
		port = 5037
	}
	return dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
}

func sendADBService(ctx context.Context, conn net.Conn, service string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(conn, "%04x%s", len(service), service); err != nil {
		return err
	}
	return readADBStatus(conn)
}

func readADBStatus(conn net.Conn) error {
	status := make([]byte, 4)
	if _, err := io.ReadFull(conn, status); err != nil {
		return err
	}
	switch string(status) {
	case "OKAY":
		return nil
	case "FAIL":
		message, err := readADBLengthPrefixedString(conn)
		if err != nil {
			return err
		}
		return errors.New(message)
	default:
		return fmt.Errorf("unexpected adb status %q", string(status))
	}
}

func readADBLengthPrefixedString(conn net.Conn) (string, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return "", err
	}
	length, err := strconv.ParseUint(string(lengthBytes), 16, 32)
	if err != nil {
		return "", err
	}
	message := make([]byte, length)
	if _, err := io.ReadFull(conn, message); err != nil {
		return "", err
	}
	return string(message), nil
}

const (
	// Linux/Android stat mode bits used by adb sync.
	syncModeTypeMask = 0o170000
	syncModeSymlink  = 0o120000
	maxSymlinkDepth  = 5
)

func (s *fileSyncSession) Stat(ctx context.Context, path string) (filelisting.Entry, error) {
	return s.statResolved(ctx, path, 0)
}

func (s *fileSyncSession) statResolved(ctx context.Context, path string, depth int) (filelisting.Entry, error) {
	entry, err := s.statOnce(ctx, path)
	if err != nil {
		return filelisting.Entry{}, err
	}
	if entry.Mode == 0 || !isSyncSymlink(entry.Mode) {
		return entry, nil
	}
	if depth >= maxSymlinkDepth {
		// Preserve the last observed link metadata when resolution is too deep,
		// matching the old Node AdbUtils.stats() fallback behavior.
		return entry, nil
	}
	// adb sync resolves a trailing slash through one symlink hop for directory-like links
	// such as /sdcard -> /storage/self/primary.
	nextPath := path
	if !strings.HasSuffix(nextPath, "/") {
		nextPath += "/"
	}
	resolved, err := s.statResolved(ctx, nextPath, depth+1)
	if err != nil {
		return entry, nil
	}
	if resolved.Mode == 0 {
		return entry, nil
	}
	return resolved, nil
}

func (s *fileSyncSession) statOnce(ctx context.Context, path string) (filelisting.Entry, error) {
	if err := s.writeSyncRequest(ctx, "STAT", path); err != nil {
		return filelisting.Entry{}, err
	}
	header := make([]byte, 16)
	if _, err := io.ReadFull(s.conn, header); err != nil {
		return filelisting.Entry{}, err
	}
	if id := string(header[0:4]); id != "STAT" {
		return filelisting.Entry{}, fmt.Errorf("unexpected sync response %q", id)
	}
	return filelisting.Entry{
		Mode:  binary.LittleEndian.Uint32(header[4:8]),
		Size:  binary.LittleEndian.Uint32(header[8:12]),
		MTime: binary.LittleEndian.Uint32(header[12:16]),
	}, nil
}

func isSyncSymlink(mode uint32) bool {
	return mode&syncModeTypeMask == syncModeSymlink
}

func (s *fileSyncSession) List(ctx context.Context, path string) ([]filelisting.Entry, error) {
	if err := s.writeSyncRequest(ctx, "LIST", path); err != nil {
		return nil, err
	}
	var entries []filelisting.Entry
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(s.conn, header); err != nil {
			return nil, err
		}
		switch id := string(header); id {
		case "DENT":
			entry, err := s.readDENT()
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		case "DONE":
			if err := discardSyncDoneTail(s.conn); err != nil {
				return nil, err
			}
			return entries, nil
		case "FAIL":
			return nil, readSyncFail(s.conn)
		default:
			return nil, fmt.Errorf("unexpected sync response %q", id)
		}
	}
}

func (s *fileSyncSession) Recv(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := s.writeSyncRequest(ctx, "RECV", path); err != nil {
		return nil, err
	}
	return &syncRecvReader{conn: s.conn}, nil
}

func (s *fileSyncSession) Close() error {
	return s.conn.Close()
}

func (s *fileSyncSession) writeSyncRequest(ctx context.Context, command string, path string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	payload := []byte(path)
	request := make([]byte, 8+len(payload))
	copy(request[0:4], command)
	binary.LittleEndian.PutUint32(request[4:8], uint32(len(payload)))
	copy(request[8:], payload)
	_, err := s.conn.Write(request)
	return err
}

func (s *fileSyncSession) readDENT() (filelisting.Entry, error) {
	stat := make([]byte, 16)
	if _, err := io.ReadFull(s.conn, stat); err != nil {
		return filelisting.Entry{}, err
	}
	nameLen := binary.LittleEndian.Uint32(stat[12:16])
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(s.conn, name); err != nil {
		return filelisting.Entry{}, err
	}
	return filelisting.Entry{
		Mode:  binary.LittleEndian.Uint32(stat[0:4]),
		Size:  binary.LittleEndian.Uint32(stat[4:8]),
		MTime: binary.LittleEndian.Uint32(stat[8:12]),
		Name:  string(name),
	}, nil
}

type syncRecvReader struct {
	conn    net.Conn
	pending []byte
	done    bool
}

func (r *syncRecvReader) Read(p []byte) (int, error) {
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}
	for {
		header := make([]byte, 8)
		if _, err := io.ReadFull(r.conn, header); err != nil {
			return 0, err
		}
		size := binary.LittleEndian.Uint32(header[4:8])
		switch id := string(header[0:4]); id {
		case "DATA":
			chunk := make([]byte, size)
			if _, err := io.ReadFull(r.conn, chunk); err != nil {
				return 0, err
			}
			n := copy(p, chunk)
			if n < len(chunk) {
				r.pending = chunk[n:]
			}
			return n, nil
		case "DONE":
			r.done = true
			return 0, io.EOF
		case "FAIL":
			message := make([]byte, size)
			if _, err := io.ReadFull(r.conn, message); err != nil {
				return 0, err
			}
			return 0, errors.New(string(message))
		default:
			return 0, fmt.Errorf("unexpected sync response %q", id)
		}
	}
}

func (r *syncRecvReader) Close() error {
	return nil
}

func readSyncFail(conn net.Conn) error {
	length := make([]byte, 4)
	if _, err := io.ReadFull(conn, length); err != nil {
		return err
	}
	message := make([]byte, binary.LittleEndian.Uint32(length))
	if _, err := io.ReadFull(conn, message); err != nil {
		return err
	}
	return errors.New(string(message))
}

func discardSyncDoneTail(conn net.Conn) error {
	tail := make([]byte, 16)
	_, err := io.ReadFull(conn, tail)
	return err
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
