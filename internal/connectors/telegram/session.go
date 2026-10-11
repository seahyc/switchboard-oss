package telegram

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/gotd/td/session"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// stringSessionStorage implements gotd's session.Storage by persisting the
// MTProto session as a portable base64 STRING in a file. Storing a string (vs
// raw bytes) makes the session easy to back up, move between hosts, or inject
// via env — and satisfies the task's "persist the MTProto session string".
//
// It is concurrency-safe; gotd may load/store from multiple goroutines.
type stringSessionStorage struct {
	path string
	mu   sync.RWMutex
}

var _ session.Storage = (*stringSessionStorage)(nil)

func newStringSessionStorage(path string) *stringSessionStorage {
	return &stringSessionStorage{path: path}
}

// LoadSession returns the stored session bytes, decoding the base64 string on
// disk. A missing file yields session.ErrNotFound so gotd triggers fresh auth.
func (s *stringSessionStorage) LoadSession(_ context.Context) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	raw, err := privatefs.ReadSecret(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, session.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, session.ErrNotFound
	}
	data, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// StoreSession writes the session bytes as a base64 string, atomically.
func (s *stringSessionStorage) StoreSession(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	return privatefs.WriteFile(s.path, []byte(encoded))
}

// String returns the current session as the portable base64 string, or "" if
// none is stored yet.
func (s *stringSessionStorage) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	raw, err := privatefs.ReadSecret(s.path)
	if err != nil {
		return ""
	}
	return string(raw)
}

// Exists reports whether a session string is already persisted.
func (s *stringSessionStorage) Exists() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, err := os.Lstat(s.path)
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}
