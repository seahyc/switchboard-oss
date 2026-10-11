package gmail

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// DefaultStatePath is where the per-account Gmail history cursor is persisted.
func DefaultStatePath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".switchboard", "gmail-state.json")
}

// stateFile is the on-disk shape: account email -> last processed historyId.
// Gmail has no free push; Connect polls users.history.list since this cursor, so
// it must survive restarts to avoid gaps or a full re-scan.
type stateFile struct {
	History map[string]uint64 `json:"history"`
}

// stateStore persists per-account history cursors atomically.
type stateStore struct {
	path string
	mu   sync.Mutex
	sf   stateFile
}

func newStateStore(path string) (*stateStore, error) {
	if path == "" {
		path = DefaultStatePath()
	}
	s := &stateStore{path: path, sf: stateFile{History: map[string]uint64{}}}
	b, err := privatefs.ReadSecret(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil // first run; empty cursors
		}
		return nil, fmt.Errorf("gmail: read state %s: %w", path, err)
	}
	if err := json.Unmarshal(b, &s.sf); err != nil {
		return nil, fmt.Errorf("gmail: parse state %s: %w", path, err)
	}
	if s.sf.History == nil {
		s.sf.History = map[string]uint64{}
	}
	return s, nil
}

// historyID returns the stored cursor for an account (0 if none).
func (s *stateStore) historyID(account string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sf.History[account]
}

// setHistoryID records a new cursor for an account and persists atomically.
func (s *stateStore) setHistoryID(account string, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id <= s.sf.History[account] {
		return nil // never move the cursor backwards
	}
	s.sf.History[account] = id
	return s.saveLocked()
}

func (s *stateStore) saveLocked() error {
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(s.sf, "", "  ")
	if err != nil {
		return err
	}
	return privatefs.WriteFile(s.path, b)
}
