package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// state is the connector's persisted, restart-safe bookkeeping:
//   - a bidirectional Matrix room_id ↔ canonical chat_id index (the Send path
//     needs chat_id → room_id; ingestion needs room_id → chat_id + network),
//   - per-chat backfill watermarks (newest ingested ts) so live Connect doesn't
//     replay history and Backfill resumes without gaps/dupes.
//
// It is plain JSON on disk (like the beeper connector's state.go) and safe for
// concurrent use. No external deps → fully unit-testable.
type state struct {
	mu   sync.Mutex
	path string

	// rooms maps a Matrix room id → its room record.
	rooms map[string]*roomRecord
	// chatToRoom maps canonical chat_id → Matrix room id (Send routing).
	chatToRoom map[string]string
}

type roomRecord struct {
	RoomID    string `json:"room_id"`
	ChatID    string `json:"chat_id"`
	Network   string `json:"network"` // instagram | messenger
	IsGroup   bool   `json:"is_group"`
	Title     string `json:"title"`
	Watermark int64  `json:"watermark_ms"` // newest ingested ts (unix millis)
}

type stateFile struct {
	Rooms []*roomRecord `json:"rooms"`
}

func newState(path string) *state {
	return &state{
		path:       path,
		rooms:      map[string]*roomRecord{},
		chatToRoom: map[string]string{},
	}
}

// load reads the state file if it exists. A missing file is not an error.
func (s *state) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := privatefs.ReadSecret(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var sf stateFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return err
	}
	for _, r := range sf.Rooms {
		s.rooms[r.RoomID] = r
		if r.ChatID != "" {
			s.chatToRoom[r.ChatID] = r.RoomID
		}
	}
	return nil
}

// save atomically writes the state file (tmp + rename).
func (s *state) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *state) saveLocked() error {
	sf := stateFile{Rooms: make([]*roomRecord, 0, len(s.rooms))}
	for _, r := range s.rooms {
		sf.Rooms = append(sf.Rooms, r)
	}
	b, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return privatefs.WriteFile(s.path, b)
}

// upsertRoom records/updates a portal room's mapping. Returns the record.
func (s *state) upsertRoom(roomID, chatID, network string, isGroup bool, title string) *roomRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rooms[roomID]
	if r == nil {
		r = &roomRecord{RoomID: roomID}
		s.rooms[roomID] = r
	}
	if chatID != "" {
		// Re-point the chat index if the chat id changed.
		if r.ChatID != "" && r.ChatID != chatID {
			delete(s.chatToRoom, r.ChatID)
		}
		r.ChatID = chatID
		s.chatToRoom[chatID] = roomID
	}
	if network != "" {
		r.Network = network
	}
	r.IsGroup = isGroup
	if title != "" {
		r.Title = title
	}
	return r
}

// roomForChat resolves a canonical chat_id → Matrix room id (Send routing).
func (s *state) roomForChat(chatID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rid, ok := s.chatToRoom[chatID]
	return rid, ok
}

// record returns a copy-safe view of a room record by room id.
func (s *state) record(roomID string) (roomRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[roomID]
	if !ok {
		return roomRecord{}, false
	}
	return *r, true
}

// watermark returns the newest ingested ts (millis) for a room, 0 if none.
func (s *state) watermark(roomID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rooms[roomID]; ok {
		return r.Watermark
	}
	return 0
}

// advanceWatermark bumps a room's watermark if ts is newer, persisting the file.
func (s *state) advanceWatermark(roomID string, tsMillis int64) {
	s.mu.Lock()
	r := s.rooms[roomID]
	if r == nil {
		r = &roomRecord{RoomID: roomID}
		s.rooms[roomID] = r
	}
	if tsMillis > r.Watermark {
		r.Watermark = tsMillis
		_ = s.saveLocked()
	}
	s.mu.Unlock()
}

// roomsForNetwork lists room records belonging to a network (for Backfill "all"
// and ChatMetadata enumeration).
func (s *state) roomsForNetwork(network string) []roomRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []roomRecord
	for _, r := range s.rooms {
		if r.Network == network {
			out = append(out, *r)
		}
	}
	return out
}

// touch is a tiny helper used by callers wanting a monotonic-ish save cadence;
// kept simple here (immediate save) — the volume is low (portal count, not msgs).
func (s *state) touch() { _ = s.save() }

var _ = time.Now // reserved: future TTL pruning of stale portals
