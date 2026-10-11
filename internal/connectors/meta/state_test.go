package meta

import (
	"path/filepath"
	"testing"
)

func TestState_RoundTripAndRouting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta-state.json")

	s := newState(path)
	if err := s.load(); err != nil {
		t.Fatalf("load empty: %v", err)
	}
	s.upsertRoom("!room1:meta.local", "instagram:340", PlatformInstagram, false, "Jane Doe")
	s.upsertRoom("!room2:meta.local", "messenger:777", PlatformMessenger, true, "Family")
	s.advanceWatermark("!room1:meta.local", 1000)
	s.advanceWatermark("!room1:meta.local", 500) // older — must not regress
	if err := s.save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Reload into a fresh instance.
	s2 := newState(path)
	if err := s2.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if rid, ok := s2.roomForChat("instagram:340"); !ok || rid != "!room1:meta.local" {
		t.Errorf("roomForChat = %q,%v", rid, ok)
	}
	if wm := s2.watermark("!room1:meta.local"); wm != 1000 {
		t.Errorf("watermark = %d, want 1000 (no regression)", wm)
	}
	rec, ok := s2.record("!room2:meta.local")
	if !ok || rec.Network != PlatformMessenger || !rec.IsGroup || rec.Title != "Family" {
		t.Errorf("record = %+v ok=%v", rec, ok)
	}
	igRooms := s2.roomsForNetwork(PlatformInstagram)
	if len(igRooms) != 1 || igRooms[0].ChatID != "instagram:340" {
		t.Errorf("roomsForNetwork(instagram) = %+v", igRooms)
	}
}

func TestState_ChatIDRepoint(t *testing.T) {
	s := newState(filepath.Join(t.TempDir(), "s.json"))
	s.upsertRoom("!r:meta.local", "instagram:room:!r:meta.local", PlatformInstagram, false, "")
	// Later the bridge stamps the real remote thread id → chat id changes.
	s.upsertRoom("!r:meta.local", "instagram:REAL", PlatformInstagram, false, "")
	if _, ok := s.roomForChat("instagram:room:!r:meta.local"); ok {
		t.Error("stale chat id should be removed from the index")
	}
	if rid, ok := s.roomForChat("instagram:REAL"); !ok || rid != "!r:meta.local" {
		t.Errorf("repointed chat id not resolvable: %q,%v", rid, ok)
	}
}
