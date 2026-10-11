package telegram

import (
	"testing"

	"github.com/gotd/td/tg"
)

// TestSenderNameCacheFallback proves the #54 fix: a DM message whose own batch does NOT
// carry the sender's user entity still resolves a name/handle via the persistent peer
// cache (populated from any earlier sighting of that user), instead of going blank.
func TestSenderNameCacheFallback(t *testing.T) {
	pc := newPeerCache()
	// An earlier sighting (e.g. from a dialog list) cached the user's identity.
	pc.rememberUser(&tg.User{ID: 555, FirstName: "Casey", LastName: "Example", Username: "caseyexample"})

	// An incoming DM whose batch has NO users[] (the blank-sender case): sender = peer user.
	m := &tg.Message{ID: 9, PeerID: &tg.PeerUser{UserID: 555}, Message: "hi"}
	ix := entityIndex{selfID: 1, cache: pc} // note: users map empty → per-batch miss

	s := ix.senderFromMessage(m)
	if s.PlatformUserID != "555" {
		t.Fatalf("sender id = %q, want 555", s.PlatformUserID)
	}
	if s.DisplayName != "Casey Example" || s.Handle != "caseyexample" {
		t.Fatalf("blank-batch DM sender should resolve from cache: name=%q handle=%q", s.DisplayName, s.Handle)
	}

	// No cache entry → still blank (no panic, graceful).
	m2 := &tg.Message{ID: 10, PeerID: &tg.PeerUser{UserID: 999}, Message: "x"}
	if s2 := ix.senderFromMessage(m2); s2.DisplayName != "" || s2.PlatformUserID != "999" {
		t.Fatalf("uncached sender: name=%q id=%q", s2.DisplayName, s2.PlatformUserID)
	}
}
