package lark

import (
	"testing"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func TestRegisterDiscovered(t *testing.T) {
	c := &Connector{discovered: map[string]chatRef{}}

	// First sighting of a p2p chat → newly added.
	if !c.registerDiscovered("oc_mai", "ou_mai") {
		t.Fatal("first registerDiscovered should report newly-added=true")
	}
	// Idempotent: a chat already discovered → no-op.
	if c.registerDiscovered("oc_mai", "ou_mai") {
		t.Fatal("duplicate registerDiscovered should report false")
	}
	// A distinct chat → added.
	if !c.registerDiscovered("oc_bob", "ou_bob") {
		t.Fatal("distinct chat should be newly-added")
	}
	// Empty chat id is ignored (never registered).
	if c.registerDiscovered("", "ou_x") {
		t.Fatal("empty chat id must not register")
	}

	if len(c.discovered) != 2 {
		t.Fatalf("want 2 discovered chats, got %d", len(c.discovered))
	}
	ref, ok := c.discovered["oc_mai"]
	if !ok {
		t.Fatal("oc_mai missing from discovered set")
	}
	// Discovered chats are typed as DMs and carry the counterpart open_id for the
	// list_chats display-name enrich.
	if ref.chatType != event.ChatDM {
		t.Fatalf("discovered chat type want dm, got %q", ref.chatType)
	}
	if ref.id != "oc_mai" || ref.p2pTargetID != "ou_mai" {
		t.Fatalf("discovered ref fields wrong: %+v", ref)
	}
}
