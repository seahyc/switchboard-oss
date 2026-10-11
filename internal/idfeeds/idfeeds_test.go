package idfeeds

import (
	"context"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/store"
)

func mustAppend(t *testing.T, st *store.Store, m event.Message, self map[string]bool) {
	t.Helper()
	if _, err := st.Append(context.Background(), m, self); err != nil {
		t.Fatalf("append %s: %v", m.ID, err)
	}
}

func ts(s string) time.Time {
	tm, _ := time.Parse(time.RFC3339, s)
	return tm
}

// buildScenario seeds a small graph:
//   - DM (wa:dm-alice): you send first (you-initiated), Alice replies to you (replied-to-you),
//     Alice mentions you once. Chat pinned.
//   - Group (wa:grp): Alice + Bob both post.
func buildScenario(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	self := map[string]bool{"wa:me": true}
	me := event.Sender{PlatformUserID: "wa:me", DisplayName: "Me", IsSelf: true}
	alice := event.Sender{PlatformUserID: "wa:alice", DisplayName: "Alice", Handle: "alice99"}
	bob := event.Sender{PlatformUserID: "wa:bob", DisplayName: "Bob"}

	// DM: you first, then Alice replies to you + mentions you.
	mustAppend(t, st, event.Message{ID: "whatsapp:1", Platform: "whatsapp", ChatID: "wa:dm-alice", ChatType: "dm", Sender: me, TS: ts("2026-06-01T10:00:00Z"), Kind: "text", Text: "hey alice"}, self)
	mustAppend(t, st, event.Message{ID: "whatsapp:2", Platform: "whatsapp", ChatID: "wa:dm-alice", ChatType: "dm", Sender: alice, TS: ts("2026-06-01T10:05:00Z"), Kind: "text", Text: "hi! how are you", ReplyTo: "whatsapp:1", Mentions: []string{"wa:me"}}, self)
	mustAppend(t, st, event.Message{ID: "whatsapp:3", Platform: "whatsapp", ChatID: "wa:dm-alice", ChatType: "dm", Sender: alice, TS: ts("2026-06-01T10:06:00Z"), Kind: "text", Text: "lunch tomorrow?"}, self)

	// Group: Alice + Bob.
	mustAppend(t, st, event.Message{ID: "whatsapp:4", Platform: "whatsapp", ChatID: "wa:grp", ChatType: "group", Sender: alice, TS: ts("2026-06-02T09:00:00Z"), Kind: "text", Text: "morning team"}, self)
	mustAppend(t, st, event.Message{ID: "whatsapp:5", Platform: "whatsapp", ChatID: "wa:grp", ChatType: "group", Sender: bob, TS: ts("2026-06-02T09:01:00Z"), Kind: "text", Text: "morning"}, self)

	// Pin the DM (meta upsert after messages, so chat_type stays 'dm').
	if err := st.UpsertChatMeta(context.Background(), store.ChatMeta{ChatID: "wa:dm-alice", Platform: "whatsapp", Pinned: true}); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestPersonStatsFeed(t *testing.T) {
	st := buildScenario(t)
	defer st.Close()
	f := NewIdentityFeeds(st.DB())
	stats, err := f.PersonStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var alice *struct {
		MsgCount, DM, Grp, Yours, Init, Ment int
		HasDM, Replied, Pinned               bool
	}
	for _, s := range stats {
		if s.SenderID == "wa:alice" {
			alice = &struct {
				MsgCount, DM, Grp, Yours, Init, Ment int
				HasDM, Replied, Pinned               bool
			}{s.MsgCount, s.DMMsgCount, s.GroupMsgCount, s.YourMsgCount, s.YouInitiated, s.MentionsYou, s.HasDM, s.RepliedToYou, s.DMPinned}
		}
	}
	if alice == nil {
		t.Fatal("alice not in stats")
	}
	if alice.MsgCount != 3 || alice.DM != 2 || alice.Grp != 1 {
		t.Errorf("alice counts: msg=%d dm=%d grp=%d (want 3/2/1)", alice.MsgCount, alice.DM, alice.Grp)
	}
	if !alice.HasDM || !alice.Pinned {
		t.Errorf("alice should have a pinned DM: hasDM=%v pinned=%v", alice.HasDM, alice.Pinned)
	}
	if !alice.Replied {
		t.Error("alice replied to your message → RepliedToYou should be true")
	}
	if alice.Init < 1 {
		t.Errorf("you started the DM → YouInitiated >=1, got %d", alice.Init)
	}
	if alice.Ment != 1 {
		t.Errorf("alice mentioned you once, got %d", alice.Ment)
	}
	// Your messages in chats Alice participates in: the 1 DM message (you sent 0 in group).
	if alice.Yours != 1 {
		t.Errorf("YourMsgCount in Alice's chats = %d, want 1", alice.Yours)
	}
}

func TestIdentityContextFeed(t *testing.T) {
	st := buildScenario(t)
	defer st.Close()
	f := NewIdentityFeeds(st.DB())
	ctxs, err := f.IdentityContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var alice *gbContext
	for i := range ctxs {
		if ctxs[i].SenderID == "wa:alice" {
			alice = &gbContext{ctxs[i].Handles, ctxs[i].ChatIDs, ctxs[i].Snippets}
		}
	}
	if alice == nil {
		t.Fatal("alice not in identity context")
	}
	if len(alice.Handles) != 1 || alice.Handles[0] != "alice99" {
		t.Errorf("alice handle = %v, want [alice99]", alice.Handles)
	}
	// Only the GROUP chat is a shared-group block (DM excluded).
	if len(alice.ChatIDs) != 1 || alice.ChatIDs[0] != "wa:grp" {
		t.Errorf("alice chat ids = %v, want [wa:grp] (DM excluded)", alice.ChatIDs)
	}
	if len(alice.Snippets) != 3 {
		t.Errorf("alice should have 3 snippets, got %d: %v", len(alice.Snippets), alice.Snippets)
	}
}

type gbContext struct {
	Handles  []string
	ChatIDs  []string
	Snippets []string
}

