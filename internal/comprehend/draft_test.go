package comprehend

import (
	"context"
	"testing"
	"time"
)

func TestDrafter_GeneratesAndStoresForImportantWaiting(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	addChat(t, r, "telegram:alice", "dm", "Alice", "important")
	addMsg(t, r, "m1", "telegram:alice", "2026-06-11T08:00:00Z", "hey can you review the deck?", "Alice", 0, 0)
	addMsg(t, r, "m2", "telegram:alice", "2026-06-11T09:00:00Z", "by EOD ideally", "Alice", 0, 0)
	// a normal-tier thread should NOT be drafted (gate = important only)
	addChat(t, r, "telegram:bob", "dm", "Bob", "normal")
	addMsg(t, r, "n1", "telegram:bob", "2026-06-11T08:00:00Z", "ping", "Bob", 0, 0)

	now := mustTime("2026-06-12T07:00:00Z")
	proj, err := r.Project(context.Background(), ProjectionConfig{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	local := &fakeModel{replies: []string{"sure, taking a look now — will get it back to you by EOD"}}
	dr := &Drafter{Reader: r, Store: st, Local: local}
	stats, err := dr.GenerateForWaiting(context.Background(), proj.WaitingOnMe)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Considered != 1 || stats.Drafted != 1 {
		t.Fatalf("expected 1 important thread drafted, got %+v", stats)
	}

	// Attach should fill Alice's draft (fresh: last_msg_id matches m2).
	if err := st.AttachDrafts(context.Background(), proj.WaitingOnMe); err != nil {
		t.Fatal(err)
	}
	var alice *WaitingCard
	for i := range proj.WaitingOnMe {
		if proj.WaitingOnMe[i].ChatID == "telegram:alice" {
			alice = &proj.WaitingOnMe[i]
		}
		if proj.WaitingOnMe[i].ChatID == "telegram:bob" && proj.WaitingOnMe[i].Draft != nil {
			t.Error("normal-tier Bob should not have a draft")
		}
	}
	if alice == nil || alice.Draft == nil {
		t.Fatalf("Alice should have a draft attached; card=%+v", alice)
	}
	if *alice.Draft == "" {
		t.Error("draft is empty")
	}
}

func TestAttachDrafts_StaleDraftSuppressed(t *testing.T) {
	st, _ := OpenStore(":memory:")
	defer st.Close()
	// Draft answers an OLD message id.
	if err := st.PutDraft(context.Background(), "telegram:alice", "old-msg", "stale reply", "", "local"); err != nil {
		t.Fatal(err)
	}
	cards := []WaitingCard{{ChatID: "telegram:alice", LastMsgID: "new-msg"}}
	if err := st.AttachDrafts(context.Background(), cards); err != nil {
		t.Fatal(err)
	}
	if cards[0].Draft != nil {
		t.Errorf("stale draft (answers old-msg) must not attach to new-msg card, got %q", *cards[0].Draft)
	}
	// Now matching last_msg_id attaches.
	cards2 := []WaitingCard{{ChatID: "telegram:alice", LastMsgID: "old-msg"}}
	_ = st.AttachDrafts(context.Background(), cards2)
	if cards2[0].Draft == nil || *cards2[0].Draft != "stale reply" {
		t.Errorf("matching draft should attach, got %v", cards2[0].Draft)
	}
}

func TestCleanDraft(t *testing.T) {
	cases := map[string]string{
		"  hello there  ":  "hello there",
		"\"quoted reply\"": "quoted reply",
		"me: actual text":  "actual text",
		"```\nfenced\n```": "fenced",
		"Reply: on my way": "on my way",
		"<think>let me reason about this carefully</think>ofc! lmk 🙏": "ofc! lmk 🙏",
		"<think>reasoning that got truncated":                         "",
	}
	for in, want := range cases {
		if got := cleanDraft(in); got != want {
			t.Errorf("cleanDraft(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDetectLang_Chinese(t *testing.T) {
	zh := []ThreadMsg{{Text: "你好，请问可以帮我看一下吗？", IsSelf: false}}
	if detectLang(zh) != "zh" {
		t.Error("expected zh for Chinese thread")
	}
	en := []ThreadMsg{{Text: "hey can you review this?", IsSelf: false}}
	if detectLang(en) != "" {
		t.Error("expected empty lang for English thread")
	}
	_ = time.Now
}
