package pipeline

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func msg(id, chat, text string, ts time.Time, src string) event.Message {
	m := event.Message{
		Platform: event.PlatformWhatsApp,
		ChatID:   chat,
		ChatType: event.ChatDM,
		Sender:   event.Sender{PlatformUserID: "u1", DisplayName: "A"},
		TS:       ts,
		Kind:     event.KindText,
		Text:     text,
		Source:   src,
	}
	m.SetID(id)
	return m
}

func TestIngestDedupeAcrossLiveAndBackfill(t *testing.T) {
	s := newTestStore(t)
	var mu sync.Mutex
	var ingested []string
	p := New(s, WithSink(SinkFunc(func(m event.Message) {
		mu.Lock()
		ingested = append(ingested, m.ID)
		mu.Unlock()
	})))
	ctx := context.Background()
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// Same id arrives via backfill then live (the overlap case).
	in1, err := p.Ingest(ctx, msg("X", "whatsapp:c1", "hi", ts, event.SourceBackfill))
	if err != nil || !in1 {
		t.Fatalf("first ingest: %v %v", in1, err)
	}
	in2, err := p.Ingest(ctx, msg("X", "whatsapp:c1", "hi", ts, event.SourceLive))
	if err != nil {
		t.Fatalf("second ingest err: %v", err)
	}
	if in2 {
		t.Fatalf("overlap should dedupe, got inserted=true")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ingested) != 1 {
		t.Fatalf("sink fired %d times, want 1 (only on real insert)", len(ingested))
	}
}

func TestIngestSetsIngestedAt(t *testing.T) {
	s := newTestStore(t)
	fixed := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	p := New(s)
	p.clock = func() time.Time { return fixed }
	ctx := context.Background()

	m := msg("Y", "whatsapp:c1", "yo", time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC), event.SourceLive)
	m.IngestedAt = time.Time{} // unset
	if _, err := p.Ingest(ctx, m); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	got, err := s.Read(ctx, "whatsapp:c1", time.Time{}, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("read: %v %+v", err, got)
	}
	if !got[0].IngestedAt.Equal(fixed) {
		t.Fatalf("ingested_at=%v, want %v", got[0].IngestedAt, fixed)
	}
}

func TestSanitizeRawNeverDropsMessage(t *testing.T) {
	s := newTestStore(t)
	p := New(s)
	ctx := context.Background()
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// A connector emits a message with INVALID JSON in Raw and a UI field —
	// previously this failed json.Marshal in the store and dropped the message.
	m := msg("BAD", "lark:c1", "hello", ts, event.SourceBackfill)
	m.Raw = json.RawMessage("This is not JSON")  // invalid
	m.UI.Card = json.RawMessage("also not json") // invalid
	m.UI.Poll = json.RawMessage(`{"q":"ok"}`)    // valid, must survive
	m.UI.Buttons = []json.RawMessage{
		json.RawMessage(`{"id":"y"}`), // valid
		json.RawMessage("garbage"),    // invalid → dropped
	}

	inserted, err := p.Ingest(ctx, m)
	if err != nil {
		t.Fatalf("ingest must not fail on bad raw: %v", err)
	}
	if !inserted {
		t.Fatal("message should have been ingested")
	}

	got, err := s.Read(ctx, "lark:c1", time.Time{}, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("read: %v %+v", err, got)
	}
	// Raw preserved as a JSON string (data kept, valid JSON).
	var rawStr string
	if err := json.Unmarshal(got[0].Raw, &rawStr); err != nil || rawStr != "This is not JSON" {
		t.Fatalf("Raw not preserved as JSON string: %s (err %v)", got[0].Raw, err)
	}
	// Invalid card → null; valid poll survives; only the valid button kept.
	if string(got[0].UI.Card) != "null" {
		t.Fatalf("UI.Card = %s, want null", got[0].UI.Card)
	}
	if string(got[0].UI.Poll) != `{"q":"ok"}` {
		t.Fatalf("UI.Poll not preserved: %s", got[0].UI.Poll)
	}
	if len(got[0].UI.Buttons) != 1 {
		t.Fatalf("UI.Buttons = %v, want 1 valid kept", got[0].UI.Buttons)
	}
}

func TestResolveEmptyChatID(t *testing.T) {
	s := newTestStore(t)
	p := New(s)
	ctx := context.Background()
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// Original message establishes chat_id.
	orig := msg("M1", "lark:oc_room", "hi there", ts, event.SourceLive)
	if _, err := p.Ingest(ctx, orig); err != nil {
		t.Fatalf("ingest orig: %v", err)
	}

	// A reaction arrives with empty chat_id, only reply_to → must be resolved.
	react := event.Message{
		Platform: event.PlatformLark,
		ChatID:   "", // empty, like Lark reaction events
		Kind:     event.KindReaction,
		ReplyTo:  "telegram:M1-wrong", // simulate: set below correctly
		Sender:   event.Sender{PlatformUserID: "u2", DisplayName: "B"},
		TS:       ts.Add(time.Minute),
		Source:   event.SourceLive,
	}
	react.ReplyTo = orig.ID // points at the original message id
	react.SetID("R1")
	if _, err := p.Ingest(ctx, react); err != nil {
		t.Fatalf("ingest react: %v", err)
	}

	// The reaction should now be readable under the resolved chat_id.
	got, err := s.Read(ctx, "lark:oc_room", time.Time{}, 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var foundReaction bool
	for _, m := range got {
		if m.ID == "lark:R1" {
			foundReaction = true
			if m.ChatID != "lark:oc_room" {
				t.Fatalf("reaction chat_id=%q, want lark:oc_room", m.ChatID)
			}
		}
	}
	if !foundReaction {
		t.Fatalf("reaction not resolved into chat; got %d msgs", len(got))
	}
}

func TestRunDrainsChannel(t *testing.T) {
	s := newTestStore(t)
	p := New(s)
	ctx := context.Background()
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	in := make(chan event.Message, 3)
	in <- msg("A", "whatsapp:c1", "1", ts, event.SourceLive)
	in <- msg("B", "whatsapp:c1", "2", ts.Add(time.Minute), event.SourceLive)
	in <- msg("A", "whatsapp:c1", "1", ts, event.SourceLive) // dup
	close(in)

	if err := p.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}
	got, _ := s.Read(ctx, "whatsapp:c1", time.Time{}, 10)
	if len(got) != 2 {
		t.Fatalf("want 2 unique messages, got %d", len(got))
	}
}
