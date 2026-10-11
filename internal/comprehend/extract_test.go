package comprehend

import (
	"context"
	"strings"
	"testing"
	"time"
)

// fakeModel is a scripted ChatModel: it returns canned JSON per call, recording
// invocations so escalation behaviour can be asserted without a real LLM.
type fakeModel struct {
	replies []string
	calls   int
}

func (f *fakeModel) Chat(_ context.Context, _, _, _ string, _ bool) (string, error) {
	r := ""
	if f.calls < len(f.replies) {
		r = f.replies[f.calls]
	}
	f.calls++
	return r, nil
}

// errModel always fails (simulates ollama OOM-evicted under the cap).
type errModel struct{ calls int }

func (e *errModel) Chat(_ context.Context, _, _, _ string, _ bool) (string, error) {
	e.calls++
	return "", &netErr{}
}

type netErr struct{}

func (netErr) Error() string { return "ollama /api/chat: status 500: llama-server killed" }

func seedCandidate(t *testing.T, r *Reader, id, chatID, when, text, sender string, isSelf int, importance string) {
	t.Helper()
	if _, err := r.DB().Exec(`INSERT OR IGNORE INTO chats(chat_id,chat_type,title,importance) VALUES (?,?,?,?)`,
		chatID, "dm", "", importance); err != nil {
		t.Fatal(err)
	}
	addMsg(t, r, id, chatID, when, text, sender, isSelf, 0)
}

func TestExtractor_LocalCommitmentPersisted(t *testing.T) {
	r := testReader(t)
	st, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seedCandidate(t, r, "telegram:x:1", "telegram:x", "2026-06-11T07:00:00Z",
		"I'll send you the deck by Friday", "me", 1, "important")

	local := &fakeModel{replies: []string{
		`{"commitments":[{"who":"me","to_whom":"Bob","what":"send the deck","due":"2026-06-13","direction":"mine","confidence":0.9}]}`,
	}}
	ex := &Extractor{Reader: r, Store: st, Local: local, Now: func() time.Time { return mustTime("2026-06-11T08:00:00Z") }}
	stats, err := ex.Run(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Committed != 1 {
		t.Fatalf("want 1 committed, got %+v", stats)
	}
	cs, _ := st.Commitments(context.Background(), CommitmentOpen)
	if len(cs) != 1 {
		t.Fatalf("want 1 commitment row, got %d", len(cs))
	}
	c := cs[0]
	if c.What != "send the deck" || c.Due != "2026-06-13" || c.Direction != DirectionMine {
		t.Errorf("bad commitment: %+v", c)
	}
	if c.SourceMsgID != "telegram:x:1" {
		t.Errorf("provenance lost: %+v", c)
	}
	if c.Extractor != ExtractorLocal {
		t.Errorf("extractor = %q", c.Extractor)
	}
}

func TestExtractor_CacheSkipsReprocessing(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	seedCandidate(t, r, "telegram:x:1", "telegram:x", "2026-06-11T07:00:00Z", "ping", "Bob", 0, "normal")

	local := &fakeModel{replies: []string{`{"commitments":[]}`, `{"commitments":[]}`}}
	ex := &Extractor{Reader: r, Store: st, Local: local, Now: func() time.Time { return mustTime("2026-06-11T08:00:00Z") }}

	if _, err := ex.Run(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	// Reset cursor so the same candidate is re-scanned; the content cache must skip it.
	if err := st.SetCursor(context.Background(), extractScope, time.Time{}); err != nil {
		t.Fatal(err)
	}
	stats, err := ex.Run(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CacheHits != 1 {
		t.Errorf("want 1 cache hit on re-scan, got %+v", stats)
	}
	if local.calls != 1 {
		t.Errorf("model called %d times; cache should have prevented the 2nd", local.calls)
	}
}

func TestExtractor_EscalatesUncertainToSmart(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	seedCandidate(t, r, "telegram:x:1", "telegram:x", "2026-06-11T07:00:00Z",
		"maybe I can look at it tomorrow", "me", 1, "normal")

	// Local is uncertain (0.5 → inside escalate band); smart resolves higher.
	local := &fakeModel{replies: []string{`{"commitments":[{"who":"me","to_whom":"Bob","what":"look at it","due":"","direction":"mine","confidence":0.5}]}`}}
	smart := &fakeModel{replies: []string{`{"commitments":[{"who":"me","to_whom":"Bob","what":"look at it","due":"2026-06-12","direction":"mine","confidence":0.92}]}`}}
	ex := &Extractor{Reader: r, Store: st, Local: local, Smart: smart, Now: func() time.Time { return mustTime("2026-06-11T08:00:00Z") }}
	stats, err := ex.Run(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Escalated != 1 {
		t.Errorf("want 1 escalation, got %+v", stats)
	}
	cs, _ := st.Commitments(context.Background(), "")
	if len(cs) != 1 || cs[0].Extractor != ExtractorSmart || cs[0].Due != "2026-06-12" {
		t.Errorf("smart verdict should win: %+v", cs)
	}
}

func TestBrief_RendersWaitingAndNeverClaimsSent(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:alice", "dm", "Alice", "important")
	addMsg(t, r, "m1", "telegram:alice", "2026-06-10T07:00:00Z", "can you review the deck?", "Alice", 0, 0)

	now := mustTime("2026-06-12T07:00:00Z")
	b, err := r.AssembleBrief(context.Background(), BriefConfig{Now: now}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := b.RenderText(BriefConfig{Now: now})
	if !strings.Contains(out, "Alice") || !strings.Contains(out, "review the deck") {
		t.Errorf("brief missing the waiting thread:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(out), "nothing is sent") {
		t.Errorf("brief must state suggest-only invariant:\n%s", out)
	}
	if len(b.WaitingOnMe) != 1 {
		t.Errorf("brief.WaitingOnMe = %d", len(b.WaitingOnMe))
	}
}

func TestBrief_DueTodayFromCommitments(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	now := mustTime("2026-06-12T03:00:00Z") // 11:00 SGT on the 12th
	_ = st.PutCommitment(context.Background(), Commitment{
		Who: "me", ToWhom: "Bob", What: "send the deck", Due: "2026-06-12",
		Status: CommitmentOpen, SourceMsgID: "telegram:x:1", SourceTS: now,
	})
	b, err := r.AssembleBrief(context.Background(), BriefConfig{Now: now}, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.DueToday) != 1 {
		t.Fatalf("want 1 due-today, got %+v", b.DueToday)
	}
	out := b.RenderText(BriefConfig{Now: now})
	if !strings.Contains(out, "Due today") || !strings.Contains(out, "send the deck") {
		t.Errorf("brief missing due-today section:\n%s", out)
	}
}

func TestExtractor_FallsBackToChatmockWhenLocalFails(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	seedCandidate(t, r, "telegram:user:100000001:1", "telegram:user:100000001", "2026-06-11T07:00:00Z",
		"I'll send Bob the deck by next Tuesday NONCE-42", "me", 1, "important")

	local := &errModel{} // ollama evicted/unavailable
	smart := &fakeModel{replies: []string{`{"commitments":[{"who":"me","to_whom":"Bob","what":"send the deck NONCE-42","due":"2026-06-16","direction":"mine","confidence":0.93}]}`}}
	ex := &Extractor{Reader: r, Store: st, Local: local, Smart: smart, Now: func() time.Time { return mustTime("2026-06-11T08:00:00Z") }}
	stats, err := ex.Run(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Committed != 1 {
		t.Fatalf("fallback should still commit, got %+v (local calls=%d smart calls=%d)", stats, local.calls, smart.calls)
	}
	cs, _ := st.Commitments(context.Background(), "")
	if len(cs) != 1 || cs[0].Extractor != ExtractorSmart {
		t.Fatalf("expected 1 smart-extracted commitment, got %+v", cs)
	}
	if cs[0].What != "send the deck NONCE-42" || cs[0].Due != "2026-06-16" {
		t.Errorf("fallback commitment wrong: %+v", cs[0])
	}
}

func TestBrief_OpenCommitmentSurfacesEvenIfNotDueToday(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	now := mustTime("2026-06-12T03:00:00Z")
	// A commitment captured today, due next Tuesday (NOT today) — G3 needs it in the brief.
	if err := st.PutCommitment(context.Background(), Commitment{
		Who: "me", ToWhom: "Bob", What: "send the deck NONCE-42", Due: "2026-06-16",
		Status: CommitmentOpen, SourceMsgID: "telegram:user:100000001:1", SourceTS: now,
	}); err != nil {
		t.Fatal(err)
	}
	b, err := r.AssembleBrief(context.Background(), BriefConfig{Now: now}, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.OpenCommitments) != 1 {
		t.Fatalf("open commitment missing from brief: %+v", b.OpenCommitments)
	}
	if len(b.DueToday) != 0 {
		t.Errorf("commitment due next Tuesday must NOT be in due-today: %+v", b.DueToday)
	}
	out := b.RenderText(BriefConfig{Now: now})
	if !strings.Contains(out, "NONCE-42") || !strings.Contains(out, "On your plate") {
		t.Errorf("brief must surface the captured commitment nonce:\n%s", out)
	}
}

func TestParseCommitments_TolerantOfFences(t *testing.T) {
	in := "```json\n{\"commitments\":[{\"what\":\"x\",\"confidence\":0.8}]}\n```"
	pc, ok := parseCommitments(in)
	if !ok || len(pc) != 1 || pc[0].What != "x" {
		t.Errorf("tolerant parse failed: ok=%v pc=%+v", ok, pc)
	}
}
