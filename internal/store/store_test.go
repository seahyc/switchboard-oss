package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func mustOpen(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mkMsg(platform, msgID, chat, text string, ts time.Time) event.Message {
	m := event.Message{
		Platform:  platform,
		AccountID: platform + ":self",
		ChatID:    chat,
		ChatType:  event.ChatDM,
		Sender:    event.Sender{PlatformUserID: "u1", DisplayName: "Alice", Phone: "+65123"},
		TS:        ts,
		Kind:      event.KindText,
		Text:      text,
		Source:    event.SourceLive,
		Raw:       json.RawMessage(`{}`),
	}
	m.SetID(msgID)
	return m
}

func TestAppendIdempotent(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	m := mkMsg(event.PlatformWhatsApp, "AAA", "whatsapp:c1", "hello", ts)

	r1, err := s.Append(ctx, m, nil)
	if err != nil || !r1.Inserted {
		t.Fatalf("first append: inserted=%v err=%v", r1.Inserted, err)
	}
	// Re-append same id (replay/overlap) — must be a no-op.
	r2, err := s.Append(ctx, m, nil)
	if err != nil {
		t.Fatalf("second append err: %v", err)
	}
	if r2.Inserted {
		t.Fatalf("expected idempotent no-op on duplicate id")
	}

	// Exactly one log row and one message row.
	var logN, msgN int
	s.db.QueryRow(`SELECT COUNT(*) FROM event_log`).Scan(&logN)
	s.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&msgN)
	if logN != 1 || msgN != 1 {
		t.Fatalf("dup leaked: log=%d msg=%d", logN, msgN)
	}
	// chats.msg_count must not double-count.
	var cnt int
	s.db.QueryRow(`SELECT msg_count FROM chats WHERE chat_id='whatsapp:c1'`).Scan(&cnt)
	if cnt != 1 {
		t.Fatalf("chat msg_count=%d, want 1", cnt)
	}
}

func TestCursorAdvances(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "old", t0), nil)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "B", "whatsapp:c1", "new", t1), nil)

	wm, lastID, err := s.Cursor(ctx, "whatsapp:c1")
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if !wm.Equal(t1) {
		t.Fatalf("watermark=%v, want %v", wm, t1)
	}
	if lastID != "whatsapp:B" {
		t.Fatalf("lastID=%q, want whatsapp:B", lastID)
	}

	// Backfill an OLDER message — watermark must not regress.
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "Z", "whatsapp:c1", "older", t0.Add(-time.Hour)), nil)
	wm2, lastID2, _ := s.Cursor(ctx, "whatsapp:c1")
	if !wm2.Equal(t1) || lastID2 != "whatsapp:B" {
		t.Fatalf("watermark regressed: %v %q", wm2, lastID2)
	}
}

func TestSearchAndRead(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "lunch tomorrow?", t0), nil)
	s.Append(ctx, mkMsg(event.PlatformTelegram, "B", "telegram:c2", "dinner plans", t0.Add(time.Minute)), nil)

	hits, err := s.Search(ctx, "lunch", "", "", time.Time{}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "whatsapp:A" {
		t.Fatalf("search lunch -> %+v", hits)
	}

	// platform filter
	hits, _ = s.Search(ctx, "dinner", event.PlatformWhatsApp, "", time.Time{}, 10)
	if len(hits) != 0 {
		t.Fatalf("platform filter failed: %+v", hits)
	}

	msgs, err := s.Read(ctx, "whatsapp:c1", time.Time{}, 10)
	if err != nil || len(msgs) != 1 || msgs[0].Text != "lunch tomorrow?" {
		t.Fatalf("read: %v %+v", err, msgs)
	}
}

// TestSearchSanitizesFTS reproduces the agent-log failures (#: fts5 syntax error,
// no such column) where raw queries with punctuation / FTS5 keywords / column
// syntax were passed straight into MATCH. After sanitization none of these error.
func TestSearchSanitizesFTS(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "ping me at user@example.com about the invoice", t0), nil)
	s.Append(ctx, mkMsg(event.PlatformTelegram, "B", "telegram:c2", "the report is from Casey", t0.Add(time.Minute)), nil)

	for _, q := range []string{
		"user@example.com", // '@' -> fts5 syntax error (was 500)
		"report?",          // '?'
		"from",             // bareword that parsed as a column
		"msgs from:casey",  // 'col:term' column-filter syntax
		"what about @x?",   // mixed punctuation
		"???",              // pure punctuation -> falls back to no-MATCH scan
		"",                 // empty -> no-MATCH scan
	} {
		if _, err := s.Search(ctx, q, "", "", time.Time{}, 10); err != nil {
			t.Fatalf("Search(%q) errored (should be sanitized): %v", q, err)
		}
	}

	// Still actually matches: the ecaseyl query finds its message.
	hits, err := s.Search(ctx, "user@example.com", "", "", time.Time{}, 10)
	if err != nil {
		t.Fatalf("search ecaseyl: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "whatsapp:A" {
		t.Fatalf("ecaseyl search -> %+v", hits)
	}
}

func TestFtsQuote(t *testing.T) {
	cases := []struct{ in, want string }{
		{"hello world", `"hello" "world"`},
		{"user@x.com", `"user@x.com"`},
		{"from:casey", `"from:casey"`},
		{`he said "hi"`, `"he" "said" """hi"""`},
		{"???", ""},
		{"", ""},
		{"   ", ""},
	}
	for _, c := range cases {
		if got := ftsQuote(c.in); got != c.want {
			t.Errorf("ftsQuote(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSearchBySender(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	casey1 := mkMsg(event.PlatformLark, "M1", "lark:dm", "older note from casey", t0)
	casey1.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	casey2 := mkMsg(event.PlatformLark, "M2", "lark:dm", "newer note about ExampleVendor audit", t0.Add(2*time.Hour))
	casey2.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	other := mkMsg(event.PlatformLark, "O1", "lark:dm", "ExampleVendor mention from someone else", t0.Add(time.Hour))
	other.Sender = event.Sender{PlatformUserID: "ou_other"}
	for _, m := range []event.Message{casey1, casey2, other} {
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	refs := []SenderRef{{Platform: event.PlatformLark, SenderID: "ou_counterparty"}}

	// person-only (NO query): Casey's messages newest-first, excluding other senders.
	hits, err := s.SearchBySender(ctx, "", "", refs, time.Time{}, 10)
	if err != nil || len(hits) != 2 {
		t.Fatalf("person-only: want 2 of casey's, got %d (%v)", len(hits), err)
	}
	if hits[0].ID != "lark:M2" {
		t.Fatalf("person-only must be newest-first (M2 then M1), got %s first", hits[0].ID)
	}

	// query + person: FTS author-filtered — "ExampleVendor" from Casey is only M2, NOT other's.
	hits, err = s.SearchBySender(ctx, "ExampleVendor", "", refs, time.Time{}, 10)
	if err != nil || len(hits) != 1 || hits[0].ID != "lark:M2" {
		t.Fatalf("query+person: want only casey's M2, got %+v (%v)", hits, err)
	}

	// empty sender set → no rows (person resolved to nobody).
	if h, _ := s.SearchBySender(ctx, "", "", nil, time.Time{}, 10); len(h) != 0 {
		t.Fatalf("empty senders must be 0, got %d", len(h))
	}
}

func TestRecentChatsForSenders(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// Casey (ou_counterparty) is active in two chats: a p2p DM (oc_dm) and a group (oc_grp).
	// Her latest p2p message is the newest of all → must sort first (the sendable
	// chat pointer). A different sender in oc_grp must NOT leak in.
	dmOld := mkMsg(event.PlatformLark, "D1", "lark:oc_dm", "older dm note", t0)
	dmOld.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	grp := mkMsg(event.PlatformLark, "G1", "lark:oc_grp", "group ping", t0.Add(time.Hour))
	grp.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	grp.ChatType = event.ChatGroup
	dmNew := mkMsg(event.PlatformLark, "D2", "lark:oc_dm", "latest dm: the ExampleVendor contact is Jordan", t0.Add(3*time.Hour))
	dmNew.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	noise := mkMsg(event.PlatformLark, "N1", "lark:oc_grp", "someone else newest", t0.Add(5*time.Hour))
	noise.Sender = event.Sender{PlatformUserID: "ou_other"}
	for _, m := range []event.Message{dmOld, grp, dmNew, noise} {
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	refs := []SenderRef{{Platform: event.PlatformLark, SenderID: "ou_counterparty"}}

	chats, err := s.RecentChatsForSenders(ctx, refs, 10)
	if err != nil {
		t.Fatalf("RecentChatsForSenders: %v", err)
	}
	if len(chats) != 2 {
		t.Fatalf("want 2 chats Casey is active in (dm+grp), got %d: %+v", len(chats), chats)
	}
	// Newest-first: her latest message is in oc_dm (t0+3h) → it leads, and is the
	// directly-sendable pointer.
	if chats[0].ChatID != "lark:oc_dm" {
		t.Fatalf("newest-first: want oc_dm first, got %s", chats[0].ChatID)
	}
	// Snippet = her most-recent text in that chat (D2), not the older D1.
	if chats[0].Snippet != "latest dm: the ExampleVendor contact is Jordan" {
		t.Fatalf("snippet must be Casey's latest in the chat, got %q", chats[0].Snippet)
	}
	// last_ts is her own last message there (t0+3h), NOT the noise sender's t0+5h.
	if !chats[0].LastTS.Equal(t0.Add(3 * time.Hour)) {
		t.Fatalf("last_ts must be Casey's last (t0+3h), got %v", chats[0].LastTS)
	}
	if chats[0].ChatType != event.ChatDM || !chats[0].IsDM {
		t.Fatalf("oc_dm want chat_type=dm & IsDM=true, got type=%q IsDM=%v", chats[0].ChatType, chats[0].IsDM)
	}
	if chats[1].ChatID != "lark:oc_grp" {
		t.Fatalf("second chat want oc_grp, got %s", chats[1].ChatID)
	}
	// oc_grp has 2 distinct senders but NEITHER is self → a real group, IsDM=false.
	if chats[1].ChatType != event.ChatGroup || chats[1].IsDM {
		t.Fatalf("oc_grp want chat_type=group & IsDM=false, got type=%q IsDM=%v", chats[1].ChatType, chats[1].IsDM)
	}

	// limit caps the result.
	if one, _ := s.RecentChatsForSenders(ctx, refs, 1); len(one) != 1 || one[0].ChatID != "lark:oc_dm" {
		t.Fatalf("limit=1 must return only the newest chat, got %+v", one)
	}
	// empty sender set → no rows.
	if z, _ := s.RecentChatsForSenders(ctx, nil, 10); len(z) != 0 {
		t.Fatalf("empty senders must be 0, got %d", len(z))
	}
}

// TestRecentChatsMislabeledDM covers the #31/#35 title-gap: a genuine 1:1 stored
// with chat_type="group". Cardinality —
// exactly 2 distinct senders, one self — must still flag it IsDM so the resolve
// layer titles it by the counterparty, despite the wrong stored label.
func TestRecentChatsMislabeledDM(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	self := map[string]bool{"ou_self": true}

	// A 1:1 between a counterparty and self, but MISLABELED chat_type="group" (the #31 defect).
	mine := mkMsg(event.PlatformLark, "S1", "lark:oc_mislabel", "hi counterparty", t0)
	mine.Sender = event.Sender{PlatformUserID: "ou_self", IsSelf: true}
	mine.ChatType = event.ChatGroup
	hers := mkMsg(event.PlatformLark, "H1", "lark:oc_mislabel", "hi self", t0.Add(time.Hour))
	hers.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	hers.ChatType = event.ChatGroup
	for _, m := range []event.Message{mine, hers} {
		if _, err := s.Append(ctx, m, self); err != nil {
			t.Fatal(err)
		}
	}

	chats, err := s.RecentChatsForSenders(ctx, []SenderRef{{Platform: event.PlatformLark, SenderID: "ou_counterparty"}}, 10)
	if err != nil || len(chats) != 1 {
		t.Fatalf("want 1 chat, got %d (%v)", len(chats), err)
	}
	// Stored label is "group", but cardinality (2 senders, one self) → IsDM true.
	if chats[0].ChatType != event.ChatGroup {
		t.Fatalf("precondition: stored chat_type must be the mislabel group, got %q", chats[0].ChatType)
	}
	if !chats[0].IsDM {
		t.Fatalf("mislabeled 1:1 (2 distinct senders incl self) must be IsDM=true")
	}
}

func TestGrep(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	m1 := mkMsg(event.PlatformLark, "L1", "lark:dm", "ship invoice INV-2026-0042 by friday", t0)
	m1.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	m2 := mkMsg(event.PlatformLark, "L2", "lark:dm", "no numbers here", t0.Add(time.Hour))
	m2.Sender = event.Sender{PlatformUserID: "ou_counterparty"}
	m3 := mkMsg(event.PlatformWhatsApp, "W1", "wa:c", "invoice INV-2026-0099 paid", t0.Add(2*time.Hour))
	m3.Sender = event.Sender{PlatformUserID: "u_bob"}
	for _, m := range []event.Message{m1, m2, m3} {
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Regex over the whole firehose: the invoice-id pattern matches m1 + m3, newest-first.
	r, err := s.Grep(ctx, `INV-\d{4}-\d{4}`, "", nil, time.Time{}, 0)
	if err != nil || len(r.Matches) != 2 {
		t.Fatalf("grep all: want 2, got %d (%v)", len(r.Matches), err)
	}
	if r.Matches[0].ID != "whatsapp:W1" {
		t.Fatalf("grep must be newest-first, got %s first", r.Matches[0].ID)
	}
	if r.Truncated {
		t.Fatalf("small corpus must not report truncated")
	}

	// platform filter narrows to Lark only → just m1.
	r, _ = s.Grep(ctx, `INV-\d{4}-\d{4}`, event.PlatformLark, nil, time.Time{}, 0)
	if len(r.Matches) != 1 || r.Matches[0].ID != "lark:L1" {
		t.Fatalf("grep platform=lark: want only L1, got %+v", r.Matches)
	}

	// author filter (#32 sender set) → only Casey's match (L1), not Bob's W1.
	refs := []SenderRef{{Platform: event.PlatformLark, SenderID: "ou_counterparty"}}
	r, _ = s.Grep(ctx, `INV-\d{4}-\d{4}`, "", refs, time.Time{}, 0)
	if len(r.Matches) != 1 || r.Matches[0].ID != "lark:L1" {
		t.Fatalf("grep person: want only Casey's L1, got %+v", r.Matches)
	}

	// inline case-insensitive flag (RE2).
	if r, _ := s.Grep(ctx, `(?i)FRIDAY`, "", nil, time.Time{}, 0); len(r.Matches) != 1 || r.Matches[0].ID != "lark:L1" {
		t.Fatalf("grep (?i): want L1, got %+v", r.Matches)
	}

	// limit caps results (and a limit hit is NOT truncation).
	if r, _ := s.Grep(ctx, `INV-\d{4}-\d{4}`, "", nil, time.Time{}, 1); len(r.Matches) != 1 || r.Truncated {
		t.Fatalf("grep limit=1: want 1 match & not truncated, got %d truncated=%v", len(r.Matches), r.Truncated)
	}

	// invalid pattern → clean error.
	if _, err := s.Grep(ctx, `(unclosed`, "", nil, time.Time{}, 0); err == nil {
		t.Fatal("grep with invalid pattern must error")
	}
}

func TestCoverage(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// Two WhatsApp messages in one chat (Append upserts the chat index too), one Lark.
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "hi", t0), nil)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "B", "whatsapp:c1", "bye", t0.Add(2*time.Hour)), nil)
	s.Append(ctx, mkMsg(event.PlatformLark, "L", "lark:oc1", "yo", t0.Add(time.Hour)), nil)
	// A known-but-empty "shell" chat (in the index, no messages) — the #13 gap signal.
	if err := s.UpsertChatMeta(ctx, ChatMeta{ChatID: "lark:oc_shell", Platform: event.PlatformLark, Title: "Empty"}); err != nil {
		t.Fatalf("upsert shell: %v", err)
	}

	cov, err := s.Coverage(ctx)
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	by := map[string]PlatformCoverage{}
	for _, c := range cov {
		by[c.Platform] = c
	}
	wa := by[event.PlatformWhatsApp]
	if wa.Messages != 2 || wa.ChatsWithMessages != 1 || wa.ChatsKnown != 1 || wa.ChatsEmpty != 0 {
		t.Fatalf("whatsapp coverage wrong: %+v", wa)
	}
	if !wa.OldestTS.Equal(t0) || !wa.NewestTS.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("whatsapp depth wrong: oldest=%v newest=%v", wa.OldestTS, wa.NewestTS)
	}
	lk := by[event.PlatformLark]
	// Lark: 2 known chats (oc1 from the message + oc_shell), 1 with messages → 1 empty.
	if lk.Messages != 1 || lk.ChatsWithMessages != 1 || lk.ChatsKnown != 2 || lk.ChatsEmpty != 1 {
		t.Fatalf("lark coverage wrong (want 1 shell chat): %+v", lk)
	}
}

// TestSocialActivitySubFilter locks the #49 read path: SocialActivity returns
// social_activity events newest-first, and the optional subs filter restricts to
// given Social.Sub values (the briefing passes post,story to exclude identity-only
// follow rows) via json_extract over the body.
func TestSocialActivitySubFilter(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC)

	mkSocial := func(id, sub string, ts time.Time) event.Message {
		m := event.Message{
			Platform: event.PlatformInstagram,
			ChatID:   "instagram:feed:1",
			ChatType: event.ChatFeed,
			Sender:   event.Sender{PlatformUserID: "1", Handle: "a"},
			TS:       ts,
			Kind:     event.KindSocialActivity,
			Source:   event.SourceLive,
			Social:   &event.Social{Sub: sub},
			Raw:      json.RawMessage(`{}`),
		}
		m.SetID(id)
		return m
	}
	if _, err := s.Append(ctx, mkSocial("post:p1", "post", t0), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, mkSocial("story:s1", "story", t0.Add(time.Hour)), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, mkSocial("follow:f1", "follow", t0.Add(2*time.Hour)), nil); err != nil {
		t.Fatal(err)
	}

	// No filter → all three, newest-first.
	all, err := s.SocialActivity(ctx, "", time.Time{}, 100)
	if err != nil || len(all) != 3 {
		t.Fatalf("all: n=%d err=%v", len(all), err)
	}
	if all[0].Social.Sub != "follow" || all[2].Social.Sub != "post" {
		t.Fatalf("ordering not newest-first: %q..%q", all[0].Social.Sub, all[2].Social.Sub)
	}
	// Briefing filter → posts + stories only, no follow.
	brief, err := s.SocialActivity(ctx, "", time.Time{}, 100, "post", "story")
	if err != nil || len(brief) != 2 {
		t.Fatalf("briefing: n=%d err=%v", len(brief), err)
	}
	for _, m := range brief {
		if m.Social.Sub == "follow" {
			t.Fatalf("follow leaked into briefing: %+v", m.Social)
		}
	}
	// Platform filter: these rows are all instagram → x filter returns none, instagram all.
	if x, err := s.SocialActivity(ctx, "x", time.Time{}, 100); err != nil || len(x) != 0 {
		t.Fatalf("platform=x should match none: n=%d err=%v", len(x), err)
	}
	if ig, err := s.SocialActivity(ctx, event.PlatformInstagram, time.Time{}, 100); err != nil || len(ig) != 3 {
		t.Fatalf("platform=instagram should match all 3: n=%d err=%v", len(ig), err)
	}
}

func TestBackfillCandidates(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	mk := func(chatID, imp string, ts time.Time) {
		m := mkMsg(event.PlatformTelegram, "id-"+chatID, chatID, "x", ts)
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE chats SET importance=? WHERE chat_id=?`, imp, chatID); err != nil {
			t.Fatal(err)
		}
	}
	mk("telegram:imp", "important", t0.Add(time.Hour))
	mk("telegram:norm", "normal", t0.Add(3*time.Hour))
	mk("telegram:unscored", "", t0.Add(2*time.Hour))
	mk("telegram:low1", "low", t0)
	mk("telegram:low2", "low", t0.Add(time.Minute))

	// Candidates: ALL tiers swept, ordered important → normal → unscored → low (low
	// last, NOT excluded — importance is under-populated, user wants maximal ingestion).
	cands, err := s.BackfillCandidates(ctx, event.PlatformTelegram, 10)
	if err != nil || len(cands) != 5 {
		t.Fatalf("candidates: want 5 (all tiers, low last), got %d (%v)", len(cands), err)
	}
	if cands[0].ChatID != "telegram:imp" || cands[1].ChatID != "telegram:norm" || cands[2].ChatID != "telegram:unscored" {
		t.Fatalf("salience order wrong: %v", []string{cands[0].ChatID, cands[1].ChatID, cands[2].ChatID})
	}
	// The two 'low' chats sort LAST (after unscored), newest-first within the tier.
	if cands[3].ChatID != "telegram:low2" || cands[4].ChatID != "telegram:low1" {
		t.Fatalf("low tier must sort last (newest-first): %v", []string{cands[3].ChatID, cands[4].ChatID})
	}

	// limit caps to the top-salience slice.
	if c, _ := s.BackfillCandidates(ctx, event.PlatformTelegram, 1); len(c) != 1 || c[0].ChatID != "telegram:imp" {
		t.Fatalf("limit=1 must return the top candidate, got %+v", c)
	}

	// low-tier gated stats: 2 low chats + a sample.
	n, sample, err := s.LowTierBackfillStats(ctx, event.PlatformTelegram, 10)
	if err != nil || n != 2 || len(sample) != 2 {
		t.Fatalf("low-tier stats: want 2+2, got n=%d sample=%v (%v)", n, sample, err)
	}

	// Platform scoping: a lark-scoped candidate query returns ZERO telegram candidates
	// (#13 — a scoped sweep must never pull another platform's chats → TG flood).
	larkMsg := mkMsg(event.PlatformLark, "Lc1", "lark:oc_a", "hi", t0)
	if _, err := s.Append(ctx, larkMsg, nil); err != nil {
		t.Fatal(err)
	}
	larkCands, _ := s.BackfillCandidates(ctx, event.PlatformLark, 50)
	if len(larkCands) == 0 {
		t.Fatal("expected at least one lark candidate")
	}
	for _, c := range larkCands {
		if c.Platform != event.PlatformLark {
			t.Fatalf("platform=lark sweep leaked a %s candidate (%s) — scoping broken", c.Platform, c.ChatID)
		}
	}

	// MarkChatBackfilled → the chat drops out of the candidate set (convergence).
	if err := s.MarkChatBackfilled(ctx, "telegram:imp", t0.Add(10*time.Hour)); err != nil {
		t.Fatal(err)
	}
	cands2, _ := s.BackfillCandidates(ctx, event.PlatformTelegram, 10)
	if len(cands2) != 4 || cands2[0].ChatID != "telegram:norm" {
		t.Fatalf("after mark: want 4 starting at norm (imp dropped out), got %d (%+v)", len(cands2), cands2)
	}
}

func TestQuerySQL(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	for i, txt := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		m := mkMsg(event.PlatformTelegram, "q"+txt, "telegram:c1", txt, t0.Add(time.Duration(i)*time.Minute))
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Valid SELECT: aggregate.
	r, err := s.QuerySQL(ctx, "SELECT COUNT(*) AS n FROM messages", 100)
	if err != nil || len(r.Rows) != 1 || len(r.Columns) != 1 || r.Columns[0] != "n" {
		t.Fatalf("count select: %v cols=%v rows=%v", err, r.Columns, r.Rows)
	}
	if n, _ := r.Rows[0][0].(int64); n != 5 {
		t.Fatalf("count: want 5, got %v", r.Rows[0][0])
	}

	// CTE (WITH) is allowed.
	if r, err := s.QuerySQL(ctx, "WITH x AS (SELECT 1 AS a) SELECT a FROM x", 10); err != nil || len(r.Rows) != 1 {
		t.Fatalf("CTE: %v %+v", err, r)
	}

	// Row cap → truncated (5 rows, cap 2).
	if r, err := s.QuerySQL(ctx, "SELECT id FROM messages", 2); err != nil || len(r.Rows) != 2 || !r.Truncated {
		t.Fatalf("row cap: want 2 rows truncated, got %d trunc=%v (%v)", len(r.Rows), r.Truncated, err)
	}

	// Writes + dangerous statements rejected by the parse-guard (ErrUnsafeSQL).
	for _, bad := range []string{
		"UPDATE messages SET text='x'",
		"DELETE FROM messages",
		"INSERT INTO messages(id) VALUES('z')",
		"DROP TABLE messages",
		"SELECT 1; DROP TABLE messages", // multi-statement
		"PRAGMA table_info(messages)",
		"ATTACH DATABASE 'x.db' AS y",
		"", // empty
	} {
		if _, err := s.QuerySQL(ctx, bad, 100); !errors.Is(err, ErrUnsafeSQL) {
			t.Fatalf("must reject %q with ErrUnsafeSQL, got %v", bad, err)
		}
	}
}

// TestQuerySQLReadOnlyConn proves the mode=ro hard layer (not just the parse-guard):
// a write issued DIRECTLY on the read-only connection must fail. Needs a file DB —
// :memory: aliases the RW conn.
func TestQuerySQLReadOnlyConn(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir + "/ro.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if s.roDB == s.db {
		t.Fatal("file store must have a SEPARATE ro connection")
	}
	// A write on the RO connection is physically rejected (read-only database).
	if _, err := s.roDB.ExecContext(context.Background(), "CREATE TABLE z(a)"); err == nil {
		t.Fatal("write on the read-only connection must fail")
	}
}

// TestBlankSenderBackfillHelpers proves the #54(b) store seam: BlankSenderIDs lists the
// distinct blank-sender ids, SetSenderNameBySender fills them blank-guarded + idempotent.
func TestBlankSenderBackfillHelpers(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	blank1 := event.Message{Platform: event.PlatformTelegram, ChatID: "telegram:user:5", ChatType: event.ChatDM,
		Sender: event.Sender{PlatformUserID: "5"}, TS: t0, Kind: event.KindText, Text: "hi", Source: event.SourceLive}
	blank1.SetID("telegram:user:5:1")
	blank2 := blank1
	blank2.Text = "again"
	blank2.TS = t0.Add(time.Minute)
	blank2.SetID("telegram:user:5:2")
	named := event.Message{Platform: event.PlatformTelegram, ChatID: "telegram:user:9", ChatType: event.ChatDM,
		Sender: event.Sender{PlatformUserID: "9", DisplayName: "Real Name"}, TS: t0, Kind: event.KindText, Text: "yo", Source: event.SourceLive}
	named.SetID("telegram:user:9:1")
	for _, m := range []event.Message{blank1, blank2, named} {
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := s.BlankSenderIDs(ctx, event.PlatformTelegram)
	if err != nil || len(ids) != 1 || ids[0] != "5" {
		t.Fatalf("BlankSenderIDs = %v (err %v), want [5]", ids, err)
	}
	n, err := s.SetSenderNameBySender(ctx, event.PlatformTelegram, "5", "Casey Example", "caseyexample")
	if err != nil || n != 2 {
		t.Fatalf("SetSenderNameBySender updated %d (err %v), want 2", n, err)
	}
	if n2, _ := s.SetSenderNameBySender(ctx, event.PlatformTelegram, "5", "X", "y"); n2 != 0 {
		t.Fatalf("re-run updated %d, want 0 (blank-guard)", n2)
	}
	if n3, _ := s.SetSenderNameBySender(ctx, event.PlatformTelegram, "9", "WRONG", "wrong"); n3 != 0 {
		t.Fatalf("clobbered a real name: updated %d, want 0", n3)
	}
	if ids, _ := s.BlankSenderIDs(ctx, event.PlatformTelegram); len(ids) != 0 {
		t.Fatalf("after backfill, BlankSenderIDs = %v, want empty", ids)
	}
}

// TestReadWindowNewestFirst proves #45: desc returns the newest N by ts (recent context),
// before is a backward cursor, and ordering is strictly by ts — so an OLD message ingested
// LAST (a history backfill) sorts by its real timestamp, NOT as the head of the read.
func TestReadWindowNewestFirst(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	chat := "whatsapp:rw"
	mar := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC) // ancient
	jun1 := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	jun2 := time.Date(2026, 6, 12, 9, 0, 0, 0, time.UTC) // newest
	// Append recent live msgs FIRST, then the ancient one LAST — simulating a backfill
	// pulling in old history after recent messages already landed.
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "J1", chat, "june-1", jun1), nil)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "J2", chat, "june-2", jun2), nil)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "M1", chat, "march", mar), nil)

	// Default (oldest-first) limit=2 → the OLDEST two (the documented old behavior).
	old, _ := s.Read(ctx, chat, time.Time{}, 2)
	if len(old) != 2 || old[0].Text != "march" || old[1].Text != "june-1" {
		t.Fatalf("oldest-first limit=2: got %v", texts(old))
	}

	// desc=true limit=2 → the NEWEST two, newest-first; the ancient backfilled msg is NOT here.
	recent, _ := s.ReadWindow(ctx, chat, ReadOpts{Desc: true, Limit: 2})
	if len(recent) != 2 || recent[0].Text != "june-2" || recent[1].Text != "june-1" {
		t.Fatalf("newest-first limit=2: got %v (want [june-2 june-1])", texts(recent))
	}
	for _, m := range recent {
		if m.Text == "march" {
			t.Fatal("ancient backfilled message must NOT surface in the newest-2 read")
		}
	}

	// before cursor: ts < jun2, newest-first → [june-1, march], excludes jun2.
	older, _ := s.ReadWindow(ctx, chat, ReadOpts{Before: jun2, Desc: true})
	if len(older) != 2 || older[0].Text != "june-1" || older[1].Text != "march" {
		t.Fatalf("before=jun2 desc: got %v (want [june-1 march])", texts(older))
	}
}

func texts(ms []event.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Text
	}
	return out
}

// TestBackfillRunRing proves the convergence-observability ring (phase-2): runs persist
// oldest→newest, cap at backfillRunsCap, and LastBackfillRun returns the most-recent run
// for a SPECIFIC platform (not just the global tail).
func TestBackfillRunRing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir + "/runs.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	base := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	// Interleave two platforms, then overflow the cap with telegram runs.
	if err := s.RecordBackfillRun(ctx, BackfillRun{TS: base, Platform: "lark", Swept: 3, IngestedAfter: 3}); err != nil {
		t.Fatalf("record lark: %v", err)
	}
	for i := 0; i < backfillRunsCap+5; i++ {
		r := BackfillRun{TS: base.Add(time.Duration(i+1) * time.Minute), Platform: "telegram", Swept: i, IngestedAfter: i + 1}
		if err := s.RecordBackfillRun(ctx, r); err != nil {
			t.Fatalf("record tg %d: %v", i, err)
		}
	}

	runs, err := s.BackfillRuns(ctx)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != backfillRunsCap {
		t.Fatalf("ring not capped: got %d want %d", len(runs), backfillRunsCap)
	}
	// The single old lark run was the oldest → evicted by the overflow.
	for _, r := range runs {
		if r.Platform == "lark" {
			t.Fatal("oldest (lark) run should have been evicted past the cap")
		}
	}
	// Newest telegram run wins LastBackfillRun.
	last, ok := s.LastBackfillRun(ctx, "telegram")
	if !ok {
		t.Fatal("expected a telegram run")
	}
	if last.Swept != backfillRunsCap+4 {
		t.Fatalf("LastBackfillRun(telegram).Swept = %d, want %d", last.Swept, backfillRunsCap+4)
	}
	if _, ok := s.LastBackfillRun(ctx, "lark"); ok {
		t.Fatal("lark run was evicted; LastBackfillRun(lark) must be false")
	}
	if _, ok := s.LastBackfillRun(ctx, "whatsapp"); ok {
		t.Fatal("never-recorded platform must return false")
	}
}

// TestBackfillCandidateMigratedShell proves the convergence marker is NOT pre-stamped
// by the migration (#13): a chat created WITHOUT an explicit backfilled_at — exactly
// like the 2278 empty Lark / 752 empty TG shells that pre-existed the column (they get
// the NOT NULL DEFAULT empty string, NOT a timestamp) — IS a sweep candidate, and
// drops out only after it's actually marked. (Disproves the "migration default-stamped
// everything" theory.)
// TestBackfillCandidatesBreadthFirst proves the #42 breadth-first ordering: a never-
// covered chat (msg_count=0) sorts AHEAD of an already-covered one even when the covered
// chat has a recent last_ts — so the daily sweep fills empty chats before re-deepening
// covered ones (the only way coverage % climbs). Without this, a platform with more
// covered-but-unmarked chats than the per-fire cap would never reach its empty chats.
func TestBackfillCandidatesBreadthFirst(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	// A COVERED chat with a recent last_ts (would sort first under the old importance,
	// last_ts DESC ordering).
	recent := time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC)
	if _, err := s.Append(ctx, mkMsg(event.PlatformLark, "C1", "lark:covered", "hi", recent), nil); err != nil {
		t.Fatal(err)
	}
	// An EMPTY shell chat (msg_count=0, no last_ts) created via metadata upsert.
	if err := s.UpsertChatMeta(ctx, ChatMeta{ChatID: "lark:empty", Platform: event.PlatformLark, Title: "Empty DM"}); err != nil {
		t.Fatal(err)
	}
	cands, err := s.BackfillCandidates(ctx, event.PlatformLark, 10)
	if err != nil {
		t.Fatal(err)
	}
	var idxEmpty, idxCovered = -1, -1
	for i, c := range cands {
		switch c.ChatID {
		case "lark:empty":
			idxEmpty = i
		case "lark:covered":
			idxCovered = i
		}
	}
	if idxEmpty == -1 || idxCovered == -1 {
		t.Fatalf("both chats must be candidates: empty=%d covered=%d", idxEmpty, idxCovered)
	}
	if idxEmpty > idxCovered {
		t.Fatalf("breadth-first: empty chat (idx %d) must sort BEFORE covered chat (idx %d)", idxEmpty, idxCovered)
	}
}

func TestBackfillCandidateMigratedShell(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	// A discovered-but-unbackfilled shell chat (no messages), created via metadata
	// upsert — the default backfilled_at is '' (empty), not a timestamp.
	if err := s.UpsertChatMeta(ctx, ChatMeta{ChatID: "lark:oc_shell", Platform: event.PlatformLark, Title: "Empty Group"}); err != nil {
		t.Fatal(err)
	}
	var bf string
	if err := s.db.QueryRowContext(ctx, `SELECT backfilled_at FROM chats WHERE chat_id='lark:oc_shell'`).Scan(&bf); err != nil {
		t.Fatal(err)
	}
	if bf != "" {
		t.Fatalf("migrated/new shell must have EMPTY backfilled_at (not a stamp), got %q", bf)
	}
	// It IS a candidate (unswept), proving the marker isn't pre-populated.
	cands, _ := s.BackfillCandidates(ctx, event.PlatformLark, 10)
	found := false
	for _, c := range cands {
		if c.ChatID == "lark:oc_shell" {
			found = true
		}
	}
	if !found {
		t.Fatal("an un-backfilled shell chat MUST be a sweep candidate (marker not pre-stamped)")
	}
	// After marking, it drops out (convergence).
	if err := s.MarkChatBackfilled(ctx, "lark:oc_shell", time.Now()); err != nil {
		t.Fatal(err)
	}
	cands2, _ := s.BackfillCandidates(ctx, event.PlatformLark, 10)
	for _, c := range cands2 {
		if c.ChatID == "lark:oc_shell" {
			t.Fatal("a marked chat must DROP from candidates (convergence)")
		}
	}
}

func TestMetaKV(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()

	// Missing key → empty, no error.
	if v, err := s.GetMeta(ctx, "lark_discover_offset"); err != nil || v != "" {
		t.Fatalf("missing key: want '',nil got %q,%v", v, err)
	}
	// Set then get.
	if err := s.SetMeta(ctx, "lark_discover_offset", "300"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, _ := s.GetMeta(ctx, "lark_discover_offset"); v != "300" {
		t.Fatalf("get after set: want 300, got %q", v)
	}
	// Upsert overwrites.
	if err := s.SetMeta(ctx, "lark_discover_offset", "600"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if v, _ := s.GetMeta(ctx, "lark_discover_offset"); v != "600" {
		t.Fatalf("get after upsert: want 600, got %q", v)
	}
}

func TestMentionsSelf(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	self := map[string]bool{"me@s.whatsapp.net": true}

	m := mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "hey @you", t0)
	m.Mentions = []string{"me@s.whatsapp.net"}
	s.Append(ctx, m, self)

	other := mkMsg(event.PlatformWhatsApp, "B", "whatsapp:c1", "noise", t0.Add(time.Minute))
	other.Mentions = []string{"someoneelse"}
	s.Append(ctx, other, self)

	ms, err := s.Mentions(ctx, time.Time{}, 10)
	if err != nil {
		t.Fatalf("mentions: %v", err)
	}
	if len(ms) != 1 || ms[0].ID != "whatsapp:A" {
		t.Fatalf("mentions -> %+v", ms)
	}
}

func TestChatMetadataAndNames(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

	// DM: counterpart name should be derived from the non-self sender.
	dm := mkMsg(event.PlatformTelegram, "A", "telegram:user:100000004", "hi", t0)
	dm.Sender.DisplayName = "Jordan Example"
	dm.Sender.IsSelf = false
	s.Append(ctx, dm, nil)
	// A self message in the same DM must NOT overwrite the derived counterpart name.
	self := mkMsg(event.PlatformTelegram, "B", "telegram:user:100000004", "yo", t0.Add(time.Minute))
	self.Sender.DisplayName = "Me"
	self.Sender.IsSelf = true
	s.Append(ctx, self, nil)

	// A group: connector supplies an explicit title + org signals.
	grp := mkMsg(event.PlatformWhatsApp, "C", "whatsapp:grp1", "team msg", t0)
	grp.ChatType = event.ChatGroup
	s.Append(ctx, grp, nil)
	if err := s.UpsertChatMeta(ctx, ChatMeta{
		ChatID: "whatsapp:grp1", Title: "Project X", Pinned: true, Muted: true,
		Labels: []string{"work", "important"},
	}); err != nil {
		t.Fatalf("upsert meta: %v", err)
	}

	chats, err := s.ListChats(ctx, "", time.Time{}, 10)
	if err != nil {
		t.Fatalf("listchats: %v", err)
	}
	byID := map[string]ChatSummary{}
	for _, c := range chats {
		byID[c.ChatID] = c
	}
	// DM title = derived counterpart name (not "Me", not the raw id).
	if got := byID["telegram:user:100000004"].Title; got != "Jordan Example" {
		t.Fatalf("DM title = %q, want Jordan Example", got)
	}
	// Group title + signals from connector metadata.
	g := byID["whatsapp:grp1"]
	if g.Title != "Project X" || !g.Pinned || !g.Muted {
		t.Fatalf("group meta wrong: %+v", g)
	}
	if len(g.Labels) != 2 || g.Labels[0] != "work" {
		t.Fatalf("labels wrong: %+v", g.Labels)
	}
	// Pinned chat sorts first.
	if chats[0].ChatID != "whatsapp:grp1" {
		t.Fatalf("pinned chat should sort first, got %s", chats[0].ChatID)
	}
}

func TestUpsertChatMetaBeforeAnyMessage(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	// Metadata can arrive before the first message — chat row is created.
	if err := s.UpsertChatMeta(ctx, ChatMeta{ChatID: "lark:oc_x", Title: "Lark Group", Muted: true}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	chats, _ := s.ListChats(ctx, "", time.Time{}, 10)
	if len(chats) != 1 || chats[0].Title != "Lark Group" || !chats[0].Muted {
		t.Fatalf("meta-first chat: %+v", chats)
	}
	if chats[0].Platform != event.PlatformLark {
		t.Fatalf("platform inferred wrong: %q", chats[0].Platform)
	}
}

func TestScoreImportance(t *testing.T) {
	cases := []struct {
		name                        string
		chatType                    string
		pinned, muted, archived     bool
		selfSent, othersSent        int
		mentions, msgCount, ageDays int
		override                    string
		wantTier                    string
	}{
		{"override wins over muted", "channel", false, true, true, 0, 0, 0, 0, -1, "important", "important"},
		{"pinned dm you-reply", "dm", true, false, false, 9, 0, 0, 0, -1, "", "important"},
		{"muted archived channel", "channel", false, true, true, 0, 0, 0, 0, -1, "", "low"},
		{"mention only group", "group", false, false, false, 0, 0, 1, 0, -1, "", "normal"},
		{"one-way dm still important", "dm", false, false, false, 3, 0, 0, 0, -1, "", "important"},
		{"silent big channel low", "channel", false, false, false, 0, 0, 0, 0, -1, "", "low"},
		{"plain group normal", "group", false, false, false, 0, 0, 0, 0, -1, "", "normal"},
		// #15 fixes — real chats the old scorer dumped to 'low' now score up:
		{"two-way conversation important", "group", false, false, false, 8, 40, 0, 300, 5, "", "important"},
		{"high-volume recent group rescued", "group", false, false, false, 0, 0, 0, 500, 3, "", "important"},
		{"muted low-volume two-way stays normal", "group", false, true, false, 2, 3, 0, 10, 100, "", "normal"},
		{"stale dead group sinks to -1 (still normal)", "group", false, false, false, 0, 0, 0, 5, 800, "", "normal"},
	}
	for _, c := range cases {
		tier, reason := scoreImportance(c.chatType, c.pinned, c.muted, c.archived, c.selfSent, c.othersSent, c.mentions, c.msgCount, c.ageDays, c.override)
		if tier != c.wantTier {
			t.Errorf("%s: tier=%q want %q (reason %q)", c.name, tier, c.wantTier, reason)
		}
		if reason == "" {
			t.Errorf("%s: empty reason (should be transparent)", c.name)
		}
	}
}

func TestRecomputeImportanceEndToEnd(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	// A DM where the user replies → should score important.
	in := mkMsg(event.PlatformWhatsApp, "A", "whatsapp:dm1", "hi", t0)
	in.Sender.IsSelf = false
	s.Append(ctx, in, nil)
	mine := mkMsg(event.PlatformWhatsApp, "B", "whatsapp:dm1", "reply", t0.Add(time.Minute))
	mine.Sender.IsSelf = true
	s.Append(ctx, mine, nil)
	// A muted channel the user never touches → low.
	noise := mkMsg(event.PlatformTelegram, "C", "telegram:channel:9", "spam", t0)
	noise.ChatType = event.ChatChannel
	s.Append(ctx, noise, nil)
	s.UpsertChatMeta(ctx, ChatMeta{ChatID: "telegram:channel:9", Muted: true})

	if _, err := s.RecomputeImportance(ctx); err != nil {
		t.Fatalf("recompute: %v", err)
	}
	chats, _ := s.ListChats(ctx, "", time.Time{}, 10)
	got := map[string]string{}
	for _, c := range chats {
		got[c.ChatID] = c.Importance
	}
	if got["whatsapp:dm1"] != "important" {
		t.Fatalf("dm1 importance=%q, want important", got["whatsapp:dm1"])
	}
	if got["telegram:channel:9"] != "low" {
		t.Fatalf("channel importance=%q, want low", got["telegram:channel:9"])
	}

	// Override flips it.
	if err := s.SetImportanceOverride(ctx, "telegram:channel:9", "important"); err != nil {
		t.Fatalf("override: %v", err)
	}
	s.RecomputeImportance(ctx)
	chats, _ = s.ListChats(ctx, "", time.Time{}, 10)
	for _, c := range chats {
		if c.ChatID == "telegram:channel:9" && c.Importance != "important" {
			t.Fatalf("override not respected: %q (%s)", c.Importance, c.ImportanceReason)
		}
	}
}

func TestListChats(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "A", "whatsapp:c1", "first", t0), nil)
	s.Append(ctx, mkMsg(event.PlatformWhatsApp, "B", "whatsapp:c1", "latest", t0.Add(time.Hour)), nil)
	s.Append(ctx, mkMsg(event.PlatformTelegram, "C", "telegram:c2", "tg", t0.Add(2*time.Hour)), nil)

	all, err := s.ListChats(ctx, "", time.Time{}, 10)
	if err != nil {
		t.Fatalf("listchats: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("want 2 chats, got %d", len(all))
	}
	// Newest activity first.
	if all[0].ChatID != "telegram:c2" {
		t.Fatalf("order wrong: %+v", all)
	}
	// c1's last_text should be the newer message.
	for _, c := range all {
		if c.ChatID == "whatsapp:c1" && c.LastText != "latest" {
			t.Fatalf("c1 last_text=%q, want latest", c.LastText)
		}
	}

	wa, _ := s.ListChats(ctx, event.PlatformWhatsApp, time.Time{}, 10)
	if len(wa) != 1 || wa[0].ChatID != "whatsapp:c1" {
		t.Fatalf("platform filter: %+v", wa)
	}
}
