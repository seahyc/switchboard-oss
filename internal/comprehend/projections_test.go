package comprehend

import (
	"context"
	"testing"
	"time"
)

// testReader builds an in-memory reader with a minimal subset of the switchboard
// store schema (just the columns the projections read), so the deterministic
// reply-graph logic can be exercised without the full store package.
func testReader(t *testing.T) *Reader {
	t.Helper()
	r, err := OpenReader(":memory:")
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	_, err = r.DB().Exec(`
CREATE TABLE messages (
  id TEXT PRIMARY KEY, platform TEXT, chat_id TEXT, ts TEXT, kind TEXT DEFAULT 'text',
  text TEXT, sender_name TEXT, is_self INTEGER DEFAULT 0, mentions_self INTEGER DEFAULT 0,
  deleted INTEGER DEFAULT 0
);
CREATE TABLE chats (
  chat_id TEXT PRIMARY KEY, chat_type TEXT, title TEXT DEFAULT '', derived_name TEXT DEFAULT '',
  importance TEXT DEFAULT '', muted INTEGER DEFAULT 0, archived INTEGER DEFAULT 0
);`)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	return r
}

func ts(s string) string {
	tm, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		panic(err)
	}
	return tm.UTC().Format(time.RFC3339Nano)
}

func addChat(t *testing.T, r *Reader, id, typ, title, importance string) {
	t.Helper()
	_, err := r.DB().Exec(`INSERT INTO chats(chat_id,chat_type,title,importance) VALUES (?,?,?,?)`, id, typ, title, importance)
	if err != nil {
		t.Fatal(err)
	}
}

func addMsg(t *testing.T, r *Reader, id, chatID, when, text, sender string, isSelf, mentionsSelf int) {
	t.Helper()
	_, err := r.DB().Exec(
		`INSERT INTO messages(id,platform,chat_id,ts,kind,text,sender_name,is_self,mentions_self) VALUES (?,?,?,?,?,?,?,?,?)`,
		id, "telegram", chatID, ts(when), "text", text, sender, isSelf, mentionsSelf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitingOnMe_InboundLastUnanswered(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:alice", "dm", "Alice", "important")
	// User replied, then Alice sent two more — current unanswered streak starts at the first of those.
	addMsg(t, r, "m1", "telegram:alice", "2026-06-10T08:00:00Z", "hey", "Alice", 0, 0)
	addMsg(t, r, "m2", "telegram:alice", "2026-06-10T09:00:00Z", "ok thanks", "me", 1, 0)
	addMsg(t, r, "m3", "telegram:alice", "2026-06-11T07:00:00Z", "can you review the deck?", "Alice", 0, 0)
	addMsg(t, r, "m4", "telegram:alice", "2026-06-11T07:30:00Z", "by EOD?", "Alice", 0, 0)

	now := mustTime("2026-06-12T07:00:00Z")
	p, err := r.Project(context.Background(), ProjectionConfig{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.WaitingOnMe) != 1 {
		t.Fatalf("want 1 waiting card, got %d: %+v", len(p.WaitingOnMe), p.WaitingOnMe)
	}
	c := p.WaitingOnMe[0]
	if c.Person != "Alice" || c.ChatID != "telegram:alice" || c.Platform != "telegram" {
		t.Errorf("bad card: %+v", c)
	}
	// since = start of streak (m3 at 07:00 on the 11th), not m1.
	if want := mustTime("2026-06-11T07:00:00Z"); !c.Since.Equal(want) {
		t.Errorf("since = %v, want %v", c.Since, want)
	}
	if c.LastMsgText != "by EOD?" {
		t.Errorf("last msg = %q", c.LastMsgText)
	}
	if c.Direction != DirWaitingOnMe {
		t.Errorf("direction = %q", c.Direction)
	}
	if len(p.Ghosting) != 0 {
		t.Errorf("expected no ghosting, got %+v", p.Ghosting)
	}
}

func TestWaitingOnMe_SelfRepliedLast_NotWaiting(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:bob", "dm", "Bob", "normal")
	addMsg(t, r, "m1", "telegram:bob", "2026-06-11T07:00:00Z", "you around?", "Bob", 0, 0)
	addMsg(t, r, "m2", "telegram:bob", "2026-06-11T08:00:00Z", "yes!", "me", 1, 0)

	now := mustTime("2026-06-11T09:00:00Z") // only 1h since self reply → not ghosting yet
	p, err := r.Project(context.Background(), ProjectionConfig{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.WaitingOnMe) != 0 {
		t.Errorf("self replied last; expected no waiting-on-me, got %+v", p.WaitingOnMe)
	}
	if len(p.Ghosting) != 0 {
		t.Errorf("only 1h elapsed; expected no ghosting, got %+v", p.Ghosting)
	}
}

func TestGhosting_SelfLastNoReplyBeyondThreshold(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:carol", "dm", "Carol", "important")
	addMsg(t, r, "m1", "telegram:carol", "2026-06-08T10:00:00Z", "hi!", "Carol", 0, 0)
	addMsg(t, r, "m2", "telegram:carol", "2026-06-08T11:00:00Z", "want to grab lunch?", "me", 1, 0)

	now := mustTime("2026-06-12T07:00:00Z") // ~4 days later
	p, err := r.Project(context.Background(), ProjectionConfig{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Ghosting) != 1 {
		t.Fatalf("want 1 ghosting card, got %d: %+v", len(p.Ghosting), p.Ghosting)
	}
	c := p.Ghosting[0]
	if c.Person != "Carol" || c.Direction != DirGhosting {
		t.Errorf("bad ghosting card: %+v", c)
	}
	if c.WaitingHours < 90 { // ~92h
		t.Errorf("waiting hours = %d, want ~92", c.WaitingHours)
	}
}

func TestProjection_GatesMutedAndLow(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:low", "dm", "Spammy", "low")
	addMsg(t, r, "l1", "telegram:low", "2026-06-11T07:00:00Z", "buy now!!!", "Spammy", 0, 0)
	addChat(t, r, "telegram:muted", "dm", "Muted", "normal")
	_, _ = r.DB().Exec(`UPDATE chats SET muted=1 WHERE chat_id='telegram:muted'`)
	addMsg(t, r, "u1", "telegram:muted", "2026-06-11T07:00:00Z", "hello?", "Muted", 0, 0)

	now := mustTime("2026-06-12T07:00:00Z")
	p, err := r.Project(context.Background(), ProjectionConfig{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.WaitingOnMe) != 0 {
		t.Errorf("low + muted chats must be gated out, got %+v", p.WaitingOnMe)
	}
}

func TestProjection_GroupRequiresMention(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:grp", "group", "Project X", "normal")
	addMsg(t, r, "g1", "telegram:grp", "2026-06-11T07:00:00Z", "random chatter", "Dave", 0, 0)

	now := mustTime("2026-06-12T07:00:00Z")
	p, _ := r.Project(context.Background(), ProjectionConfig{Now: now})
	if len(p.WaitingOnMe) != 0 {
		t.Errorf("group without mention should be excluded, got %+v", p.WaitingOnMe)
	}

	// Now add a mention-self message as the latest.
	addMsg(t, r, "g2", "telegram:grp", "2026-06-11T08:00:00Z", "@me can you confirm?", "Dave", 0, 1)
	p, _ = r.Project(context.Background(), ProjectionConfig{Now: now})
	if len(p.WaitingOnMe) != 1 {
		t.Errorf("group with mention should surface, got %+v", p.WaitingOnMe)
	}
}

func TestProjection_StaleBeyondMaxAgeDropped(t *testing.T) {
	r := testReader(t)
	addChat(t, r, "telegram:old", "dm", "Old", "normal")
	addMsg(t, r, "o1", "telegram:old", "2026-01-01T07:00:00Z", "ping", "Old", 0, 0)
	now := mustTime("2026-06-12T07:00:00Z")
	p, _ := r.Project(context.Background(), ProjectionConfig{Now: now})
	if len(p.WaitingOnMe) != 0 {
		t.Errorf("ancient thread beyond max age should drop, got %+v", p.WaitingOnMe)
	}
}

func TestProjection_ExcludesConfiguredBotChat(t *testing.T) {
	t.Setenv("SWITCHBOARD_EXCLUDED_CHAT_IDS", "telegram:user:100000002")
	r := testReader(t)
	addChat(t, r, "telegram:user:100000002", "dm", "Mamba", "important")
	addMsg(t, r, "b1", "telegram:user:100000002", "2026-06-12T06:59:00Z", "ACK-PREBRIEF", "Mamba", 0, 0)
	now := mustTime("2026-06-12T07:00:00Z")
	p, _ := r.Project(context.Background(), ProjectionConfig{Now: now})
	for _, c := range p.WaitingOnMe {
		if c.ChatID == "telegram:user:100000002" {
			t.Errorf("Mamba bot chat must be excluded from projections, got %+v", c)
		}
	}
}

func TestCandidateMessages_ExcludesBotAndTestProbes(t *testing.T) {
	t.Setenv("SWITCHBOARD_EXCLUDED_CHAT_IDS", "telegram:user:100000002")
	r := testReader(t)
	addChat(t, r, "telegram:user:100000002", "dm", "Mamba", "important")
	addChat(t, r, "telegram:user:100000001", "dm", "Self", "important")
	addMsg(t, r, "b1", "telegram:user:100000002", "2026-06-12T06:00:00Z", "Open Olympus test surface", "Mamba", 0, 0)
	addMsg(t, r, "t1", "telegram:user:100000001", "2026-06-12T06:01:00Z", "[olympus E2E test] probe ping", "Self", 1, 0)
	addMsg(t, r, "real", "telegram:user:100000001", "2026-06-12T06:02:00Z", "I'll send Bob the deck by Friday", "Self", 1, 0)

	cands, err := r.CandidateMessages(context.Background(), time.Time{}, 50)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, c := range cands {
		ids[c.ID] = true
	}
	if ids["b1"] {
		t.Error("Mamba bot chat message must be excluded from extraction candidates")
	}
	if ids["t1"] {
		t.Error("[olympus E2E test] probe must be excluded from extraction candidates")
	}
	if !ids["real"] {
		t.Error("the genuine (unmarked) commitment message must still be a candidate")
	}
}

func TestPrettyName(t *testing.T) {
	cases := []struct{ name, chatID, want string }{
		{"Alice", "telegram:alice", "Alice"},
		{"", "whatsapp:13135550002@s.whatsapp.net", "unknown (+13135550002)"},
		{"whatsapp:13135550002@s.whatsapp.net", "whatsapp:13135550002@s.whatsapp.net", "unknown (+13135550002)"},
		{"", "whatsapp:100000000000003@lid", "unknown"},
		{"", "telegram:user:999", "someone"},
	}
	for _, c := range cases {
		if got := prettyName(c.name, c.chatID); got != c.want {
			t.Errorf("prettyName(%q,%q) = %q, want %q", c.name, c.chatID, got, c.want)
		}
	}
}

func mustTime(s string) time.Time {
	tm, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		panic(err)
	}
	return tm
}
