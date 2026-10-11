package store

import (
	"context"
	"github.com/seahyc/switchboard-oss/internal/event"
	"reflect"
	"strings"
	"testing"
	"time"
)

func purgeTestStore(t *testing.T, withVersions bool) *Store {
	t.Helper()
	s := mustOpen(t)
	if _, err := s.db.Exec(purgeSchema); err != nil {
		t.Fatalf("create purge schema: %v", err)
	}
	if withVersions {
		_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS message_versions (
			id TEXT PRIMARY KEY, version_ts TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0
		)`)
		if err != nil {
			t.Fatalf("create message versions: %v", err)
		}
	}
	return s
}

func TestPurgeChatRemovesLocalRowsAndKeepsSharedBlob(t *testing.T) {
	ctx := context.Background()
	s := purgeTestStore(t, true)
	t0 := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	chatID := "whatsapp:purge-me"
	otherChat := "whatsapp:keep-me"
	for i, item := range []struct{ chat, id, body string }{
		{chatID, "purge-1", "private local body"},
		{otherChat, "keep-1", "other chat body"},
	} {
		m := mkMsg("whatsapp", item.id, item.chat, item.body, t0.Add(time.Duration(i)*time.Minute))
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatalf("append %s: %v", item.chat, err)
		}
		if _, err := s.EnqueueMedia(ctx, []MediaItem{{
			ID: m.ID + "#0", EventID: m.ID, ChatID: item.chat, Platform: "whatsapp",
			Idx: 0, Type: "image", MsgTS: m.TS,
		}}); err != nil {
			t.Fatalf("enqueue media %s: %v", item.chat, err)
		}
	}
	if _, err := s.db.Exec(`UPDATE media SET hash='shared-hash',stored=1,status='stored' WHERE chat_id=?`, otherChat); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE media SET hash='orphan-hash',stored=1,status='stored' WHERE chat_id=?`, chatID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO message_versions(id,version_ts,deleted) VALUES (?, ?, 0)`, "whatsapp:purge-1", t0.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}

	got, err := s.PurgeChat(ctx, chatID)
	if err != nil {
		t.Fatalf("PurgeChat: %v", err)
	}
	if got.Events != 1 || got.Messages != 1 || got.Media != 1 {
		t.Fatalf("counts = %+v, want one event, message, and media", got)
	}
	if !reflect.DeepEqual(got.OrphanHashes, []string{"orphan-hash"}) {
		t.Fatalf("orphan hashes = %v", got.OrphanHashes)
	}

	for _, check := range []struct {
		query string
		args  []any
		want  int
	}{
		{`SELECT COUNT(*) FROM event_log WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM event_log WHERE body LIKE '%private local body%'`, nil, 0},
		{`SELECT COUNT(*) FROM messages WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM messages_fts WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM media WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM cursors WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM chats WHERE chat_id=?`, []any{chatID}, 0},
		{`SELECT COUNT(*) FROM message_versions WHERE id=?`, []any{"whatsapp:purge-1"}, 0},
		{`SELECT COUNT(*) FROM purged_chats WHERE chat_id=?`, []any{chatID}, 1},
		{`SELECT COUNT(*) FROM messages WHERE chat_id=?`, []any{otherChat}, 1},
		{`SELECT COUNT(*) FROM event_log WHERE chat_id=?`, []any{otherChat}, 1},
		{`SELECT COUNT(*) FROM media WHERE hash='shared-hash' AND stored=1`, nil, 1},
	} {
		var n int
		if err := s.db.QueryRow(check.query, check.args...).Scan(&n); err != nil {
			t.Fatalf("query %q: %v", check.query, err)
		}
		if n != check.want {
			t.Errorf("query %q = %d, want %d", check.query, n, check.want)
		}
	}
}

func TestPurgeChatWithoutOptionalVersionsTable(t *testing.T) {
	ctx := context.Background()
	s := purgeTestStore(t, false)
	m := mkMsg("telegram", "without-versions", "telegram:purge-me", "body", time.Now().UTC())
	if _, err := s.Append(ctx, m, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.PurgeChat(ctx, m.ChatID)
	if err != nil || got.Messages != 1 {
		t.Fatalf("PurgeChat = %+v, %v", got, err)
	}
}

func TestPurgeChatRejectsEmptyChatID(t *testing.T) {
	s := purgeTestStore(t, false)
	if _, err := s.PurgeChat(context.Background(), ""); err == nil {
		t.Fatal("expected empty chat id error")
	}
}

func TestPurgeChatRollsBackOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	s := purgeTestStore(t, false)
	m := mkMsg("telegram", "rollback", "telegram:rollback", "keep until commit", time.Now().UTC())
	if _, err := s.Append(ctx, m, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER reject_purge_message BEFORE DELETE ON messages
		BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PurgeChat(ctx, m.ChatID); err == nil || !strings.Contains(err.Error(), "injected purge failure") {
		t.Fatalf("PurgeChat error = %v, want injected failure", err)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM purged_chats WHERE chat_id='telegram:rollback'`,
		`SELECT COUNT(*) FROM event_log WHERE chat_id='telegram:rollback'`,
		`SELECT COUNT(*) FROM messages WHERE chat_id='telegram:rollback'`,
		`SELECT COUNT(*) FROM messages_fts WHERE chat_id='telegram:rollback'`,
	} {
		var n int
		if err := s.db.QueryRow(query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		want := 1
		if strings.Contains(query, "purged_chats") {
			want = 0
		}
		if n != want {
			t.Errorf("after rollback %q = %d, want %d", query, n, want)
		}
	}
}

func TestPurgeRejectsUnresolvedLateEdit(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	m := event.Message{ID: "slack:T:C:1", Platform: event.PlatformSlack, ChatID: "slack:T:C", Kind: event.KindText, Text: "original"}
	if _, err := s.Append(ctx, m, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PurgeChat(ctx, m.ChatID); err != nil {
		t.Fatal(err)
	}
	m.ChatID = ""
	m.Kind = event.KindEdit
	m.Text = "late private content"
	if _, err := s.Append(ctx, m, nil); err == nil {
		t.Fatal("unresolved late edit bypassed purge")
	}
	var count int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM messages").Scan(&count); err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
