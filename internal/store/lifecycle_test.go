package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func TestMessageDeleteUpdatesExistingProjection(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	original := mkMsg(event.PlatformTelegram, "C:1", "telegram:C", "privateword", time.Now().UTC())
	if _, err := s.Append(ctx, original, nil); err != nil {
		t.Fatal(err)
	}
	deleted := original
	deleted.Kind = event.KindDelete
	deleted.Deleted = true
	deleted.Text = ""
	deleted.Raw = nil
	deleted.TS = original.TS.Add(time.Minute)
	if res, err := s.Append(ctx, deleted, nil); err != nil || !res.Inserted {
		t.Fatalf("delete must be a distinct event: %+v %v", res, err)
	}
	msg, ok, err := s.MessageByID(ctx, original.ID)
	if err != nil || !ok || !msg.Deleted || msg.Text != "" {
		t.Fatalf("stored message: %+v %v", msg, err)
	}
	hits, err := s.Search(ctx, "privateword", "", "", time.Time{}, 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("deleted text still searchable: %v %v", hits, err)
	}
	if _, err := s.Append(ctx, original, nil); err != nil {
		t.Fatal(err)
	}
	msg, _, _ = s.MessageByID(ctx, original.ID)
	if !msg.Deleted {
		t.Fatal("backfill resurrected deleted message")
	}
}

func TestMessageEditsReplaceSearchWithoutDuplicatingMessages(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	original := mkMsg(event.PlatformTelegram, "C:2", "telegram:C", "oldword", time.Now().UTC())
	if _, err := s.Append(ctx, original, nil); err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.Text = "newword"
	ts := original.TS.Add(time.Minute)
	changed.EditedTS = &ts
	if res, err := s.Append(ctx, changed, nil); err != nil || !res.Inserted {
		t.Fatalf("edit: %+v %v", res, err)
	}
	msg, _, err := s.MessageByID(ctx, original.ID)
	if err != nil || msg.Text != "newword" || msg.Sender.PlatformUserID != original.Sender.PlatformUserID {
		t.Fatalf("projection: %+v %v", msg, err)
	}
	hits, err := s.Search(ctx, "oldword", "", "", time.Time{}, 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("old word: %v %v", hits, err)
	}
	if res, err := s.Append(ctx, changed, nil); err != nil || res.Inserted {
		t.Fatalf("duplicate edit: %+v %v", res, err)
	}
	msgs, err := s.Read(ctx, original.ChatID, time.Time{}, 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %v %v", msgs, err)
	}
	stale := changed
	stale.Text = "stale"
	oldts := ts.Add(-time.Second)
	stale.EditedTS = &oldts
	if _, err := s.Append(ctx, stale, nil); err != nil {
		t.Fatal(err)
	}
	msg, _, _ = s.MessageByID(ctx, original.ID)
	if msg.Text != "newword" {
		t.Fatalf("stale edit changed projection: %+v", msg)
	}
}

func TestDeleteBeforeBackfillDoesNotExposeText(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	original := mkMsg(event.PlatformLark, "M1", "lark:C", "secretword", time.Now().UTC())
	deleted := original
	deleted.Text = ""
	deleted.Kind = event.KindDelete
	deleted.Deleted = true
	if _, err := s.Append(ctx, deleted, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, original, nil); err != nil {
		t.Fatal(err)
	}
	msg, _, err := s.MessageByID(ctx, original.ID)
	if err != nil || !msg.Deleted || msg.Text != "" {
		t.Fatalf("late backfill resurrected text: %+v %v", msg, err)
	}
}

func TestMutationLogReplayPreservesFinalState(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	original := mkMsg(event.PlatformWhatsApp, "A", "whatsapp:C", "original", time.Now().UTC())
	changed := original
	changed.ID = "whatsapp:edit-envelope"
	changed.Kind = event.KindEdit
	changed.ReplyTo = original.ID
	changed.Text = "edited"
	changed.TS = original.TS.Add(time.Minute)
	deleted := original
	deleted.ID = "whatsapp:delete-envelope"
	deleted.Kind = event.KindDelete
	deleted.ReplyTo = original.ID
	deleted.Deleted = true
	deleted.Text = ""
	deleted.TS = changed.TS.Add(time.Minute)
	for _, m := range []event.Message{original, changed, deleted} {
		if _, err := s.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	log, err := s.QuerySQL(ctx, "SELECT body FROM event_log ORDER BY seq", 10)
	if err != nil {
		t.Fatal(err)
	}
	replay := mustOpen(t)
	for _, row := range log.Rows {
		var m event.Message
		if err := json.Unmarshal([]byte(row[0].(string)), &m); err != nil {
			t.Fatal(err)
		}
		if _, err := replay.Append(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	msg, ok, err := replay.MessageByID(ctx, original.ID)
	if err != nil || !ok || !msg.Deleted || msg.Text != "" {
		t.Fatalf("replay: %+v %v", msg, err)
	}
}
