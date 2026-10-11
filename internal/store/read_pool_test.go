package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func TestReadPoolDoesNotWaitForWriterConnection(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "switchboard.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	message := mkMsg(event.PlatformWhatsApp, "read-pool-fixture", "whatsapp:read-pool", "isolationfixture committed message", time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))
	if _, err := s.Append(ctx, message, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMeta(ctx, "read-pool", "committed"); err != nil {
		t.Fatal(err)
	}
	writer, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if s.roDB == s.db {
		t.Fatal("file-backed read pool aliases writer")
	}
	checks := []struct {
		name string
		read func(*testing.T, context.Context) error
	}{
		{"ListChats", func(t *testing.T, ctx context.Context) error {
			rows, err := s.ListChats(ctx, "", time.Time{}, 1)
			if err == nil && (len(rows) != 1 || rows[0].ChatID != message.ChatID) {
				t.Errorf("chats = %+v", rows)
			}
			return err
		}},
		{"ReadWindow", func(t *testing.T, ctx context.Context) error {
			rows, err := s.ReadWindow(ctx, message.ChatID, ReadOpts{Limit: 1})
			if err == nil && (len(rows) != 1 || rows[0].ID != message.ID) {
				t.Errorf("messages = %+v", rows)
			}
			return err
		}},
		{"MessageByID", func(t *testing.T, ctx context.Context) error {
			row, ok, err := s.MessageByID(ctx, message.ID)
			if err == nil && (!ok || row.ID != message.ID) {
				t.Errorf("message found=%v id=%q", ok, row.ID)
			}
			return err
		}},
		{"MessageRef", func(t *testing.T, ctx context.Context) error {
			chat, sender, ok, err := s.MessageRef(ctx, message.ID)
			if err == nil && (!ok || chat != message.ChatID || sender != message.Sender.PlatformUserID) {
				t.Errorf("reference found=%v chat=%q sender=%q", ok, chat, sender)
			}
			return err
		}},
		{"Cursor", func(t *testing.T, ctx context.Context) error {
			watermark, id, err := s.Cursor(ctx, message.ChatID)
			if err == nil && (!watermark.Equal(message.TS) || id != message.ID) {
				t.Errorf("cursor = %v %q", watermark, id)
			}
			return err
		}},
		{"PlatformNewestAndAccount", func(t *testing.T, ctx context.Context) error {
			newest, account, err := s.PlatformNewestAndAccount(ctx, message.Platform)
			if err == nil && (!newest.Equal(message.TS) || account != message.AccountID) {
				t.Errorf("platform newest=%v account=%q", newest, account)
			}
			return err
		}},
		{"GetMeta", func(t *testing.T, ctx context.Context) error {
			value, err := s.GetMeta(ctx, "read-pool")
			if err == nil && value != "committed" {
				t.Errorf("meta=%q", value)
			}
			return err
		}},
		{"Search", func(t *testing.T, ctx context.Context) error {
			rows, err := s.Search(ctx, "isolationfixture", "", "", time.Time{}, 1)
			if err == nil && (len(rows) != 1 || rows[0].ID != message.ID) {
				t.Errorf("search=%+v", rows)
			}
			return err
		}},
		{"Grep", func(t *testing.T, ctx context.Context) error {
			rows, err := s.Grep(ctx, "isolationfixture", "", nil, time.Time{}, 1)
			if err == nil && (len(rows.Matches) != 1 || rows.Matches[0].ID != message.ID) {
				t.Errorf("grep=%+v", rows)
			}
			return err
		}},
		{"MediaByID", func(t *testing.T, ctx context.Context) error {
			_, ok, err := s.MediaByID(ctx, "missing-fixture-media")
			if err == nil && ok {
				t.Error("missing media found")
			}
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			bounded, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if err := check.read(t, bounded); err != nil {
				t.Fatalf("read waited for writer connection: %v", err)
			}
		})
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMeta(ctx, "read-pool", "updated"); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	value, err := s.GetMeta(bounded, "read-pool")
	if err != nil || value != "updated" {
		t.Fatalf("committed update value=%q err=%v", value, err)
	}
}
