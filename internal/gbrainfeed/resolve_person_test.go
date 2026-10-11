package gbrainfeed

import (
	"context"
	"testing"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// The #24 bug: a de-accented query must resolve an accented canonical — including
// the Vietnamese đ that NFD doesn't fold ("Bình An Đỗ" ⇒ "Binh An Do").
func TestResolvePersonAccentFold(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformLark, "ou_abc123", "Bình An Đỗ", "", "person@example.com"))
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Morgan Example", "+6590000001", ""))
	p.Close()

	// De-accented query (the exact failing case) resolves the accented person.
	cards := p.ResolvePerson("Binh An Do")
	if len(cards) == 0 {
		t.Fatal("de-accented query 'Binh An Do' should resolve 'Bình An Đỗ'")
	}
	c := cards[0]
	if c.Name != "Bình An Đỗ" {
		t.Fatalf("card name = %q, want the accented canonical name", c.Name)
	}
	if c.MatchedOn == "" {
		t.Fatal("card should report what it matched on")
	}
	// open_id (lark ou_…) + email surface on the card.
	hasOpen := false
	for _, o := range c.OpenIDs {
		if o == "ou_abc123" {
			hasOpen = true
		}
	}
	if !hasOpen {
		t.Fatalf("lark open_id should surface in OpenIDs, got %+v", c.OpenIDs)
	}
	if len(c.Emails) == 0 || c.Emails[0] != "person@example.com" {
		t.Fatalf("email should surface, got %+v", c.Emails)
	}

	// Token-subset match: a partial query still resolves.
	if len(p.ResolvePerson("Morgan")) == 0 {
		t.Fatal("subset query 'Morgan' should resolve 'Morgan Example'")
	}

	// No match → empty (not nil) so the caller's "not in graph" is clean.
	none := p.ResolvePerson("Zzyzx Nonexistent")
	if none == nil {
		t.Fatal("no-match must return empty slice, not nil")
	}
	if len(none) != 0 {
		t.Fatalf("no-match should be empty, got %d", len(none))
	}
}
