package gbrainfeed

import "testing"

// #24 phase 2: a de-accented query resolves a Lark org-directory colleague who is
// NOT in the message graph (the real "Bình An Đỗ" failure).
func TestLarkDirectoryCards(t *testing.T) {
	entries := []LarkDirectoryEntry{
		{Name: "Bình An Đỗ", OpenID: "ou_example", Email: "person@example.com", Mobile: "+84900000"},
		{Name: "Someone Else", OpenID: "ou_x"}, // must NOT match "Binh An Do"
	}
	cards := LarkDirectoryCards("Binh An Do", entries)
	if len(cards) != 1 {
		t.Fatalf("de-accented query should match exactly the 1 directory entry, got %d", len(cards))
	}
	c := cards[0]
	if c.Source != "lark" {
		t.Fatalf("directory card Source = %q, want lark", c.Source)
	}
	if c.Name != "Bình An Đỗ" {
		t.Fatalf("card name = %q", c.Name)
	}
	if len(c.OpenIDs) == 0 || c.OpenIDs[0] != "ou_example" {
		t.Fatalf("open_id should surface, got %+v", c.OpenIDs)
	}
	if len(c.Emails) == 0 || c.Emails[0] != "person@example.com" {
		t.Fatalf("email should surface, got %+v", c.Emails)
	}
	if len(c.Identities) != 1 || c.Identities[0].Platform != "lark" || c.Identities[0].UserID != "ou_example" {
		t.Fatalf("identity should be the lark open_id, got %+v", c.Identities)
	}

	// Order-independent token-subset (directory name flipped, partial query).
	flipped := LarkDirectoryCards("Binh Do", []LarkDirectoryEntry{{Name: "Đỗ An Bình", OpenID: "ou_2"}})
	if len(flipped) != 1 {
		t.Fatalf("order-independent subset should match flipped name, got %d", len(flipped))
	}

	// No match → empty, not nil.
	none := LarkDirectoryCards("Zzyzx", entries)
	if none == nil || len(none) != 0 {
		t.Fatalf("no-match must be empty (not nil), got %+v", none)
	}
}
