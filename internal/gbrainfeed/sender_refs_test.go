package gbrainfeed

import (
	"testing"

	"github.com/seahyc/switchboard-oss/internal/api"
)

// #32: the search person= filter resolves a name → its identity set → sender_ids,
// then matches messages by sender_id (NOT the always-blank sender_name). These tests
// pin the people-layer half: card → (platform, sender_id) flattening, for BOTH a
// directory-only colleague (Casey) and a cross-platform composite (Morgan).
func TestSenderIDsFromCards(t *testing.T) {
	// Directory-only person (the Casey case): her ONLY identity is the lark open_id,
	// which IS the sender_id on her DM rows → search can finally find her messages.
	mai := LarkDirectoryCards("Binh An Do", []LarkDirectoryEntry{
		{Name: "Bình An Đỗ", OpenID: "ou_example", Email: "person@example.com"},
	})
	refs := SenderIDsFromCards(mai)
	if len(refs) != 1 || refs[0].Platform != "lark" || refs[0].SenderID != "ou_example" {
		t.Fatalf("directory card must yield her lark open_id as sender_id, got %+v", refs)
	}

	// Composite person (Morgan across platforms): every platform identity surfaces,
	// so search returns his messages across ALL channels under one name.
	osw := mergeOnRead([]api.PersonCard{
		{CanonicalID: "c1", Name: "Morgan Example", Platforms: []string{"telegram"}, Handles: []string{"morganexample"},
			Identities: []api.CardIdentity{{Platform: "telegram", UserID: "t1"}}},
		{CanonicalID: "c2", Name: "Morgan Example", Platforms: []string{"gmail"}, Handles: []string{"morganexample"},
			Identities: []api.CardIdentity{{Platform: "gmail", UserID: "morgan@example.com"}}},
	})
	if len(osw) != 1 { // sanity: corroborated → 1 composite
		t.Fatalf("precondition: Morgan fragments should composite to 1, got %d", len(osw))
	}
	got := SenderIDsFromCards(osw)
	if len(got) != 2 {
		t.Fatalf("composite must expose both platform sender_ids, got %+v", got)
	}
	want := map[string]bool{"telegram:t1": false, "gmail:morgan@example.com": false}
	for _, r := range got {
		want[r.Platform+":"+r.SenderID] = true
	}
	for k, seen := range want {
		if !seen {
			t.Fatalf("missing sender_id %q in %+v", k, got)
		}
	}

	// Dedup: the same identity appearing on two cards collapses to one ref.
	dup := SenderIDsFromCards([]api.PersonCard{
		{Identities: []api.CardIdentity{{Platform: "lark", UserID: "ou_dup"}}},
		{Identities: []api.CardIdentity{{Platform: "lark", UserID: "ou_dup"}}},
	})
	if len(dup) != 1 {
		t.Fatalf("duplicate (platform,user_id) must dedup to 1, got %+v", dup)
	}

	// Empty identities / no-match → empty, never a phantom ref.
	if r := SenderIDsFromCards(nil); len(r) != 0 {
		t.Fatalf("no cards → no refs, got %+v", r)
	}
	if r := SenderIDsFromCards([]api.PersonCard{{Identities: []api.CardIdentity{{Platform: "lark", UserID: ""}}}}); len(r) != 0 {
		t.Fatalf("blank user_id must be skipped, got %+v", r)
	}
}
