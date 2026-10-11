package gbrainfeed

import (
	"testing"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func waID(user, name, phone, email string) identity {
	return identity{
		Platform: event.PlatformWhatsApp, UserID: user,
		Names: NameSet{Display: name}, Phone: normalizePhone(phone),
		Email: email,
	}
}

func TestResolverSingletonNoSignal(t *testing.T) {
	r := NewResolver()
	a := r.Observe(identity{Platform: "whatsapp", UserID: "x@lid"})
	if a.Canonical.ID != "canon-whatsapp-x-lid" {
		t.Fatalf("singleton id=%q", a.Canonical.ID)
	}
	if a.Canonical.Identified {
		t.Fatal("nameless identity must be unidentified")
	}
}

func TestResolverAutoMergeViaSharedPhone(t *testing.T) {
	r := NewResolver()
	r.Observe(waID("wa1", "Carol", "+6580001111", ""))
	a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{Display: "Carol T"}, Phone: "+6580001111"})
	if a.Canonical.ID != "canon-phone-6580001111" {
		t.Fatalf("want shared phone canonical, got %q", a.Canonical.ID)
	}
	if len(a.Canonical.Members) != 2 {
		t.Fatalf("want 2 members, got %d", len(a.Canonical.Members))
	}
}

func TestResolverBridgeMergesTwoCanonicals(t *testing.T) {
	r := NewResolver()
	// Two separate canonicals form first: one by phone, one by email.
	r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{Display: "X"}, Phone: "+6580002222"})
	r.Observe(identity{Platform: "gmail", UserID: "g1", Names: NameSet{Display: "Y"}, Email: "z@example.com"})
	// A third identity carries BOTH signals → bridges + merges them.
	a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{Display: "Z"}, Phone: "+6580002222", Email: "z@example.com"})
	if len(a.AutoMerged) != 1 {
		t.Fatalf("want 1 auto-merge event, got %d", len(a.AutoMerged))
	}
	if len(a.Canonical.Members) != 3 {
		t.Fatalf("want 3 members after merge, got %d: %+v", len(a.Canonical.Members), a.Canonical.Members)
	}
}

func TestResolverFuzzyNameProposesNotMerges(t *testing.T) {
	r := NewResolver()
	// Two different platform users, same name, NO shared strong signal.
	r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: "Mary Tan"}, Phone: "+6580003333"})
	a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: "Mary Tan"}, Phone: "+6580004444"})
	if len(a.AutoMerged) != 0 {
		t.Fatalf("name-only match must NOT auto-merge, got %d merges", len(a.AutoMerged))
	}
	if len(a.Proposals) != 1 {
		t.Fatalf("want 1 fuzzy proposal, got %d", len(a.Proposals))
	}
	if a.Proposals[0].Score != 1.0 {
		t.Fatalf("identical-name score should be 1.0, got %v", a.Proposals[0].Score)
	}
}

func TestResolverNoProposalForCommonOrShortName(t *testing.T) {
	// Bare/short/common names must NOT seed a proposal (likely different people).
	for _, name := range []string{"Sean", "S", "Jon", "Mom", "Ben"} {
		r := NewResolver()
		r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: name}, Phone: "+6581110000"})
		a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: name}, Phone: "+6582220000"})
		if len(a.Proposals) != 0 {
			t.Fatalf("non-distinctive name %q must not be proposed, got %d", name, len(a.Proposals))
		}
	}
}

func TestResolverNoProposalForPlaceholderName(t *testing.T) {
	// "Identified As Spam" is a WhatsApp label, not a name → unidentified + no proposal.
	r := NewResolver()
	a1 := r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: "Identified As Spam"}, Phone: "+6583330000"})
	if a1.Canonical.Identified {
		t.Fatal("placeholder name must not identify a person")
	}
	a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: "Identified As Spam"}, Phone: "+6584440000"})
	if len(a.Proposals) != 0 {
		t.Fatalf("placeholder-named pair must not be proposed, got %d", len(a.Proposals))
	}
}

func TestResolverNoProposalForOrgEmailPair(t *testing.T) {
	// Two email-anchored canonicals with the same distinctive name = org no-reply
	// addresses, not a person → no proposal.
	r := NewResolver()
	r.Observe(identity{Platform: "gmail", UserID: "noreply1@godaddy.com", Names: NameSet{Display: "GoDaddy Support"}, Email: "noreply1@godaddy.com"})
	a := r.Observe(identity{Platform: "gmail", UserID: "noreply2@godaddy.com", Names: NameSet{Display: "GoDaddy Support"}, Email: "noreply2@godaddy.com"})
	if len(a.Proposals) != 0 {
		t.Fatalf("org email↔email pair must not be proposed, got %d", len(a.Proposals))
	}
}

func TestDistinctiveName(t *testing.T) {
	for _, c := range []struct {
		n    string
		want bool
	}{
		{"Morgan Example", true}, {"Jamie Example", true}, {"Mary Tan", true},
		{"Sean", false}, {"S", false}, {"Jon", false}, {"Mom", false}, {"AB CD", false},
	} {
		if distinctiveName(c.n) != c.want {
			t.Errorf("distinctiveName(%q)=%v want %v", c.n, distinctiveName(c.n), c.want)
		}
	}
}

func TestResolverRejectionSuppressesProposal(t *testing.T) {
	r := NewResolver()
	r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: "Sam Wong"}, Phone: "+6580005555"})
	r.Reject("canon-phone-6580005555", "canon-phone-6580006666")
	a := r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: "Sam Wong"}, Phone: "+6580006666"})
	if len(a.Proposals) != 0 {
		t.Fatalf("rejected pair must never be re-proposed, got %d", len(a.Proposals))
	}
}

func TestResolverForceMergeAndRedirect(t *testing.T) {
	r := NewResolver()
	r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: "Pat Lim"}, Phone: "+6580007777"})
	r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: "Pat Lim"}, Phone: "+6580008888"})
	surv, absorbed, ok := r.ForceMerge("canon-phone-6580007777", "canon-phone-6580008888")
	if !ok {
		t.Fatal("force merge should succeed")
	}
	if len(surv.Members) != 2 {
		t.Fatalf("want 2 members after force merge, got %d", len(surv.Members))
	}
	// The absorbed id now redirects to the survivor.
	snap, ok := r.Snapshot(absorbed)
	if !ok || snap.ID != surv.ID {
		t.Fatalf("absorbed id should redirect to survivor %q, got %q ok=%v", surv.ID, snap.ID, ok)
	}
}

func TestResolverDeterministicNameOnTie(t *testing.T) {
	// Two address-book names (same source rank) on one merged person must resolve
	// to a DETERMINISTIC display name (sorted member order), not a map-order coin
	// flip — so a merge survivor's name can't change run-to-run. Run many times.
	want := ""
	for i := 0; i < 50; i++ {
		r := NewResolver()
		// Same phone → one canonical with two address-book-named identities.
		r.Observe(identity{Platform: "telegram", UserID: "tg1", Names: NameSet{AddressBook: "Zed Name"}, Phone: "+6580001234"})
		a := r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{AddressBook: "Abe Name"}, Phone: "+6580001234"})
		if len(a.Canonical.Members) != 2 {
			t.Fatalf("expected merge to one canonical, got %d members", len(a.Canonical.Members))
		}
		if want == "" {
			want = a.Canonical.Name
		} else if a.Canonical.Name != want {
			t.Fatalf("non-deterministic name on tie: got %q, earlier %q", a.Canonical.Name, want)
		}
	}
	// Sorted member order: platform "telegram" < "whatsapp"? No — sort is by
	// platform then userID; telegram sorts before whatsapp, so "Zed Name" (tg) wins.
	if want != "Zed Name" {
		t.Fatalf("deterministic tie-break should pick the sorted-first member's name, got %q", want)
	}
}

func TestResolverSetNameOverride(t *testing.T) {
	r := NewResolver()
	a := r.Observe(identity{Platform: "whatsapp", UserID: "wa1", Names: NameSet{Display: "wa-display"}, Phone: "+6580009999"})
	cp, ok := r.SetName(a.Canonical.ID, "Real Human Name")
	if !ok || cp.Name != "Real Human Name" || cp.NameSrc != srcManual {
		t.Fatalf("override failed: %+v ok=%v", cp, ok)
	}
}

// #18: two email-anchored canonicals sharing a distinctive name → PROPOSE when BOTH
// local-parts are personal (one real person, two personal emails: Morgan), but still
// SKIP when either is a role/service mailbox (no-reply/support → one service).
func TestEmailEmailProposeOnlyWhenPersonal(t *testing.T) {
	// personal ↔ personal, same name → proposed
	r := NewResolver()
	r.Observe(identity{Platform: "gmail", UserID: "morgan@example.com", Email: "morgan@example.com", Names: NameSet{Display: "Morgan Example"}})
	a := r.Observe(identity{Platform: "gmail", UserID: "morganexample@example.org", Email: "morganexample@example.org", Names: NameSet{Display: "Morgan Example"}})
	if len(a.Proposals) == 0 {
		t.Fatal("two PERSONAL-email canonicals with the same name must be proposed (#18)")
	}

	// role ↔ role, same name → still skipped
	r2 := NewResolver()
	r2.Observe(identity{Platform: "gmail", UserID: "no-reply@godaddy.com", Email: "no-reply@godaddy.com", Names: NameSet{Display: "GoDaddy Notice"}})
	b := r2.Observe(identity{Platform: "gmail", UserID: "support@godaddy.com", Email: "support@godaddy.com", Names: NameSet{Display: "GoDaddy Notice"}})
	if len(b.Proposals) != 0 {
		t.Fatalf("role-email pairs must still be skipped, got %d proposals", len(b.Proposals))
	}

	// personal ↔ role, same name → skipped (one side is a service)
	r3 := NewResolver()
	r3.Observe(identity{Platform: "gmail", UserID: "jane.doe@corp.com", Email: "jane.doe@corp.com", Names: NameSet{Display: "Jane Doe Smith"}})
	c := r3.Observe(identity{Platform: "gmail", UserID: "noreply@corp.com", Email: "noreply@corp.com", Names: NameSet{Display: "Jane Doe Smith"}})
	if len(c.Proposals) != 0 {
		t.Fatalf("personal↔role pair must be skipped, got %d", len(c.Proposals))
	}
}

// #49: node type/category set + resolve-by-identity for the social-author→node seam.
func TestNodeTypeAndResolveIdentity(t *testing.T) {
	r := NewResolver()
	a := r.Observe(identity{Platform: "instagram", UserID: "ig123", Handle: "caseyexample", Names: NameSet{Display: "Casey Example"}})
	cid := a.Canonical.ID
	// default snapshot: nodeType unset (card-layer defaults to "person")
	if s, _ := r.Snapshot(cid); s.NodeType != "" {
		t.Fatalf("default nodeType should be unset at resolver layer, got %q", s.NodeType)
	}
	// classify it as a friend/person
	if !r.SetNodeType(cid, "person", "friend") {
		t.Fatal("SetNodeType should succeed for a known canonical")
	}
	s, _ := r.Snapshot(cid)
	if s.NodeType != "person" || s.NodeCategory != "friend" {
		t.Fatalf("node type/category not set, got %q/%q", s.NodeType, s.NodeCategory)
	}
	// resolve by IG identity → the node
	node, ok := r.ResolveIdentityNode("instagram", "ig123")
	if !ok || node.ID != cid || node.NodeCategory != "friend" {
		t.Fatalf("ResolveIdentityNode failed: ok=%v node=%+v", ok, node)
	}
	if _, ok := r.ResolveIdentityNode("instagram", "nope"); ok {
		t.Fatal("unknown identity must resolve to ok=false")
	}
}
