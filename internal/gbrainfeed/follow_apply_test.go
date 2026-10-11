package gbrainfeed

import (
	"context"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// Project-time classification (#49 Option B): a Sub=="follow" social_activity event
// carries the classify signals, so projecting it types the resulting node — friend
// (mutual, non-business) → person/friend; one-way/business → entity — with NO merge
// target needed and durable-by-construction (re-derived on every reprojection).
func TestFollowEventProjectionTypesNode(t *testing.T) {
	followMsg := func(pk, handle, name string, mutual, biz bool) event.Message {
		return event.Message{
			Platform: "instagram",
			Kind:     event.KindSocialActivity,
			Sender:   event.Sender{PlatformUserID: pk, DisplayName: name, Handle: handle},
			Social:   &event.Social{Sub: "follow", Mutual: &mutual, IsBusiness: &biz, Followers: 1000},
			TS:       time.Now(),
		}
	}
	p := NewProjector(newFake(), nil)
	p.Start(context.Background())
	p.OnIngest(followMsg("111", "afriend", "A Friend", true, false))  // mutual, not business → person
	p.OnIngest(followMsg("222", "abrand", "A Brand Co", false, true)) // one-way + business → entity
	p.Close()

	if c, ok := p.ResolveSocialAuthor("instagram", "111", "afriend"); !ok || c.Type != "person" || c.Category != "friend" {
		t.Fatalf("mutual non-business follow → person/friend, got %q/%q (ok=%v)", c.Type, c.Category, ok)
	}
	if c, ok := p.ResolveSocialAuthor("instagram", "222", "abrand"); !ok || c.Type != "entity" {
		t.Fatalf("one-way business follow → entity, got %q (ok=%v)", c.Type, ok)
	}
}

// Coalescing must not drop the #49 follow signals: a bare post/sender event arriving
// AFTER a follow event for the same identity (the active-poster case, caseyexample/morgan)
// must still carry the classify signals so the project-time hook fires.
func TestMergeIdentityPreservesFollowSignals(t *testing.T) {
	sig := &FollowSignals{Mutual: true, Name: "A Friend"}
	withF := identity{Platform: "instagram", UserID: "1", follow: sig}
	bare := identity{Platform: "instagram", UserID: "1"}
	if got := mergeIdentity(withF, bare); got.follow == nil { // bare lands second (b)
		t.Fatal("follow signal dropped when a bare event coalesces after the follow event")
	}
	if got := mergeIdentity(bare, withF); got.follow == nil { // follow lands second (b)
		t.Fatal("follow signal lost when follow event coalesces after a bare event")
	}
}

// A typed follow node absorbed into an untyped person carries its #49 classification
// to the survivor (the caseyexample/morganexample case: IG node typed person/friend, then
// merged into the existing phone/email person — the type must not vanish).
func TestMergeCarriesNodeType(t *testing.T) {
	p := NewProjector(newFake(), nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa-x", "Casey Example", "+15555550124", "")) // strong, untyped
	p.OnIngest(senderMsg("instagram", "100000001", "Casey Example", "", ""))                   // will be typed
	p.Close()

	ig, _ := p.resolver.ResolveIdentityNode("instagram", "100000001")
	ph, _ := p.resolver.ResolveIdentityNode(event.PlatformWhatsApp, "wa-x")
	p.resolver.SetNodeType(ig.ID, "person", "friend") // type the (soon-absorbed) IG node
	if _, _, ok := p.resolver.ForceMerge(ph.ID, ig.ID); !ok {
		t.Fatal("force-merge failed")
	}
	// survivor is the strong phone canonical, which had NO type — must inherit person/friend
	if c, ok := p.ResolveSocialAuthor("instagram", "100000001", ""); !ok || c.Type != "person" || c.Category != "friend" {
		t.Fatalf("survivor must inherit person/friend from absorbed IG node, got %q/%q (ok=%v)", c.Type, c.Category, ok)
	}
}

// The #49 apply: a friend follow with a merge target is absorbed into the existing
// person (durable ApproveMerge + provenance) and the survivor is typed person/friend;
// an entity follow becomes its own entity node; an un-ingested follow is reported
// unresolved (not failed).
func TestApplyFollowClassifications(t *testing.T) {
	ctx := context.Background()
	p := NewProjector(newFake(), nil)
	p.Start(ctx)
	// Existing person: Telegram+phone "Casey Example" (the merge target).
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg-casey", "Casey Example", "+15555550124", ""))
	// IG follow identities (as if just ingested by the social step).
	p.OnIngest(senderMsg("instagram", "100000001", "Casey Example", "", ""))  // friend → merge
	p.OnIngest(senderMsg("instagram", "100000002", "Morgan Example", "", "")) // entity
	p.Close()

	target, ok := p.resolver.ResolveIdentityNode(event.PlatformTelegram, "tg-casey")
	if !ok {
		t.Fatal("setup: telegram Casey not in graph")
	}

	follows := []FollowToApply{
		{Platform: "instagram", UserID: "100000001", Handle: "caseyexample", Name: "Casey Example",
			Signals: FollowSignals{Handle: "caseyexample", Name: "Casey Example", Mutual: true}, MergeTarget: target.ID},
		{Platform: "instagram", UserID: "100000002", Handle: "morganexample", Name: "Morgan Example",
			Signals: FollowSignals{Handle: "morganexample", Name: "Morgan Example", IsBusiness: true}},
		{Platform: "instagram", UserID: "999999999", Handle: "ghost", Name: "Not Ingested",
			Signals: FollowSignals{Mutual: true}}, // never ingested → unresolved
	}

	res, err := p.ApplyFollowClassifications(ctx, follows)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatalf("want 3 audit rows, got %d", len(res))
	}

	// friend → merged into the existing person, survivor typed person/friend
	if res[0].Action != "merged" {
		t.Fatalf("friend should merge, got %q", res[0].Action)
	}
	if res[0].SurvivorID != target.ID {
		t.Fatalf("survivor should be the existing person %q, got %q", target.ID, res[0].SurvivorID)
	}
	if res[0].NodeType != "person" || res[0].Category != "friend" {
		t.Fatalf("merged friend type/cat = %q/%q, want person/friend", res[0].NodeType, res[0].Category)
	}
	// the IG identity now resolves to the merged survivor + carries person type
	card, ok := p.ResolveSocialAuthor("instagram", "100000001", "caseyexample")
	if !ok || card.CanonicalID != target.ID {
		t.Fatalf("IG identity should resolve to survivor %q, got %q (ok=%v)", target.ID, card.CanonicalID, ok)
	}
	if card.Type != "person" {
		t.Fatalf("survivor node type should be person, got %q", card.Type)
	}

	// entity → its own entity node
	if res[1].Action != "entity-node" || res[1].NodeType != "entity" {
		t.Fatalf("Morgan Example should be entity-node/entity, got %q/%q", res[1].Action, res[1].NodeType)
	}
	if ec, ok := p.ResolveSocialAuthor("instagram", "100000002", "morganexample"); !ok || ec.Type != "entity" {
		t.Fatalf("entity node type should be entity, got %q (ok=%v)", ec.Type, ok)
	}

	// un-ingested → unresolved, not an error
	if res[2].Action != "unresolved" {
		t.Fatalf("un-ingested follow should be unresolved, got %q", res[2].Action)
	}
}

// compositeCard (merge-on-read) must carry the #49 node type/category — else a
// multi-canonical person (IG follow merged w/ phone/email) loses person/entity in the
// resolve view (the caseyexample/morganexample null), even though the underlying node is typed.
func TestCompositeCardCarriesNodeType(t *testing.T) {
	g := []api.PersonCard{
		{CanonicalID: "canon-phone-1", Name: "Casey Example", Identified: true, SalienceScore: 9}, // untyped, highest salience → base
		{CanonicalID: "canon-instagram-2", Name: "Casey Example", Identified: true, Type: "person", Category: "friend"},
	}
	got := compositeCard(g, "high")
	if got.Type != "person" || got.Category != "friend" {
		t.Fatalf("composite must carry person/friend from the typed member, got %q/%q", got.Type, got.Category)
	}
	// all-untyped composite defaults to person
	g2 := []api.PersonCard{{CanonicalID: "canon-phone-3", Name: "X"}, {CanonicalID: "canon-email-4", Name: "X"}}
	if compositeCard(g2, "high").Type != "person" {
		t.Fatal("all-untyped composite should default to person")
	}
}

// #54 backfill resolve: a known telegram identity resolves to its canonical name +
// that identity's handle; an unknown id, and a nameless/unidentified one, return
// ok=false so the backfill leaves the row blank rather than writing a fake name.
func TestResolveIdentityName(t *testing.T) {
	p := NewProjector(newFake(), nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg-known", "Jordan Example", "+15555550125", ""))
	p.Close()

	name, _, ok := p.ResolveIdentityName(event.PlatformTelegram, "tg-known")
	if !ok || name != "Jordan Example" {
		t.Fatalf("known telegram id should resolve to its name, got %q (ok=%v)", name, ok)
	}
	if _, _, ok := p.ResolveIdentityName(event.PlatformTelegram, "tg-never-seen"); ok {
		t.Fatal("unknown id must return ok=false (don't backfill)")
	}
}
