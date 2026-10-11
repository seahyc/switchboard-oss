package gbrainfeed

import (
	"strings"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// followSignalsFromEvent extracts the #49 classify signals from a Sub=="follow"
// social_activity event (the only event that carries them). Returns nil for every
// other event so the projector skips classification. The *bool signals deref to false
// when unset — a missing mutual/business flag conservatively reads as not-a-friend.
func followSignalsFromEvent(m event.Message) *FollowSignals {
	if m.Kind != event.KindSocialActivity || m.Social == nil || m.Social.Sub != "follow" {
		return nil
	}
	deref := func(b *bool) bool { return b != nil && *b }
	return &FollowSignals{
		Handle:     m.Sender.Handle,
		Name:       m.Sender.DisplayName,
		Mutual:     deref(m.Social.Mutual),
		IsBusiness: deref(m.Social.IsBusiness),
		Followers:  m.Social.Followers,
	}
}

// FollowSignals are the social-profile signals the #49 classifier reads to decide
// whether a followed account (IG today; X/etc. later) is a personal friend (a
// "person" node) or a creator/brand/org ("entity" node). Sourced from
// igweb.ProfileCard / the /instagram/following entries — kept as a local struct
// (no igweb import) so the rule unit-tests dependency-free; the orchestration layer
// (daemon) populates it from the profile read at the call site.
type FollowSignals struct {
	Handle     string
	Name       string
	Bio        string
	Mutual     bool // the user follows them AND they follow back
	IsBusiness bool // IG business/creator account flag
	Followers  int
	Following  int
}

// FollowClass is the classifier output fed to Projector.SetNodeType: a node type
// ("person"|"entity") plus a coarse category. Type is DETERMINISTIC; Category is a
// best-effort hint that the human-in-the-loop eyeball may override.
type FollowClass struct {
	Type     string // "person" | "entity"
	Category string // person: friend (default, human may set colleague/family); entity: creator|brand|org
}

// ClassifyFollow decides person-vs-entity for a followed account, DETERMINISTICALLY
// (no LLM): a MUTUAL follow (you follow each other) that is NOT a business account
// and whose name carries no registered-company marker is a personal friend; anything
// else — a one-way follow, a business/creator account, or a brand/org name — is an
// entity. Classification uses account signals without inference.
func ClassifyFollow(s FollowSignals) FollowClass {
	if s.Mutual && !s.IsBusiness && !orgMarkerRe.MatchString(s.Name) {
		return FollowClass{Type: "person", Category: "friend"}
	}
	return FollowClass{Type: "entity", Category: entityCategory(s)}
}

// entityCategory is a COARSE sub-label hint for an entity follow (override-able by
// the eyeball): org for charities/foundations/registered companies, brand for a
// business account whose name reads as a product/company rather than a person, else
// creator (a public personality / one-way-followed individual).
func entityCategory(s FollowSignals) string {
	n := strings.ToLower(s.Name + " " + s.Handle)
	if orgMarkerRe.MatchString(s.Name) ||
		strings.Contains(n, "org") || strings.Contains(n, "foundation") ||
		strings.Contains(n, "charity") || strings.Contains(n, "office") {
		return "org"
	}
	if s.IsBusiness && !looksPersonalName(s.Name) {
		return "brand"
	}
	return "creator"
}

// looksPersonalName is a light heuristic: a 1–3 word name with no brand/company
// punctuation reads as a person (so a business-flagged personal creator account —
// e.g. "James Clear" — is a creator, not a brand).
func looksPersonalName(name string) bool {
	n := strings.TrimSpace(name)
	if n == "" {
		return false
	}
	if strings.ContainsAny(n, "&.@/|") {
		return false
	}
	w := strings.Fields(n)
	return len(w) >= 1 && len(w) <= 3
}
