package gbrainfeed

import (
	"sort"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// mergeOnRead (#24) composes resolve_person hits that are FRAGMENTS of one person
// (the same de-accented name spread across per-platform canonicals — e.g. Morgan ×4)
// into ONE composite card, at DISPLAY time only — it never mutates the graph (the
// eval→auto-merge does the real merge; a confirmed composite can feed a proposal).
//
// Critically it never silently fuses DISTINCT same-named people: a name-group
// collapses to a high-confidence composite ONLY when its fragments share a
// corroborating signal (handle / phone / email local-part). Otherwise the fragments
// stay as SEPARATE cards flagged "same_name_unverified", so the agent/user can tell
// "Morgan's 4 platforms" (one person) from "2 different John Tans" (distinct).
func mergeOnRead(cards []api.PersonCard) []api.PersonCard {
	groups := map[string][]api.PersonCard{}
	var order []string
	for _, c := range cards {
		k := idmatch.NormName(c.Name)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], c)
	}
	out := make([]api.PersonCard, 0, len(cards))
	for _, k := range order {
		g := groups[k]
		switch {
		case len(g) == 1:
			out = append(out, g[0]) // single canonical — unchanged
		case fragmentsCorroborated(g):
			out = append(out, compositeCard(g, "high"))
		default:
			// same name, no corroboration → may be DISTINCT people: keep separate,
			// flag each so nothing downstream silently fuses them.
			for _, c := range g {
				c.MergeConfidence = "same_name_unverified"
				out = append(out, c)
			}
		}
	}
	return out
}

// fragmentsCorroborated reports whether ≥2 DISTINCT fragments share a phone or a
// distinctive IDENTIFIER — evidence they're the same person, not a name clash.
//
// The identifier pool deliberately unifies HANDLES and email LOCAL-PARTS so the
// match is CROSS-TYPE: a person's username appears as a handle on one platform
// (telegram "morganexample") and as the local-part of their email on another
// (morganexample@example.org) — same identifier, different field. Matching only within
// type would miss exactly that cross-platform link (Morgan's case). A distinctiveness
// guard (length + generic-localpart stoplist) keeps a shared generic local-part
// ("info"/"admin") from fusing two same-named strangers.
func fragmentsCorroborated(g []api.PersonCard) bool {
	phone, ident := map[string]int{}, map[string]int{}
	for _, c := range g {
		seenP, seenI := map[string]bool{}, map[string]bool{}
		add := func(m map[string]int, seen map[string]bool, n string) {
			if n == "" || seen[n] {
				return
			}
			seen[n] = true
			m[n]++ // count DISTINCT fragments carrying this value
		}
		for _, p := range c.Phones {
			add(phone, seenP, digitsOnly(p))
		}
		for _, h := range c.Handles {
			if n := lowerTrim(h); distinctiveIdent(n) {
				add(ident, seenI, n)
			}
		}
		for _, e := range c.Emails {
			if n := emailLocalPart(e); distinctiveIdent(n) {
				add(ident, seenI, n)
			}
		}
	}
	for _, m := range []map[string]int{phone, ident} {
		for _, n := range m {
			if n >= 2 {
				return true
			}
		}
	}
	return false
}

// distinctiveIdent gates the cross-type identifier match to values specific enough
// that a coincidental match implies the same person — not a generic role local-part.
func distinctiveIdent(s string) bool {
	if len(s) < 4 {
		return false
	}
	switch s {
	case "info", "admin", "contact", "noreply", "no-reply", "donotreply", "do-not-reply",
		"hello", "team", "support", "sales", "help", "mail", "email", "office", "accounts",
		"account", "billing", "marketing", "notifications", "notification", "service",
		"system", "alert", "alerts", "updates", "news", "newsletter", "digest", "receipts",
		"receipt", "orders", "order", "delivery", "booking", "bookings", "survey", "feedback":
		return false
	}
	return true
}

// compositeCard unions all fragment fields into one card, retaining every member
// canonical_id (so the user can confirm→merge or split).
func compositeCard(g []api.PersonCard, conf string) api.PersonCard {
	base := g[0]
	for _, c := range g {
		if c.SalienceScore > base.SalienceScore {
			base = c
		}
	}
	plats, emails, phones, handles, opens := set{}, set{}, set{}, set{}, set{}
	var idents []api.CardIdentity
	var memberIDs []string
	seenIdent := map[string]bool{}
	for _, c := range g {
		memberIDs = append(memberIDs, c.CanonicalID)
		for _, p := range c.Platforms {
			plats.add(p)
		}
		for _, e := range c.Emails {
			emails.add(e)
		}
		for _, p := range c.Phones {
			phones.add(p)
		}
		for _, h := range c.Handles {
			handles.add(h)
		}
		for _, o := range c.OpenIDs {
			opens.add(o)
		}
		for _, idn := range c.Identities {
			k := idn.Platform + ":" + idn.UserID
			if !seenIdent[k] {
				seenIdent[k] = true
				idents = append(idents, idn)
			}
		}
	}
	sort.Strings(memberIDs)
	// Carry the #49 node type/category into the composite (otherwise a multi-canonical
	// person — e.g. an IG follow merged with phone/email identities — loses its
	// person/entity classification in the merge-on-read view). Prefer any member's
	// explicit type; default to "person" (matching buildCard), since a resolved
	// multi-identity human is a person unless explicitly classed otherwise.
	nodeType, nodeCat := base.Type, base.Category
	for _, c := range g {
		if nodeType == "" || nodeType == "person" {
			if c.Type != "" && (nodeType == "" || c.Category != "") {
				nodeType, nodeCat = c.Type, c.Category
			}
		}
	}
	if nodeType == "" {
		nodeType = "person"
	}
	return api.PersonCard{
		CanonicalID:     base.CanonicalID,
		Name:            base.Name,
		Identified:      base.Identified,
		Platforms:       plats.sorted(),
		Identities:      idents,
		Emails:          emails.sorted(),
		Phones:          phones.sorted(),
		Handles:         handles.sorted(),
		OpenIDs:         opens.sorted(),
		SalienceScore:   base.SalienceScore,
		MatchedOn:       base.MatchedOn,
		Source:          "graph",
		MemberIDs:       memberIDs,
		MergeConfidence: conf,
		Type:            nodeType,
		Category:        nodeCat,
	}
}

func lowerTrim(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func emailLocalPart(s string) string {
	s = lowerTrim(s)
	if i := strings.IndexByte(s, '@'); i > 0 {
		return s[:i]
	}
	return ""
}
