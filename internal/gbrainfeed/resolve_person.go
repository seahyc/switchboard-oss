package gbrainfeed

import (
	"sort"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// SenderRef is a (platform, sender_id) pointer into the message store. It's the
// identity set that `search person=<name>` must match messages on — by sender_id,
// NOT sender_name (which is blank for lark + most platforms, since names resolve via
// the people-graph, not message rows). This is the fix for the broken
// name→messages path (#32): resolve the name → identities → sender_ids → filter.
type SenderRef struct {
	Platform string `json:"platform"`
	SenderID string `json:"sender_id"`
}

// SenderIDsFromCards flattens resolved PersonCards into the DISTINCT (platform,
// sender_id) set across every identity — for the search person= filter. Works on
// WHATEVER the resolve produced: graph hits, a merge-on-read composite (all of
// Morgan's platforms), OR a Lark-directory card (Binh's open_id) — so "read X's
// messages" works even for a colleague only in the org directory, as long as her
// messages carry that sender_id. The CardIdentity.UserID IS the message sender_id
// (lark open_id ou_…, whatsapp …@s.whatsapp.net, telegram id, etc.).
func SenderIDsFromCards(cards []api.PersonCard) []SenderRef {
	seen := map[string]bool{}
	var out []SenderRef
	for _, c := range cards {
		for _, idn := range c.Identities {
			if idn.UserID == "" {
				continue
			}
			k := idn.Platform + ":" + idn.UserID
			if !seen[k] {
				seen[k] = true
				out = append(out, SenderRef{Platform: idn.Platform, SenderID: idn.UserID})
			}
		}
	}
	return out
}

// ResolveSenderIDs is the graph-side convenience: resolve a name through the people
// graph + merge-on-read, then flatten to sender refs. The search handler should
// prefer SenderIDsFromCards over its FINAL card set (graph→directory fallback), so a
// directory-only person (e.g. Casey) still yields her sender_id; ResolveSenderIDs
// covers only the graph path on its own.
func (p *Projector) ResolveSenderIDs(query string) []SenderRef {
	return SenderIDsFromCards(p.ResolvePerson(query))
}

// ResolvePerson finds canonical people whose name — or any alias / per-platform
// display name — matches the accent-FOLDED query (idmatch.NormName, which now folds
// Vietnamese đ→d and friends, so "Bình An Đỗ" resolves "Binh An Do"). Matching is
// folded at query+compare time (no FTS reindex needed): an EXACT folded-name match
// ranks first, then a token-SUBSET match ("Morgan" → "Morgan Example"). Results are
// ordered exact-first, then by salience, then name. Returns an EMPTY slice (never
// nil) when nothing matches → the caller's clean "not in graph" → try the directory.
func (p *Projector) ResolvePerson(query string) []api.PersonCard {
	q := idmatch.NormName(query)
	if q == "" {
		return []api.PersonCard{}
	}
	qset := map[string]bool{}
	for _, t := range strings.Fields(q) {
		qset[t] = true
	}

	type scored struct {
		card  api.PersonCard
		exact bool
		sal   float64
	}
	var hits []scored
	for _, cp := range p.resolver.AllCanonicals() {
		matchedOn, exact := "", false
		candidates := append([]string{cp.Name}, memberNames(cp)...)
		for _, nm := range candidates {
			fn := idmatch.NormName(nm)
			if fn == "" {
				continue
			}
			if fn == q {
				matchedOn, exact = nm, true
				break
			}
			if matchedOn == "" && tokenSubset(qset, fn) {
				matchedOn = nm // keep scanning in case a later name is an exact hit
			}
		}
		if matchedOn == "" {
			continue
		}
		hits = append(hits, scored{p.buildCard(cp, matchedOn), exact, p.salience[cp.ID]})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].exact != hits[j].exact {
			return hits[i].exact
		}
		if hits[i].sal != hits[j].sal {
			return hits[i].sal > hits[j].sal
		}
		return hits[i].card.Name < hits[j].card.Name
	})
	out := make([]api.PersonCard, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.card)
	}
	// Merge-on-read (#24): collapse per-platform fragments of one person into a
	// single composite card (confident) or keep distinct same-names separable.
	return mergeOnRead(out)
}

// memberNames returns every display name across a canonical's member identities.
func memberNames(cp CanonicalPerson) []string {
	var out []string
	for _, m := range cp.Members {
		out = append(out, m.Names.allNames()...)
	}
	return out
}

// tokenSubset reports whether every query token appears in the folded name's tokens.
func tokenSubset(qset map[string]bool, foldedName string) bool {
	ntok := map[string]bool{}
	for _, t := range strings.Fields(foldedName) {
		ntok[t] = true
	}
	for t := range qset {
		if !ntok[t] {
			return false
		}
	}
	return true
}

// buildCard projects a canonical person into a PersonCard.
func (p *Projector) buildCard(cp CanonicalPerson, matchedOn string) api.PersonCard {
	card := api.PersonCard{
		CanonicalID:   cp.ID,
		Name:          cp.Name,
		Identified:    cp.Identified,
		MatchedOn:     matchedOn,
		SalienceScore: p.salience[cp.ID],
		SalienceTier:  p.TierOf(cp.ID), // so resolved cards carry the tier (honcho/briefing gates depend on it)
		Source:        "graph",
	}
	plats, emails, phones, handles, openIDs := set{}, set{}, set{}, set{}, set{}
	for _, m := range cp.Members {
		plats.add(m.Platform)
		dn, _, _ := m.Names.Resolve()
		card.Identities = append(card.Identities, api.CardIdentity{
			Platform: m.Platform, UserID: m.UserID, DisplayName: dn, Handle: m.Handle,
		})
		emails.add(m.Email)
		phones.add(m.Phone)
		handles.add(m.Handle)
		if strings.HasPrefix(m.UserID, "ou_") { // lark open_id
			openIDs.add(m.UserID)
		}
	}
	card.Platforms = plats.sorted()
	card.Emails = emails.sorted()
	card.Phones = phones.sorted()
	card.Handles = handles.sorted()
	card.OpenIDs = openIDs.sorted()
	// Node type/category (#49). Default unset → "person" so existing nodes read as
	// people for the briefing's type=="person" filter; the classifier sets entities.
	card.Type = cp.NodeType
	if card.Type == "" {
		card.Type = "person"
	}
	card.Category = cp.NodeCategory
	return card
}

// ResolveSocialAuthor resolves a social-post author identity (#49) — an IG/X/etc.
// author_id (+ optional @handle fallback) — to its canonical NODE card carrying
// type/category + salience_tier. The briefing join: for each social_activity event,
// keep it iff card.Type=="person" && card.SalienceTier ∈ {inner,active}. Resolution:
// exact identityKey first, then a handle match. ok=false when unresolved.
func (p *Projector) ResolveSocialAuthor(platform, userID, handle string) (api.PersonCard, bool) {
	if cp, ok := p.resolver.ResolveIdentityNode(platform, userID); ok {
		return p.buildCard(cp, "id:"+userID), true
	}
	if handle != "" {
		h := idmatch.NormName(strings.TrimPrefix(handle, "@"))
		for _, cp := range p.resolver.AllCanonicals() {
			for _, m := range cp.Members {
				if idmatch.NormName(m.Handle) == h && h != "" {
					return p.buildCard(cp, "handle:"+handle), true
				}
			}
		}
	}
	return api.PersonCard{}, false
}

// SetNodeType tags a canonical's #49 node type/category (the classifier's output:
// friend/creator axis). Passthrough to the resolver; returns false if unknown.
func (p *Projector) SetNodeType(canonID, nodeType, category string) bool {
	return p.resolver.SetNodeType(canonID, nodeType, category)
}

// ResolveIdentityName maps a (platform, userID) sender to the people-graph's resolved
// display name + that identity's handle — the #54 backfill seam for blank Telegram DM
// sender_name rows. Returns ok=false when the identity isn't in the graph OR resolves
// to a nameless/unidentified canonical, so the caller skips it rather than writing an
// empty name over the blank.
func (p *Projector) ResolveIdentityName(platform, userID string) (name, handle string, ok bool) {
	cp, found := p.resolver.ResolveIdentityNode(platform, userID)
	if !found || !cp.Identified || strings.TrimSpace(cp.Name) == "" {
		return "", "", false
	}
	for _, m := range cp.Members {
		if m.Platform == platform && m.UserID == userID {
			handle = m.Handle
			break
		}
	}
	return cp.Name, handle, true
}

// CanonicalCount returns how many canonical persons are currently in the resolver
// (the in-memory identity-graph size). Diagnostic: a near-empty count when a known id
// fails to resolve means the resolver isn't built yet (timing/race); a large count
// with the id still missing means a keying/store mismatch.
func (p *Projector) CanonicalCount() int {
	return len(p.resolver.AllCanonicals())
}

// set is a tiny string set that drops empties and returns sorted keys.
type set map[string]bool

func (s set) add(v string) {
	if v != "" {
		s[v] = true
	}
}

func (s set) sorted() []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
