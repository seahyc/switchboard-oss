package gbrainfeed

import (
	"sort"
	"strings"
	"sync"
)

// CanonicalPerson is ONE real human — the queryable entity. It is the primary
// home page; the per-platform identities (person-<platform>-<id>) become aliases
// that link up to it. A canonical person aggregates every identity switchboard
// believes is the same human, the union of their external ids (platform ids +
// phones + emails), and the best human name resolvable across them.
type CanonicalPerson struct {
	ID         string     // stable canonical slug, e.g. "canon-phone-6591234567"
	Name       string     // best resolved name across members
	NameSrc    nameSource // provenance of Name
	Identified bool       // false → only platform IDs, no real name (flag, don't fake)
	IsSelf     bool       // any member is the account owner
	Members    []identity // every per-platform identity merged into this person

	// Node type/category (#49) — the friend/creator axis, orthogonal to the salience
	// tier. type: "person" (default) | "entity"; category: person→friend|family|
	// colleague, entity→creator|brand|org|service. Set by the #49 classifier on IG
	// follows (a creator = type:entity, NOT merged into the inner circle). "" = unset.
	NodeType     string
	NodeCategory string
}

// nameRank orders name provenance; lower is more authoritative.
func nameRank(s nameSource) int {
	switch s {
	case srcManual:
		return -1
	case srcAddressBook:
		return 0
	case srcContactName:
		return 1
	case srcBusiness:
		return 2
	case srcPush:
		return 3
	case srcDisplay:
		return 4
	case srcUsername:
		return 5
	default:
		return 6
	}
}

// MergeEvent records a deterministic auto-merge (two canonicals collapsed because
// an identity bridged them via a shared phone/email).
type MergeEvent struct {
	Survivor string // canonical ID that remains
	Absorbed string // canonical ID folded into the survivor
	Via      string // signal label that bridged them ("phone"/"email")
}

// MergeProposal is a FUZZY (name-only) match that must NOT auto-merge — it is
// surfaced to the user for one-tap approval. A and B are canonical IDs.
type MergeProposal struct {
	A, B  string
	AName string
	BName string
	Score float64 // token-overlap similarity [0,1]
	Via   string  // e.g. "same name \"Mom\""
}

// Assignment is what an Observe produced: the up-to-date canonical snapshot plus
// any deterministic merges and any fuzzy proposals the caller must act on.
type Assignment struct {
	Canonical  CanonicalPerson
	AutoMerged []MergeEvent
	Proposals  []MergeProposal
}

// Resolver groups per-platform identities into canonical persons. Deterministic
// merges (shared phone/email) happen automatically; name-only matches are emitted
// as proposals. Safe for concurrent use, though the projector drives it serially.
type Resolver struct {
	mu        sync.Mutex
	sigIndex  map[string]string          // "phone:+65.."/"email:x" -> canonical ID
	idToCanon map[string]string          // identityKey -> canonical ID
	canon     map[string]*canonicalData  // canonical ID -> mutable person
	nameIndex map[string]map[string]bool // normName -> set of canonical IDs
	proposed  map[string]bool            // dedupe proposal pairs
	rejected  map[string]bool            // pairs the user said "not same" — never re-propose
	redirect  map[string]string          // absorbed canonical ID -> survivor (follow on lookup)
	override  map[string]string          // canonical ID -> human-confirmed name override
}

type canonicalData struct {
	id           string
	members      map[string]identity // identityKey -> richest identity seen
	nodeType     string              // #49: "person"(default)|"entity" — friend/creator axis
	nodeCategory string              // #49: friend|family|colleague | creator|brand|org|service
}

// NewResolver builds an empty Resolver.
func NewResolver() *Resolver {
	return &Resolver{
		sigIndex:  map[string]string{},
		idToCanon: map[string]string{},
		canon:     map[string]*canonicalData{},
		nameIndex: map[string]map[string]bool{},
		proposed:  map[string]bool{},
		rejected:  map[string]bool{},
		redirect:  map[string]string{},
		override:  map[string]string{},
	}
}

// resolveID follows the redirect chain to a canonical's current survivor.
func (r *Resolver) resolveID(id string) string {
	for {
		next, ok := r.redirect[id]
		if !ok || next == id {
			return id
		}
		id = next
	}
}

// Redirects returns a snapshot of every absorbed→survivor mapping, each followed to
// its FINAL survivor — the resolver's authoritative merge state. Callers reconcile
// the durable provenance audit (gbrain merged_into links) against this (#17), since
// links can drift (a pre-stub-fix silent link failure, or a reproject that forms a
// canonical by accretion rather than re-emitting a MergeEvent).
func (r *Resolver) Redirects() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.redirect))
	for absorbed := range r.redirect {
		if final := r.resolveID(absorbed); final != absorbed {
			out[absorbed] = final
		}
	}
	return out
}

// ForceMerge merges two canonicals on human approval, regardless of signals. It
// returns the survivor snapshot and the absorbed id (now redirected). ok is false
// if either id is unknown or they are already the same person.
func (r *Resolver) ForceMerge(a, b string) (survivor CanonicalPerson, absorbed string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ra, rb := r.resolveID(a), r.resolveID(b)
	if ra == rb {
		return CanonicalPerson{}, "", false
	}
	ca, cb := r.canon[ra], r.canon[rb]
	if ca == nil || cb == nil {
		return CanonicalPerson{}, "", false
	}
	// Survivor preference: strong-signal ID over singleton, then lexicographic.
	keep, drop := ra, rb
	if !preferSurvivor(ra, rb) {
		keep, drop = rb, ra
	}
	r.mergeInto(keep, drop)
	r.redirect[drop] = keep
	// Approving a merge clears any standing rejection of the pair.
	delete(r.rejected, pairKey(a, b))
	return r.snapshot(r.canon[keep]), drop, true
}

// preferSurvivor reports whether a should win over b as the merge survivor.
func preferSurvivor(a, b string) bool {
	sa := strings.HasPrefix(a, "canon-phone-") || strings.HasPrefix(a, "canon-email-")
	sb := strings.HasPrefix(b, "canon-phone-") || strings.HasPrefix(b, "canon-email-")
	if sa != sb {
		return sa
	}
	return a < b
}

// mergeInto folds the absorbed canonical's members + index entries into survivor.
func (r *Resolver) mergeInto(survivor, absorbed string) {
	ac := r.canon[absorbed]
	if ac == nil {
		return
	}
	sc := r.canon[survivor]
	for ik, m := range ac.members {
		sc.members[ik] = mergeIdentity(sc.members[ik], m)
		r.idToCanon[ik] = survivor
	}
	for s, owner := range r.sigIndex {
		if owner == absorbed {
			r.sigIndex[s] = survivor
		}
	}
	for _, set := range r.nameIndex {
		if set[absorbed] {
			delete(set, absorbed)
			set[survivor] = true
		}
	}
	if nm, ok := r.override[absorbed]; ok {
		if _, has := r.override[survivor]; !has {
			r.override[survivor] = nm
		}
		delete(r.override, absorbed)
	}
	// Carry the #49 node type/category: if a typed node (e.g. a classified IG follow)
	// is absorbed into an untyped person, the survivor inherits the classification so
	// it isn't lost in the merge (the projection-time ClassifyFollow also re-asserts it,
	// but this keeps the type correct regardless of follow-vs-merge event ordering).
	if sc.nodeType == "" && ac.nodeType != "" {
		sc.nodeType, sc.nodeCategory = ac.nodeType, ac.nodeCategory
	}
	delete(r.canon, absorbed)
}

// SetName records a human-confirmed name override for a canonical person (the
// "Edit name" review action) and returns the updated snapshot.
func (r *Resolver) SetName(canonID, name string) (CanonicalPerson, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.resolveID(canonID)
	cd := r.canon[id]
	if cd == nil {
		return CanonicalPerson{}, false
	}
	r.override[id] = strings.TrimSpace(name)
	return r.snapshot(cd), true
}

// SameCanonical reports whether two canonical ids now resolve to the SAME person
// (following merge redirects).
func (r *Resolver) SameCanonical(a, b string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resolveID(a) == r.resolveID(b)
}

// AllCanonicals returns a snapshot of every live canonical person — used by the
// LLM identity matcher (#19) to build its candidate set.
func (r *Resolver) AllCanonicals() []CanonicalPerson {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]CanonicalPerson, 0, len(r.canon))
	for _, cd := range r.canon {
		out = append(out, r.snapshot(cd))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SameLiveCanonical reports whether a and b resolve to the SAME, LIVE canonical
// person — i.e. they are genuinely already merged (a permanent, unambiguous
// outcome). Distinct from "either id is dead", which is ambiguous (could be a
// genuinely-absorbed id OR a resolver that hasn't rebuilt that canonical yet,
// e.g. right after a restart).
func (r *Resolver) SameLiveCanonical(a, b string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ra := r.resolveID(a)
	return ra == r.resolveID(b) && r.canon[ra] != nil
}

// Actionable reports whether a fuzzy proposal for (a,b) would still produce a REAL
// merge — i.e. exactly the condition ForceMerge needs to succeed: the two ids
// resolve to DISTINCT, LIVE canonical persons. It returns false when they're
// already the same person OR when either id is dead (absorbed elsewhere, or never
// created in this run — e.g. a durable proposal persisted across a reproject whose
// canonical no longer exists). Used to drop no-op proposals from the review queue.
func (r *Resolver) Actionable(a, b string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ra, rb := r.resolveID(a), r.resolveID(b)
	if ra == rb {
		return false
	}
	return r.canon[ra] != nil && r.canon[rb] != nil
}

// Snapshot returns the current canonical snapshot for an id (following redirects).
func (r *Resolver) Snapshot(canonID string) (CanonicalPerson, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cd := r.canon[r.resolveID(canonID)]
	if cd == nil {
		return CanonicalPerson{}, false
	}
	return r.snapshot(cd), true
}

// Reject records that the user declined a fuzzy merge of a/b so it is never
// re-proposed. Call from the review flow when the user taps "Not same".
func (r *Resolver) Reject(a, b string) {
	r.mu.Lock()
	r.rejected[pairKey(a, b)] = true
	r.mu.Unlock()
}

// MarkProposed seeds the in-memory proposed-set for a pair so indexNameAndPropose
// skips it — replayed on startup from the persisted PENDING proposals so the reproject
// doesn't re-emit (re-log + re-Add) every already-pending merge each restart (#61).
// Idempotent.
func (r *Resolver) MarkProposed(a, b string) {
	r.mu.Lock()
	r.proposed[pairKey(a, b)] = true
	r.mu.Unlock()
}

// Observe folds an identity into the canonical graph and returns the resulting
// assignment (snapshot + any auto-merges + any fuzzy proposals).
func (r *Resolver) Observe(id identity) Assignment {
	r.mu.Lock()
	defer r.mu.Unlock()

	ik := identityKey(id.Platform, id.UserID)
	sigs := strongSignals(id)

	// 1. Find candidate canonicals: the one this identity already belongs to, plus
	// any owning a shared strong signal.
	targets := map[string]bool{}
	if c, ok := r.idToCanon[ik]; ok {
		targets[c] = true
	}
	for _, s := range sigs {
		if c, ok := r.sigIndex[s]; ok {
			targets[c] = true
		}
	}

	var cid string
	var merges []MergeEvent
	switch len(targets) {
	case 0:
		// Brand-new canonical, keyed on the best available anchor.
		cid = r.newCanonicalID(id)
		r.canon[cid] = &canonicalData{id: cid, members: map[string]identity{}}
	case 1:
		for c := range targets {
			cid = c
		}
	default:
		// This identity bridges multiple canonicals → deterministic auto-merge.
		cid, merges = r.mergeAll(targets, sigs)
	}

	// 2. Attach/refresh the identity on the chosen canonical (richer name wins).
	cd := r.canon[cid]
	cd.members[ik] = mergeIdentity(cd.members[ik], id)
	r.idToCanon[ik] = cid
	for _, s := range sigs {
		r.sigIndex[s] = cid
	}

	// 3. Index the resolved name + look for fuzzy (name-only) matches to propose.
	snap := r.snapshot(cd)
	proposals := r.indexNameAndPropose(cid, snap)

	return Assignment{Canonical: snap, AutoMerged: merges, Proposals: proposals}
}

// mergeAll collapses every canonical in targets into one survivor and returns the
// survivor ID + the merge events. Survivor preference: a strong-signal-derived ID
// over a singleton, then lexicographically smaller (stable + deterministic).
func (r *Resolver) mergeAll(targets map[string]bool, sigs []string) (string, []MergeEvent) {
	ids := make([]string, 0, len(targets))
	for c := range targets {
		ids = append(ids, c)
	}
	sort.Slice(ids, func(i, j int) bool {
		si, sj := strings.HasPrefix(ids[i], "canon-phone-") || strings.HasPrefix(ids[i], "canon-email-"),
			strings.HasPrefix(ids[j], "canon-phone-") || strings.HasPrefix(ids[j], "canon-email-")
		if si != sj {
			return si // strong-signal IDs sort first → become survivor
		}
		return ids[i] < ids[j]
	})
	survivor := ids[0]
	via := "identity"
	if len(sigs) > 0 {
		via = sigLabel(sigs[0])
	}
	var events []MergeEvent
	for _, absorbed := range ids[1:] {
		r.mergeInto(survivor, absorbed)
		r.redirect[absorbed] = survivor
		events = append(events, MergeEvent{Survivor: survivor, Absorbed: absorbed, Via: via})
	}
	return survivor, events
}

// indexNameAndPropose records the canonical's resolved name and proposes a fuzzy
// merge with any OTHER canonical sharing the same normalized name (not already
// merged, proposed, or rejected). Exact-normalized name equality is high-precision
// fuzzy; the score is token-overlap for the reviewer's benefit.
func (r *Resolver) indexNameAndPropose(cid string, snap CanonicalPerson) []MergeProposal {
	if !snap.Identified {
		return nil
	}
	// Only propose on DISTINCTIVE names. A bare/short/common name ("Sean", "S",
	// "Jon") matches many DIFFERENT people, so a name-only merge there is likely
	// wrong + just creates review fatigue. Such names are neither indexed nor
	// proposed (strong-signal phone/email auto-merge still merges real dupes).
	if !distinctiveName(snap.Name) {
		return nil
	}
	nn := normName(snap.Name)
	if nn == "" {
		return nil
	}
	set := r.nameIndex[nn]
	if set == nil {
		set = map[string]bool{}
		r.nameIndex[nn] = set
	}
	var proposals []MergeProposal
	for other := range set {
		if other == cid {
			continue
		}
		// Two email-anchored canonicals sharing a name (#18): skip ONLY when either
		// side is a ROLE/service mailbox (no-reply/support/notifications/…) — those
		// are multiple addresses of one service (GoDaddy/AWS), not a person. But when
		// BOTH local-parts look PERSONAL, a same-name pair is likely ONE real human
		// with two personal emails (morgan@example.com + morganexample@example.org) → propose
		// it for review instead of blanket-skipping (which left him as 2 records).
		if isEmailCanonical(cid) && isEmailCanonical(other) {
			if r.hasRoleEmail(cid) || r.hasRoleEmail(other) {
				continue
			}
		}
		pk := pairKey(cid, other)
		if r.proposed[pk] || r.rejected[pk] {
			continue
		}
		r.proposed[pk] = true
		oc := r.canon[other]
		if oc == nil {
			continue
		}
		oSnap := r.snapshot(oc)
		proposals = append(proposals, MergeProposal{
			A: cid, B: other, AName: snap.Name, BName: oSnap.Name,
			Score: nameSimilarity(snap.Name, oSnap.Name),
			Via:   "same name " + strconvQuote(snap.Name),
		})
	}
	set[cid] = true
	return proposals
}

// minDistinctiveNameLen / minDistinctiveTokens gate which names are confident
// enough to seed a fuzzy merge proposal. Conservative + tunable.
const (
	minDistinctiveNameLen = 6
	minDistinctiveTokens  = 2
)

// distinctiveName reports whether a name is specific enough that two people
// sharing it are plausibly the SAME person (so a name-only merge is worth
// proposing). Requires multiple tokens + a reasonable length — single-token or
// short names (bare first names, initials) are too ambiguous.
func distinctiveName(name string) bool {
	name = strings.TrimSpace(name)
	if len([]rune(name)) < minDistinctiveNameLen {
		return false
	}
	return len(strings.Fields(name)) >= minDistinctiveTokens
}

func isEmailCanonical(id string) bool { return strings.HasPrefix(id, "canon-email-") }

// hasRoleEmail reports whether a canonical carries a ROLE/service-like email — any
// member email whose local-part is a generic role word (no-reply/support/info/…),
// i.e. not distinctive enough to be a person (#18). Reuses the merge-on-read role
// stoplist so the merge + propose layers agree on what's "personal".
func (r *Resolver) hasRoleEmail(canonID string) bool {
	c := r.canon[canonID]
	if c == nil {
		return false
	}
	for _, m := range c.members {
		lp := emailLocalPart(m.Email)
		if lp == "" {
			continue
		}
		// Strip a trailing numeric suffix so numbered service variants (noreply1,
		// noreply2, support3) read as their role word.
		if !distinctiveIdent(strings.TrimRight(lp, "0123456789")) {
			return true
		}
	}
	return false
}

// snapshot renders the immutable view of a canonical person: best name, identified
// flag, self flag, members sorted for determinism.
func (r *Resolver) snapshot(cd *canonicalData) CanonicalPerson {
	out := CanonicalPerson{ID: cd.id, NameSrc: srcNone}
	for _, m := range cd.members {
		out.Members = append(out.Members, m)
		if m.IsSelf {
			out.IsSelf = true
		}
	}
	// Sort members FIRST, then pick the name by iterating the sorted slice — so the
	// display name is fully DETERMINISTIC: best name-source wins, and ties (same
	// source rank, e.g. two address-book names) break by sorted member order, not
	// by random map iteration. This matters for merges: the survivor's name can't
	// flip run-to-run.
	sort.Slice(out.Members, func(i, j int) bool {
		if out.Members[i].Platform != out.Members[j].Platform {
			return out.Members[i].Platform < out.Members[j].Platform
		}
		return out.Members[i].UserID < out.Members[j].UserID
	})
	bestRank := 99
	for _, m := range out.Members {
		if name, src, ok := m.Names.Resolve(); ok && nameRank(src) < bestRank {
			bestRank = nameRank(src)
			out.Name = name
			out.NameSrc = src
			out.Identified = true
		}
	}
	// A human-confirmed name override always wins (the "Edit name" action).
	if nm, ok := r.override[cd.id]; ok && nm != "" {
		out.Name = nm
		out.NameSrc = srcManual
		out.Identified = true
	}
	out.NodeType = cd.nodeType
	out.NodeCategory = cd.nodeCategory
	return out
}

// SetNodeType tags a canonical's #49 node type/category (person|entity + the
// friend/creator subcategory), following the redirect chain to the live survivor.
// Returns false if the canonical is unknown. Idempotent.
func (r *Resolver) SetNodeType(canonID, nodeType, category string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cd := r.canon[r.resolveID(canonID)]
	if cd == nil {
		return false
	}
	cd.nodeType = nodeType
	cd.nodeCategory = category
	return true
}

// ResolveIdentityNode returns the live canonical snapshot for a platform identity
// (exact identityKey, following redirects), or ok=false. The #49 author→node seam:
// a social_activity author_id/handle resolves to its person/entity node + tier.
func (r *Resolver) ResolveIdentityNode(platform, userID string) (CanonicalPerson, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cid, ok := r.idToCanon[identityKey(platform, userID)]
	if !ok {
		return CanonicalPerson{}, false
	}
	cd := r.canon[r.resolveID(cid)]
	if cd == nil {
		return CanonicalPerson{}, false
	}
	return r.snapshot(cd), true
}

// newCanonicalID derives a stable canonical slug from the identity's strongest
// anchor: phone > email > the platform identity itself (singleton).
func (r *Resolver) newCanonicalID(id identity) string {
	switch {
	case id.Phone != "":
		return "canon-phone-" + slugFragment(id.Phone)
	case id.Email != "":
		return "canon-email-" + slugFragment(id.Email)
	default:
		return "canon-" + id.Platform + "-" + slugFragment(id.UserID)
	}
}

// mergeIdentity keeps the richer of two identity records for the same key: prefer
// a resolvable name, fill missing strong signals, prefer address-book provenance.
func mergeIdentity(a, b identity) identity {
	// b is the newly observed one; start from it and backfill from a.
	out := b
	if out.Phone == "" {
		out.Phone = a.Phone
	}
	if out.Email == "" {
		out.Email = a.Email
	}
	if out.Handle == "" {
		out.Handle = a.Handle
	}
	out.Names = mergeNames(a.Names, b.Names)
	out.IsSelf = a.IsSelf || b.IsSelf
	out.FromContact = a.FromContact || b.FromContact
	// Preserve the #49 follow-classify signals when coalescing: a follow event and a
	// bare post/sender event for the same identity collide in the queue, and whichever
	// lands second is `b`. If `b` has no signals, keep `a`'s — else an active poster's
	// post event arriving after its follow event would drop the classification and the
	// project-time ClassifyFollow hook would never fire (the caseyexample/morganexample null).
	if out.follow == nil {
		out.follow = a.follow
	}
	return out
}

// mergeNames unions two NameSets, preferring non-empty newer values but never
// dropping an address-book name in favor of a weaker source.
func mergeNames(a, b NameSet) NameSet {
	pick := func(newer, older string) string {
		if strings.TrimSpace(newer) != "" {
			return newer
		}
		return older
	}
	return NameSet{
		AddressBook: pick(b.AddressBook, a.AddressBook),
		First:       pick(b.First, a.First),
		Last:        pick(b.Last, a.Last),
		Business:    pick(b.Business, a.Business),
		Push:        pick(b.Push, a.Push),
		Display:     pick(b.Display, a.Display),
		Username:    pick(b.Username, a.Username),
	}
}

// nameSimilarity is token-set Jaccard over normalized names, in [0,1].
func nameSimilarity(a, b string) float64 {
	ta, tb := tokenSet(a), tokenSet(b)
	if len(ta) == 0 || len(tb) == 0 {
		return 0
	}
	inter := 0
	for t := range ta {
		if tb[t] {
			inter++
		}
	}
	union := len(ta) + len(tb) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func tokenSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, t := range strings.Fields(normName(s)) {
		out[t] = true
	}
	return out
}

// pairKey is an order-independent key for a canonical pair.
func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// slugFragment cleans a value into a slug-safe fragment.
func slugFragment(s string) string {
	clean := slugBad.ReplaceAllString(strings.ToLower(s), "-")
	return strings.Trim(clean, "-")
}

func strconvQuote(s string) string { return `"` + s + `"` }
