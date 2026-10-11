package gbrainfeed

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ProposalStatus is the lifecycle of a fuzzy-merge proposal.
type ProposalStatus string

const (
	ProposalPending  ProposalStatus = "pending"
	ProposalApproved ProposalStatus = "approved"
	ProposalRejected ProposalStatus = "rejected"
	// ProposalSuppressed = dropped by the high-confidence policy (placeholder /
	// non-distinctive / org-email), NOT a user decision. Used to retroactively
	// clear noise persisted before the proposer policy tightened.
	ProposalSuppressed ProposalStatus = "suppressed"
)

// Proposal is a name-only (fuzzy) merge awaiting human approval. Strong-signal
// merges happen automatically and never become proposals. A and B are canonical
// person IDs; Reason explains the match for the review prompt.
type Proposal struct {
	ID     string         `json:"id"` // stable id = pairKey(A,B)
	A      string         `json:"a"`
	B      string         `json:"b"`
	AName  string         `json:"a_name"`
	BName  string         `json:"b_name"`
	Score  float64        `json:"score"`
	Reason string         `json:"reason"`
	Status ProposalStatus `json:"status"`
}

// ProposalStore persists fuzzy-merge proposals and their resolutions. The review
// flow (Telegram clarify buttons) lists Pending(), then calls the projector's
// ApproveMerge / RejectProposal, which mark the proposal here. A persistent
// implementation can survive restarts so a once-rejected pair is never re-asked.
type ProposalStore interface {
	Add(p Proposal)
	Pending() []Proposal
	Get(id string) (Proposal, bool)
	SetStatus(id string, status ProposalStatus)
	// Rejected returns all proposals the user marked "Not same", so the resolver
	// can replay them on startup and never re-propose a rejected pair.
	Rejected() []Proposal
	// Approved returns all proposals confirmed as a merge (human "Same person" OR
	// #19 auto-merge), so the projector can REPLAY them on startup — a force-merge
	// lives only in the resolver's in-memory redirect map and is otherwise lost when
	// the resolver rebuilds from the firehose (which only re-derives strong-signal
	// merges). Without this replay, every restart silently un-does approved merges.
	Approved() []Proposal
}

// MemoryProposalStore is an in-process ProposalStore. Adds are idempotent on the
// pair id and never downgrade a resolved proposal back to pending.
type MemoryProposalStore struct {
	mu sync.Mutex
	m  map[string]Proposal
}

// NewMemoryProposalStore builds an empty in-memory store.
func NewMemoryProposalStore() *MemoryProposalStore {
	return &MemoryProposalStore{m: map[string]Proposal{}}
}

func (s *MemoryProposalStore) Add(p Proposal) {
	if p.ID == "" {
		p.ID = pairKey(p.A, p.B)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.m[p.ID]; ok && existing.Status != ProposalPending {
		return // already resolved; don't re-open
	}
	if p.Status == "" {
		p.Status = ProposalPending
	}
	s.m[p.ID] = p
}

func (s *MemoryProposalStore) Pending() []Proposal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Proposal
	for _, p := range s.m {
		if p.Status == ProposalPending {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *MemoryProposalStore) Get(id string) (Proposal, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.m[id]
	return p, ok
}

func (s *MemoryProposalStore) SetStatus(id string, status ProposalStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.m[id]; ok {
		p.Status = status
		s.m[id] = p
	}
}

func (s *MemoryProposalStore) Rejected() []Proposal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Proposal
	for _, p := range s.m {
		if p.Status == ProposalRejected {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *MemoryProposalStore) Approved() []Proposal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Proposal
	for _, p := range s.m {
		if p.Status == ProposalApproved {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// --- Review actions the bot drives (human-in-the-loop) ---

// MergeOutcome reports what an approved merge produced so the review surface can
// confirm the result ("merged → now shown as «X»"), resolve a rename in a pair
// context (rename the survivor), and offer a one-tap rename. Both names are always
// retained as aliases on the survivor — a merge never loses a name.
type MergeOutcome struct {
	SurvivorID    string `json:"survivor_id"`
	SurvivorName  string `json:"survivor_name"`
	AbsorbedID    string `json:"absorbed_id,omitempty"`
	Identified    bool   `json:"identified"`
	AlreadyMerged bool   `json:"already_merged,omitempty"` // a,b were already one person
}

// ApproveMerge confirms a fuzzy proposal: it force-merges the two canonical
// persons, re-puts the survivor page, links the absorbed page merged_into the
// survivor, and marks the proposal approved. Returns the survivor {id,name} so the
// caller can confirm/rename. Idempotent.
func (p *Projector) ApproveMerge(ctx context.Context, a, b string) (MergeOutcome, error) {
	// Capture both names BEFORE the merge (the absorbed snapshot is gone after) so
	// the durable approved record keeps them.
	aName, bName := p.snapName(a), p.snapName(b)
	survivor, absorbed, ok := p.resolver.ForceMerge(a, b)
	if !ok {
		// Already merged or unknown — record the approval durably + report current.
		p.persistApproved(a, b, aName, bName)
		if cp, found := p.resolver.Snapshot(a); found {
			return MergeOutcome{SurvivorID: cp.ID, SurvivorName: cp.Name, Identified: cp.Identified, AlreadyMerged: true}, nil
		}
		return MergeOutcome{AlreadyMerged: true}, nil
	}
	// Route through commitCanonical so this write serializes (and re-snapshots)
	// against any concurrent pool worker touching the same canonical slug.
	if err := p.commitCanonical(ctx, survivor.ID, survivor); err != nil {
		return MergeOutcome{}, err
	}
	// #17: stub the absorbed page so the merged_into link has a real source (clean
	// provenance for auto-merge reversibility/audit).
	if err := p.writeMergedStub(ctx, absorbed, survivor.ID, survivor.Name); err != nil {
		p.log.Warn("merged stub write failed", "absorbed", absorbed, "err", err)
	}
	p.linkOnce(ctx, "merged:"+absorbed+"|"+survivor.ID, absorbed, survivor.ID,
		"merged_into", "human-approved merge")
	// Durably record the approval so it REPLAYS on restart (LoadApprovedMerges) —
	// SetStatus alone no-ops when there was no pending row (e.g. an auto-merge or a
	// direct approve), which is exactly how merges were silently lost before.
	p.persistApproved(a, b, aName, bName)
	p.log.Info("gbrain merge approved by user", "survivor", survivor.ID, "absorbed", absorbed, "name", survivor.Name)
	return MergeOutcome{
		SurvivorID:   survivor.ID,
		SurvivorName: survivor.Name,
		AbsorbedID:   absorbed,
		Identified:   survivor.Identified,
	}, nil
}

// snapName returns the resolver's current name for a canonical id, or the id when
// unknown — used to label a durable approved record.
func (p *Projector) snapName(id string) string {
	if cp, ok := p.resolver.Snapshot(id); ok && cp.Name != "" {
		return cp.Name
	}
	return id
}

// persistApproved durably records that (a,b) is an approved merge so it survives a
// restart and replays via LoadApprovedMerges. It UPSERTS the row (Add inserts when
// absent) and then forces the status to approved (covering a pair that pre-existed
// as pending or rejected). Without this, SetStatus alone silently dropped approvals
// that had no pending row — the auto-merge and direct-approve paths.
func (p *Projector) persistApproved(a, b, aName, bName string) {
	id := pairKey(a, b)
	p.proposals.Add(Proposal{ID: id, A: a, B: b, AName: aName, BName: bName, Status: ProposalApproved})
	p.proposals.SetStatus(id, ProposalApproved)
}

// RejectProposal records a "Not same" decision: the pair is never re-proposed and
// the proposal is marked rejected.
func (p *Projector) RejectProposal(a, b string) {
	p.resolver.Reject(a, b)
	p.proposals.SetStatus(pairKey(a, b), ProposalRejected)
	p.log.Info("gbrain merge rejected by user", "a", a, "b", b)
}

// PendingProposals returns the pending proposals that are still ACTIONABLE,
// dropping any whose two canonicals have since become the SAME person — i.e. a
// strong-signal phone/email auto-merge collapsed them AFTER the fuzzy name
// proposal was raised, so approving would be a no-op. Such stale proposals are
// self-healed (marked resolved) so they also drop out of the persisted store.
//
// The daemon should read THIS (not Proposals().Pending() directly) so the review
// bot only ever shows genuinely-actionable merges.
func (p *Projector) PendingProposals() []Proposal {
	all := p.proposals.Pending()
	out := make([]Proposal, 0, len(all))
	for _, pr := range all {
		// (a) Policy noise (placeholder / non-distinctive / org-email) is decided
		// purely from the proposal's names + slugs — resolver-INDEPENDENT, so it is
		// always safe to persist-suppress.
		if !proposalWorthShowing(pr) {
			p.proposals.SetStatus(pr.ID, ProposalSuppressed)
			continue
		}
		// (b) Genuinely already merged: both ids resolve to ONE LIVE canonical. A
		// permanent, unambiguous outcome → safe to persist-approve.
		if p.resolver.SameLiveCanonical(pr.A, pr.B) {
			p.proposals.SetStatus(pr.ID, ProposalApproved)
			continue
		}
		// (c) Actionable now (distinct + both live) → show it.
		if p.resolver.Actionable(pr.A, pr.B) {
			out = append(out, pr)
			continue
		}
		// (d) Otherwise at least one id is not a live canonical. This is AMBIGUOUS:
		// the canonical may be genuinely absorbed, OR the resolver simply hasn't
		// rebuilt it yet (e.g. right after a daemon restart, before reprojection).
		// Filter from view but NEVER persist — persisting here destructively discards
		// a genuine proposal that becomes actionable once the resolver finishes
		// rebuilding. (This was the bug that bulk-"approved" real merges on an
		// incomplete post-restart resolver.)
	}
	return out
}

// proposalWorthShowing applies the same high-confidence policy as the fuzzy
// proposer (placeholder names out, distinctive names only, no org email↔email),
// so noise persisted before the policy tightened is also dropped from the queue.
func proposalWorthShowing(pr Proposal) bool {
	if isPlaceholderName(pr.AName) || isPlaceholderName(pr.BName) {
		return false
	}
	if !distinctiveName(pr.AName) {
		return false
	}
	if isEmailCanonical(pr.A) && isEmailCanonical(pr.B) {
		return false
	}
	return true
}

// LoadRejections replays persisted "Not same" decisions into the resolver so a
// previously rejected pair is never re-proposed after a daemon restart. Call once
// after SetProposalStore and before Start (no-op for the in-memory store on a
// fresh process). Safe + idempotent.
func (p *Projector) LoadRejections() {
	n := 0
	for _, pr := range p.proposals.Rejected() {
		p.resolver.Reject(pr.A, pr.B)
		n++
	}
	if n > 0 {
		p.log.Info("replayed persisted merge rejections", "count", n)
	}
}

// LoadPendingProposals replays persisted PENDING proposals into the resolver's
// proposed-set so the startup reproject's indexNameAndPropose skips pairs that are
// already queued — instead of re-emitting (re-log + re-Add) every existing proposal on
// every restart (#61: a 12min cold reproject window on a large graph, "gbrain merge
// proposed" wall-to-wall). Mirrors LoadRejections; call after SetProposalStore + before
// the reproject. Propose-only + idempotent — never changes auto-merge behavior, only
// suppresses duplicate re-proposals of pairs already pending review.
func (p *Projector) LoadPendingProposals() {
	n := 0
	for _, pr := range p.proposals.Pending() {
		p.resolver.MarkProposed(pr.A, pr.B)
		n++
	}
	if n > 0 {
		p.log.Info("replayed persisted pending merge proposals", "count", n)
	}
}

// LoadApprovedMerges replays every CONFIRMED merge (human "Same person" and #19
// auto-merge) back into the resolver so it survives a restart. A force-merge lives
// only in the resolver's in-memory redirect map; on restart the resolver rebuilds
// from the firehose, which re-derives ONLY strong-signal (shared phone/email)
// merges — so without this replay, every restart silently splits approved people
// back apart (observed: Morgan split across his phone- and email-canonicals after
// a redeploy). MUST run AFTER the reproject has built the canonicals (a pair whose
// canonical isn't built yet is reported as "deferred"); it is idempotent and cheap
// on re-run, so the daemon should also call it at each matcher tick to self-heal.
//
// Returns applied (merges re-established this call) and deferred (approved pairs
// whose canonicals aren't live yet — a later call, post-reproject, will apply
// them). Both names are always retained as aliases; a replayed merge never loses a
// name. Critically, this MUST be wired before #19 auto-merge flips on, or every
// auto-merge would be undone on the next restart.
func (p *Projector) LoadApprovedMerges(ctx context.Context) (applied, deferred int) {
	for _, pr := range p.proposals.Approved() {
		if p.resolver.SameLiveCanonical(pr.A, pr.B) {
			continue // already one live person — nothing to replay
		}
		if !p.resolver.Actionable(pr.A, pr.B) {
			deferred++ // a canonical isn't rebuilt yet; a post-reproject re-run applies it
			continue
		}
		if _, err := p.ApproveMerge(ctx, pr.A, pr.B); err != nil {
			p.log.Warn("replay approved merge failed", "a", pr.A, "b", pr.B, "err", err)
			deferred++
			continue
		}
		applied++
	}
	if applied > 0 || deferred > 0 {
		p.log.Info("replayed approved merges", "reapplied", applied, "deferred", deferred)
	}
	return applied, deferred
}

// ResolveProposal applies a human decision to a pending proposal by id — the
// convenience the daemon's POST /people/proposals/{id}/resolve calls so it never
// has to unpack the A/B pair itself. decision is "approve" (merge) or "reject"
// (never re-propose). Unknown id or decision returns an error.
// ResolveProposal applies a decision by proposal id. Back-compatible signature
// (error only) — the daemon's existing api.Backend uses this. For the survivor
// {id,name} (to confirm/rename in the bot), call ResolveProposalOutcome.
func (p *Projector) ResolveProposal(ctx context.Context, id, decision string) error {
	_, err := p.ResolveProposalOutcome(ctx, id, decision)
	return err
}

// ResolveProposalOutcome is ResolveProposal plus the merge result: a non-nil
// *MergeOutcome for an "approve" (survivor {id,name} so the caller can confirm
// "merged → now «X»" and offer a rename) and nil for a "reject".
func (p *Projector) ResolveProposalOutcome(ctx context.Context, id, decision string) (*MergeOutcome, error) {
	pr, ok := p.proposals.Get(id)
	if !ok {
		return nil, fmt.Errorf("unknown proposal %q", id)
	}
	switch decision {
	case "approve":
		out, err := p.ApproveMerge(ctx, pr.A, pr.B)
		if err != nil {
			return nil, err
		}
		return &out, nil
	case "reject":
		p.RejectProposal(pr.A, pr.B)
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid decision %q (want \"approve\" or \"reject\")", decision)
	}
}

// RenameCanonical applies a human-confirmed name ("Edit name") to a canonical
// person and re-puts its page.
func (p *Projector) RenameCanonical(ctx context.Context, canonID, name string) error {
	cp, ok := p.resolver.SetName(canonID, name)
	if !ok {
		return nil
	}
	return p.commitCanonical(ctx, cp.ID, cp)
}
