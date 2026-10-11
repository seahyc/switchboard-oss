package idmatch

import (
	"sort"
	"strings"
)

// LabelIdentity is a per-identity row (from the message store / graph) used to
// DERIVE ground-truth labels WITHOUT the model: confirmed phone/email establishes
// same-vs-different. Phone/Email are pre-normalized ("" if absent).
type LabelIdentity struct {
	ID       string // stable record id, e.g. "whatsapp:15555550101@s.whatsapp.net"
	Platform string
	Name     string
	Phone    string
	Email    string
}

// LabelConfig bounds the labeled set so a few shared family numbers / very common
// names don't dominate it.
type LabelConfig struct {
	MaxPairsPerSignalGroup int // cap positive pairs from one phone/email group (default 6)
	MaxPairsPerNameGroup   int // cap negative pairs from one same-name group (default 6)
}

var DefaultLabelConfig = LabelConfig{MaxPairsPerSignalGroup: 6, MaxPairsPerNameGroup: 6}

// BuildLabeledSet derives the eval gate's ground truth from confirmed signals:
//   - POSITIVES: identities sharing a confirmed phone OR email are the SAME person.
//     The emitted PersonRecords have the phone/email STRIPPED, so the matcher must
//     recover them from name+context alone — testing recall + precision honestly.
//   - NEGATIVES: identities with the same normalized name but CONFLICTING confirmed
//     signals (each has a phone/email and they differ) are DIFFERENT people.
//
// Returns the deduped record set + the labeled pairs. Pure + deterministic.
func BuildLabeledSet(ids []LabelIdentity, cfg LabelConfig) ([]PersonRecord, []LabeledPair) {
	if cfg.MaxPairsPerSignalGroup <= 0 {
		cfg = DefaultLabelConfig
	}
	// Stable order for determinism.
	sort.Slice(ids, func(i, j int) bool { return ids[i].ID < ids[j].ID })

	used := map[string]bool{} // record ids referenced by some label
	var labeled []LabeledPair
	seenPair := map[string]bool{}

	emit := func(a, b LabelIdentity, same bool, note string) {
		if a.ID == b.ID {
			return
		}
		k := pairKey(a.ID, b.ID)
		if seenPair[k] {
			return
		}
		seenPair[k] = true
		labeled = append(labeled, LabeledPair{A: a.ID, B: b.ID, SamePerson: same, Note: note})
		used[a.ID] = true
		used[b.ID] = true
	}

	// POSITIVES — group by phone, then by email.
	for _, by := range []func(LabelIdentity) string{
		func(x LabelIdentity) string { return x.Phone },
		func(x LabelIdentity) string { return x.Email },
	} {
		groups := map[string][]LabelIdentity{}
		for _, x := range ids {
			if k := by(x); k != "" {
				groups[k] = append(groups[k], x)
			}
		}
		for sig, g := range groups {
			n := 0
			for i := 0; i < len(g) && n < cfg.MaxPairsPerSignalGroup; i++ {
				for j := i + 1; j < len(g) && n < cfg.MaxPairsPerSignalGroup; j++ {
					// Only a useful positive if the names differ at least a little
					// (identical-name same-signal is trivial); but keep all — recall
					// over a confirmed-same pair is exactly what we measure.
					emit(g[i], g[j], true, "shared "+sigLabel(sig))
					n++
				}
			}
		}
	}

	// NEGATIVES — same normalized name, conflicting confirmed signals.
	nameGroups := map[string][]LabelIdentity{}
	for _, x := range ids {
		if nn := normName(x.Name); nn != "" {
			nameGroups[nn] = append(nameGroups[nn], x)
		}
	}
	for _, g := range nameGroups {
		n := 0
		for i := 0; i < len(g) && n < cfg.MaxPairsPerNameGroup; i++ {
			for j := i + 1; j < len(g) && n < cfg.MaxPairsPerNameGroup; j++ {
				if conflictingSignals(g[i], g[j]) && !seenPair[pairKey(g[i].ID, g[j].ID)] {
					emit(g[i], g[j], false, "same name, conflicting "+conflictLabel(g[i], g[j]))
					n++
				}
			}
		}
	}

	// Build the record set (signal-STRIPPED) for every identity referenced by a label.
	var people []PersonRecord
	for _, x := range ids {
		if !used[x.ID] {
			continue
		}
		people = append(people, PersonRecord{
			CanonicalID: x.ID,
			PrimaryName: x.Name,
			Names:       []string{x.Name},
			Platforms:   []string{x.Platform},
			// Phone/Email intentionally omitted — the matcher must not see ground truth.
		})
	}
	return people, labeled
}

// MemberID is one identity of a canonical the resolver already merged.
type MemberID struct {
	ID       string // record id, e.g. "whatsapp:15555550101@s.whatsapp.net"
	Platform string
	Name     string
}

// CanonicalMembers is a canonical person + the member identities it already
// unified (via strong phone/email signal).
type CanonicalMembers struct {
	CanonicalID string
	Members     []MemberID
}

// MembershipPositives derives MANY more ground-truth POSITIVES from canonicals the
// resolver already merged: every pair of members of one canonical is confirmed the
// same person. Signal-stripped records → tests whether the matcher recovers a
// known-same pair from name+context ALONE (the honest recall measure). Hard cases
// (members with different display names) are KEPT — they are exactly the recall
// ceiling we want to see.
func MembershipPositives(cms []CanonicalMembers, maxPairsPerCanon int) ([]PersonRecord, []LabeledPair) {
	if maxPairsPerCanon <= 0 {
		maxPairsPerCanon = 6
	}
	used := map[string]MemberID{}
	seen := map[string]bool{}
	var labeled []LabeledPair
	for _, cm := range cms {
		// stable order
		ms := append([]MemberID(nil), cm.Members...)
		sort.Slice(ms, func(i, j int) bool { return ms[i].ID < ms[j].ID })
		n := 0
		for i := 0; i < len(ms) && n < maxPairsPerCanon; i++ {
			for j := i + 1; j < len(ms) && n < maxPairsPerCanon; j++ {
				if ms[i].ID == ms[j].ID {
					continue
				}
				k := pairKey(ms[i].ID, ms[j].ID)
				if seen[k] {
					continue
				}
				seen[k] = true
				labeled = append(labeled, LabeledPair{A: ms[i].ID, B: ms[j].ID, SamePerson: true, Note: "merged member of " + cm.CanonicalID})
				used[ms[i].ID] = ms[i]
				used[ms[j].ID] = ms[j]
				n++
			}
		}
	}
	ids := make([]string, 0, len(used))
	for id := range used {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	people := make([]PersonRecord, 0, len(used))
	for _, id := range ids {
		m := used[id]
		people = append(people, PersonRecord{
			CanonicalID: m.ID, PrimaryName: m.Name, Names: []string{m.Name}, Platforms: []string{m.Platform},
		})
	}
	return people, labeled
}

// MergeLabeled combines two (people, labeled) sets, deduping records by id and
// pairs by key (a pair already present keeps its first label).
func MergeLabeled(p1 []PersonRecord, l1 []LabeledPair, p2 []PersonRecord, l2 []LabeledPair) ([]PersonRecord, []LabeledPair) {
	recs := map[string]PersonRecord{}
	for _, p := range append(append([]PersonRecord(nil), p1...), p2...) {
		if _, ok := recs[p.CanonicalID]; !ok {
			recs[p.CanonicalID] = p
		}
	}
	seen := map[string]bool{}
	var labeled []LabeledPair
	for _, lp := range append(append([]LabeledPair(nil), l1...), l2...) {
		k := pairKey(lp.A, lp.B)
		if seen[k] {
			continue
		}
		seen[k] = true
		labeled = append(labeled, lp)
	}
	ids := make([]string, 0, len(recs))
	for id := range recs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	people := make([]PersonRecord, 0, len(recs))
	for _, id := range ids {
		people = append(people, recs[id])
	}
	return people, labeled
}

// conflictingSignals reports whether two identities have a CONFLICTING confirmed
// signal: each has a phone (or email) and they differ, with none shared.
func conflictingSignals(a, b LabelIdentity) bool {
	sharePhone := a.Phone != "" && a.Phone == b.Phone
	shareEmail := a.Email != "" && a.Email == b.Email
	if sharePhone || shareEmail {
		return false // a shared signal makes them SAME, not a negative
	}
	phoneConflict := a.Phone != "" && b.Phone != "" && a.Phone != b.Phone
	emailConflict := a.Email != "" && b.Email != "" && a.Email != b.Email
	return phoneConflict || emailConflict
}

func conflictLabel(a, b LabelIdentity) string {
	if a.Phone != "" && b.Phone != "" && a.Phone != b.Phone {
		return "phone"
	}
	return "email"
}

func sigLabel(sig string) string {
	if strings.Contains(sig, "@") {
		return "email"
	}
	return "phone"
}
