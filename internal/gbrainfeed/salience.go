package gbrainfeed

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// PersonSignals is the rolled-up raw interaction-signal vector for ONE canonical
// person — the input to the (pure, deterministic, LLM-free) salience scorer. It
// is produced once by rolling PersonStat (per-identity store aggregates) up across
// a canonical person's member identities + the canonical's own metadata (name,
// platforms). RecencyDays is computed at collection time so re-scoring a cached
// vector during calibration is stable (doesn't drift with wall-clock).
type PersonSignals struct {
	CanonicalID string    `json:"canonical_id"`
	Name        string    `json:"name"` // "" if unidentified
	Identified  bool      `json:"identified"`
	Platforms   []string  `json:"platforms"` // distinct platforms (breadth)
	LastSeen    time.Time `json:"last_seen"`
	RecencyDays int       `json:"recency_days"` // days since LastSeen at collection time

	MsgCount         int     `json:"msg_count"`         // their messages (summed)
	ReciprocityRatio float64 `json:"reciprocity_ratio"` // min(you,them)/max in shared DM, 0..1
	Initiated        int     `json:"initiated"`         // conversations you started w/ them
	MentionsExch     int     `json:"mentions_exch"`     // you<->them @mentions

	HasDM            bool `json:"has_dm"`
	AddressedYou     bool `json:"addressed_you"`      // they @mentioned or replied to you
	YouAddressedThem bool `json:"you_addressed_them"` // you sent in a shared DM/thread
	DMPinned         bool `json:"dm_pinned"`
	DMMuted          bool `json:"dm_muted"`
	DMArchived       bool `json:"dm_archived"`

	// Override is the sticky Tier-0 user decision ("inner"/"dormant"/…) — trumps
	// the computed tier, mirroring scoreImportance's importance_override.
	Override string `json:"override,omitempty"`

	// IsSelf marks the account owner's own canonical (any member identity is self).
	// Self is excluded from the core network — "who matters to ME" is never me/the
	// agent's own bot identity (e.g. the Mamba telegram bot ranked into inner).
	IsSelf bool `json:"is_self,omitempty"`
}

// SalienceWeights is the tunable point table — a NAMED config block so calibration
// is a one-line (or JSON-override) change, never inline magic numbers. All fields
// are signed point contributions; bucket boundaries that aren't points live here too.
type SalienceWeights struct {
	VolumeCap int `json:"volume_cap"` // max points volume can contribute

	RecencyVeryRecent int `json:"recency_very_recent"` // < 7d
	Recency30d        int `json:"recency_30d"`         // < 30d
	Recency90d        int `json:"recency_90d"`         // < 90d
	Recency1y         int `json:"recency_1y"`          // < 365d
	RecencyDormant    int `json:"recency_dormant"`     // >= 365d (negative)

	ReciprocityTwoWay int     `json:"reciprocity_two_way"`
	ReciprocityOneWay int     `json:"reciprocity_one_way"`
	TwoWayMinRatio    float64 `json:"two_way_min_ratio"` // ratio >= this → two-way

	Initiated int `json:"initiated"`
	Breadth2  int `json:"breadth_2"`     // 2 platforms
	Breadth3  int `json:"breadth_3plus"` // 3+ platforms
	HasDM     int `json:"has_dm"`
	GroupOnly int `json:"group_only"` // no DM (negative)
	Pinned    int `json:"pinned"`
	Muted     int `json:"muted"`    // negative
	Archived  int `json:"archived"` // negative
	Mentions  int `json:"mentions"`
}

// DefaultSalienceWeights are sensible starting points (tuned empirically with the
// user during the calibration session).
var DefaultSalienceWeights = SalienceWeights{
	VolumeCap:         4,
	RecencyVeryRecent: 3,
	Recency30d:        2,
	Recency90d:        1,
	Recency1y:         0,
	RecencyDormant:    -2,
	ReciprocityTwoWay: 3,
	ReciprocityOneWay: 1,
	TwoWayMinRatio:    0.25,
	Initiated:         1,
	Breadth2:          1,
	Breadth3:          2,
	HasDM:             2,
	GroupOnly:         -1,
	Pinned:            3,
	Muted:             -3,
	Archived:          -2,
	Mentions:          1,
}

// SalienceThresholds are the tier cut-lines — ALSO config, because they are the
// primary thing the calibration session tunes against the user's judgment.
type SalienceThresholds struct {
	Inner       int `json:"inner"`        // salience >= Inner
	Active      int `json:"active"`       // >= Active
	Peripheral  int `json:"peripheral"`   // >= Peripheral; below → dormant
	DormantDays int `json:"dormant_days"` // last seen older than this → dormant
}

// DefaultSalienceThresholds — starting cut-lines; calibrated with the user.
// Scores of 8 or 9 fall in the active tier.
var DefaultSalienceThresholds = SalienceThresholds{
	Inner:       10,
	Active:      4,
	Peripheral:  0,
	DormantDays: 365,
}

// component is one contributing term in the salience sum (kept for the reason
// string + the "top driver" column the calibration export shows).
type component struct {
	Label  string
	Points int
}

// SalienceResult is the scored output for one person.
type SalienceResult struct {
	Salience   int         `json:"salience"`
	Reason     string      `json:"reason"`     // full breakdown ("two-way DM +3, 120 msgs +4, …")
	TopDriver  string      `json:"top_driver"` // largest-magnitude component label
	Components []component `json:"components"`
}

// Score computes salience for a person from its signals + weights. PURE and
// deterministic — the same function powers both the page-writer and the
// calibration harness, so they can never disagree.
func Score(s PersonSignals, w SalienceWeights) SalienceResult {
	var cs []component
	add := func(label string, pts int) {
		if pts != 0 {
			cs = append(cs, component{Label: label, Points: pts})
		}
	}

	// Volume — log-scaled, capped.
	if v := capInt(volumePoints(s.MsgCount), w.VolumeCap); v != 0 {
		add(fmt.Sprintf("%d msgs", s.MsgCount), v)
	}
	// Recency — decay buckets.
	add(recencyLabel(s.RecencyDays), recencyPoints(s.RecencyDays, w))
	// Reciprocity — two-way conversation is the strongest closeness signal.
	switch {
	case s.ReciprocityRatio >= w.TwoWayMinRatio && s.HasDM:
		add("two-way DM", w.ReciprocityTwoWay)
	case s.ReciprocityRatio > 0 || s.MsgCount > 0:
		add("one-way", w.ReciprocityOneWay)
	}
	// Initiation.
	if s.Initiated > 0 {
		add("you initiate", w.Initiated)
	}
	// Breadth — multi-platform = closer (under-counts pre cross-platform merge).
	switch n := len(s.Platforms); {
	case n >= 3:
		add(fmt.Sprintf("%d platforms", n), w.Breadth3)
	case n == 2:
		add("2 platforms", w.Breadth2)
	}
	// DM vs group-only.
	if s.HasDM {
		add("DM", w.HasDM)
	} else {
		add("group-only", w.GroupOnly)
	}
	// Curation (can stack).
	if s.DMPinned {
		add("pinned", w.Pinned)
	}
	if s.DMMuted {
		add("muted", w.Muted)
	}
	if s.DMArchived {
		add("archived", w.Archived)
	}
	// Mentions.
	if s.MentionsExch > 0 {
		add("mentions", w.Mentions)
	}

	total := 0
	for _, c := range cs {
		total += c.Points
	}

	// Top driver = largest-magnitude contributor (positive or negative).
	top := ""
	best := -1
	for _, c := range cs {
		if abs(c.Points) > best {
			best = abs(c.Points)
			top = c.Label
		}
	}

	return SalienceResult{
		Salience:   total,
		Reason:     reasonString(cs),
		TopDriver:  top,
		Components: cs,
	}
}

// Tier maps a salience score to a tier, honoring a sticky override and the
// dormant-by-staleness rule. PURE. (The engine withholds writing the tier until
// thresholds are calibrated; the harness uses this to preview tiers.)
func Tier(salience, recencyDays int, t SalienceThresholds, override string) string {
	if override != "" {
		return override
	}
	if t.DormantDays > 0 && recencyDays > t.DormantDays {
		return "dormant"
	}
	switch {
	case salience >= t.Inner:
		return "inner"
	case salience >= t.Active:
		return "active"
	case salience >= t.Peripheral:
		return "peripheral"
	default:
		return "dormant"
	}
}

// InNetwork is the honest-view predicate: a canonical person counts as "my
// network" iff they're named OR there's any direct interaction (a DM, they
// addressed you, or you addressed them) — AND they're not the account owner.
// Self is never part of "who matters to ME". Nameless group-only bystanders are
// EXCLUDED from the headline count (but never deleted). PURE.
func InNetwork(s PersonSignals) bool {
	if s.IsSelf {
		return false
	}
	return s.Identified || s.HasDM || s.AddressedYou || s.YouAddressedThem
}

// --- pure helpers ---

func volumePoints(n int) int {
	switch {
	case n <= 0:
		return 0
	case n < 10:
		return 1
	case n < 100:
		return 2
	case n < 1000:
		return 3
	default:
		return 4
	}
}

func recencyPoints(days int, w SalienceWeights) int {
	switch {
	case days < 0:
		return 0 // unknown
	case days < 7:
		return w.RecencyVeryRecent
	case days < 30:
		return w.Recency30d
	case days < 90:
		return w.Recency90d
	case days < 365:
		return w.Recency1y
	default:
		return w.RecencyDormant
	}
}

func recencyLabel(days int) string {
	switch {
	case days < 0:
		return "recency unknown"
	case days < 7:
		return "<7d"
	case days < 30:
		return "<30d"
	case days < 90:
		return "<90d"
	case days < 365:
		return "<1y"
	default:
		return "dormant >1y"
	}
}

func reasonString(cs []component) string {
	if len(cs) == 0 {
		return "no signals"
	}
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		parts = append(parts, fmt.Sprintf("%s %+d", c.Label, c.Points))
	}
	return strings.Join(parts, ", ")
}

func capInt(v, cap int) int {
	if cap > 0 && v > cap {
		return cap
	}
	return v
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// ScoredPerson pairs a person's signals with its score — the unit the calibration
// harness ranks + renders.
type ScoredPerson struct {
	Signals PersonSignals
	Result  SalienceResult
}

// ScoreAll scores every in_network person (bystanders get no score) and returns
// them sorted by salience descending (ties broken by name for determinism).
func ScoreAll(people []PersonSignals, w SalienceWeights) []ScoredPerson {
	out := make([]ScoredPerson, 0, len(people))
	for _, p := range people {
		if !InNetwork(p) {
			continue
		}
		out = append(out, ScoredPerson{Signals: p, Result: Score(p, w)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Result.Salience != out[j].Result.Salience {
			return out[i].Result.Salience > out[j].Result.Salience
		}
		return out[i].Signals.Name < out[j].Signals.Name
	})
	return out
}
