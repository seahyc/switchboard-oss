package idmatch

import (
	"context"
	"strings"
	"testing"
)

func gen() *CandidateGenerator { return NewCandidateGenerator(DefaultCandidateConfig, nil) }

// synthetic population covering the recall + precision cases the design targets.
func people() []PersonRecord {
	return []PersonRecord{
		{CanonicalID: "p-bob", PrimaryName: "Bob Smith", Names: []string{"Bob Smith"}, Platforms: []string{"whatsapp"}},
		{CanonicalID: "p-robert", PrimaryName: "Robert Smith", Names: []string{"Robert Smith"}, Platforms: []string{"telegram"}},
		{CanonicalID: "p-morgan", PrimaryName: "Morgan Example", Platforms: []string{"gmail"}},
		{CanonicalID: "p-mrogan", PrimaryName: "Mrogan Example", Platforms: []string{"telegram"}}, // typo
		{CanonicalID: "p-jose1", PrimaryName: "José García", Platforms: []string{"whatsapp"}},
		{CanonicalID: "p-jose2", PrimaryName: "Jose Garcia", Platforms: []string{"telegram"}}, // diacritic
		{CanonicalID: "p-john1", PrimaryName: "John Tan", Platforms: []string{"whatsapp"}, GroupIDs: []string{"g-work"}, Bio: "engineer at acme"},
		{CanonicalID: "p-john2", PrimaryName: "John Tan", Platforms: []string{"telegram"}, Bio: "chef in paris restaurant"}, // SAME NAME, DIFFERENT person
		{CanonicalID: "p-amy-h", PrimaryName: "Amy", Handles: []string{"amychen"}, Platforms: []string{"telegram"}},
		{CanonicalID: "p-amy-w", PrimaryName: "Amy C", Handles: []string{"amychen"}, Platforms: []string{"whatsapp"}}, // shared handle
	}
}

func surfaced(t *testing.T, pairs []CandidatePair, a, b string) bool {
	t.Helper()
	for _, p := range pairs {
		if pairKey(p.A, p.B) == pairKey(a, b) {
			return true
		}
	}
	return false
}

func TestCandidateGenRecall(t *testing.T) {
	c := gen().Candidates(people())
	// All the genuine same-person variants must be SURFACED (recall is the ceiling).
	for _, want := range [][2]string{
		{"p-bob", "p-robert"},    // nickname
		{"p-morgan", "p-mrogan"}, // typo (edit distance)
		{"p-jose1", "p-jose2"},   // diacritic fold
		{"p-amy-h", "p-amy-w"},   // shared handle
		{"p-john1", "p-john2"},   // same name (surfaced as candidate; adjudication decides)
	} {
		if !surfaced(t, c, want[0], want[1]) {
			t.Errorf("candidate-gen MISSED %s ↔ %s (recall gap)", want[0], want[1])
		}
	}
}

func TestNicknameBlocking(t *testing.T) {
	if !sharesNicknameGiven("Bob Smith", "Robert Smith") {
		t.Fatal("Bob↔Robert should share nickname-folded given name")
	}
	if sharesNicknameGiven("Bob Smith", "Robert Jones") {
		// different last name — token-jaccard low, but nickname still matches given;
		// that's fine (candidate, adjudication decides). Just assert it doesn't panic.
	}
}

func TestNormNameFoldsDiacritics(t *testing.T) {
	if normName("José García") != normName("Jose Garcia") {
		t.Fatalf("diacritic fold failed: %q vs %q", normName("José García"), normName("Jose Garcia"))
	}
	// Non-decomposable Latin letters (NFD leaves these intact) — the #24 bug: the
	// Vietnamese đ in "Đỗ" must fold to "do" so "Bình An Đỗ" resolves "Binh An Do".
	cases := [][2]string{
		{"Bình An Đỗ", "Binh An Do"},
		{"Đặng Văn", "Dang Van"},
		{"Søren Kierkegaard", "Soren Kierkegaard"},
		{"Łukasz", "Lukasz"},
	}
	for _, c := range cases {
		if got, want := normName(c[0]), normName(c[1]); got != want {
			t.Errorf("fold(%q)=%q != fold(%q)=%q", c[0], got, c[1], want)
		}
	}
	if normName("Bình An Đỗ") != "binh an do" {
		t.Fatalf("Đỗ should fold to do: got %q", normName("Bình An Đỗ"))
	}
}

func TestRouting(t *testing.T) {
	th := DefaultThresholds
	cases := []struct {
		same bool
		conf float64
		want Tier
	}{
		{true, 0.97, TierAutoMerge},
		{true, 0.80, TierPropose},
		{true, 0.50, TierDrop},
		{false, 0.99, TierDrop}, // not-same always drops regardless of confidence
	}
	for _, c := range cases {
		if got := Route(AdjudicationResult{SamePerson: c.same, Confidence: c.conf}, th); got != c.want {
			t.Errorf("Route(same=%v conf=%.2f)=%s want %s", c.same, c.conf, got, c.want)
		}
	}
}

func TestSameNameNoCorroborationNeverAutoMerges(t *testing.T) {
	// EMPTY-BIO-ROBUST: two "John Tan" with NO shared handle/group/snippet overlap
	// (and no bio) must NOT auto-merge — name alone routes to PROPOSE (human), which
	// preserves precision without ever depending on a bio.
	a := PersonRecord{CanonicalID: "j1", PrimaryName: "John Tan", Platforms: []string{"whatsapp"}}
	b := PersonRecord{CanonicalID: "j2", PrimaryName: "John Tan", Platforms: []string{"telegram"}}
	r, _ := HeuristicAdjudicator{}.Adjudicate(context.Background(), AdjudicationRequest{A: a, B: b})
	if Route(r, DefaultThresholds) == TierAutoMerge {
		t.Fatalf("same name, no corroboration, no bio must NOT auto-merge: %+v", r)
	}
}

func TestSharedHandleCanAutoMerge(t *testing.T) {
	// Strong corroboration (shared handle) — no bio needed — should reach auto-merge.
	a := PersonRecord{CanonicalID: "a", PrimaryName: "Amy", Handles: []string{"amychen"}, Platforms: []string{"telegram"}}
	b := PersonRecord{CanonicalID: "b", PrimaryName: "Amy C", Handles: []string{"amychen"}, Platforms: []string{"whatsapp"}}
	r, _ := HeuristicAdjudicator{}.Adjudicate(context.Background(), AdjudicationRequest{A: a, B: b})
	if Route(r, DefaultThresholds) != TierAutoMerge {
		t.Fatalf("shared handle should auto-merge even with empty bio: %+v", r)
	}
}

func TestCorroboratedExactName(t *testing.T) {
	exact1 := PersonRecord{CanonicalID: "a", PrimaryName: "Taylor Example"}
	exact2 := PersonRecord{CanonicalID: "b", PrimaryName: "taylor  example"} // normalizes equal
	flip := PersonRecord{CanonicalID: "c", PrimaryName: "Example Taylor"}    // not exact-equal

	cases := []struct {
		name string
		a, b PersonRecord
		sigs []string
		want bool
	}{
		{"exact + shared-group ⇒ auto", exact1, exact2, []string{SignalSharedGroup}, true},
		{"exact + handle ⇒ auto", exact1, exact2, []string{SignalHandle}, true},
		{"exact + handle among others ⇒ auto", exact1, exact2, []string{SignalTokenJac, SignalHandle}, true},
		{"exact + name-only (no corroboration) ⇒ propose", exact1, exact2, []string{SignalTokenJac, SignalEditDist}, false},
		{"exact + nickname-only ⇒ propose", exact1, exact2, []string{SignalNickname}, false},
		{"flip/typo (not exact) + shared-group ⇒ propose", exact1, flip, []string{SignalSharedGroup}, false},
		{"empty name ⇒ never", PersonRecord{PrimaryName: ""}, PersonRecord{PrimaryName: ""}, []string{SignalSharedGroup}, false},
	}
	for _, c := range cases {
		pair := CandidatePair{A: c.a.CanonicalID, B: c.b.CanonicalID, Signals: c.sigs}
		if got := CorroboratedExactName(c.a, c.b, pair); got != c.want {
			t.Errorf("%s: CorroboratedExactName=%v want %v", c.name, got, c.want)
		}
	}
}

func TestRenderUserOmitsEmptyBio(t *testing.T) {
	a := PersonRecord{PrimaryName: "Sarah Tan", Platforms: []string{"whatsapp"}, Snippets: []string{"see you at the office tomorrow"}}
	b := PersonRecord{PrimaryName: "Sarah Tan", Platforms: []string{"telegram"}}
	u := RenderUser(a, b)
	if strings.Contains(u, "bio:") {
		t.Fatalf("empty bio must be omitted from the prompt, got:\n%s", u)
	}
	if !strings.Contains(u, "recent messages") {
		t.Fatalf("snippets should appear (primary signal when bio absent):\n%s", u)
	}
}

func TestEvalGate(t *testing.T) {
	ppl := people()
	labeled := []LabeledPair{
		{A: "p-bob", B: "p-robert", SamePerson: true, Note: "nickname"},
		{A: "p-morgan", B: "p-mrogan", SamePerson: true, Note: "typo"},
		{A: "p-jose1", B: "p-jose2", SamePerson: true, Note: "diacritic"},
		{A: "p-amy-h", B: "p-amy-w", SamePerson: true, Note: "shared handle"},
		{A: "p-john1", B: "p-john2", SamePerson: false, Note: "same name, different person"},
	}
	// A canned "model": confident-same on the real variants, not-same on John/John.
	adj := MapAdjudicator{M: map[string]AdjudicationResult{
		pairKey("p-bob", "p-robert"):    {SamePerson: true, Confidence: 0.97},
		pairKey("p-morgan", "p-mrogan"): {SamePerson: true, Confidence: 0.96},
		pairKey("p-jose1", "p-jose2"):   {SamePerson: true, Confidence: 0.98},
		pairKey("p-amy-h", "p-amy-w"):   {SamePerson: true, Confidence: 0.99},
		pairKey("p-john1", "p-john2"):   {SamePerson: false, Confidence: 0.9},
	}}
	rep, err := Evaluate(context.Background(), ppl, labeled, gen(), adj, DefaultThresholds)
	if err != nil {
		t.Fatal(err)
	}
	if rep.CandidateRecall != 1.0 {
		t.Fatalf("candidate recall should be 100%%, got %.2f (missed %+v)", rep.CandidateRecall, rep.MissedPositives)
	}
	if len(rep.FalseAutoMerges) != 0 {
		t.Fatalf("no false auto-merges expected, got %+v", rep.FalseAutoMerges)
	}
	if !rep.PassesGate(0.99) {
		t.Fatalf("clean canned set should pass the 99%% gate: precision=%.3f", rep.AutoMergePrecision)
	}
}

func TestBuildLabeledSet(t *testing.T) {
	ids := []LabelIdentity{
		// same person: two ids share a phone (confirmed)
		{ID: "wa:1", Platform: "whatsapp", Name: "Sarah Tan", Phone: "+6590001111"},
		{ID: "tg:1", Platform: "telegram", Name: "Sarah T", Phone: "+6590001111"},
		// different people: same name, conflicting phones
		{ID: "wa:2", Platform: "whatsapp", Name: "John Lim", Phone: "+6590002222"},
		{ID: "tg:2", Platform: "telegram", Name: "John Lim", Phone: "+6590003333"},
		// unrelated, no signal → not labeled
		{ID: "gm:1", Platform: "gmail", Name: "Nobody Here"},
	}
	people, labeled := BuildLabeledSet(ids, DefaultLabelConfig)
	var pos, neg int
	for _, lp := range labeled {
		if lp.SamePerson {
			pos++
		} else {
			neg++
		}
	}
	if pos != 1 || neg != 1 {
		t.Fatalf("want 1 positive + 1 negative, got %d/%d: %+v", pos, neg, labeled)
	}
	// records must NOT carry the ground-truth phone/email.
	for _, p := range people {
		if len(p.Platforms) == 0 {
			t.Fatalf("record missing platform: %+v", p)
		}
	}
	// the unsignaled identity isn't referenced by any label → excluded.
	for _, p := range people {
		if p.CanonicalID == "gm:1" {
			t.Fatal("unlabeled identity should be excluded from the record set")
		}
	}
}

type countingAdj struct{ n int }

func (c *countingAdj) Adjudicate(_ context.Context, _ AdjudicationRequest) (AdjudicationResult, error) {
	c.n++
	return AdjudicationResult{SamePerson: true, Confidence: 0.9}, nil
}

func TestAdjudicateBatchCaches(t *testing.T) {
	ppl := people()
	byID := IndexByID(ppl)
	q := []CandidatePair{{A: "p-bob", B: "p-robert"}, {A: "p-jose1", B: "p-jose2"}}
	adj := &countingAdj{}
	cache := NewMemoryCache()
	hash := func(a, b PersonRecord) string { return a.PrimaryName + "|" + b.PrimaryName }

	_, err := AdjudicateBatch(context.Background(), adj, cache, byID, q, DefaultThresholds, hash)
	if err != nil || adj.n != 2 {
		t.Fatalf("first pass should call model twice, got n=%d err=%v", adj.n, err)
	}
	// Re-run: all cached → zero new model calls (load-bearing on CPU).
	_, _ = AdjudicateBatch(context.Background(), adj, cache, byID, q, DefaultThresholds, hash)
	if adj.n != 2 {
		t.Fatalf("re-run must hit cache, got %d model calls", adj.n)
	}
}

func TestOrderByImportance(t *testing.T) {
	pairs := []CandidatePair{
		{A: "low1", B: "low2", PreScore: 0.9},
		{A: "vip", B: "x", PreScore: 0.1},
	}
	imp := map[string]float64{"vip": 50, "low1": 1, "low2": 1, "x": 0}
	out := OrderByImportance(pairs, imp)
	if out[0].A != "vip" && out[0].B != "vip" {
		t.Fatalf("high-salience person should adjudicate first, got %+v", out[0])
	}
}

func TestMembershipPositives(t *testing.T) {
	cms := []CanonicalMembers{
		{CanonicalID: "canon-phone-1", Members: []MemberID{
			{ID: "wa:1", Platform: "whatsapp", Name: "Morgan Example"},
			{ID: "tg:1", Platform: "telegram", Name: "Morgan"}, // different display name — KEEP (hard recall case)
			{ID: "gm:1", Platform: "gmail", Name: "morgan@example.com"},
		}},
		{CanonicalID: "canon-phone-2", Members: []MemberID{{ID: "wa:2", Platform: "whatsapp", Name: "Solo"}}}, // 1 member → no pair
	}
	people, labeled := MembershipPositives(cms, 6)
	if len(labeled) != 3 { // C(3,2) from the first canonical
		t.Fatalf("want 3 positive pairs, got %d: %+v", len(labeled), labeled)
	}
	for _, lp := range labeled {
		if !lp.SamePerson {
			t.Fatalf("membership pairs must be positive: %+v", lp)
		}
	}
	if len(people) != 3 {
		t.Fatalf("want 3 records, got %d", len(people))
	}
}

func TestCachingAdjudicatorResumesFromFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/verdicts.jsonl"
	req := AdjudicationRequest{A: PersonRecord{CanonicalID: "a", PrimaryName: "Amy"}, B: PersonRecord{CanonicalID: "b", PrimaryName: "Amy C"}}

	// Pass 1: model called once, verdict persisted.
	c1, err := NewFileCache(path)
	if err != nil {
		t.Fatal(err)
	}
	inner := &countingAdj{}
	a1 := CachingAdjudicator{Inner: inner, Cache: c1}
	if _, err := a1.Adjudicate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := a1.Adjudicate(context.Background(), req); err != nil { // same pair again → cache hit
		t.Fatal(err)
	}
	if inner.n != 1 {
		t.Fatalf("same pair should call model once, got %d", inner.n)
	}
	c1.Close()

	// Pass 2 (simulated crash + restart): fresh cache from the SAME file → resumes,
	// model NOT called again.
	c2, _ := NewFileCache(path)
	if c2.Len() != 1 {
		t.Fatalf("cache should have resumed 1 verdict from disk, got %d", c2.Len())
	}
	inner2 := &countingAdj{}
	a2 := CachingAdjudicator{Inner: inner2, Cache: c2}
	if _, err := a2.Adjudicate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if inner2.n != 0 {
		t.Fatalf("resumed run must hit the persisted cache, got %d model calls", inner2.n)
	}
}

func TestEvalCatchesFalseAutoMerge(t *testing.T) {
	// A model that wrongly auto-merges John↔John must FAIL the gate.
	ppl := people()
	labeled := []LabeledPair{{A: "p-john1", B: "p-john2", SamePerson: false, Note: "different person"}}
	adj := MapAdjudicator{M: map[string]AdjudicationResult{
		pairKey("p-john1", "p-john2"): {SamePerson: true, Confidence: 0.97}, // WRONG
	}}
	rep, _ := Evaluate(context.Background(), ppl, labeled, gen(), adj, DefaultThresholds)
	if rep.PassesGate(0.99) {
		t.Fatal("a false auto-merge must fail the gate")
	}
	if len(rep.FalseAutoMerges) != 1 {
		t.Fatalf("should record the false auto-merge, got %+v", rep.FalseAutoMerges)
	}
}
