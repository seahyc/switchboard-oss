package comprehend

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Extractor runs the two-pass commitment funnel over the importance-gated event
// stream: a cheap LOCAL pass (ollama qwen2.5:3b) on the bulk, escalating only the
// uncertain residue to the SMART pass (chatmock). Content-hash cached so a message
// is never re-paid, cursor-driven so it's incremental.
//
// SUGGEST-ONLY: it writes commitment records to comprehend.db and nothing else.
type Extractor struct {
	Reader *Reader
	Store  *Store
	Local  ChatModel // ollama (required)
	Smart  ChatModel // chatmock (optional; nil disables the smart pass)

	LocalModel string // default qwen2.5:3b-instruct (OOM-safe; 7b SIGKILLs under the cap)
	SmartModel string // default gpt-5.4-mini

	// AcceptConfidence is the minimum confidence to persist a commitment (default 0.5).
	AcceptConfidence float64
	// Escalate band: a local commitment whose confidence is in [Lo,Hi) is re-run
	// through the smart pass for a better verdict (defaults 0.35..0.85).
	EscalateLo, EscalateHi float64

	// Scope names the persistent cursor (default "commitments"). A recent-first
	// fast scan (for /goal-1 freshness) uses a separate scope so it never disturbs
	// the backfill watermark; both write the same idempotent commitments table.
	Scope string
	// Floor, if set, raises the effective start watermark to max(storedCursor,
	// Floor) — used by a `--since` recent scan to skip the old backlog.
	Floor time.Time

	Now    func() time.Time
	Logger *slog.Logger
}

// ExtractStats reports a run.
type ExtractStats struct {
	Scanned   int
	CacheHits int
	Escalated int
	Committed int
	Watermark time.Time
}

const extractScope = "commitments"

func (e *Extractor) scope() string {
	if e.Scope != "" {
		return e.Scope
	}
	return extractScope
}

func (e *Extractor) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Extractor) logger() *slog.Logger {
	if e.Logger != nil {
		return e.Logger
	}
	return slog.Default()
}

func (e *Extractor) withDefaults() {
	if e.LocalModel == "" {
		e.LocalModel = "qwen2.5:3b-instruct"
	}
	if e.SmartModel == "" {
		e.SmartModel = "gpt-5.4-mini"
	}
	if e.AcceptConfidence == 0 {
		e.AcceptConfidence = 0.5
	}
	if e.EscalateLo == 0 {
		e.EscalateLo = 0.35
	}
	if e.EscalateHi == 0 {
		e.EscalateHi = 0.85
	}
}

// Run processes up to batchLimit new candidates from the cursor forward. It is
// safe to call repeatedly (cron); each call advances the watermark and skips
// content already processed.
func (e *Extractor) Run(ctx context.Context, batchLimit int) (ExtractStats, error) {
	e.withDefaults()
	var st ExtractStats
	cursor, err := e.Store.Cursor(ctx, e.scope())
	if err != nil {
		return st, err
	}
	if e.Floor.After(cursor) {
		cursor = e.Floor // recent-first fast scan: skip the old backlog
	}
	cands, err := e.Reader.CandidateMessages(ctx, cursor, batchLimit)
	if err != nil {
		return st, err
	}
	st.Watermark = cursor
	for _, cd := range cands {
		if ctx.Err() != nil {
			return st, ctx.Err()
		}
		st.Scanned++
		if cd.TS.After(st.Watermark) {
			st.Watermark = cd.TS
		}

		seen, err := e.Store.Seen(ctx, ExtractorLocal, cd.Text)
		if err != nil {
			return st, err
		}
		if seen {
			st.CacheHits++
			continue
		}

		commits, err := e.extractOne(ctx, e.Local, e.LocalModel, ExtractorLocal, cd)
		usedSmart := false
		if err != nil {
			// The local model is unavailable (e.g. ollama OOM-evicted under the
			// 6.5G cap while the #19 eval runs). Rather than stall extraction, fall
			// back to the chatmock smart pass as the PRIMARY extractor — recent-
			// window volume is small, so the scarce subscription load is bounded.
			if e.Smart != nil {
				e.logger().Warn("local extract failed; falling back to chatmock", "msg", cd.ID, "err", err)
				commits, err = e.extractOne(ctx, e.Smart, e.SmartModel, ExtractorSmart, cd)
				usedSmart = true
			}
			if err != nil {
				// No fallback or it also failed: don't mark seen, don't skip past it,
				// stop the batch here and resume next run.
				e.logger().Warn("extract failed; pausing batch", "msg", cd.ID, "err", err)
				st.Watermark = cursorBefore(cd.TS)
				break
			}
		}

		var accepted []Commitment
		for _, c := range commits {
			// Escalate the uncertain local residue to the smart pass — unless we
			// already extracted via smart (the fallback path).
			if !usedSmart && e.Smart != nil && c.Confidence >= e.EscalateLo && c.Confidence < e.EscalateHi {
				st.Escalated++
				smart, serr := e.extractOne(ctx, e.Smart, e.SmartModel, ExtractorSmart, cd)
				if serr != nil {
					e.logger().Warn("smart escalation failed; keeping local verdict", "msg", cd.ID, "err", serr)
				} else if best := pickMatching(smart, c); best != nil {
					c = *best
				}
			}
			if c.Confidence >= e.AcceptConfidence {
				accepted = append(accepted, c)
			}
		}

		for _, c := range accepted {
			if err := e.Store.PutCommitment(ctx, c); err != nil {
				return st, err
			}
			st.Committed++
		}
		if err := e.Store.MarkSeen(ctx, ExtractorLocal, cd.Text, cd.ID, len(accepted)); err != nil {
			return st, err
		}
	}

	if !st.Watermark.IsZero() && st.Watermark.After(cursor) {
		if err := e.Store.SetCursor(ctx, e.scope(), st.Watermark); err != nil {
			return st, err
		}
	}
	return st, nil
}

// extractOne runs one extraction call and maps the JSON to provenance-stamped
// Commitments. A parse failure yields zero commitments (never an error) so one bad
// generation never aborts a batch — only a transport error is returned as error.
func (e *Extractor) extractOne(ctx context.Context, model ChatModel, modelName, extractor string, cd Candidate) ([]Commitment, error) {
	user := renderExtractUser(cd, e.now())
	out, err := model.Chat(ctx, modelName, extractSystemPrompt, user, true)
	if err != nil {
		return nil, err
	}
	parsed, ok := parseCommitments(out)
	if !ok {
		e.logger().Debug("unparseable extraction; zero commitments", "msg", cd.ID, "extractor", extractor)
		return nil, nil
	}
	var res []Commitment
	for _, pc := range parsed {
		what := strings.TrimSpace(pc.What)
		if what == "" || isTrivialCommitment(what) {
			// The small local model over-extracts: bare acks/pleasantries ("thanks!",
			// "ok for me", "noted") are not commitments. Drop them deterministically
			// so the brief stays high-signal — a real obligation has substance.
			continue
		}
		who, toWhom, dir := resolveParties(cd, pc)
		due := normalizeDue(pc.Due)
		if due == "" {
			// The small model routinely drops an explicitly-stated date. Recover it
			// deterministically from the source text, anchored to the message time.
			anchor := cd.TS
			if anchor.IsZero() {
				anchor = e.now()
			}
			due = parseDueFromText(cd.Text, anchor, SGT)
		}
		res = append(res, Commitment{
			ID:          CommitmentID(cd.ID, what),
			Who:         who,
			ToWhom:      toWhom,
			What:        what,
			Due:         due,
			Status:      CommitmentOpen,
			SourceMsgID: cd.ID,
			ChatID:      cd.ChatID,
			Platform:    cd.Platform,
			Direction:   dir,
			Confidence:  clamp01(pc.Confidence),
			Extractor:   extractor,
			SourceTS:    cd.TS,
			ExtractedAt: e.now(),
		})
	}
	return res, nil
}

// parsedCommitment is the model's JSON shape.
type parsedCommitment struct {
	Who        string  `json:"who"`
	ToWhom     string  `json:"to_whom"`
	What       string  `json:"what"`
	Due        string  `json:"due"`
	Direction  string  `json:"direction"`
	Confidence float64 `json:"confidence"`
}

type extractEnvelope struct {
	Commitments []parsedCommitment `json:"commitments"`
}

// extractJSONObject returns the first balanced {...} object in s (handling braces
// inside strings), or "" if none — robustness for models that wrap JSON in
// ```json fences or add a stray sentence.
func extractJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	depth, inStr, esc := 0, false, false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case ch == '\\':
				esc = true
			case ch == '"':
				inStr = false
			}
			continue
		}
		switch ch {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}

func parseCommitments(content string) ([]parsedCommitment, bool) {
	j := extractJSONObject(content)
	if j == "" {
		return nil, false
	}
	var env extractEnvelope
	if err := json.Unmarshal([]byte(j), &env); err != nil {
		return nil, false
	}
	return env.Commitments, true
}

// resolveParties fills who/to_whom/direction from the model output, falling back
// to the message's own provenance (sender vs counterpart) when the model is vague
// or uses first-person pronouns.
func resolveParties(cd Candidate, pc parsedCommitment) (who, toWhom, dir string) {
	me := "me"
	other := firstNonEmpty(cd.Counterpart, "them")
	self := cd.Sender
	if cd.IsSelf {
		self = me
	}

	who = strings.TrimSpace(pc.Who)
	toWhom = strings.TrimSpace(pc.ToWhom)
	if who == "" || isFirstPerson(who) {
		if cd.IsSelf {
			who = me
		} else {
			who = self
		}
	}
	if toWhom == "" || isFirstPerson(toWhom) {
		if cd.IsSelf {
			toWhom = other
		} else {
			toWhom = me
		}
	}

	dir = strings.TrimSpace(strings.ToLower(pc.Direction))
	if dir != DirectionMine && dir != DirectionTheirs {
		if who == me || cd.IsSelf {
			dir = DirectionMine
		} else {
			dir = DirectionTheirs
		}
	}
	return who, toWhom, dir
}

// isTrivialCommitment rejects bare acknowledgements/pleasantries the small local
// model mislabels as commitments. A real obligation has a verb + object; a 1–2
// word ack does not. Deterministic, no inference.
func isTrivialCommitment(what string) bool {
	w := strings.ToLower(strings.TrimSpace(what))
	w = strings.TrimRight(w, "!.? ")
	switch w {
	case "thanks", "thank you", "thankyou", "ok", "okay", "ok for me", "noted", "sure",
		"yes", "no", "got it", "great", "cool", "done", "sounds good", "will do", "yup",
		"see you", "see you soon", "bye", "welcome", "no worries", "np", "haha", "lol":
		return true
	}
	// Single-token "commitments" are almost always acks, not obligations.
	if len(strings.Fields(w)) < 2 {
		return true
	}
	return false
}

func isFirstPerson(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "i", "me", "myself", "self":
		return true
	}
	return false
}

// normalizeDue passes through RFC3339 / YYYY-MM-DD as-is; empties anything else.
func normalizeDue(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if _, err := time.Parse(time.RFC3339, s); err == nil {
		return s
	}
	if _, err := time.Parse("2006-01-02", s); err == nil {
		return s
	}
	return "" // unparseable/relative leftover — keep it open rather than guess
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

// pickMatching returns the smart-pass commitment that best corresponds to a local
// one (same source msg; pick the highest-confidence), or nil if the smart pass
// found nothing — in which case the smart pass is effectively vetoing the local
// finding, so the caller keeps the (lower) local verdict but it may fall below the
// accept threshold. We return the smart finding when present.
func pickMatching(smart []Commitment, _ Commitment) *Commitment {
	var best *Commitment
	for i := range smart {
		if best == nil || smart[i].Confidence > best.Confidence {
			best = &smart[i]
		}
	}
	return best
}

func cursorBefore(ts time.Time) time.Time {
	return ts.Add(-time.Nanosecond)
}

const extractSystemPrompt = `You extract COMMITMENTS from a single chat message. A commitment is an explicit promise, obligation, or agreed deliverable with an actor — e.g. "I'll send the deck by Friday", "can you review this by EOD?", "we'll ship Tuesday". Ignore vague intentions, pleasantries, and hypotheticals.

Return ONLY a JSON object: {"commitments":[{"who","to_whom","what","due","direction","confidence"}]}.
- who: the party who owes the action. Use "me" if it is the message author and they are the user; otherwise the person's name.
- to_whom: the beneficiary. Use "me" if it is the user.
- what: the obligation, one short phrase.
- due: an absolute date "YYYY-MM-DD" if one is stated or clearly implied by the given current date; otherwise "".
- direction: "mine" if the user owes it, "theirs" if someone owes the user.
- confidence: 0..1, your certainty this is a real commitment.
If there is no commitment, return {"commitments":[]}. No prose, JSON only.`

func renderExtractUser(cd Candidate, now time.Time) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Current date: %s\n", now.In(SGT).Format("2006-01-02 (Monday)"))
	speaker := cd.Sender
	if cd.IsSelf {
		speaker = "the user (me)"
	}
	fmt.Fprintf(&sb, "Speaker: %s\n", speaker)
	if cd.Counterpart != "" {
		fmt.Fprintf(&sb, "Other party in chat: %s\n", cd.Counterpart)
	}
	fmt.Fprintf(&sb, "Platform: %s\n", cd.Platform)
	fmt.Fprintf(&sb, "Message: %q\n", clip(cd.Text, 600))
	return sb.String()
}
