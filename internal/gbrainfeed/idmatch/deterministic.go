package idmatch

import "strings"

// Signal names emitted by candidate generation (CandidatePair.Signals).
const (
	SignalHandle      = "handle"       // shared platform handle/@username
	SignalSharedGroup = "shared-group" // co-members of the same chat/group
	SignalTokenJac    = "token-jaccard"
	SignalEditDist    = "edit-distance"
	SignalNickname    = "nickname"
	SignalSharedPhone = "shared-phone" // exact-equal normalized phone (#33, strong)
	SignalSharedEmail = "shared-email" // exact-equal normalized email (#33, strong)
	SignalCrossIdent  = "cross-ident"  // handle ↔ email-localpart cross-type match (#2 coverage)
)

// emailLocalPart returns the lowercased local-part of an email (before "@"), or "".
func emailLocalPart(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if i := strings.IndexByte(s, '@'); i > 0 {
		return s[:i]
	}
	return ""
}

// SharedStrongIdentifier reports whether a and b share an EXACT, distinctive strong
// identifier — a phone or an email — which is a DETERMINISTIC same-person signal that
// needs NO name match and NO LLM (#33, option A). It returns the kind ("phone" or
// "email") and the shared value so the caller can log the provenance.
//
// This is the recovery net for the cross-platform fragments the live phone/email
// union-find misses: e.g. a WhatsApp @lid identity whose phone is recovered post-hoc
// (from SenderAlt) lands in a different canonical than its phone-twin — both now carry
// the same phone on a member, so this collapses them. Phone-linked positives were the
// bulk of the eval's missed recall.
//
// Precision guards (the merge is ~100%-precision or it doesn't fire):
//   - EXACT normalized equality only — two DIFFERENT phones/emails never match (this
//     alone immunizes against the google-noreply ↔ accounts-google false-merge class,
//     which are DIFFERENT addresses the LLM fused on name similarity);
//   - phones must look like real subscriber numbers (≥7 digits) so short-codes /
//     service numbers don't bridge strangers;
//   - emails must have a DISTINCTIVE local-part (distinctiveIdent) so an exactly-shared
//     role address (noreply@, info@, support@…) — a service, not a person — can't fuse
//     two different humans.
//
// Inputs are assumed projector-normalized (phone digits-only, email lowercased) but the
// guards re-normalize defensively.
func SharedStrongIdentifier(a, b PersonRecord) (kind, value string, ok bool) {
	if v := firstSharedExact(a.Phones, b.Phones, eligiblePhone); v != "" {
		return "phone", v, true
	}
	if v := firstSharedExact(a.Emails, b.Emails, eligibleEmail); v != "" {
		return "email", v, true
	}
	return "", "", false
}

// firstSharedExact returns the first value present in BOTH lists that passes the
// eligibility guard (so a stoplisted/short value can't bridge), or "".
func firstSharedExact(as, bs []string, eligible func(string) bool) string {
	set := map[string]bool{}
	for _, a := range as {
		if n := strings.ToLower(strings.TrimSpace(a)); n != "" {
			set[n] = true
		}
	}
	for _, b := range bs {
		n := strings.ToLower(strings.TrimSpace(b))
		if n != "" && set[n] && eligible(n) {
			return n
		}
	}
	return ""
}

// eligiblePhone accepts only real subscriber-length numbers (≥7 digits) so a shared
// short-code/service number can't bridge two strangers.
func eligiblePhone(s string) bool {
	d := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			d++
		}
	}
	return d >= 7
}

// eligibleEmail accepts only addresses with a distinctive (non-role, non-system)
// local-part, so an exactly-shared noreply@/info@/support@ — a service identity, not a
// person — never fuses two different humans.
func eligibleEmail(s string) bool {
	local := s
	if i := strings.IndexByte(s, '@'); i > 0 {
		local = s[:i]
	}
	return distinctiveIdent(local)
}

// distinctiveIdent gates a shared email local-part to values specific enough that an
// exact coincidence implies the same person — not a generic role/system mailbox.
// (Mirrors the projector's merge-on-read stoplist so both layers reject the same set.)
func distinctiveIdent(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) < 4 {
		return false
	}
	switch s {
	case "info", "admin", "contact", "noreply", "no-reply", "donotreply", "do-not-reply",
		"hello", "team", "support", "sales", "help", "mail", "email", "office",
		"accounts", "account", "billing", "marketing", "notifications", "notification",
		"service", "system", "alert", "alerts", "updates", "news", "newsletter":
		return false
	}
	return true
}

// CorroboratedExactName is the DETERMINISTIC high-precision auto-merge rule (the
// user's #19/#12 policy: "corroborated exact-name → auto-merge; bare same-name →
// human review"). It fires when the two records share an EXACT normalized primary
// name AND at least one INDEPENDENT corroborating signal — a shared handle or a
// shared group. Phone/email already auto-merge upstream and are intentionally not
// considered here.
//
// It does NOT fire on a bare same-name match (the name is the only signal): that is
// the risky common-name collision (two different "John Tan"), which stays for human
// review. It also does NOT fire on cross-name fuzzy pairs (flip/typo/nickname) —
// those aren't exact-equal, so they fall through to the LLM/propose path.
//
// Pure + model-free: runs BEFORE the LLM tier, so a corroborated exact-name pair is
// auto-merged without spending a qwen call. Callers SHOULD additionally gate on a
// distinctive, non-placeholder name (done on the projector side, where the
// placeholder/ distinctiveness policy lives) — this predicate is the necessary
// condition, not the whole gate.
func CorroboratedExactName(a, b PersonRecord, pair CandidatePair) bool {
	na := normName(a.PrimaryName)
	if na == "" || na != normName(b.PrimaryName) {
		return false
	}
	for _, s := range pair.Signals {
		if s == SignalHandle || s == SignalSharedGroup {
			return true
		}
	}
	return false
}
