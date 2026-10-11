package idmatch

import (
	"fmt"
	"strings"
)

// SystemPrompt is the conservative, EMPTY-BIO-ROBUST instruction for the local LLM
// adjudicator (P2). Precision is existential: a wrong auto-merge corrupts the graph,
// so the model must default to NOT same when corroboration is weak. Bio is usually
// ABSENT across platforms — the prompt explicitly tells the model not to require it
// and to lean on the signals that ARE present (message snippets, shared groups,
// name variants/handles, platform mix).
const SystemPrompt = `You decide whether two contact records describe the SAME real person.

This is a high-stakes de-duplication: merging two DIFFERENT people is a serious error, so be CONSERVATIVE. When the evidence is weak or ambiguous, answer same_person=false.

Rules:
- A shared or similar NAME is NOT proof. Many different people share a name (common names, transliterations). Name similarity alone is weak evidence.
- Do NOT require a bio/profile — it is usually missing. Its absence is NOT evidence either way.
- Weigh the signals that ARE present:
  * message snippets — do the two sets of messages sound like the same individual (style, role, topics, relationships, language)? Clearly different roles/topics/people => different.
  * shared group chats — being in the same groups is moderate evidence FOR same.
  * matching @handles/usernames — strong evidence FOR same.
  * name variants — nicknames (Bob/Robert), spelling/diacritic/transliteration variants of the SAME name support same; clearly different names weaken it.
  * platform mix — the same person often appears across WhatsApp/Telegram/email; that's expected, not evidence either way by itself.
- Strong corroboration (matching handle, or shared groups + consistent snippets) => high confidence same.
- Same/similar name with NO corroborating context => low confidence; prefer same_person=false (a human can still review).

Respond ONLY with JSON: {"same_person": bool, "confidence": 0.0-1.0, "reason": "...", "evidence": ["..."]}.
confidence is your certainty in the same_person verdict.`

// RenderUser builds the user message: the two records' evidence, with empty fields
// omitted (so an absent bio simply doesn't appear — the model never sees "Bio: ").
func RenderUser(a, b PersonRecord) string {
	var sb strings.Builder
	sb.WriteString("Record A:\n")
	writeRecord(&sb, a)
	sb.WriteString("\nRecord B:\n")
	writeRecord(&sb, b)
	if shared := sharedGroups(a, b); len(shared) > 0 {
		fmt.Fprintf(&sb, "\nShared group chats: %d\n", len(shared))
	}
	if hs := sharedFold(a.Handles, b.Handles); len(hs) > 0 {
		fmt.Fprintf(&sb, "Shared handles: %s\n", strings.Join(hs, ", "))
	}
	sb.WriteString("\nAre Record A and Record B the same real person?")
	return sb.String()
}

func writeRecord(sb *strings.Builder, p PersonRecord) {
	fmt.Fprintf(sb, "  name: %s\n", p.PrimaryName)
	if alts := otherNames(p); len(alts) > 0 {
		fmt.Fprintf(sb, "  name variants: %s\n", strings.Join(alts, " / "))
	}
	if len(p.Platforms) > 0 {
		fmt.Fprintf(sb, "  platforms: %s\n", strings.Join(p.Platforms, ", "))
	}
	if len(p.Handles) > 0 {
		fmt.Fprintf(sb, "  handles: %s\n", strings.Join(p.Handles, ", "))
	}
	if strings.TrimSpace(p.Bio) != "" { // usually absent — omitted when empty
		fmt.Fprintf(sb, "  bio: %s\n", oneLine(p.Bio))
	}
	if len(p.Snippets) > 0 {
		sb.WriteString("  recent messages:\n")
		for _, s := range p.Snippets {
			if t := oneLine(s); t != "" {
				fmt.Fprintf(sb, "    - %s\n", clip(t, 160))
			}
		}
	}
}

func otherNames(p PersonRecord) []string {
	seen := map[string]bool{normName(p.PrimaryName): true}
	var out []string
	for _, n := range p.Names {
		k := normName(n)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, n)
	}
	return out
}

func sharedGroups(a, b PersonRecord) []string {
	s := map[string]bool{}
	for _, g := range a.GroupIDs {
		s[g] = true
	}
	var out []string
	for _, g := range b.GroupIDs {
		if s[g] {
			out = append(out, g)
		}
	}
	return out
}

func sharedFold(a, b []string) []string {
	s := map[string]bool{}
	for _, x := range a {
		if n := normName(x); n != "" {
			s[n] = true
		}
	}
	var out []string
	for _, x := range b {
		if s[normName(x)] {
			out = append(out, x)
		}
	}
	return out
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
