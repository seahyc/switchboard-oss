package gbrainfeed

import "strings"

// NameSet collects every name candidate switchboard can observe for a single
// platform identity, from the most authoritative (a user-assigned address-book
// entry) down to the weakest (a bare @handle). Message-sender display names are
// blank for many senders (e.g. WhatsApp @lid participants), so the address book
// — surfaced by a connector's ContactsProvider — is the primary name source and
// the message display name is only a fallback.
type NameSet struct {
	AddressBook string // user-assigned contact name (whatsmeow FullName / TG saved name)
	First       string // contact first name
	Last        string // contact last name
	Business    string // verified business name (WhatsApp business JIDs)
	Push        string // self-set profile name (whatsmeow PushName)
	Display     string // per-message sender display name
	Username    string // @handle / username
}

// nameSource labels where a resolved name came from, so the canonical page can
// record provenance and the review flow can explain a proposal.
type nameSource string

const (
	srcAddressBook nameSource = "address_book"
	srcContactName nameSource = "contact_name"
	srcBusiness    nameSource = "business"
	srcPush        nameSource = "push"
	srcDisplay     nameSource = "display"
	srcUsername    nameSource = "username"
	srcManual      nameSource = "manual" // human-confirmed via the review flow
	srcNone        nameSource = "none"
)

// placeholderNames are platform-generated LABELS that masquerade as names but are
// not human names — e.g. WhatsApp's "Identified As Spam" tag surfaces in the
// contact store's name fields. They must never identify a person (the page stays
// unidentified) or seed a fuzzy merge (otherwise every spam-flagged number merges
// with every other). Lowercased for comparison.
var placeholderNames = map[string]bool{
	"identified as spam": true,
	"spam":               true,
	"unknown":            true,
	"unknown contact":    true,
	"whatsapp user":      true,
	"no name":            true,
}

// isPlaceholderName reports whether s is a non-name system label.
func isPlaceholderName(s string) bool {
	return placeholderNames[strings.ToLower(strings.TrimSpace(s))]
}

// Resolve picks the best human name for an identity in strict priority order and
// reports its provenance. ok is false when no real name is available at all — the
// caller must then keep the platform ID but flag the page as unidentified rather
// than passing a raw ID off as a person's name. Platform placeholder labels (e.g.
// "Identified As Spam") are skipped as if absent.
func (n NameSet) Resolve() (name string, source nameSource, ok bool) {
	first := strings.TrimSpace(n.First)
	last := strings.TrimSpace(n.Last)
	full := strings.TrimSpace(first + " " + last)

	for _, c := range []struct {
		val string
		src nameSource
	}{
		{strings.TrimSpace(n.AddressBook), srcAddressBook},
		{full, srcContactName},
		{strings.TrimSpace(n.Business), srcBusiness},
		{strings.TrimSpace(n.Push), srcPush},
		{strings.TrimSpace(n.Display), srcDisplay},
		{strings.TrimSpace(n.Username), srcUsername},
	} {
		if c.val != "" && !isPlaceholderName(c.val) {
			return c.val, c.src, true
		}
	}
	return "", srcNone, false
}

// allNames returns every distinct, non-empty name candidate (for the page's
// aliases block), most-authoritative first.
func (n NameSet) allNames() []string {
	first := strings.TrimSpace(n.First)
	last := strings.TrimSpace(n.Last)
	cands := []string{
		strings.TrimSpace(n.AddressBook),
		strings.TrimSpace(first + " " + last),
		strings.TrimSpace(n.Business),
		strings.TrimSpace(n.Push),
		strings.TrimSpace(n.Display),
		strings.TrimSpace(n.Username),
	}
	seen := map[string]bool{}
	var out []string
	for _, c := range cands {
		if c == "" || isPlaceholderName(c) || seen[strings.ToLower(c)] {
			continue
		}
		seen[strings.ToLower(c)] = true
		out = append(out, c)
	}
	return out
}

// normName lowercases + collapses whitespace for fuzzy comparison.
func normName(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(s))), " ")
}
