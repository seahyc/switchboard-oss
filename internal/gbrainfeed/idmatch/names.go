package idmatch

import (
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// NormName is the exported fold (people-resolution + cross-platform matching both
// need the identical normalization). See normName.
func NormName(s string) string { return normName(s) }

// latinFold maps non-decomposable Latin letters that NFD+Mn-strip does NOT handle —
// these are distinct letters, not base+combining-mark, so diacritic stripping leaves
// them intact. Without this, "Bình An Đỗ" folds to "binh an đo" (đ kept) and never
// matches "Binh An Do". Applied after lowercasing.
var latinFold = strings.NewReplacer(
	"đ", "d", "ø", "o", "ł", "l", "ı", "i", "ß", "ss",
	"æ", "ae", "œ", "oe", "ð", "d", "þ", "th", "ħ", "h", "ŋ", "n",
)

// normName lowercases, strips diacritics (transliteration fold: "José"→"jose",
// "Thị"→"thi"), folds non-decomposable Latin letters ("Đỗ"→"do"), and collapses
// whitespace — so spelling/accent variants block (and resolve) together.
func normName(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	folded, _, err := transform.String(t, s)
	if err != nil {
		folded = s
	}
	lowered := latinFold.Replace(strings.ToLower(folded))
	return strings.Join(strings.FieldsFunc(lowered, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}), " ")
}

// tokens returns the normalized name tokens.
func tokens(s string) []string {
	n := normName(s)
	if n == "" {
		return nil
	}
	return strings.Fields(n)
}

// nicknames maps common diminutives to canonical given names (both directions are
// generated at block time). Intentionally small + high-precision; extend over time.
var nicknames = map[string]string{
	"bob": "robert", "rob": "robert", "robbie": "robert",
	"bill": "william", "will": "william", "billy": "william",
	"jim": "james", "jimmy": "james", "jamie": "james",
	"joe": "joseph", "joey": "joseph",
	"mike": "michael", "mick": "michael", "mickey": "michael",
	"dick": "richard", "rick": "richard", "ricky": "richard", "rich": "richard",
	"tom": "thomas", "tommy": "thomas",
	"dave": "david", "davy": "david",
	"steve": "steven", "stevie": "steven",
	"chris": "christopher", "topher": "christopher",
	"matt": "matthew", "tony": "anthony", "ant": "anthony",
	"dan": "daniel", "danny": "daniel",
	"ben": "benjamin", "benny": "benjamin",
	"sam": "samuel", "alex": "alexander", "xander": "alexander",
	"nick": "nicholas", "nico": "nicholas",
	"kate": "katherine", "katie": "katherine", "kathy": "katherine",
	"liz": "elizabeth", "beth": "elizabeth", "betty": "elizabeth", "eliza": "elizabeth",
	"peggy": "margaret", "maggie": "margaret", "meg": "margaret",
	"sue": "susan", "suzy": "susan",
	"jen": "jennifer", "jenny": "jennifer",
	"abby": "abigail", "becky": "rebecca", "cathy": "catherine",
}

// canonicalGiven maps a token through the nickname table to its canonical form (or
// returns it unchanged). Used so "Bob" and "Robert" share a block key.
func canonicalGiven(tok string) string {
	if c, ok := nicknames[tok]; ok {
		return c
	}
	return tok
}

// levenshtein is the edit distance between two strings (bounded use: short names).
func levenshtein(a, b string) int {
	ar, br := []rune(a), []rune(b)
	la, lb := len(ar), len(br)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			cur[j] = min3(cur[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[lb]
}

func min3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

// tokenJaccard is token-set overlap of two normalized names, in [0,1].
func tokenJaccard(a, b string) float64 {
	ta, tb := tokens(a), tokens(b)
	if len(ta) == 0 || len(tb) == 0 {
		return 0
	}
	sa := map[string]bool{}
	for _, t := range ta {
		sa[canonicalGiven(t)] = true
	}
	inter := 0
	sb := map[string]bool{}
	for _, t := range tb {
		ct := canonicalGiven(t)
		if sb[ct] {
			continue
		}
		sb[ct] = true
		if sa[ct] {
			inter++
		}
	}
	union := len(sa) + len(sb) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}
