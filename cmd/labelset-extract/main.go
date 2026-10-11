// Command labelset-extract builds the #19 eval gate's GROUND-TRUTH labeled set
// from the switchboard message store — with NO model and NO writes. Positives are
// identities sharing a confirmed phone/email (same person); negatives are
// same-name identities with conflicting confirmed signals (different people). It
// emits people.json + labeled.json in the cmd/match-eval format, plus a candidate-
// pair count estimate (for the CPU batch-duration estimate).
//
// Usage:
//
//	labelset-extract -db /path/switchboard.db -out-people people.json -out-labeled labeled.json
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

func main() {
	dbPath := flag.String("db", "", "switchboard.db path (required)")
	membersPath := flag.String("members", "", "optional JSON of []idmatch.CanonicalMembers (gbrain canonical-membership positives)")
	outPeople := flag.String("out-people", "people.json", "output people set")
	outLabeled := flag.String("out-labeled", "labeled.json", "output labeled pairs")
	flag.Parse()
	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "error: -db required")
		os.Exit(2)
	}

	db, err := sql.Open("sqlite", *dbPath)
	must(err, "open db")
	defer db.Close()

	// Distinct sender identities with their best name + a confirmed phone/email.
	rows, err := db.Query(`
		SELECT platform, sender_id,
		       MAX(NULLIF(sender_name,'')) AS name,
		       MAX(NULLIF(sender_phone,'')) AS phone,
		       MAX(NULLIF(sender_email,'')) AS email
		FROM messages
		WHERE is_self=0 AND sender_id<>''
		GROUP BY platform, sender_id`)
	must(err, "query identities")
	defer rows.Close()

	var ids []idmatch.LabelIdentity
	for rows.Next() {
		var plat, sid string
		var name, phone, email sql.NullString
		must(rows.Scan(&plat, &sid, &name, &phone, &email), "scan")
		ids = append(ids, idmatch.LabelIdentity{
			ID:       plat + ":" + sid,
			Platform: plat,
			Name:     name.String,
			Phone:    normPhone(phone.String),
			Email:    strings.ToLower(strings.TrimSpace(email.String)),
		})
	}
	must(rows.Err(), "rows")

	people, labeled := idmatch.BuildLabeledSet(ids, idmatch.DefaultLabelConfig)

	// Augment POSITIVES from gbrain canonical-membership (canonicals the resolver
	// already merged from ≥2 identities = confirmed-same pairs → honest recall).
	if *membersPath != "" {
		var cms []idmatch.CanonicalMembers
		must(readJSON(*membersPath, &cms), "read members")
		mp, ml := idmatch.MembershipPositives(cms, 6)
		people, labeled = idmatch.MergeLabeled(people, labeled, mp, ml)
		fmt.Printf("membership positives added: %d records, %d pairs\n", len(mp), len(ml))
	}

	// IDENTITYCONTEXT (#19 eval methodology fix): populate handles + shared-groups +
	// recent snippets per identity from the SAME store data the live IdentityFeeds
	// use — so an eval record == a prod record. Without this the LLM sees only
	// names+platforms and the conservative prompt correctly refuses every name-only
	// pair (the vacuous-pass bug: 0 recall). Keyed by "platform:sender_id" == the
	// record CanonicalID.
	ctxMap, err := loadIdentityContext(db)
	must(err, "load identity-context")
	withCtx := 0
	for i := range people {
		c, ok := ctxMap[people[i].CanonicalID]
		if !ok {
			continue
		}
		people[i].Handles = c.Handles
		people[i].GroupIDs = c.GroupIDs
		people[i].Snippets = c.Snippets
		if len(c.Handles) > 0 || len(c.GroupIDs) > 0 || len(c.Snippets) > 0 {
			withCtx++
		}
	}

	var pos, neg int
	for _, lp := range labeled {
		if lp.SamePerson {
			pos++
		} else {
			neg++
		}
	}
	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)
	_ = context.Background()

	// FULL-GRAPH candidate count = the initial-sweep batch size (every named
	// identity as a record; the real matcher uses canonicals, similar magnitude).
	var allPeople []idmatch.PersonRecord
	for _, x := range ids {
		if strings.TrimSpace(x.Name) == "" {
			continue
		}
		allPeople = append(allPeople, idmatch.PersonRecord{
			CanonicalID: x.ID, PrimaryName: x.Name, Names: []string{x.Name}, Platforms: []string{x.Platform},
		})
	}
	fullCands := gen.Candidates(allPeople)
	labeledCands := gen.Candidates(people)

	must(writeJSON(*outPeople, people), "write people")
	must(writeJSON(*outLabeled, labeled), "write labeled")

	fmt.Printf("identities scanned:      %d\n", len(ids))
	fmt.Printf("named records (graph):   %d\n", len(allPeople))
	fmt.Printf("labeled records:         %d\n", len(people))
	fmt.Printf("  …with IdentityContext: %d  (handles/shared-groups/snippets — MUST be >0 or the eval is vacuous)\n", withCtx)
	fmt.Printf("labeled pairs:           %d  (positives=%d, negatives=%d)\n", len(labeled), pos, neg)
	fmt.Printf("candidates over labeled: %d\n", len(labeledCands))
	fmt.Printf("FULL-GRAPH candidates:   %d  → initial-sweep batch; @ ~3s/pair ≈ %d min (importance-ordered, cached)\n",
		len(fullCands), len(fullCands)*3/60)
	fmt.Fprintf(os.Stderr, "wrote %s + %s\n", *outPeople, *outLabeled)
}

// recCtx is the per-identity context (handles/groups/snippets) for one record.
type recCtx struct {
	Handles  []string
	GroupIDs []string
	Snippets []string
}

// loadIdentityContext replicates internal/daemon IdentityFeeds.IdentityContext EXACTLY
// (same SQL, same snippet cap=8) so the eval record shape == the prod record shape —
// the whole point of the re-eval. Keyed by "platform:sender_id".
func loadIdentityContext(db *sql.DB) (map[string]recCtx, error) {
	acc := map[string]*recCtx{}
	seenHandle := map[string]map[string]bool{}
	get := func(plat, sid string) *recCtx {
		k := plat + ":" + sid
		c := acc[k]
		if c == nil {
			c = &recCtx{}
			acc[k] = c
			seenHandle[k] = map[string]bool{}
		}
		return c
	}

	// Handles.
	rows, err := db.Query(`SELECT DISTINCT platform, sender_id, sender_handle
		FROM messages WHERE is_self=0 AND sender_id<>'' AND sender_handle<>''`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, h string
		if err := rows.Scan(&plat, &sid, &h); err != nil {
			rows.Close()
			return nil, err
		}
		k := plat + ":" + sid
		c := get(plat, sid)
		if !seenHandle[k][h] {
			seenHandle[k][h] = true
			c.Handles = append(c.Handles, h)
		}
	}
	rows.Close()

	// Group/channel chats (shared-group block).
	rows, err = db.Query(`SELECT DISTINCT platform, sender_id, chat_id
		FROM messages WHERE is_self=0 AND sender_id<>'' AND chat_type IN ('group','channel')
		ORDER BY platform, sender_id, chat_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, chatID string
		if err := rows.Scan(&plat, &sid, &chatID); err != nil {
			rows.Close()
			return nil, err
		}
		get(plat, sid).GroupIDs = append(get(plat, sid).GroupIDs, chatID)
	}
	rows.Close()

	// Recent snippets (top-8 by recency — the primary corroborating signal).
	rows, err = db.Query(`SELECT platform, sender_id, text FROM (
			SELECT platform, sender_id, text,
			       ROW_NUMBER() OVER (PARTITION BY platform, sender_id ORDER BY ts DESC, id DESC) rn
			FROM messages WHERE is_self=0 AND sender_id<>'' AND text<>'' AND kind='text'
		) WHERE rn <= 8 ORDER BY platform, sender_id, rn`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, text string
		if err := rows.Scan(&plat, &sid, &text); err != nil {
			rows.Close()
			return nil, err
		}
		get(plat, sid).Snippets = append(get(plat, sid).Snippets, text)
	}
	rows.Close()

	out := make(map[string]recCtx, len(acc))
	for k, v := range acc {
		out[k] = *v
	}
	return out, nil
}

var nonPhone = regexp.MustCompile(`[^0-9+]`)

func normPhone(s string) string { return nonPhone.ReplaceAllString(strings.TrimSpace(s), "") }

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func must(err error, ctx string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s: %v\n", ctx, err)
		os.Exit(1)
	}
}
