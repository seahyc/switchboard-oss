package daemon

import (
	"context"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// Search backs GET /search. The person= filter is the #32 fix: message rows carry
// a blank sender_name by design (attribution is by sender_id, resolved via the
// people graph), so the old "sender_name LIKE name" match returned nothing. Now,
// when person is set, we RESOLVE the name to its identity set (sender_ids) — graph
// hits + merge-on-read composites + the Lark org-directory fallback — and filter
// messages by AUTHOR. So "what did Casey say about X" works even when Casey is only in
// the org directory, as long as her messages carry that sender_id.
func (d *Daemon) Search(ctx context.Context, query, platform, person string, since time.Time, limit int) ([]event.Message, error) {
	if person == "" {
		return d.store.Search(ctx, query, platform, "", since, limit)
	}
	cards, err := d.ResolvePerson(ctx, person)
	if err != nil {
		return nil, err
	}
	refs := gbrainfeed.SenderIDsFromCards(cards)
	if len(refs) == 0 {
		return nil, nil // person resolved to nobody → no authored messages
	}
	srefs := make([]store.SenderRef, 0, len(refs))
	for _, r := range refs {
		srefs = append(srefs, store.SenderRef{Platform: r.Platform, SenderID: r.SenderID})
	}
	return d.store.SearchBySender(ctx, query, platform, srefs, since, limit)
}

// Grep backs GET /grep + the grep MCP tool (#16): a regex (RE2) scan over message text,
// the regex counterpart to Search's FTS. Like Search, person= resolves a NAME to its
// identity set (sender_ids) and author-filters; platform/since narrow further. Returns
// the matches plus scan accounting (Truncated → the scan cap was hit before exhausting
// history; Hint tells the agent how to reach older matches).
func (d *Daemon) Grep(ctx context.Context, pattern, platform, person string, since time.Time, limit int) (api.GrepResult, error) {
	var srefs []store.SenderRef
	if person != "" {
		cards, err := d.ResolvePerson(ctx, person)
		if err != nil {
			return api.GrepResult{}, err
		}
		refs := gbrainfeed.SenderIDsFromCards(cards)
		if len(refs) == 0 {
			return api.GrepResult{}, nil // person resolved to nobody → no authored messages
		}
		for _, r := range refs {
			srefs = append(srefs, store.SenderRef{Platform: r.Platform, SenderID: r.SenderID})
		}
	}
	res, err := d.store.Grep(ctx, pattern, platform, srefs, since, limit)
	if err != nil {
		return api.GrepResult{}, err
	}
	out := api.GrepResult{Matches: res.Matches, Scanned: res.Scanned, Truncated: res.Truncated}
	if res.Truncated {
		out.Hint = "scan cap reached before the whole history was searched — narrow with platform/person/since or tighten the pattern to reach older matches"
	}
	return out, nil
}

// querySQLTimeout / querySQLMaxRows bound the #36 read-only SQL passthrough: a
// pathological query is interrupted at the deadline (modernc sqlite honours ctx
// cancellation), and at most maxRows are returned (Truncated flags the cap).
const (
	querySQLTimeout = 5 * time.Second
	querySQLMaxRows = 1000
)

// QuerySQL backs POST /query-sql + the query_sql MCP tool (#36): an arbitrary READ-ONLY
// SELECT over the firehose. The store enforces the parse-guard + the mode=ro connection;
// the daemon adds the statement timeout, the row cap, and an audit log of every query.
func (d *Daemon) QuerySQL(ctx context.Context, query string) (store.QuerySQLResult, error) {
	d.log.Info("query_sql", "query_bytes", len(query))
	cctx, cancel := context.WithTimeout(ctx, querySQLTimeout)
	defer cancel()
	return d.store.QuerySQL(cctx, query, querySQLMaxRows)
}
