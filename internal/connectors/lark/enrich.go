package lark

import (
	"context"
	"fmt"
	"strings"
	"sync"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

// contactBatchMax is the max number of user_ids the contact/v3 users batch
// endpoint accepts per call.
const contactBatchMax = 100

// isUserOpenID reports whether id is a real Lark *user* open_id (prefix "ou_").
// The im stream also surfaces sender ids for apps/bots (prefix "cli_") and other
// non-user principals; those are NOT resolvable via contact/v3 users.batch. The
// batch endpoint is all-or-nothing — a single invalid id (e.g. one "cli_…" bot)
// fails the WHOLE request with code=99992351, silently losing every valid id in
// the same chunk. Filtering to ou_ ids before batching is what keeps one poison
// sender from sinking the entire drain's enrichment.
func isUserOpenID(id string) bool { return strings.HasPrefix(id, "ou_") }

// Profile is the resolved contact identity for a Lark open_id. It supplies the
// alias signals (email / phone / display name) that let gbrain's entity registry
// auto-merge a Lark identity with the same person seen on other channels.
type Profile struct {
	OpenID      string
	UserID      string
	DisplayName string
	Email       string // personal email; falls back to enterprise_email
	Phone       string // mobile (may be empty if the phone scope is not approved)
}

// fetchFunc fetches profiles for a chunk of open_ids (≤ contactBatchMax). It is a
// seam so the cache/batch/dedup logic can be unit-tested without HTTP, while the
// production implementation calls the real contact API.
type fetchFunc func(ctx context.Context, openIDs []string) ([]Profile, error)

// Enricher resolves Lark open_ids to contact Profiles, batching requests and
// caching results so repeated senders are fetched once. Safe for concurrent use.
type Enricher struct {
	fetch fetchFunc

	mu    sync.Mutex
	cache map[string]Profile
}

// NewEnricher builds an Enricher backed by the live Lark contact API, using the
// supplied per-request token option (typically the user access token).
func NewEnricher(client contactBatcher, tokenOpt func(ctx context.Context) (larkcore.RequestOptionFunc, error)) *Enricher {
	return &Enricher{
		fetch: contactBatchFetch(client, tokenOpt),
		cache: map[string]Profile{},
	}
}

// newEnricherWithFetch is the test/seam constructor.
func newEnricherWithFetch(fetch fetchFunc) *Enricher {
	return &Enricher{fetch: fetch, cache: map[string]Profile{}}
}

// Enricher returns a contact Enricher wired to this connector's user access
// token and an in-process cache. The gbrain-feed projector calls this to fill
// alias signals (email/phone/name) that im events omit.
func (c *Connector) Enricher() *Enricher {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.enricher == nil {
		c.enricher = NewEnricher(c.client.Contact.User, c.userOpt)
	}
	return c.enricher
}

// EnrichOne resolves a single open_id. The bool is false (no error) when the id
// is unknown to the directory (e.g. external/anonymous user).
func (e *Enricher) EnrichOne(ctx context.Context, openID string) (Profile, bool, error) {
	out, err := e.Enrich(ctx, []string{openID})
	if err != nil {
		return Profile{}, false, err
	}
	p, ok := out[openID]
	return p, ok, nil
}

// Enrich resolves a set of open_ids to Profiles. Already-cached ids are served
// from cache; the rest are fetched in chunks of ≤100 and cached. Unknown ids are
// simply absent from the returned map (not an error). Order-independent; blanks
// and duplicates in the input are ignored.
func (e *Enricher) Enrich(ctx context.Context, openIDs []string) (map[string]Profile, error) {
	result := make(map[string]Profile, len(openIDs))

	// Partition into cache hits and the unique set of misses.
	var misses []string
	seenMiss := map[string]struct{}{}
	e.mu.Lock()
	for _, id := range openIDs {
		if id == "" {
			continue
		}
		// Skip non-user principals (app/bot "cli_…" ids, etc.). Including them
		// would poison the all-or-nothing users.batch call and lose every valid
		// id in the same chunk.
		if !isUserOpenID(id) {
			continue
		}
		if p, ok := e.cache[id]; ok {
			result[id] = p
			continue
		}
		if _, dup := seenMiss[id]; dup {
			continue
		}
		seenMiss[id] = struct{}{}
		misses = append(misses, id)
	}
	e.mu.Unlock()

	if len(misses) == 0 {
		return result, nil
	}

	for _, chunk := range chunkStrings(misses, contactBatchMax) {
		profiles, err := e.fetch(ctx, chunk)
		if err != nil {
			return result, fmt.Errorf("lark enrich: %w", err)
		}
		e.mu.Lock()
		for _, p := range profiles {
			if p.OpenID == "" {
				continue
			}
			e.cache[p.OpenID] = p
			result[p.OpenID] = p
		}
		e.mu.Unlock()
	}
	return result, nil
}

// contactBatcher is the slice of the SDK contact client the enricher needs,
// narrowed to an interface for testability.
type contactBatcher interface {
	Batch(ctx context.Context, req *larkcontact.BatchUserReq, options ...larkcore.RequestOptionFunc) (*larkcontact.BatchUserResp, error)
}

// contactBatchFetch returns a fetchFunc that calls the real contact/v3 users
// batch endpoint and maps the SDK User to our Profile.
func contactBatchFetch(client contactBatcher, tokenOpt func(ctx context.Context) (larkcore.RequestOptionFunc, error)) fetchFunc {
	return func(ctx context.Context, openIDs []string) ([]Profile, error) {
		opt, err := tokenOpt(ctx)
		if err != nil {
			return nil, err
		}
		req := larkcontact.NewBatchUserReqBuilder().
			UserIds(openIDs).
			UserIdType("open_id").
			Build()
		resp, err := client.Batch(ctx, req, opt)
		if err != nil {
			return nil, err
		}
		if !resp.Success() {
			return nil, fmt.Errorf("contact batch: code=%d msg=%s", resp.Code, resp.Msg)
		}
		return profilesFromUsers(resp.Data.Items), nil
	}
}

// profilesFromUsers maps SDK contact users to Profiles (pure; unit-tested).
func profilesFromUsers(users []*larkcontact.User) []Profile {
	out := make([]Profile, 0, len(users))
	for _, u := range users {
		if u == nil {
			continue
		}
		email := deref(u.Email)
		if email == "" {
			email = deref(u.EnterpriseEmail)
		}
		out = append(out, Profile{
			OpenID:      deref(u.OpenId),
			UserID:      deref(u.UserId),
			DisplayName: deref(u.Name),
			Email:       email,
			Phone:       deref(u.Mobile),
		})
	}
	return out
}

// chunkStrings splits s into consecutive slices of at most size elements.
func chunkStrings(s []string, size int) [][]string {
	if size <= 0 {
		return [][]string{s}
	}
	var out [][]string
	for i := 0; i < len(s); i += size {
		end := i + size
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}
