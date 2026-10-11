package lark

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

func TestProfilesFromUsers(t *testing.T) {
	users := []*larkcontact.User{
		{
			OpenId: ptr("ou_a"), UserId: ptr("u_a"), Name: ptr("Alice"),
			Email: ptr("alice@personal.com"), Mobile: ptr("+6591234567"),
		},
		{
			// no personal email → fall back to enterprise_email
			OpenId: ptr("ou_b"), Name: ptr("Bob"),
			EnterpriseEmail: ptr("bob@corp.com"),
		},
		nil, // nil entries skipped
	}
	got := profilesFromUsers(users)
	if len(got) != 2 {
		t.Fatalf("want 2 profiles, got %d", len(got))
	}
	if got[0] != (Profile{OpenID: "ou_a", UserID: "u_a", DisplayName: "Alice", Email: "alice@personal.com", Phone: "+6591234567"}) {
		t.Errorf("profile A = %+v", got[0])
	}
	if got[1].Email != "bob@corp.com" {
		t.Errorf("enterprise_email fallback failed: %+v", got[1])
	}
}

func TestChunkStrings(t *testing.T) {
	in := []string{"a", "b", "c", "d", "e"}
	got := chunkStrings(in, 2)
	if len(got) != 3 || len(got[0]) != 2 || len(got[2]) != 1 {
		t.Fatalf("chunking wrong: %v", got)
	}
	if len(chunkStrings(nil, 2)) != 0 {
		t.Errorf("empty input should yield no chunks")
	}
}

func TestEnrichCachesAndDedupes(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	fetch := func(_ context.Context, ids []string) ([]Profile, error) {
		mu.Lock()
		calls = append(calls, ids)
		mu.Unlock()
		out := make([]Profile, len(ids))
		for i, id := range ids {
			out[i] = Profile{OpenID: id, DisplayName: "name-" + id}
		}
		return out, nil
	}
	e := newEnricherWithFetch(fetch)

	// First call: ou_1 twice + blank → one unique fetch of [ou_1].
	got, err := e.Enrich(context.Background(), []string{"ou_1", "ou_1", ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["ou_1"].DisplayName != "name-ou_1" {
		t.Errorf("result = %+v", got)
	}
	if len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != "ou_1" {
		t.Errorf("expected single fetch of [ou_1], got %v", calls)
	}

	// Second call: ou_1 (cached) + ou_2 (new) → only ou_2 fetched.
	got, err = e.Enrich(context.Background(), []string{"ou_1", "ou_2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 results, got %+v", got)
	}
	if len(calls) != 2 || len(calls[1]) != 1 || calls[1][0] != "ou_2" {
		t.Errorf("expected second fetch of only [ou_2], got %v", calls)
	}
}

func TestEnrichSkipsNonUserIDs(t *testing.T) {
	// App/bot sender ids ("cli_…") must never reach the all-or-nothing batch
	// call — one invalid id fails the whole chunk. Only ou_ ids are fetched.
	var calls [][]string
	fetch := func(_ context.Context, ids []string) ([]Profile, error) {
		calls = append(calls, ids)
		out := make([]Profile, len(ids))
		for i, id := range ids {
			out[i] = Profile{OpenID: id, DisplayName: "name-" + id}
		}
		return out, nil
	}
	e := newEnricherWithFetch(fetch)

	got, err := e.Enrich(context.Background(), []string{"ou_real", "cli_a961b7c1a8785eef", "cli_bot", "ou_other"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["ou_real"].DisplayName == "" || got["ou_other"].DisplayName == "" {
		t.Errorf("want only the two ou_ ids resolved, got %+v", got)
	}
	if len(calls) != 1 {
		t.Fatalf("want a single batch call, got %d", len(calls))
	}
	for _, id := range calls[0] {
		if !isUserOpenID(id) {
			t.Errorf("non-user id %q leaked into the batch", id)
		}
	}

	// A bot-only id resolves to absent without any API call at all.
	calls = nil
	p, ok, err := e.EnrichOne(context.Background(), "cli_solo")
	if err != nil || ok || p.DisplayName != "" {
		t.Errorf("cli_ id should be absent/no-error, got p=%+v ok=%v err=%v", p, ok, err)
	}
	if len(calls) != 0 {
		t.Errorf("cli_ id must not trigger a fetch, got calls=%v", calls)
	}
}

func TestEnrichChunksAtBatchMax(t *testing.T) {
	var maxChunk int
	fetch := func(_ context.Context, ids []string) ([]Profile, error) {
		if len(ids) > maxChunk {
			maxChunk = len(ids)
		}
		out := make([]Profile, len(ids))
		for i, id := range ids {
			out[i] = Profile{OpenID: id}
		}
		return out, nil
	}
	e := newEnricherWithFetch(fetch)

	ids := make([]string, 250)
	for i := range ids {
		ids[i] = fmt.Sprintf("ou_%d", i)
	}
	got, err := e.Enrich(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 250 {
		t.Errorf("want 250 results, got %d", len(got))
	}
	if maxChunk > contactBatchMax {
		t.Errorf("chunk exceeded batch max: %d > %d", maxChunk, contactBatchMax)
	}
}

func TestEnrichUnknownIDAbsent(t *testing.T) {
	// Directory returns nothing for the id (external user) → absent, no error.
	fetch := func(_ context.Context, _ []string) ([]Profile, error) { return nil, nil }
	e := newEnricherWithFetch(fetch)
	p, ok, err := e.EnrichOne(context.Background(), "ou_external")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("unknown id should be absent, got %+v", p)
	}
}

func TestEnrichPropagatesFetchError(t *testing.T) {
	fetch := func(_ context.Context, _ []string) ([]Profile, error) {
		return nil, fmt.Errorf("boom")
	}
	e := newEnricherWithFetch(fetch)
	if _, err := e.Enrich(context.Background(), []string{"ou_x"}); err == nil {
		t.Error("expected error to propagate")
	}
}

// TestContactBatchFetchMockedAPI exercises the real SDK contact path against a
// mocked contact/v3 users batch HTTP response.
func TestContactBatchFetchMockedAPI(t *testing.T) {
	const body = `{
	  "code": 0,
	  "msg": "success",
	  "data": {
	    "items": [
	      {"open_id":"ou_a","user_id":"u_a","name":"Alice","email":"alice@personal.com","mobile":"+6591234567"},
	      {"open_id":"ou_b","name":"Bob","enterprise_email":"bob@corp.com"}
	    ]
	  }
	}`
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	client := lark.NewClient("app", "secret", lark.WithOpenBaseUrl(srv.URL))
	tokenOpt := func(_ context.Context) (larkcore.RequestOptionFunc, error) {
		return larkcore.WithUserAccessToken("u-test-token"), nil
	}
	fetch := contactBatchFetch(client.Contact.User, tokenOpt)

	profiles, err := fetch(context.Background(), []string{"ou_a", "ou_b"})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("want 2 profiles, got %d", len(profiles))
	}
	byID := map[string]Profile{}
	for _, p := range profiles {
		byID[p.OpenID] = p
	}
	if byID["ou_a"].Email != "alice@personal.com" || byID["ou_a"].Phone != "+6591234567" {
		t.Errorf("ou_a = %+v", byID["ou_a"])
	}
	if byID["ou_b"].Email != "bob@corp.com" {
		t.Errorf("ou_b enterprise_email fallback = %+v", byID["ou_b"])
	}
	if gotAuth != "Bearer u-test-token" {
		t.Errorf("expected user access token in Authorization, got %q", gotAuth)
	}
}
