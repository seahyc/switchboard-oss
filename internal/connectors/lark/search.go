package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// SearchByName resolves a free-text name keyword to Lark org-directory users —
// the #24 phase-2 fallback for resolve_person when the message graph has no hit
// (e.g. a colleague the user has never messaged). It hits the keyword user-search
// endpoint (GET /open-apis/search/v1/user, authorized by contact:user:search,
// user_access_token) — the SDK has no typed binding for it, so this is a raw
// client.Get — then hydrates the returned open_ids to full Profiles (name + email
// + phone) via the existing cached/batched Enricher. Returns nil (no error) when
// nothing matches. limit is clamped to the endpoint's 20–200 page_size range.
func (c *Connector) SearchByName(ctx context.Context, query string, limit int) ([]Profile, error) {
	if query == "" {
		return nil, nil
	}
	switch {
	case limit < 20:
		limit = 20
	case limit > 200:
		limit = 200
	}
	opt, err := c.userOpt(ctx)
	if err != nil {
		return nil, fmt.Errorf("lark search: token: %w", err)
	}
	path := fmt.Sprintf("/open-apis/search/v1/user?query=%s&page_size=%d", url.QueryEscape(query), limit)
	resp, err := c.client.Get(ctx, path, nil, larkcore.AccessTokenTypeUser, opt)
	if err != nil {
		return nil, fmt.Errorf("lark search request: %w", err)
	}
	var out struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Users []struct {
				OpenID string `json:"open_id"`
				UserID string `json:"user_id"`
				Name   string `json:"name"`
			} `json:"users"`
			HasMore bool `json:"has_more"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.RawBody, &out); err != nil {
		return nil, fmt.Errorf("lark search decode (status=%d): %w", resp.StatusCode, err)
	}
	if out.Code != 0 {
		return nil, fmt.Errorf("lark search: code=%d msg=%q", out.Code, out.Msg)
	}
	if len(out.Data.Users) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(out.Data.Users))
	nameByID := make(map[string]string, len(out.Data.Users))
	for _, u := range out.Data.Users {
		if u.OpenID == "" {
			continue
		}
		ids = append(ids, u.OpenID)
		nameByID[u.OpenID] = u.Name
	}

	// Hydrate email/phone/name via the shared Enricher (cached + batched, all-or-
	// nothing-per-chunk). Unknown ids (external/anonymous) simply fall back to the
	// search's name + open_id so the directory card is still useful.
	hydrated, err := c.Enricher().Enrich(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("lark search hydrate: %w", err)
	}
	out2 := make([]Profile, 0, len(ids))
	for _, id := range ids {
		p, ok := hydrated[id]
		if !ok {
			p = Profile{OpenID: id}
		}
		if p.DisplayName == "" {
			p.DisplayName = nameByID[id]
		}
		out2 = append(out2, p)
	}
	return out2, nil
}
