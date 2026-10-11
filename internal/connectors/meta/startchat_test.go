package meta

import "testing"

func TestAllDigits(t *testing.T) {
	cases := map[string]bool{
		"100000003":      true,
		"0":              true,
		"exampleprofile": false,
		"123abc":         false,
		"":               false,
		"12 34":          false,
	}
	for in, want := range cases {
		if got := allDigits(in); got != want {
			t.Errorf("allDigits(%q)=%v want %v", in, got, want)
		}
	}
}

func TestParseSearchHits(t *testing.T) {
	// Exact format from the live sb-meta-ig bridge.
	body := "Search results:\n\n* `100000003` / Example Profile"
	hits := parseSearchHits(body)
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1: %+v", len(hits), hits)
	}
	if hits[0].ID != "100000003" || hits[0].Name != "Example Profile" {
		t.Errorf("hit = %+v", hits[0])
	}

	// Multiple results.
	multi := "Search results:\n\n* `111` / Alpha Co\n* `222` / Beta Bridal\n* `333` / Gamma"
	hits = parseSearchHits(multi)
	if len(hits) != 3 || hits[1].ID != "222" || hits[1].Name != "Beta Bridal" {
		t.Errorf("multi hits = %+v", hits)
	}

	// No results / non-result text.
	if h := parseSearchHits("No results found"); len(h) != 0 {
		t.Errorf("expected 0 hits, got %+v", h)
	}
}

func TestParseCreatedRoomID(t *testing.T) {
	body := "Created chat with `100000003` / Example Profile: Example Profile (https://matrix.to/#/!example:example.org)"
	if got := parseCreatedRoomID(body); got != "!example:example.org" {
		t.Errorf("room id = %q", got)
	}
	// room id carrying a :domain suffix.
	body2 := "Created chat …: x (https://matrix.to/#/!abc123:meta.local)"
	if got := parseCreatedRoomID(body2); got != "!abc123:meta.local" {
		t.Errorf("room id w/ domain = %q", got)
	}
	if got := parseCreatedRoomID("Failed to start chat"); got != "" {
		t.Errorf("expected empty for failure, got %q", got)
	}
}

func TestIsBridgeError(t *testing.T) {
	if !isBridgeError("Failed to resolve identifier: failed to parse identifier: strconv.ParseInt: parsing \"exampleprofile\": invalid syntax") {
		t.Error("expected failure reply to be detected")
	}
	if isBridgeError("Search results:\n\n* `1` / x") {
		t.Error("search results misdetected as error")
	}
}
