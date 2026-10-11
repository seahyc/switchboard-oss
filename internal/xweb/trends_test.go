package xweb

import "testing"

func TestParsePostCount(t *testing.T) {
	cases := map[string]int{
		"12.3K posts": 12300,
		"1,234 posts": 1234,
		"2M posts":    2000000,
		"5B posts":    5000000000,
		"847 posts":   847,
		"":            0,
		"Trending":    0,
		"under review": 0,
	}
	for in, want := range cases {
		if got := parsePostCount(in); got != want {
			t.Errorf("parsePostCount(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestParseTrends locks the defensive guide.json parse: extracts name/volume/context,
// dedupes, and tolerates the messy real shape without panicking.
func TestParseTrends(t *testing.T) {
	raw := []byte(`{"timeline":{"instructions":[{"addEntries":{"entries":[
	  {"content":{"timelineModule":{"items":[
	    {"item":{"content":{"trend":{"name":"WWDC","trendMetadata":{"domainContext":"Trending in Technology","metaDescription":"42.1K posts"}}}}},
	    {"item":{"content":{"trend":{"name":"#SomeMeme","trendMetadata":{"domainContext":"Entertainment · Trending"}}}}},
	    {"item":{"content":{"trend":{"name":"WWDC","trendMetadata":{"metaDescription":"42.1K posts"}}}}},
	    {"item":{"content":{"banner":{"unrelated":true}}}}
	  ]}}}
	]}}]}}`)
	got := parseTrends(raw)
	if len(got) != 2 { // WWDC + #SomeMeme, dedup drops the duplicate WWDC; banner skipped
		t.Fatalf("got %d trends, want 2: %+v", len(got), got)
	}
	if got[0].Name != "WWDC" || got[0].PostCount != 42100 || got[0].Context != "Trending in Technology" {
		t.Fatalf("trend[0] wrong: %+v", got[0])
	}
	if got[1].Name != "#SomeMeme" || got[1].PostCount != 0 {
		t.Fatalf("trend[1] (no volume) wrong: %+v", got[1])
	}
	// Garbage / renamed shape → no trends, no panic.
	if n := len(parseTrends([]byte(`{"data":{"unexpected":true}}`))); n != 0 {
		t.Fatalf("unknown shape should yield 0 trends, got %d", n)
	}
	if n := len(parseTrends([]byte(`not json`))); n != 0 {
		t.Fatalf("invalid json should yield 0 trends, got %d", n)
	}
}
