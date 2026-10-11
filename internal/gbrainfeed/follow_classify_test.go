package gbrainfeed

import "testing"

// Mutual personal accounts classify as friends; one-way follows, businesses,
// and organization names classify as entities. Fixtures are synthetic.
func TestClassifyFollow(t *testing.T) {
	cases := []struct {
		s        FollowSignals
		wantType string
	}{
		// FRIENDS — mutual, personal-scale, not business
		{FollowSignals{Handle: "taylorexample", Name: "Taylor Example", Mutual: true, Followers: 1055}, "person"},
		{FollowSignals{Handle: "caseyexample", Name: "Casey Example", Mutual: true, Followers: 2216}, "person"},
		{FollowSignals{Handle: "rileyexample", Name: "Riley Example", Mutual: true, Followers: 257}, "person"},
		{FollowSignals{Handle: "jamieexample", Name: "Jamie Example", Mutual: true, Followers: 671}, "person"},
		{FollowSignals{Handle: "averyexample", Name: "Avery Example", Mutual: true, Followers: 1076}, "person"},
		// ENTITIES — one-way follows (mutual=false), regardless of follower scale
		{FollowSignals{Handle: "taylorexample", Name: "Taylor Example", IsBusiness: true, Followers: 1814796}, "entity"},
		{FollowSignals{Handle: "examplenews", Name: "Example News", Followers: 86330}, "entity"},
		{FollowSignals{Handle: "drexample", Name: "Casey Example, Ph.D.", Followers: 119604}, "entity"},
		{FollowSignals{Handle: "rileyexample", Name: "Riley Example", Followers: 4845}, "entity"},
		// ENTITY — business account even though MUTUAL (a company office account)
		{FollowSignals{Handle: "exampleoffice", Name: "Example Company Office", Mutual: true, IsBusiness: true, Followers: 307}, "entity"},
		{FollowSignals{Handle: "example.and.co", Name: "Example & Co. by Jordan", IsBusiness: true, Followers: 42}, "entity"},
	}
	for _, c := range cases {
		got := ClassifyFollow(c.s)
		if got.Type != c.wantType {
			t.Errorf("%s: got type %q, want %q", c.s.Handle, got.Type, c.wantType)
		}
		if got.Type == "person" && got.Category != "friend" {
			t.Errorf("%s: person must default to friend, got %q", c.s.Handle, got.Category)
		}
	}
}

// Category hints: registered company / office / foundation → org; brand-named
// business → brand; business-flagged personal name → creator.
func TestEntityCategoryHint(t *testing.T) {
	if got := ClassifyFollow(FollowSignals{Name: "Example Company Office", IsBusiness: true}); got.Category != "org" {
		t.Errorf("office account → org, got %q", got.Category)
	}
	if got := ClassifyFollow(FollowSignals{Name: "Example & Co. by Jordan", IsBusiness: true}); got.Category != "brand" {
		t.Errorf("brand-named business → brand, got %q", got.Category)
	}
	if got := ClassifyFollow(FollowSignals{Name: "Taylor Example", IsBusiness: true}); got.Category != "creator" {
		t.Errorf("business-flagged personal name → creator, got %q", got.Category)
	}
}
