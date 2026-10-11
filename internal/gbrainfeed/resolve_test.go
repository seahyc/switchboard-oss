package gbrainfeed

import "testing"

func TestNameSetResolvePriority(t *testing.T) {
	cases := []struct {
		name    string
		ns      NameSet
		want    string
		wantSrc nameSource
		wantOK  bool
	}{
		{"address book wins", NameSet{AddressBook: "Mom", Push: "Jane", Display: "J", Username: "jane"}, "Mom", srcAddressBook, true},
		{"first+last over push", NameSet{First: "Jane", Last: "Doe", Push: "JD", Display: "x"}, "Jane Doe", srcContactName, true},
		{"business over push", NameSet{Business: "Acme Corp", Push: "acme"}, "Acme Corp", srcBusiness, true},
		{"push over display", NameSet{Push: "Janey", Display: "blank-ish"}, "Janey", srcPush, true},
		{"display over username", NameSet{Display: "Display Name", Username: "handle"}, "Display Name", srcDisplay, true},
		{"username last resort", NameSet{Username: "onlyhandle"}, "onlyhandle", srcUsername, true},
		{"nothing -> unidentified", NameSet{}, "", srcNone, false},
		{"whitespace ignored", NameSet{AddressBook: "   ", Push: "Push"}, "Push", srcPush, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, src, ok := c.ns.Resolve()
			if got != c.want || src != c.wantSrc || ok != c.wantOK {
				t.Fatalf("Resolve()=%q,%q,%v want %q,%q,%v", got, src, ok, c.want, c.wantSrc, c.wantOK)
			}
		})
	}
}

func TestNameSetAllNamesDeduped(t *testing.T) {
	ns := NameSet{AddressBook: "Mom", Display: "mom", Push: "Mum", Username: "mom"}
	got := ns.allNames()
	// "Mom"/"mom" collapse case-insensitively; "Mum" distinct.
	if len(got) != 2 {
		t.Fatalf("want 2 distinct names, got %d: %v", len(got), got)
	}
}

func TestNameSimilarity(t *testing.T) {
	if s := nameSimilarity("Jane Doe", "jane  doe"); s != 1.0 {
		t.Fatalf("identical normalized names should be 1.0, got %v", s)
	}
	if s := nameSimilarity("Jane Doe", "Jane Smith"); s <= 0 || s >= 1 {
		t.Fatalf("partial overlap should be in (0,1), got %v", s)
	}
	if s := nameSimilarity("Alice", "Bob"); s != 0 {
		t.Fatalf("disjoint names should be 0, got %v", s)
	}
}
