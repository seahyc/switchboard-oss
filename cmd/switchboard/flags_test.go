package main

import (
	"flag"
	"reflect"
	"testing"
)

func TestCommandFlagSetInterspersedFlags(t *testing.T) {
	fs := &commandFlagSet{FlagSet: flag.NewFlagSet("test", flag.ContinueOnError)}
	limit := fs.Int("limit", 10, "")
	offset := fs.Int("offset", 0, "")
	desc := fs.Bool("desc", false, "")
	name := fs.String("name", "", "")
	if err := fs.Parse([]string{"first", "--limit=1", "-desc=false", "second", "--name", "-value", "--offset", "-1"}); err != nil {
		t.Fatal(err)
	}
	if *limit != 1 || *desc || *name != "-value" || *offset != -1 {
		t.Fatalf("parsed flags = limit:%d desc:%t name:%q offset:%d", *limit, *desc, *name, *offset)
	}
	if got, want := fs.Args(), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("positionals = %q, want %q", got, want)
	}
}

func TestCommandFlagSetHonorsEndOfOptions(t *testing.T) {
	fs := &commandFlagSet{FlagSet: flag.NewFlagSet("test", flag.ContinueOnError)}
	limit := fs.Int("limit", 10, "")
	if err := fs.Parse([]string{"first", "--limit", "1", "--", "-query", "second"}); err != nil {
		t.Fatal(err)
	}
	if *limit != 1 {
		t.Fatalf("limit = %d, want 1", *limit)
	}
	if got, want := fs.Args(), []string{"first", "-query", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("positionals = %q, want %q", got, want)
	}
}

func TestCommandFlagSetRejectsMissingValuesAfterPositionals(t *testing.T) {
	for _, tc := range []struct {
		flag   string
		define func(*commandFlagSet)
	}{
		{flag: "--name", define: func(fs *commandFlagSet) { fs.String("name", "", "") }},
		{flag: "--limit", define: func(fs *commandFlagSet) { fs.Int("limit", 10, "") }},
	} {
		fs := &commandFlagSet{FlagSet: flag.NewFlagSet("test", flag.ContinueOnError)}
		tc.define(fs)
		if err := fs.Parse([]string{"query", tc.flag}); err == nil {
			t.Errorf("Parse(query %s) unexpectedly succeeded", tc.flag)
		}
	}
}

func TestCommandFlagSetRejectsUnknownAndMissingFlags(t *testing.T) {
	for _, args := range [][]string{{"query", "--unknown"}, {"query", "--limit"}} {
		fs := &commandFlagSet{FlagSet: flag.NewFlagSet("test", flag.ContinueOnError)}
		fs.Int("limit", 10, "")
		if err := fs.Parse(args); err == nil {
			t.Errorf("Parse(%q) unexpectedly succeeded", args)
		}
	}
}
