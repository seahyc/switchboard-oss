package store

import (
	"path/filepath"
	"testing"
)

func TestSecondStoreCannotOpenLiveDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "switchboard.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err == nil {
		second.Close()
		first.Close()
		t.Fatal("a second writer can open the live database")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("lease was not released: %v", err)
	}
	reopened.Close()
}
