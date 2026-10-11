package privatefs_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/seahyc/switchboard-oss/internal/store"
)

func TestSQLiteFilesArePrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not portable on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", filepath.Base(p), err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode/type = %v", filepath.Base(p), info.Mode())
		}
	}
}

func TestSQLiteRejectsDatabaseSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics vary on Windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target.db")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if s, err := store.Open(link); err == nil {
		s.Close()
		t.Fatal("store.Open accepted database symlink")
	}
	b, err := os.ReadFile(target)
	if err != nil || string(b) != "keep" {
		t.Fatalf("symlink target changed: %q, %v", b, err)
	}
	info, err := os.Stat(target)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("target mode changed: %v, %v", info, err)
	}
}
