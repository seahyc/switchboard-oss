package privatefs_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

func TestEnsureDirAndPrepareFileModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not portable on Windows")
	}
	root := t.TempDir()
	parent := filepath.Join(root, "shared")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(parent, "owned")
	if err := privatefs.EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	if got := mode(t, dir); got != 0o700 {
		t.Fatalf("owned dir mode = %04o", got)
	}
	if got := mode(t, parent); got != 0o755 {
		t.Fatalf("parent mode changed to %04o", got)
	}
	p := filepath.Join(dir, "state")
	if err := os.WriteFile(p, []byte("preserve"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := privatefs.PrepareFile(p); err != nil {
		t.Fatal(err)
	}
	if got := mode(t, p); got != 0o600 {
		t.Fatalf("file mode = %04o", got)
	}
	b, err := os.ReadFile(p)
	if err != nil || string(b) != "preserve" {
		t.Fatalf("contents=%q err=%v", b, err)
	}
}

func TestSecretRejectsAccessibleFileAndSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not portable on Windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := privatefs.ReadSecret(target); err == nil {
		t.Fatal("ReadSecret accepted group/other-readable file")
	}
	if err := os.Symlink(target, filepath.Join(dir, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := privatefs.PrepareFile(filepath.Join(dir, "link")); err == nil {
		t.Fatal("PrepareFile accepted symlink")
	}
	if _, err := privatefs.ReadSecret(filepath.Join(dir, "link")); err == nil {
		t.Fatal("ReadSecret accepted symlink")
	}
	if got := mode(t, target); got != 0o644 {
		t.Fatalf("symlink target mode changed to %04o", got)
	}
}

func TestWriteFileAtomicPrivateReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not portable on Windows")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "secret")
	if err := os.WriteFile(p, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := privatefs.WriteFile(p, []byte("new")); err != nil {
		t.Fatal(err)
	}
	b, err := privatefs.ReadSecret(p)
	if err != nil || string(b) != "new" {
		t.Fatalf("contents=%q err=%v", b, err)
	}
	if got := mode(t, p); got != 0o600 {
		t.Fatalf("file mode = %04o", got)
	}
}

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
