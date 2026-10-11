//go:build darwin || linux

package store

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestOpenPreservesWALSharedMemoryLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	probe := exec.Command(os.Args[0], "-test.run=^TestWALSharedMemoryLockProbe$")
	probe.Env = append(os.Environ(), "SWITCHBOARD_TEST_WAL_LOCK_PATH="+path+"-shm")
	if output, err := probe.CombinedOutput(); err != nil {
		t.Fatalf("active SQLite WAL shared-memory lock was lost: %v\n%s", err, output)
	}
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("SQLite file mode = %o, want 600", info.Mode().Perm())
		}
	}
}

func TestWALSharedMemoryLockProbe(t *testing.T) {
	path := os.Getenv("SWITCHBOARD_TEST_WAL_LOCK_PATH")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// SQLite holds a shared dead-man-switch lock at byte 128 while using WAL.
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 128, Len: 1}
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &lock)
	if err == nil {
		t.Fatal("another process can acquire the WAL dead-man-switch exclusively")
	}
	if !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EACCES) {
		t.Fatalf("unexpected lock error: %v", err)
	}
}
