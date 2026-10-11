package store

import (
	"fmt"
	"os"
	"syscall"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

func acquireLease(path string) (*os.File, error) {
	lockPath := path + ".lock"
	if err := privatefs.PrepareFile(lockPath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("database is already open by another Switchboard process; stop it before maintenance: %w", err)
	}
	return f, nil
}
