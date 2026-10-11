// Package privatefs provides small helpers for files that contain private state.
package privatefs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const dirMode os.FileMode = 0o700
const fileMode os.FileMode = 0o600

// EnsureDir creates path and ensures its final directory has private permissions.
// Existing parent directories are never chmodded.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, dirMode); err != nil {
		return fmt.Errorf("privatefs: create directory %q: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("privatefs: %q is not a real directory", path)
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	opened, err := d.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) || !opened.IsDir() {
		return fmt.Errorf("privatefs: %q changed while opening", path)
	}
	if err := d.Chmod(dirMode); err != nil {
		return fmt.Errorf("privatefs: secure directory %q: %w", path, err)
	}
	return nil
}

func privateRegular(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("privatefs: %q is not a regular file", path)
	}
	return info, nil
}

// PrepareFile creates an empty file if missing, or secures an existing regular
// file without changing its contents. Symlinks and non-regular files are refused.
func PrepareFile(path string) error {
	info, err := privateRegular(path)
	if errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, fileMode)
		if createErr != nil {
			return fmt.Errorf("privatefs: create file %q: %w", path, createErr)
		}
		if chmodErr := f.Chmod(fileMode); chmodErr != nil {
			f.Close()
			return chmodErr
		}
		return f.Close()
	}
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		return fmt.Errorf("privatefs: %q changed while opening", path)
	}
	return f.Chmod(fileMode)
}

// ReadSecret reads a regular private file. It rejects files accessible by
// group/other rather than silently changing permissions on a read path.
func ReadSecret(path string) ([]byte, error) {
	info, err := privateRegular(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("privatefs: %q permissions %04o are not private", path, info.Mode().Perm())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		return nil, fmt.Errorf("privatefs: %q changed while opening", path)
	}
	if opened.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("privatefs: %q permissions %04o are not private", path, opened.Mode().Perm())
	}
	return io.ReadAll(f)
}

// WriteFile atomically replaces path with private contents. Its parent must
// already exist; it is not chmodded or otherwise changed.
func WriteFile(path string, data []byte) error {
	if _, err := privateRegular(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".privatefs-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(fileMode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if _, err := privateRegular(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}
