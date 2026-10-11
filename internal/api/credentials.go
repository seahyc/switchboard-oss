package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

func DefaultStateDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".switchboard")
}

func LoadToken(path string) (string, error) {
	b, err := privatefs.ReadSecret(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(b))
	if len(token) < 32 {
		return "", fmt.Errorf("API credential must contain at least 32 characters")
	}
	if strings.ContainsAny(token, " \r\n\t") {
		return "", fmt.Errorf("API credential must contain no whitespace")
	}
	return token, nil
}

func LoadOrCreateToken(path string) (string, error) {
	token, err := LoadToken(path)
	if err == nil {
		return token, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return "", err
	}
	token = hex.EncodeToString(bytes)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return LoadToken(path)
	}
	if err != nil {
		return "", err
	}
	_, writeErr := f.WriteString(token + "\n")
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return token, nil
}

func clientCredential() (string, error) {
	if token := strings.TrimSpace(os.Getenv("SWITCHBOARD_TOKEN")); token != "" {
		return token, nil
	}
	path := os.Getenv("SWITCHBOARD_TOKEN_FILE")
	explicit := path != ""
	if !explicit {
		path = filepath.Join(DefaultStateDir(), "api.token")
	}
	token, err := LoadToken(path)
	if !explicit && errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	return token, err
}
