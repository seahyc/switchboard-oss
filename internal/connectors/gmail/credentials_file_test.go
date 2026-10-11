package gmail

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadCredentialsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	data := `[{"email":"person@example.test","refresh_token":"synthetic-refresh","client_id":"synthetic-client","client_secret":"synthetic-secret","scopes":["https://www.googleapis.com/auth/gmail.readonly"]},{"email":"legacy@example.test","refresh_token":"synthetic-refresh-2","client_id":"synthetic-client-2","client_secret":"synthetic-secret-2","token_uri":"https://accounts.google.com/o/oauth2/token","scopes":["https://www.googleapis.com/auth/gmail.readonly"]}]`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadCredentialsFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Email != "person@example.test" || got[0].RefreshToken != "synthetic-refresh" || got[0].TokenURI != defaultGoogleTokenURI || got[1].TokenURI != "https://accounts.google.com/o/oauth2/token" {
		t.Fatalf("loaded credentials = %#v", got)
	}
}

func TestLoadCredentialsFileRejectsInvalidInputs(t *testing.T) {
	cases := map[string]string{
		"malformed":             `[{`,
		"empty":                 `[]`,
		"missing required":      `[{"email":"person@example.test","client_id":"id","client_secret":"secret","scopes":["https://www.googleapis.com/auth/gmail.readonly"]}]`,
		"missing gmail scope":   `[{"email":"person@example.test","refresh_token":"refresh","client_id":"id","client_secret":"secret","scopes":["openid"]}]`,
		"untrusted gmail scope": `[{"email":"person@example.test","refresh_token":"refresh","client_id":"id","client_secret":"secret","scopes":["https://attacker.example/auth/gmail.readonly"]}]`,
		"untrusted token host":  `[{"email":"person@example.test","refresh_token":"refresh","client_id":"id","client_secret":"secret","token_uri":"https://attacker.example/token","scopes":["https://www.googleapis.com/auth/gmail.readonly"]}]`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.json")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadCredentialsFile(path); err == nil {
				t.Fatal("accepted invalid credentials")
			}
		})
	}
}

func TestLoadCredentialsFileRequiresPrivateRegularFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not portable on Windows")
	}
	dir := t.TempDir()
	data := `[{"email":"person@example.test","refresh_token":"synthetic-refresh","client_id":"synthetic-client","client_secret":"synthetic-secret","scopes":["https://www.googleapis.com/auth/gmail.readonly"]}]`
	path := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCredentialsFile(path); err == nil {
		t.Fatal("accepted group/other-readable credentials")
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := loadCredentialsFile(link); err == nil {
		t.Fatal("accepted credentials symlink")
	}
}
