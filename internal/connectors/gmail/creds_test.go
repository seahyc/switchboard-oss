package gmail

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// wrap splits a long base64 string into indented continuation lines, the way
// Python's ConfigParser writes multi-line values to keyring_pass.cfg.
func wrap(b64 string) string {
	const width = 76
	var sb strings.Builder
	for i := 0; i < len(b64); i += width {
		end := i + width
		if end > len(b64) {
			end = len(b64)
		}
		sb.WriteString("\t")
		sb.WriteString(b64[i:end])
		sb.WriteString("\n")
	}
	return sb.String()
}

func TestLoadCredentials(t *testing.T) {
	// Two accounts; one with gmail scope, one without — mirroring the real VM.
	gmailCreds := `{"token":"ya29.x","refresh_token":"1//rt-gmail","token_uri":"https://oauth2.googleapis.com/token","client_id":"cid-gmail","client_secret":"secret-gmail","scopes":["openid","https://www.googleapis.com/auth/gmail.readonly","https://www.googleapis.com/auth/gmail.send"],"expiry":"2026-06-09T01:00:47.986681"}`
	driveCreds := `{"token":"ya29.y","refresh_token":"1//rt-drive","token_uri":"https://oauth2.googleapis.com/token","client_id":"cid-drive","client_secret":"secret-drive","scopes":["openid","https://www.googleapis.com/auth/drive"],"expiry":"2026-02-20T07:29:48.000176"}`

	service := "hardened-google-workspace-mcp"

	var b strings.Builder
	// Section header uses UPPERCASE hex (Python %X), as on the real VM.
	b.WriteString("[hardened_2Dgoogle_2Dworkspace_2Dmcp]\n")
	// Option keys are lowercased by ConfigParser; values are base64, multi-line.
	users := base64.StdEncoding.EncodeToString([]byte(`["owner@example.com", "work@example.com"]`))
	b.WriteString("_5f_5fregistered_5fusers_5f_5f = \n")
	b.WriteString(wrap(users))
	b.WriteString("owner_40example_2ecom = \n")
	b.WriteString(wrap(base64.StdEncoding.EncodeToString([]byte(gmailCreds))))
	b.WriteString("work_40example_2ecom = \n")
	b.WriteString(wrap(base64.StdEncoding.EncodeToString([]byte(driveCreds))))

	dir := t.TempDir()
	path := filepath.Join(dir, "keyring_pass.cfg")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	creds, err := loadCredentials(path, service)
	if err != nil {
		t.Fatalf("loadCredentials: %v", err)
	}
	if len(creds) != 2 {
		t.Fatalf("got %d creds, want 2", len(creds))
	}
	// Registered-users order is preserved.
	if creds[0].Email != "owner@example.com" {
		t.Errorf("creds[0].Email = %q", creds[0].Email)
	}
	if creds[1].Email != "work@example.com" {
		t.Errorf("creds[1].Email = %q", creds[1].Email)
	}

	g := creds[0]
	if g.RefreshToken != "1//rt-gmail" || g.ClientID != "cid-gmail" || g.ClientSecret != "secret-gmail" {
		t.Errorf("gmail cred mismatch: %+v", g)
	}
	if !g.hasGmailScope() {
		t.Error("gmail account should report hasGmailScope=true")
	}
	if creds[1].hasGmailScope() {
		t.Error("drive-only account should report hasGmailScope=false")
	}
}

func TestEscapeRoundTrip(t *testing.T) {
	for _, s := range []string{
		"owner@example.com",
		"work@example.com",
		"hardened-google-workspace-mcp",
		"__registered_users__",
	} {
		esc := escapeName(s)
		got, _ := unescapeName(esc)
		if got != s {
			t.Errorf("roundtrip %q -> %q -> %q", s, esc, got)
		}
	}
	// Match the exact on-disk escaping for the account key (lowercase hex).
	if got := escapeName("owner@example.com"); got != "owner_40example_2ecom" {
		t.Errorf("escapeName = %q, want owner_40example_2ecom", got)
	}
}
