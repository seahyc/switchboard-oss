package gmail

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DefaultKeyringService is the keyring service name gw-cli / the hardened
// google-workspace MCP store the OAuth credentials under.
const DefaultKeyringService = "hardened-google-workspace-mcp"

// registeredUsersKey is the special keyring entry listing the stored accounts.
const registeredUsersKey = "__registered_users__"

// credential is the per-account OAuth material gw-cli persists (one JSON blob per
// user email). We only need the refresh_token + client id/secret + token_uri to
// mint Gmail access tokens via the oauth2 refresh grant; the rest is ignored.
type credential struct {
	Email        string   `json:"-"`
	Token        string   `json:"token"`
	RefreshToken string   `json:"refresh_token"`
	TokenURI     string   `json:"token_uri"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	Scopes       []string `json:"scopes"`
	Expiry       string   `json:"expiry"`
}

// hasGmailScope reports whether the stored grant includes any gmail.* scope.
// gw-cli's two accounts differ: owner@example.com has full gmail scopes,
// work@example.com (drive/docs/sheets only) does not, so Gmail calls for it
// would 403. The connector skips scopeless accounts rather than crash-loop.
func (c credential) hasGmailScope() bool {
	for _, s := range c.Scopes {
		if strings.Contains(s, "/auth/gmail") {
			return true
		}
	}
	return false
}

// DefaultKeyringPath resolves the keyrings.alt PlaintextKeyring file the gw
// wrapper points at (PYTHON_KEYRING_BACKEND=keyrings.alt.file.PlaintextKeyring).
func DefaultKeyringPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "python_keyring", "keyring_pass.cfg")
}

// loadCredentials reads every account's OAuth credential out of the keyrings.alt
// PlaintextKeyring file at path, under the given service. This re-implements the
// (deterministic, unencrypted) on-disk format so the Go binary is self-contained
// — no Python/keyring dependency at runtime. Returns the credentials in the
// stored __registered_users__ order, then any extras sorted for determinism.
func loadCredentials(path, service string) ([]credential, error) {
	b, err := privatefs.ReadSecret(path)
	if err != nil {
		return nil, fmt.Errorf("gmail: read keyring %s: %w", path, err)
	}
	sections := parseINI(string(b))
	sec, ok := sections[escapeName(service)]
	if !ok {
		// Section names are escaped but NOT lowercased by ConfigParser; option
		// (key) names ARE lowercased. Try a case-insensitive section match.
		for k, v := range sections {
			if strings.EqualFold(k, escapeName(service)) {
				sec, ok = v, true
				break
			}
		}
		if !ok {
			return nil, fmt.Errorf("gmail: keyring has no service %q", service)
		}
	}

	decode := func(escapedKey string) (string, bool) {
		// ConfigParser lowercases option names on write; our escaped keys use
		// lowercase hex already, but match case-insensitively to be safe.
		for k, v := range sec {
			if strings.EqualFold(k, escapedKey) {
				raw, err := base64.StdEncoding.DecodeString(stripWS(v))
				if err != nil {
					return "", false
				}
				return string(raw), true
			}
		}
		return "", false
	}

	var order []string
	if usersJSON, ok := decode(escapeName(registeredUsersKey)); ok {
		_ = json.Unmarshal([]byte(usersJSON), &order)
	}

	seen := map[string]bool{}
	var out []credential
	add := func(email string) {
		if email == "" || seen[email] {
			return
		}
		blob, ok := decode(escapeName(email))
		if !ok {
			return
		}
		var c credential
		if err := json.Unmarshal([]byte(blob), &c); err != nil {
			return
		}
		c.Email = email
		seen[email] = true
		out = append(out, c)
	}
	for _, e := range order {
		add(e)
	}
	// Any accounts present in the section but absent from the registered list.
	var extras []string
	for k := range sec {
		extras = append(extras, k)
	}
	sort.Strings(extras)
	for _, k := range extras {
		if email, ok := unescapeName(k); ok && strings.Contains(email, "@") {
			add(email)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("gmail: no credentials found in keyring service %q", service)
	}
	return out, nil
}

// --- keyrings.alt name escaping + minimal INI parsing ---------------------

// escapeName mirrors keyring.util.escape.escape: ASCII letters/digits pass
// through; every other byte becomes "_" + lowercase 2-hex of the byte. (Python
// uses uppercase for section names but ConfigParser lowercases option keys, so
// we compare case-insensitively elsewhere — we emit lowercase here.)
func escapeName(s string) string {
	var sb strings.Builder
	for _, by := range []byte(s) {
		if (by >= 'a' && by <= 'z') || (by >= 'A' && by <= 'Z') || (by >= '0' && by <= '9') {
			sb.WriteByte(by)
		} else {
			fmt.Fprintf(&sb, "_%02x", by)
		}
	}
	return sb.String()
}

// unescapeName reverses escapeName: "_XX" hex triplets become their byte.
func unescapeName(s string) (string, bool) {
	var sb strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '_' && i+2 < len(s) {
			var by int
			if _, err := fmt.Sscanf(s[i+1:i+3], "%02x", &by); err == nil {
				sb.WriteByte(byte(by))
				i += 3
				continue
			}
		}
		sb.WriteByte(s[i])
		i++
	}
	return sb.String(), true
}

// parseINI parses the subset of ConfigParser format keyrings.alt writes:
// [section] headers and `key = value` options whose (base64) values may be
// continued across indented (leading-whitespace) lines. Returns
// section -> key -> raw concatenated value.
func parseINI(s string) map[string]map[string]string {
	out := map[string]map[string]string{}
	var curSec string
	var curKey string
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		// Continuation line: indented and we're inside a key.
		if (line[0] == ' ' || line[0] == '\t') && curSec != "" && curKey != "" {
			out[curSec][curKey] += trimmed
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			curSec = trimmed[1 : len(trimmed)-1]
			curKey = ""
			if out[curSec] == nil {
				out[curSec] = map[string]string{}
			}
			continue
		}
		if i := strings.IndexByte(trimmed, '='); i >= 0 && curSec != "" {
			curKey = strings.TrimSpace(trimmed[:i])
			out[curSec][curKey] = strings.TrimSpace(trimmed[i+1:])
			continue
		}
	}
	return out
}

// stripWS removes all whitespace from a (possibly multi-line) base64 value.
func stripWS(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return -1
		}
		return r
	}, s)
}
