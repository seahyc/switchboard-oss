package gmail

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

const defaultGoogleTokenURI = "https://oauth2.googleapis.com/token"

type credentialFileRecord struct {
	Email        string   `json:"email"`
	RefreshToken string   `json:"refresh_token"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	TokenURI     string   `json:"token_uri"`
	Scopes       []string `json:"scopes"`
	Expiry       string   `json:"expiry"`
}

// loadCredentialsFile reads an explicitly supplied OAuth credential export.
// The accepted token endpoints are limited to Google's official OAuth servers.
func loadCredentialsFile(path string) ([]credential, error) {
	b, err := privatefs.ReadSecret(path)
	if err != nil {
		return nil, fmt.Errorf("gmail: read credentials file: %w", err)
	}
	var records []credentialFileRecord
	if err := json.Unmarshal(b, &records); err != nil {
		return nil, fmt.Errorf("gmail: parse credentials file: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("gmail: credentials file has no account records")
	}

	seen := make(map[string]bool, len(records))
	creds := make([]credential, 0, len(records))
	for i, r := range records {
		email := strings.TrimSpace(r.Email)
		if email == "" || strings.TrimSpace(r.RefreshToken) == "" || strings.TrimSpace(r.ClientID) == "" || strings.TrimSpace(r.ClientSecret) == "" {
			return nil, fmt.Errorf("gmail: credentials record %d is missing email, refresh_token, client_id, or client_secret", i)
		}
		key := strings.ToLower(email)
		if seen[key] {
			return nil, fmt.Errorf("gmail: credentials file contains duplicate account %q", email)
		}
		seen[key] = true
		cr := credential{
			Email:        email,
			RefreshToken: r.RefreshToken,
			ClientID:     r.ClientID,
			ClientSecret: r.ClientSecret,
			TokenURI:     strings.TrimSpace(r.TokenURI),
			Scopes:       r.Scopes,
			Expiry:       r.Expiry,
		}
		if cr.TokenURI == "" {
			cr.TokenURI = defaultGoogleTokenURI
		}
		if !isAllowedGoogleTokenURI(cr.TokenURI) {
			return nil, fmt.Errorf("gmail: credentials record %d has an untrusted token_uri", i)
		}
		if !hasGoogleGmailScope(cr.Scopes) {
			return nil, fmt.Errorf("gmail: credentials record %d lacks a Gmail scope", i)
		}
		creds = append(creds, cr)
	}
	return creds, nil
}

func hasGoogleGmailScope(scopes []string) bool {
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "https://mail.google.com/" || strings.HasPrefix(scope, "https://www.googleapis.com/auth/gmail.") {
			return true
		}
	}
	return false
}

func isAllowedGoogleTokenURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" {
		return false
	}
	if strings.EqualFold(u.Host, "oauth2.googleapis.com") && u.Path == "/token" {
		return true
	}
	return strings.EqualFold(u.Host, "accounts.google.com") && u.Path == "/o/oauth2/token"
}
