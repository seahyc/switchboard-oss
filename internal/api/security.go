package api

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Policy separates archive readers from administration and outbound actions.
type Policy struct {
	AdminToken    string
	ReadToken     string
	AllowOutbound bool
}

func ValidateListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("API address: %w", err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("API must bind to a loopback IP or localhost")
	}
	return nil
}

func tokenEqual(got, want string) bool {
	return want != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func SecureHandler(next http.Handler, policy Policy) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") != "" {
			http.Error(w, "browser origins are not allowed", http.StatusForbidden)
			return
		}
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.Trim(host, "[]")
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, "invalid API host", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		admin := tokenEqual(credential, policy.AdminToken)
		reader := admin || tokenEqual(credential, policy.ReadToken)
		if !reader || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "API credential required", http.StatusUnauthorized)
			return
		}
		outbound := false
		switch r.URL.Path {
		case "/send", "/send_media", "/react", "/start_chat", "/tap_button", "/vote_poll":
			outbound = true
		case "/group-invite":
			outbound = r.URL.Query().Get("reset") == "true"
		}
		readOnly := r.Method == http.MethodGet && r.URL.Path != "/auth/status"
		if r.Method == http.MethodPost && r.URL.Path == "/query-sql" {
			readOnly = true
		}
		if (!readOnly || outbound) && !admin {
			http.Error(w, "administrative credential required", http.StatusForbidden)
			return
		}
		if outbound && !policy.AllowOutbound {
			http.Error(w, "outbound actions are disabled", http.StatusForbidden)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		next.ServeHTTP(w, r)
	})
}
