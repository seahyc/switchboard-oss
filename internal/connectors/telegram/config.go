package telegram

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Env var names. Secrets are NEVER hardcoded or committed — the daemon supplies
// them from a gitignored .env (local) or systemd env (VM). See .env.example.
const (
	EnvAppID    = "TG_APP_ID"                // from https://my.telegram.org (API development tools)
	EnvAppHash  = "TG_APP_HASH"              // from https://my.telegram.org
	EnvPhone    = "TG_PHONE"                 // optional; userbot login phone (E.164)
	EnvSession  = "TG_SESSION_PATH"          // where the MTProto session string is persisted
	EnvBotToken = "SWITCHBOARD_TG_BOT_TOKEN" // @example_bot token for rich inline-keyboard sends
)

// Config holds everything the Telegram connector needs. App credentials and the
// bot token are required only at live-auth/runtime, not to construct/build.
type Config struct {
	AppID    int    // userbot app_id
	AppHash  string // userbot app_hash
	Phone    string // optional login phone; may be supplied interactively instead
	BotToken string // outbound rich-UI bot identity (optional)

	// SessionPath is the file where the portable MTProto session string is
	// stored so the userbot survives restarts without re-auth.
	SessionPath string
}

// ConfigFromEnv loads Config from environment variables. It does not fail when
// app credentials are missing (they arrive at auth time) — only when an AppID is
// present but unparseable. SessionPath defaults to ./telegram.session.
func ConfigFromEnv() (Config, error) {
	c := Config{
		AppHash:     strings.TrimSpace(os.Getenv(EnvAppHash)),
		Phone:       strings.TrimSpace(os.Getenv(EnvPhone)),
		BotToken:    strings.TrimSpace(os.Getenv(EnvBotToken)),
		SessionPath: strings.TrimSpace(os.Getenv(EnvSession)),
	}
	if raw := strings.TrimSpace(os.Getenv(EnvAppID)); raw != "" {
		id, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("telegram: invalid %s=%q: %w", EnvAppID, raw, err)
		}
		c.AppID = id
	}
	if c.SessionPath == "" {
		c.SessionPath = "telegram.session"
	}
	return c, nil
}

// HasCredentials reports whether app_id/app_hash are present — i.e. whether the
// connector can attempt to connect/auth. Without them it can still be built and
// its mapping unit-tested.
func (c Config) HasCredentials() bool {
	return c.AppID != 0 && c.AppHash != ""
}
