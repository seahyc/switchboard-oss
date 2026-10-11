// Package connectors defines the interface every platform connector implements.
// Connectors normalize platform events into the FROZEN canonical event.Message
// and emit them on the out channel. Auth is platform-specific (see Login below).
package connectors

import (
	"context"
	"errors"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// ErrNoConnector signals that no connector is registered for a chat's platform —
// i.e. the request named an unknown/unsupported platform or a malformed chat_id.
// It's a CALLER fault (fix the request), so the API layer maps it to 400, distinct
// from a genuine upstream/platform send failure (502). Wrap it with %w.
var ErrNoConnector = errors.New("no connector registered for platform")

// SendRequest is the unified outbound payload. Connectors map the fields they
// support; unsupported rich fields (poll/card/buttons) may be ignored or
// returned as an error per platform.
type SendRequest struct {
	ChatID      string             // canonical chat id, e.g. "whatsapp:<jid>"
	Text        string             // message body
	ReplyTo     string             // canonical event id to reply to (optional)
	Attachments []event.Attachment // media/file refs (optional)
	Poll        any                // platform-native poll spec (optional)
	Card        any                // platform-native rich card (optional)
	Buttons     any                // platform-native inline buttons (optional)
}

// LoginStatus reports a connector's auth state so the daemon can drive login.
type LoginStatus struct {
	Connected bool   // true once authenticated and live
	NeedsAuth bool   // true if interactive auth is required
	QRPNG     []byte // optional QR image (e.g. WhatsApp) to surface to the user
	Detail    string // human-readable status / next step
}

// Connector is implemented by every platform (whatsapp, telegram, lark).
type Connector interface {
	// Name returns the platform identifier, e.g. event.PlatformWhatsApp.
	Name() string

	// Connect streams live events to out until ctx is cancelled.
	Connect(ctx context.Context, out chan<- event.Message) error

	// Backfill drives history-sync for a chat (or all chats when chatID == "")
	// emitting normalized events to out. since bounds how far back to go.
	Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error

	// Send delivers an outbound message and returns the canonical sent id.
	Send(ctx context.Context, req SendRequest) (sentID string, err error)

	// React adds an emoji reaction to a message.
	React(ctx context.Context, chatID, msgID, emoji string) error

	// Login drives platform-specific authentication. The daemon polls/inspects
	// the returned status (QR, code prompt, token refresh) to involve the user.
	Login(ctx context.Context) (LoginStatus, error)

	// Status reports current auth/connection state without driving login.
	Status(ctx context.Context) (LoginStatus, error)
}
