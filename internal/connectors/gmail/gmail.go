// Package gmail implements the switchboard Connector for Gmail as a 4th channel.
//
// Design (per SPEC + lead):
//   - Auth uses OAuth credentials from the existing keyring or an explicitly
//     supplied private credential file. It mints Gmail access tokens with the
//     OAuth refresh grant and does not add a new OAuth server or login flow.
//   - An email thread is modeled as a chat (chat_type="email"); To/Cc recipients
//     are mentions (the email analogue of @-mentions). See mapping.go.
//   - Connect polls users.history.list since a persisted per-account historyId
//     cursor (Gmail has no free push). Backfill pages users.messages.list bounded
//     by `since`. Send is users.messages.send. React is unsupported.
//   - All capture is pure — no LLM, no pay-per-token API.
package gmail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// pollInterval is how often Connect checks each account for new history.
const pollInterval = 30 * time.Second

// Config configures the Gmail connector. Zero values fall back to the gw-cli
// defaults so the daemon can enable Gmail with a single flag.
type Config struct {
	CredentialsFile string   // optional private JSON credential file; requires Accounts allow-list
	KeyringPath     string   // keyrings.alt PlaintextKeyring file (default: gw-cli location)
	Service         string   // keyring service name (default: hardened-google-workspace-mcp)
	StatePath       string   // historyId cursor file (default: ~/.switchboard/gmail-state.json)
	Accounts        []string // optional allow-list; required with CredentialsFile
	Logger          *slog.Logger
}

// account holds one mailbox's authenticated Gmail service + identity.
type account struct {
	addr string
	svc  *gmailapi.Service
}

// Connector is the Gmail implementation of connectors.Connector. It manages one
// or more mailbox accounts behind the single "gmail" platform.
type Connector struct {
	cfg   Config
	log   *slog.Logger
	state *stateStore
	now   func() time.Time

	mu        sync.Mutex
	accounts  map[string]*account // addr -> account
	order     []string            // stable account order
	connected bool
}

// New builds a Gmail connector from the configured credential source and mints
// a Gmail service for each allowed, Gmail-scoped account.
func New(ctx context.Context, cfg Config) (*Connector, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	var creds []credential
	var err error
	if cfg.CredentialsFile != "" {
		allowed := false
		for _, a := range cfg.Accounts {
			if strings.TrimSpace(a) != "" {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, fmt.Errorf("gmail: CredentialsFile requires an explicit Accounts allow-list")
		}
		creds, err = loadCredentialsFile(cfg.CredentialsFile)
	} else {
		if cfg.KeyringPath == "" {
			cfg.KeyringPath = DefaultKeyringPath()
		}
		if cfg.Service == "" {
			cfg.Service = DefaultKeyringService
		}
		creds, err = loadCredentials(cfg.KeyringPath, cfg.Service)
		// The keyring flow historically always used google.Endpoint, regardless
		// of token_uri in its stored records. Keep that behavior unchanged.
		for i := range creds {
			creds[i].TokenURI = ""
		}
	}
	if err != nil {
		return nil, err
	}
	state, err := newStateStore(cfg.StatePath)
	if err != nil {
		return nil, err
	}

	allow := map[string]bool{}
	for _, a := range cfg.Accounts {
		allow[strings.ToLower(strings.TrimSpace(a))] = true
	}

	c := &Connector{
		cfg:      cfg,
		log:      cfg.Logger,
		state:    state,
		now:      time.Now,
		accounts: map[string]*account{},
	}

	for _, cr := range creds {
		addr := strings.ToLower(cr.Email)
		if len(allow) > 0 && !allow[addr] {
			continue
		}
		if cr.RefreshToken == "" {
			c.log.Warn("gmail: account has no refresh_token — skipping", "account", addr)
			continue
		}
		if !cr.hasGmailScope() {
			c.log.Warn("gmail: account lacks a gmail.* scope — skipping (re-consent needed for Gmail)", "account", addr)
			continue
		}
		svc, err := newService(ctx, cr)
		if err != nil {
			c.log.Warn("gmail: building service failed — skipping account", "account", addr, "err", err)
			continue
		}
		c.accounts[addr] = &account{addr: addr, svc: svc}
		c.order = append(c.order, addr)
	}
	if len(c.accounts) == 0 {
		return nil, fmt.Errorf("gmail: no usable (gmail-scoped) accounts in credential source")
	}
	return c, nil
}

// newService mints an auto-refreshing Gmail API client from a stored credential.
func newService(ctx context.Context, cr credential) (*gmailapi.Service, error) {
	conf := &oauth2.Config{
		ClientID:     cr.ClientID,
		ClientSecret: cr.ClientSecret,
		Endpoint:     google.Endpoint,
	}
	if cr.TokenURI != "" {
		conf.Endpoint.TokenURL = cr.TokenURI
	}
	// A token with only a refresh_token: the oauth2 TokenSource refreshes it on
	// first use and caches/rotates the access token in memory thereafter.
	tok := &oauth2.Token{RefreshToken: cr.RefreshToken}
	ts := conf.TokenSource(ctx, tok)
	return gmailapi.NewService(ctx, option.WithTokenSource(ts))
}

// Name implements connectors.Connector.
func (c *Connector) Name() string { return event.PlatformGmail }

// emit sends a canonical event to out, respecting ctx cancellation.
func (c *Connector) emit(ctx context.Context, out chan<- event.Message, m event.Message) {
	select {
	case out <- m:
	case <-ctx.Done():
	}
}

// Connect streams live events to out until ctx is cancelled. It runs one history
// poll loop per account; a single account's persistent failure (e.g. revoked
// scope) stops only that loop, not the whole connector.
func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	c.mu.Lock()
	c.connected = true
	order := append([]string(nil), c.order...)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
	}()

	var wg sync.WaitGroup
	for _, addr := range order {
		acc := c.accounts[addr]
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.pollAccount(ctx, acc, out)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// pollAccount seeds the history cursor (if unset) then polls users.history.list
// for new messages on an interval until ctx is done.
func (c *Connector) pollAccount(ctx context.Context, acc *account, out chan<- event.Message) {
	// Seed: on first run we have no cursor. Start from the mailbox's current
	// historyId so we only capture messages from now on (Backfill is the explicit
	// history path), mirroring the Lark poll-loop's "start from now" behavior.
	if c.state.historyID(acc.addr) == 0 {
		if prof, err := acc.svc.Users.GetProfile("me").Context(ctx).Do(); err == nil {
			_ = c.state.setHistoryID(acc.addr, prof.HistoryId)
		} else {
			c.log.Warn("gmail: get profile failed; will retry", "account", acc.addr, "err", err)
		}
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.historyOnce(ctx, acc, out); err != nil {
				if isInsufficientScope(err) {
					c.log.Error("gmail: insufficient scope; stopping account poll (re-consent needed)", "account", acc.addr)
					return
				}
				c.log.Warn("gmail: history poll failed; will retry", "account", acc.addr, "err", err)
			}
		}
	}
}

// historyOnce pulls users.history.list since the stored cursor, emits each newly
// added message, and advances the cursor. A 404 (cursor too old) reseeds from the
// current profile historyId.
func (c *Connector) historyOnce(ctx context.Context, acc *account, out chan<- event.Message) error {
	start := c.state.historyID(acc.addr)
	if start == 0 {
		prof, err := acc.svc.Users.GetProfile("me").Context(ctx).Do()
		if err != nil {
			return err
		}
		return c.state.setHistoryID(acc.addr, prof.HistoryId)
	}

	seen := map[string]bool{}
	latest := start
	pageToken := ""
	for {
		call := acc.svc.Users.History.List("me").
			StartHistoryId(start).
			HistoryTypes("messageAdded").
			MaxResults(500).
			Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		if err != nil {
			if isNotFound(err) {
				// Cursor expired: reseed forward so we don't loop on a dead id.
				if prof, perr := acc.svc.Users.GetProfile("me").Context(ctx).Do(); perr == nil {
					return c.state.setHistoryID(acc.addr, prof.HistoryId)
				}
			}
			return err
		}
		if resp.HistoryId > latest {
			latest = resp.HistoryId
		}
		for _, h := range resp.History {
			for _, ma := range h.MessagesAdded {
				if ma.Message == nil || seen[ma.Message.Id] {
					continue
				}
				seen[ma.Message.Id] = true
				m, ok := c.fetchAndMap(ctx, acc, ma.Message.Id, event.SourceLive)
				if ok {
					c.emit(ctx, out, m)
				}
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return c.state.setHistoryID(acc.addr, latest)
}

// fetchAndMap retrieves a full message and maps it to a canonical event.
func (c *Connector) fetchAndMap(ctx context.Context, acc *account, gmailID, src string) (event.Message, bool) {
	full, err := acc.svc.Users.Messages.Get("me", gmailID).Format("full").Context(ctx).Do()
	if err != nil {
		c.log.Warn("gmail: fetch message failed", "account", acc.addr, "id", gmailID, "err", err)
		return event.Message{}, false
	}
	return mapMessage(full, acc.addr, src, c.now().UTC())
}

// Backfill pages users.messages.list (bounded by since) for the account(s) and
// emits each message. chatID "" backfills every account; a "gmail:<account>:..."
// chat id backfills just that account.
func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	var targets []*account
	if chatID == "" || chatID == "all" {
		c.mu.Lock()
		for _, a := range c.order {
			targets = append(targets, c.accounts[a])
		}
		c.mu.Unlock()
	} else {
		acc, _ := parseChatID(chatID)
		a := c.accountFor(acc)
		if a == nil {
			return fmt.Errorf("gmail backfill: unknown account in chat %q", chatID)
		}
		targets = []*account{a}
	}

	for _, acc := range targets {
		if err := c.backfillAccount(ctx, acc, since, out); err != nil {
			if isInsufficientScope(err) {
				c.log.Error("gmail backfill: insufficient scope; skipping account", "account", acc.addr)
				continue
			}
			return err
		}
	}
	return nil
}

func (c *Connector) backfillAccount(ctx context.Context, acc *account, since time.Time, out chan<- event.Message) error {
	q := ""
	if !since.IsZero() {
		q = "after:" + strconv.FormatInt(since.Unix(), 10)
	}
	pageToken := ""
	for {
		call := acc.svc.Users.Messages.List("me").MaxResults(200).Context(ctx)
		if q != "" {
			call = call.Q(q)
		}
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		if err != nil {
			return err
		}
		for _, ref := range resp.Messages {
			m, ok := c.fetchAndMap(ctx, acc, ref.Id, event.SourceBackfill)
			if ok {
				c.emit(ctx, out, m)
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return nil
}

// Send delivers an outbound email via users.messages.send and returns the
// canonical sent id. The account is taken from the chat id ("gmail:<account>:..")
// or defaults to the first account. ReplyTo threads the message via In-Reply-To.
func (c *Connector) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	accAddr, threadID := parseChatID(req.ChatID)
	acc := c.accountFor(accAddr)
	if acc == nil {
		acc = c.firstAccount()
	}
	if acc == nil {
		return "", fmt.Errorf("gmail send: no account available")
	}

	// Resolve the recipient + threading from the message being replied to (if any).
	var to, subject, inReplyTo, references string
	if req.ReplyTo != "" {
		if parent, err := c.lookupByCanonical(ctx, acc, req.ReplyTo); err == nil && parent != nil {
			subject = ensureReSubject(header(parent.Payload, "Subject"))
			inReplyTo = header(parent.Payload, "Message-ID")
			references = strings.TrimSpace(header(parent.Payload, "References") + " " + inReplyTo)
			// Reply goes back to the original sender.
			if from := header(parent.Payload, "From"); from != "" {
				to = from
			}
			if threadID == "" {
				threadID = parent.ThreadId
			}
		}
	}

	raw := buildRFC822(rfc822{
		From:       acc.addr,
		To:         to,
		Subject:    subject,
		Body:       req.Text,
		InReplyTo:  inReplyTo,
		References: references,
	})
	msg := &gmailapi.Message{Raw: raw}
	if threadID != "" {
		msg.ThreadId = threadID
	}
	sent, err := acc.svc.Users.Messages.Send("me", msg).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("gmail send: %w", err)
	}
	// Best-effort: resolve the RFC822 Message-ID of the sent message for a stable
	// canonical id; fall back to the Gmail message id.
	if full, err := acc.svc.Users.Messages.Get("me", sent.Id).Format("metadata").
		MetadataHeaders("Message-ID").Context(ctx).Do(); err == nil {
		return canonMsgID(header(full.Payload, "Message-ID"), sent.Id), nil
	}
	return event.MakeID(event.PlatformGmail, sent.Id), nil
}

// React is unsupported on Gmail.
func (c *Connector) React(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("react unsupported on gmail")
}

// Login mints/validates tokens for every account (forces a refresh by issuing a
// cheap profile call). Reports Connected when at least one account works.
func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) {
	ok := 0
	c.mu.Lock()
	order := append([]string(nil), c.order...)
	c.mu.Unlock()
	for _, addr := range order {
		if _, err := c.accounts[addr].svc.Users.GetProfile("me").Context(ctx).Do(); err == nil {
			ok++
		} else {
			c.log.Warn("gmail login: account check failed", "account", addr, "err", err)
		}
	}
	if ok == 0 {
		return connectors.LoginStatus{
			NeedsAuth: true,
			Detail:    "gmail: all accounts failed token refresh (re-consent needed)",
		}, fmt.Errorf("gmail: no accounts authenticated")
	}
	return connectors.LoginStatus{
		Connected: true,
		Detail:    fmt.Sprintf("gmail: %d/%d account(s) authenticated", ok, len(order)),
	}, nil
}

// Status reports auth/connection state without driving login.
func (c *Connector) Status(_ context.Context) (connectors.LoginStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return connectors.LoginStatus{
		Connected: c.connected,
		Detail:    fmt.Sprintf("gmail: %d account(s) configured", len(c.order)),
	}, nil
}

// --- helpers --------------------------------------------------------------

func (c *Connector) accountFor(addr string) *account {
	if addr == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accounts[strings.ToLower(addr)]
}

func (c *Connector) firstAccount() *account {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.order) == 0 {
		return nil
	}
	return c.accounts[c.order[0]]
}

// lookupByCanonical finds a message in an account by its canonical event id
// ("gmail:<rfc822-id>"). It searches by the rfc822 Message-ID header.
func (c *Connector) lookupByCanonical(ctx context.Context, acc *account, canonicalID string) (*gmailapi.Message, error) {
	rfcID := strings.TrimPrefix(canonicalID, event.PlatformGmail+":")
	if rfcID == "" {
		return nil, fmt.Errorf("gmail: empty reply-to id")
	}
	resp, err := acc.svc.Users.Messages.List("me").
		Q("rfc822msgid:" + rfcID).MaxResults(1).Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	if len(resp.Messages) == 0 {
		return nil, fmt.Errorf("gmail: reply-to message %q not found", rfcID)
	}
	return acc.svc.Users.Messages.Get("me", resp.Messages[0].Id).
		Format("metadata").
		MetadataHeaders("Message-ID", "Subject", "From", "References").
		Context(ctx).Do()
}

// isNotFound reports a Google API 404 (e.g. expired history cursor).
func isNotFound(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == 404
	}
	return false
}

// isInsufficientScope reports a 403 insufficient-permissions error (an account
// without a gmail scope).
func isInsufficientScope(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == 403 || gerr.Code == 401
	}
	return false
}

// compile-time assertion that *Connector satisfies the interface.
var _ connectors.Connector = (*Connector)(nil)
