package gmail

import (
	"context"
	"sort"
	"strings"
	"sync"

	gmailapi "google.golang.org/api/gmail/v1"

	"github.com/seahyc/switchboard-oss/internal/connectors"
)

// metadataThreadCap bounds how many (most-recent) threads we enumerate per
// account per poll, so the 10-minute metadata refresh stays cheap regardless of
// mailbox size. Older threads still carry whatever metadata they had at ingest.
const metadataThreadCap = 500

// metadataConcurrency bounds parallel threads.get calls — each thread needs its
// own round-trip, so we fan out (well within Gmail's per-user rate quota) to keep
// a full refresh to a few seconds instead of minutes.
const metadataConcurrency = 12

// signalLabels are Gmail system labels we surface as normalized boolean signals
// (pinned/archived/importance/muted) rather than as opaque label strings, so the
// Labels list stays meaningful (categories + user labels).
var signalLabels = map[string]bool{
	"STARRED":   true, // -> Pinned
	"IMPORTANT": true, // -> Importance=high
	"INBOX":     true, // absence -> Archived
	"UNREAD":    true, // per-message read state, not a folder
	"MUTE":      true, // -> Muted
}

// ChatMetadata implements connectors.ChatMetadataProvider. It maps Gmail's native
// labels onto the normalized chat-metadata set for each thread, so list_chats can
// show subjects + starred(pinned)/important/archived/labels instead of opaque
// thread ids. The chat id matches per-message ingestion ("gmail:<account>:<thread>")
// so metadata upserts onto the same chat rows.
func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	c.mu.Lock()
	order := append([]string(nil), c.order...)
	c.mu.Unlock()

	var out []connectors.ChatMeta
	var firstErr error
	for _, addr := range order {
		acc := c.accounts[addr]
		metas, err := c.accountChatMetadata(ctx, acc)
		if err != nil {
			if isInsufficientScope(err) {
				c.log.Warn("gmail metadata: insufficient scope; skipping account", "account", addr)
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, metas...)
	}
	return out, firstErr
}

func (c *Connector) accountChatMetadata(ctx context.Context, acc *account) ([]connectors.ChatMeta, error) {
	labelNames, err := c.labelNames(ctx, acc)
	if err != nil {
		return nil, err
	}

	// 1. Enumerate the most-recent thread ids (cheap; one list call per 100).
	var threadIDs []string
	pageToken := ""
	for len(threadIDs) < metadataThreadCap {
		call := acc.svc.Users.Threads.List("me").MaxResults(100).Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		if err != nil {
			if len(threadIDs) > 0 {
				break // use what we have on a mid-enumeration error
			}
			return nil, err
		}
		for _, t := range resp.Threads {
			threadIDs = append(threadIDs, t.Id)
		}
		if resp.NextPageToken == "" || len(threadIDs) >= metadataThreadCap {
			break
		}
		pageToken = resp.NextPageToken
	}
	if len(threadIDs) > metadataThreadCap {
		threadIDs = threadIDs[:metadataThreadCap]
	}

	// 2. Fetch each thread's metadata concurrently (bounded), tolerating partial
	// results if ctx is cancelled mid-flight.
	sem := make(chan struct{}, metadataConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	out := make([]connectors.ChatMeta, 0, len(threadIDs))
	for _, id := range threadIDs {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if meta, ok := c.threadMeta(ctx, acc, id, labelNames); ok {
				mu.Lock()
				out = append(out, meta)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out, nil
}

// labelNames returns a map of Gmail label id -> display name for the account.
func (c *Connector) labelNames(ctx context.Context, acc *account) (map[string]string, error) {
	resp, err := acc.svc.Users.Labels.List("me").Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(resp.Labels))
	for _, l := range resp.Labels {
		name := l.Name
		if name == "" {
			name = l.Id
		}
		m[l.Id] = name
	}
	return m, nil
}

// threadMeta fetches a thread's metadata and maps its labels to a ChatMeta.
func (c *Connector) threadMeta(ctx context.Context, acc *account, threadID string, labelNames map[string]string) (connectors.ChatMeta, bool) {
	th, err := acc.svc.Users.Threads.Get("me", threadID).
		Format("metadata").
		MetadataHeaders("Subject").
		Context(ctx).Do()
	if err != nil || len(th.Messages) == 0 {
		if err != nil {
			c.log.Warn("gmail metadata: thread fetch failed", "account", acc.addr, "thread", threadID, "err", err)
		}
		return connectors.ChatMeta{}, false
	}

	// Union of label ids across the thread's messages.
	labelSet := map[string]bool{}
	for _, msg := range th.Messages {
		for _, id := range msg.LabelIds {
			labelSet[id] = true
		}
	}

	return buildThreadMeta(canonChatID(acc.addr, threadID), threadSubject(th), labelSet, labelNames), true
}

// buildThreadMeta maps a thread's union label set onto the normalized chat
// metadata: Gmail's native labels translate directly (STARRED→pinned,
// IMPORTANT→importance:high, absence of INBOX→archived, MUTE→muted), with the
// remaining labels surfaced by display name. Pure + unit-testable.
func buildThreadMeta(chatID, subject string, labelSet map[string]bool, labelNames map[string]string) connectors.ChatMeta {
	meta := connectors.ChatMeta{
		ChatID:   chatID,
		Title:    subject,
		Pinned:   labelSet["STARRED"],
		Archived: !labelSet["INBOX"],
		Muted:    labelSet["MUTE"],
	}
	if labelSet["IMPORTANT"] {
		meta.Importance = "high"
	}
	// Surface non-signal labels by display name (categories + user labels), sorted
	// for stable output.
	var ids []string
	for id := range labelSet {
		if signalLabels[id] {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		name := labelNames[id]
		if name == "" {
			name = id
		}
		meta.Labels = append(meta.Labels, name)
	}
	return meta
}

// threadSubject returns the thread's human-legible subject (the original/first
// message's Subject), falling back to any present subject then a placeholder.
func threadSubject(th *gmailapi.Thread) string {
	for _, msg := range th.Messages {
		if s := strings.TrimSpace(header(msg.Payload, "Subject")); s != "" {
			return s
		}
	}
	return "(no subject)"
}

// compile-time assertion that *Connector provides chat metadata.
var _ connectors.ChatMetadataProvider = (*Connector)(nil)
