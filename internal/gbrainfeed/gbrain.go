// Package gbrainfeed projects switchboard's message firehose into gbrain — the
// semantic layer. We do NOT build a parallel person graph: gbrain already has
// the entity model. In gbrain 0.18.x that model is realized as **person-type
// pages** (canonical slug + frontmatter aliases + external ids) plus **typed
// links** between them. put_page is idempotent on slug, so re-putting a person
// with merged aliases IS the "merge-as-DB-op"; links connect per-platform
// identities without ever destroying one.
//
// This file defines the thin gbrain client surface (the real MCP tool names:
// put_page, get_page, add_link) and a CLI-backed implementation that shells out
// to the `gbrain` binary via `gbrain call <tool> '<json>'`. A no-op client lets
// the daemon run where gbrain isn't installed (e.g. local dev).
package gbrainfeed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// Client is the subset of gbrain tools the projector needs. Names mirror the
// real gbrain MCP tools verified on the VM (gbrain 0.18.2).
type Client interface {
	// PutPage writes/updates a page (markdown + YAML frontmatter). Idempotent on slug.
	PutPage(ctx context.Context, slug, content string) error
	// AddLink creates a typed link between two pages (e.g. "same_as").
	AddLink(ctx context.Context, from, to, linkType, context string) error
}

// NopClient discards everything. Used when gbrain isn't configured.
type NopClient struct{}

func (NopClient) PutPage(context.Context, string, string) error                 { return nil }
func (NopClient) AddLink(context.Context, string, string, string, string) error { return nil }

// CLIClient shells out to the gbrain binary. Bin defaults to "gbrain" on PATH.
type CLIClient struct {
	Bin string
}

// NewCLIClient builds a CLIClient; bin "" → "gbrain".
func NewCLIClient(bin string) *CLIClient {
	if bin == "" {
		bin = "gbrain"
	}
	return &CLIClient{Bin: bin}
}

// call invokes `gbrain call <tool> '<json-args>'` (the raw tool-invocation path).
func (c *CLIClient) call(ctx context.Context, tool string, args any) error {
	payload, err := json.Marshal(args)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, c.Bin, "call", tool, string(payload))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gbrain call %s: %w: %s", tool, err, stderr.String())
	}
	return nil
}

// PutPage → gbrain put_page tool.
func (c *CLIClient) PutPage(ctx context.Context, slug, content string) error {
	return c.call(ctx, "put_page", map[string]string{"slug": slug, "content": content})
}

// AddLink → gbrain add_link tool.
func (c *CLIClient) AddLink(ctx context.Context, from, to, linkType, linkContext string) error {
	return c.call(ctx, "add_link", map[string]string{
		"from": from, "to": to, "link_type": linkType, "context": linkContext,
	})
}
