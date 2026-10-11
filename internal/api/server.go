package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// Server is the daemon's local HTTP API.
type Server struct {
	be      Backend
	hub     *Hub
	mux     *http.ServeMux
	version string // build version (git SHA); surfaced by /healthz for deploy verification
}

// NewServer builds the API server over a Backend and push Hub.
func NewServer(be Backend, hub *Hub) *Server {
	s := &Server{be: be, hub: hub, mux: http.NewServeMux()}
	s.routes()
	return s
}

// WithVersion stamps the build version reported by /healthz (so a deploy can
// prove the running daemon is the expected commit). Returns s for chaining.
func (s *Server) WithVersion(v string) *Server { s.version = v; return s }

// Handler exposes the mux (for httptest or custom listeners).
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, HealthResult{OK: true, Version: s.version})
	})
	s.mux.HandleFunc("GET /chats", s.handleChats)
	s.mux.HandleFunc("GET /read", s.handleRead)
	s.mux.HandleFunc("GET /message", s.handleMessage)
	s.mux.HandleFunc("GET /search", s.handleSearch)
	s.mux.HandleFunc("GET /grep", s.handleGrep)
	s.mux.HandleFunc("GET /coverage", s.handleCoverage)
	s.mux.HandleFunc("GET /observability", s.handleObservability)
	s.mux.HandleFunc("GET /connectors/{name}/health", s.handleConnectorHealth)
	s.mux.HandleFunc("POST /query-sql", s.handleQuerySQL)
	s.mux.HandleFunc("GET /mentions", s.handleMentions)
	s.mux.HandleFunc("POST /send", s.handleSend)
	s.mux.HandleFunc("POST /send_media", s.handleSendMedia)
	s.mux.HandleFunc("POST /react", s.handleReact)
	s.mux.HandleFunc("POST /tap_button", s.handleTapButton)
	s.mux.HandleFunc("GET /group-invite", s.handleGroupInvite)
	s.mux.HandleFunc("POST /honcho/backfill", s.handleHonchoBackfill)
	s.mux.HandleFunc("POST /vote_poll", s.handleVotePoll)
	s.mux.HandleFunc("POST /start_chat", s.handleStartChat)
	s.mux.HandleFunc("GET /instagram/profile/{handle}", s.handleIGProfile)
	s.mux.HandleFunc("GET /instagram/stories", s.handleIGStories)
	s.mux.HandleFunc("GET /instagram/feed", s.handleIGFeed)
	s.mux.HandleFunc("GET /instagram/following_feed", s.handleIGFollowingFeed)
	s.mux.HandleFunc("GET /instagram/following", s.handleIGFollowing)
	s.mux.HandleFunc("GET /x/following_feed", s.handleXFollowingFeed)
	s.mux.HandleFunc("GET /x/profile/{handle}", s.handleXProfile)
	s.mux.HandleFunc("POST /backfill", s.handleBackfill)
	s.mux.HandleFunc("POST /reproject", s.handleReproject)
	s.mux.HandleFunc("POST /reextract-lid-phones", s.handleReextractLIDPhones)
	s.mux.HandleFunc("POST /media/reextract-refs", s.handleReextractMediaRefs)
	s.mux.HandleFunc("POST /media/revalidate", s.handleRevalidateMedia)
	s.mux.HandleFunc("POST /social/ingest", s.handleSocialIngest)
	s.mux.HandleFunc("GET /social/activity", s.handleSocialActivity)
	s.mux.HandleFunc("POST /x/ingest", s.handleXIngest)
	s.mux.HandleFunc("POST /lark/discover-p2p", s.handleDiscoverLarkP2P)
	s.mux.HandleFunc("POST /backfill-sweep", s.handleSweepBackfill)
	s.mux.HandleFunc("GET /auth/status", s.handleAuthStatus)
	s.mux.HandleFunc("POST /auth/submit", s.handleAuthSubmit)
	s.mux.HandleFunc("GET /people/proposals", s.handleProposals)
	s.mux.HandleFunc("POST /people/proposals/{id}/resolve", s.handleResolveProposal)
	s.mux.HandleFunc("POST /people/{canon_id}/name", s.handleRenameCanonical)
	s.mux.HandleFunc("POST /people/merge", s.handleMergeCanonicals)
	s.mux.HandleFunc("POST /people/backfill-names", s.handleBackfillSenderNames)
	s.mux.HandleFunc("POST /people/merges/deterministic", s.handleDeterministicMerges)
	s.mux.HandleFunc("POST /people/proposals/deterministic", s.handleDeterministicProposals)
	s.mux.HandleFunc("GET /people/resolve", s.handleResolvePerson)
	s.mux.HandleFunc("GET /people/core_network", s.handleCoreNetwork)
	s.mux.HandleFunc("POST /people/salience/recompute", s.handleRecomputeSalience)
	s.mux.HandleFunc("POST /media/reproject", s.handleMediaReproject)
	s.mux.HandleFunc("GET /media/{id}/meta", s.handleMediaMeta)
	s.mux.HandleFunc("GET /media/{id}", s.handleMediaStream)
	s.mux.HandleFunc("GET /events", s.handleEvents)
}

func (s *Server) handleChats(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	since := parseTime(r.URL.Query().Get("since"))
	limit := parseInt(r.URL.Query().Get("limit"), 100)
	chats, err := s.be.Store().ListChats(r.Context(), platform, since, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, chats)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	chatID := r.URL.Query().Get("chat_id")
	if chatID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("chat_id required"))
		return
	}
	q := r.URL.Query()
	since := parseTime(q.Get("since"))
	limit := parseInt(q.Get("limit"), 100)
	// #45: newest-first support. desc=true|1 → newest N by ts (recent-context consumers:
	// sendgate thread lookback, briefing). before/until = exclusive upper ts bound for
	// backward pagination. Default stays oldest-first for back-compat.
	desc := q.Get("desc") == "true" || q.Get("desc") == "1"
	before := parseTime(q.Get("before"))
	if before.IsZero() {
		before = parseTime(q.Get("until")) // alias
	}
	msgs, err := s.be.Store().ReadWindow(r.Context(), chatID,
		store.ReadOpts{Since: since, Before: before, Limit: limit, Desc: desc})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, msgs)
}

// handleMessage is a single-message point lookup by canonical id (#53):
// GET /message?id=<platform:msgid>. PK fetch (no window scan) so the send-gate
// reply-quote resolver can render the actual quoted message however far back it is.
// 404 if the id isn't in the log.
func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id required"))
		return
	}
	m, ok, err := s.be.Store().MessageByID(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Errorf("message %q not found", id))
		return
	}
	writeJSON(w, m)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := q.Get("q")
	person := q.Get("person")
	// q OR person is enough: person-only ("Casey's latest?", no keyword) lists that
	// person's messages newest-first; q-only is full-text search; both narrows.
	if query == "" && person == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("q or person required"))
		return
	}
	msgs, err := s.be.Search(r.Context(), query, q.Get("platform"), person, parseTime(q.Get("since")), parseInt(q.Get("limit"), 50))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, msgs)
}

func (s *Server) handleGrep(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pattern := q.Get("pattern")
	if pattern == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("pattern required"))
		return
	}
	res, err := s.be.Grep(r.Context(), pattern, q.Get("platform"), q.Get("person"), parseTime(q.Get("since")), parseInt(q.Get("limit"), 50))
	if err != nil {
		// A bad user regex is a 400, not a 500.
		if errors.Is(err, store.ErrInvalidPattern) {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleCoverage(w http.ResponseWriter, r *http.Request) {
	cov, err := s.be.Store().Coverage(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, cov)
}

// handleObservability serves the phase-2 read-only health snapshot (#39): per-platform
// coverage + ingestion lag + connector health + last backfill-sweep convergence delta.
func (s *Server) handleObservability(w http.ResponseWriter, r *http.Request) {
	obs, err := s.be.Observability(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, obs)
}

func (s *Server) handleQuerySQL(w http.ResponseWriter, r *http.Request) {
	var body QuerySQLBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(body.SQL) == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("sql required"))
		return
	}
	res, err := s.be.QuerySQL(r.Context(), body.SQL)
	if err != nil {
		// Every query_sql failure is the CALLER's query — rejected by the guard
		// (ErrUnsafeSQL), a bad/compile error (no such table/column), or too slow (5s
		// timeout / context deadline). All map to 400 with the clean message, so the
		// agent gets a "fix your query" signal rather than a 500 it might misread as a
		// server crash + blindly retry. A read-only SELECT can't fault the server.
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleCoreNetwork(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cards, err := s.be.CoreNetwork(r.Context(), q.Get("tier"), parseInt(q.Get("limit"), 0))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if cards == nil {
		cards = []PersonCard{} // emit [] not null
	}
	writeJSON(w, cards)
}

func (s *Server) handleRecomputeSalience(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.RecomputeSalience(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"scored": n})
}

func (s *Server) handleConnectorHealth(w http.ResponseWriter, r *http.Request) {
	h, err := s.be.ConnectorHealth(r.Context(), r.PathValue("name"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err) // unknown connector
		return
	}
	writeJSON(w, h)
}

func (s *Server) handleMentions(w http.ResponseWriter, r *http.Request) {
	msgs, err := s.be.Store().Mentions(r.Context(), parseTime(r.URL.Query().Get("since")), parseInt(r.URL.Query().Get("limit"), 100))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, msgs)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var body SendBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// Up-front caller validation → 400 (a clean "fix your request", not a 502 the agent
	// might misread as a server crash + blindly retry).
	if strings.TrimSpace(body.ChatID) == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("chat_id required"))
		return
	}
	if strings.TrimSpace(body.Text) == "" && len(body.Attachments) == 0 &&
		body.Poll == nil && body.Card == nil && body.Buttons == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("empty message: text, attachments, poll, card, or buttons required"))
		return
	}
	sentID, err := s.be.Send(r.Context(), connectors.SendRequest{
		ChatID:      body.ChatID,
		Text:        body.Text,
		ReplyTo:     body.ReplyTo,
		Attachments: body.Attachments,
		Poll:        body.Poll,
		Card:        body.Card,
		Buttons:     body.Buttons,
	})
	if err != nil {
		// Unroutable chat_id (unknown platform / malformed id) is a CALLER fault → 400;
		// a genuine upstream/platform failure stays 502 (BadGateway).
		if errors.Is(err, connectors.ErrNoConnector) {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, SendResult{SentID: sentID})
}

func (s *Server) handleSendMedia(w http.ResponseWriter, r *http.Request) {
	var body SendMediaBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if body.ChatID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("chat_id required"))
		return
	}
	if body.MediaID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("media_id required"))
		return
	}
	sentID, err := s.be.SendMedia(r.Context(), body)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, SendResult{SentID: sentID})
}

func (s *Server) handleReact(w http.ResponseWriter, r *http.Request) {
	var body ReactBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.be.React(r.Context(), body.ChatID, body.MessageID, body.Emoji); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, OKResult{OK: true})
}

func (s *Server) handleTapButton(w http.ResponseWriter, r *http.Request) {
	var body TapBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.be.TapButton(r.Context(), body.ChatID, body.MessageID, body.ButtonID)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, TapResult{Result: result})
}

// handleGroupInvite (#52) returns a group's shareable invite link
// (GET /group-invite?chat_id=&reset=). reset=true rotates the link.
func (s *Server) handleGroupInvite(w http.ResponseWriter, r *http.Request) {
	chatID := r.URL.Query().Get("chat_id")
	if chatID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("chat_id required"))
		return
	}
	reset := r.URL.Query().Get("reset") == "true"
	link, err := s.be.GroupInvite(r.Context(), chatID, reset)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, map[string]string{"chat_id": chatID, "invite_link": link})
}

// handleHonchoBackfill (POST /honcho/backfill?max_chats=&per_chat=) replays inner-tier
// WhatsApp DM history into Honcho — bounded + idempotent (per-session watermark); no-op
// when ingestion is flagged off. Deliberate trigger (run after the live sink is verified).
func (s *Server) handleHonchoBackfill(w http.ResponseWriter, r *http.Request) {
	// Backfill is dispatched ASYNC by the backend (it waits for the resolver build, then
	// posts potentially thousands of messages — far longer than an HTTP request should
	// block). Return 202 immediately; the outcome is in the daemon's structured logs +
	// Honcho counts.
	if _, _, err := s.be.HonchoBackfill(r.Context(),
		parseInt(r.URL.Query().Get("max_chats"), 200), parseInt(r.URL.Query().Get("per_chat"), 500)); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "started",
		"note":   "backfill running async; watch switchboardd logs + Honcho counts",
	})
}

func (s *Server) handleVotePoll(w http.ResponseWriter, r *http.Request) {
	var body VoteBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.be.VotePoll(r.Context(), body.ChatID, body.MessageID, body.Option); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, OKResult{OK: true})
}

func (s *Server) handleStartChat(w http.ResponseWriter, r *http.Request) {
	var body StartChatBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if body.Platform == "" || body.Identifier == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("start_chat requires platform + identifier"))
		return
	}
	chatID, name, err := s.be.StartChat(r.Context(), body.Platform, body.Identifier)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, StartChatResult{ChatID: chatID, ResolvedName: name})
}

func (s *Server) handleIGProfile(w http.ResponseWriter, r *http.Request) {
	handle := strings.TrimPrefix(r.PathValue("handle"), "@")
	if handle == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("handle required"))
		return
	}
	card, err := s.be.IGProfile(r.Context(), handle)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, card)
}

func (s *Server) handleIGStories(w http.ResponseWriter, r *http.Request) {
	res, err := s.be.IGStories(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleIGFeed(w http.ResponseWriter, r *http.Request) {
	limit := 0 // 0 → single page (back-compat); >0 auto-paginates to ~limit posts
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	res, err := s.be.IGFeed(r.Context(), r.URL.Query().Get("max_id"), limit)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleIGFollowingFeed(w http.ResponseWriter, r *http.Request) {
	limit := 0 // 0 → client default
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	res, err := s.be.IGFollowingFeed(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleIGFollowing(w http.ResponseWriter, r *http.Request) {
	res, err := s.be.IGFollowing(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleXFollowingFeed(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	res, err := s.be.XFollowingFeed(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleXProfile(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	res, err := s.be.XProfile(r.Context(), r.PathValue("handle"), limit)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleBackfill(w http.ResponseWriter, r *http.Request) {
	var body BackfillBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// The daemon's Backfill resolves the connector and wires its output into the
	// shared pipeline. Kick it off in the background and ack immediately so a
	// long history sync doesn't hold the HTTP request open.
	go func() {
		if err := s.be.Backfill(context.Background(), body.ChatID, body.Since); err != nil {
			slog.Error("API backfill failed", "chat_id", body.ChatID, "err", err)
		}
	}()
	writeJSON(w, OKResult{OK: true})
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	if platform == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("platform required"))
		return
	}
	st, err := s.be.AuthStatus(r.Context(), platform)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	// Never leak the QR PNG bytes over this JSON endpoint; report presence only.
	writeJSON(w, map[string]any{
		"platform":   platform,
		"connected":  st.Connected,
		"needs_auth": st.NeedsAuth,
		"detail":     st.Detail,
		"has_qr":     len(st.QRPNG) > 0,
	})
}

func (s *Server) handleAuthSubmit(w http.ResponseWriter, r *http.Request) {
	var body AuthSubmitBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.be.AuthSubmit(r.Context(), body.Platform, body.Field, body.Value); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, OKResult{OK: true})
}

func (s *Server) handleProposals(w http.ResponseWriter, r *http.Request) {
	ps, err := s.be.PendingProposals(r.Context(), parseInt(r.URL.Query().Get("limit"), 15), parseInt(r.URL.Query().Get("offset"), 0))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if ps == nil {
		ps = []ProposalDTO{} // emit [] not null so the bot can len() safely
	}
	writeJSON(w, ps)
}

func (s *Server) handleResolveProposal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("proposal id required"))
		return
	}
	var body ResolveBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if body.Decision != "approve" && body.Decision != "reject" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf(`decision must be "approve" or "reject"`))
		return
	}
	outcome, err := s.be.ResolveProposal(r.Context(), id, body.Decision)
	if err != nil {
		// Unknown id is the caller's fault (stale list); surface as 404.
		if strings.Contains(err.Error(), "unknown") || strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if outcome != nil {
		writeJSON(w, outcome) // approve → survivor {id,name,...}
		return
	}
	writeJSON(w, OKResult{OK: true}) // reject
}

func (s *Server) handleRenameCanonical(w http.ResponseWriter, r *http.Request) {
	canonID := r.PathValue("canon_id")
	if canonID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("canon_id required"))
		return
	}
	var body NameBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if body.Name == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("name required"))
		return
	}
	if err := s.be.RenameCanonical(r.Context(), canonID, body.Name); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, OKResult{OK: true})
}

// handleMergeCanonicals force-merges two operator-chosen canonical ids
// (POST /people/merge {"a","b"}) — the human-directed merge surface for pairs no
// auto-proposal covers (e.g. an over-ambiguous name the propose logic skips by
// design). Durable via ApproveMerge (force-merge + persist + merged_into
// provenance); idempotent. Returns the survivor outcome.
func (s *Server) handleMergeCanonicals(w http.ResponseWriter, r *http.Request) {
	var body MergeBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	a, b := body.Pair()
	if a == "" || b == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf(`two canonical ids required (as {a,b} or {from_node,into_node})`))
		return
	}
	if a == b {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("the two canonical ids are identical"))
		return
	}
	outcome, err := s.be.MergeCanonicals(r.Context(), a, b)
	if err != nil {
		if strings.Contains(err.Error(), "unknown") || strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, outcome)
}

// handleBackfillSenderNames fills blank message sender_name/sender_handle for a
// platform's existing rows from the people-graph (#54; POST /people/backfill-names
// ?platform=telegram, default telegram). Idempotent — only fills blanks. Returns
// {platform, scanned, backfilled, unresolved}.
func (s *Server) handleBackfillSenderNames(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	if platform == "" {
		platform = "telegram"
	}
	res, err := s.be.BackfillSenderNames(r.Context(), platform)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, res)
}

// handleDeterministicMerges triggers the deterministic corroborated-exact-name
// merge policy + pending-queue drain (no LLM). SAFE DEFAULT: dry_run=true unless
// ?dry_run=false is explicitly passed — so a bare call only LOGS candidates,
// never mutates the graph. Returns {merged, dry_run}.
func (s *Server) handleDeterministicMerges(w http.ResponseWriter, r *http.Request) {
	dryRun := r.URL.Query().Get("dry_run") != "false" // default true (read-only)
	n, err := s.be.RunDeterministicMerges(r.Context(), dryRun)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"merged": n, "dry_run": dryRun})
}

// handleDeterministicProposals refreshes the deterministic propose-only queue (#19):
// strong cross-platform pairs surfaced for human review. No LLM, no auto-merge,
// idempotent — always safe (no dry_run needed). Returns {proposed}.
func (s *Server) handleDeterministicProposals(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.RunDeterministicProposals(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"proposed": n})
}

// handleResolvePerson resolves a name query to ranked canonical person cards.
// GET /people/resolve?q=<name>. Empty q → 400. Always emits the envelope
// {query, in_graph, hits:[]} (hits never null) so the agent can len() safely;
// in_graph=false means no one matched the people graph (a phase-2 Lark-directory
// fallback will populate hits from the org directory in that case).
func (s *Server) handleResolvePerson(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("q required"))
		return
	}
	hits, err := s.be.ResolvePerson(r.Context(), q)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if hits == nil {
		hits = []PersonCard{} // emit [] not null
	}
	// in_graph means "found in the message graph" specifically — derive it from
	// card SOURCE, not hit count, so a directory-only result (all source:"lark",
	// the #24 P2 fallback for never-messaged colleagues) correctly reports
	// in_graph:false even though it has hits.
	inGraph := false
	for _, h := range hits {
		if h.Source == "graph" {
			inGraph = true
			break
		}
	}
	writeJSON(w, ResolveResult{Query: q, InGraph: inGraph, Hits: hits})
}

func (s *Server) handleMediaReproject(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.ReprojectMedia(r.Context(), r.URL.Query().Get("chat"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"enqueued": n})
}

func (s *Server) handleMediaMeta(w http.ResponseWriter, r *http.Request) {
	meta, err := s.be.MediaInfo(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, meta)
}

func (s *Server) handleMediaStream(w http.ResponseWriter, r *http.Request) {
	rc, meta, err := s.be.MediaStream(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	defer rc.Close()
	if meta.MIME != "" {
		w.Header().Set("Content-Type", meta.MIME)
	}
	if meta.Name != "" {
		w.Header().Set("Content-Disposition", "inline; filename=\""+meta.Name+"\"")
	}
	_, _ = io.Copy(w, rc)
}

func (s *Server) handleReproject(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.Reproject(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"reprojected": n})
}

func (s *Server) handleReextractLIDPhones(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.ReextractLIDPhones(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"updated": n})
}

// handleReextractMediaRefs (#48) re-derives downloadable media refs from stored Raw for
// a platform (POST /media/reextract-refs?platform=telegram) + resets the matching
// failed/skipped rows to pending. Defaults to telegram (the row class this recovers).
func (s *Server) handleReextractMediaRefs(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	if platform == "" {
		platform = "telegram"
	}
	chat := r.URL.Query().Get("chat") // optional: scope to ONE chat (targeted recovery)
	n, err := s.be.ReextractMediaRefs(r.Context(), platform, chat)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"platform": platform, "chat": chat, "refreshed": n})
}

// handleRevalidateMedia (#48) re-checks stored media vs declared type + resets the
// undecrypted ciphertext ones for re-download (POST /media/revalidate?platform=whatsapp).
func (s *Server) handleRevalidateMedia(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	if platform == "" {
		platform = "whatsapp"
	}
	checked, decrypted, reset, err := s.be.RevalidateMedia(r.Context(), platform)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"platform": platform, "checked": checked, "decrypted": decrypted, "reset": reset})
}

// handleSocialIngest (#49) pulls the IG following-feed + stories + complete follow list
// and writes each as a social_activity event (POST /social/ingest?limit=): posts/stories
// are activity, follows are identity-establishing (Sub="follow") so every followed
// account is a durable classify/merge node. On-demand — the recurring poll is a separate
// go/no-go.
func (s *Server) handleSocialIngest(w http.ResponseWriter, r *http.Request) {
	posts, stories, follows, err := s.be.IngestSocialActivity(r.Context(), parseInt(r.URL.Query().Get("limit"), 50))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"posts": posts, "stories": stories, "follows": follows})
}

// handleSocialActivity (#49) returns recent social_activity feed events newest-first
// (GET /social/activity?since=&limit=) — the briefing's "what friends are up to" source.
// The friend/creator + tier filter is applied by the caller via the people-graph resolver.
func (s *Server) handleSocialActivity(w http.ResponseWriter, r *http.Request) {
	// Optional sub= filter (comma-separated, e.g. sub=post,story) — the briefing
	// passes sub=post,story to exclude identity-only "follow" rows; absent → all.
	var subs []string
	if v := strings.TrimSpace(r.URL.Query().Get("sub")); v != "" {
		for _, s := range strings.Split(v, ",") {
			if s = strings.TrimSpace(s); s != "" {
				subs = append(subs, s)
			}
		}
	}
	acts, err := s.be.Store().SocialActivity(r.Context(), r.URL.Query().Get("platform"), parseTime(r.URL.Query().Get("since")), parseInt(r.URL.Query().Get("limit"), 100), subs...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, acts)
}

// handleXIngest (#55) pulls the X following-feed and writes each post as a
// social_activity event (POST /x/ingest?limit=). Ingests everything to the log; the
// briefing filters at read time. On-demand. Returns {posts}.
func (s *Server) handleXIngest(w http.ResponseWriter, r *http.Request) {
	n, err := s.be.IngestXActivity(r.Context(), parseInt(r.URL.Query().Get("limit"), 50))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]int{"posts": n})
}

func (s *Server) handleSweepBackfill(w http.ResponseWriter, r *http.Request) {
	// Accept platform/max from EITHER the JSON body OR the query string (#13 fix). A
	// POST with {"platform":"lark"} was previously IGNORED (handler read only the query
	// param) → platform="" → an all-platform sweep that pulled telegram candidates and
	// tripped FLOOD_WAIT, defeating platform scoping. Body wins; query is the fallback
	// (the daily timer curls ?max=10 with no platform = all platforms, intended).
	q := r.URL.Query()
	platform := q.Get("platform")
	maxChats := parseInt(q.Get("max"), 10)
	if r.Body != nil {
		var body struct {
			Platform string `json:"platform"`
			Max      int    `json:"max"`
		}
		if json.NewDecoder(r.Body).Decode(&body) == nil {
			if body.Platform != "" {
				platform = body.Platform
			}
			if body.Max > 0 {
				maxChats = body.Max
			}
		}
	}
	res, err := s.be.SweepBackfill(r.Context(), platform, maxChats)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleDiscoverLarkP2P(w http.ResponseWriter, r *http.Request) {
	// q + seeds tune the underlying search/v2/message request shape (#31): q is the
	// search keyword; seeds caps the sender-filter (from_ids) seeded from known Lark
	// identities (seeds=0 → query-only). Defaults seed up to 300 (the eventual
	// scheduled behaviour); both overridable while validating the working shape.
	q := r.URL.Query().Get("q")
	maxSeeds := 300
	if v := r.URL.Query().Get("seeds"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxSeeds = n
		}
	}
	queued, err := s.be.DiscoverLarkP2P(r.Context(), q, maxSeeds)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// Async: discovery runs in the background (it can take minutes); the discovered-
	// count is logged. queued=false means a run was already in flight.
	writeJSON(w, map[string]bool{"queued": queued})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, cancel := s.hub.Subscribe()
	defer cancel()

	enc := json.NewEncoder(w)
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprint(w, "data: ")
			_ = enc.Encode(m) // writes the JSON + newline
			fmt.Fprint(w, "\n")
			flusher.Flush()
		}
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
