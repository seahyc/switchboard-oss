package daemon

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// SQLiteMatchCache is the durable idmatch.AdjudicationCache (the P2 persistent
// implementation the queue.go comment reserves). It memoizes the LLM's verdict per
// (pairKey, ctxHash) so a pair is NEVER re-adjudicated on a CPU-only box — load-
// bearing, since each 7B call costs seconds. Keyed by the canonical pair AND a
// content-hash of the adjudication context, so a re-run/reproject pays nothing
// unless the underlying records actually changed.
//
// It doubles as the auto-merge AUDIT LOG: every verdict (with confidence, reason,
// evidence, and timestamp) is durably recorded, so an auto-merge is always
// traceable back to the model decision that produced it (reversibility, #17/#19).
type SQLiteMatchCache struct {
	db  *sql.DB
	log *slog.Logger
}

// NewSQLiteMatchCache opens/creates the verdict-cache table on the given DB (the
// shared switchboard SQLite handle from store.DB()).
func NewSQLiteMatchCache(db *sql.DB, logger *slog.Logger) (*SQLiteMatchCache, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS idmatch_verdicts (
		pair_key    TEXT NOT NULL,
		ctx_hash    TEXT NOT NULL,
		same_person INTEGER NOT NULL,
		confidence  REAL NOT NULL,
		reason      TEXT NOT NULL DEFAULT '',
		evidence    TEXT NOT NULL DEFAULT '[]',
		decided_at  TEXT NOT NULL,
		PRIMARY KEY (pair_key, ctx_hash)
	)`); err != nil {
		return nil, err
	}
	return &SQLiteMatchCache{db: db, log: logger}, nil
}

// Get returns the cached verdict for a (pairKey, ctxHash), if present.
func (c *SQLiteMatchCache) Get(pairKey, ctxHash string) (idmatch.AdjudicationResult, bool) {
	var (
		same     int
		conf     float64
		reason   string
		evidence string
	)
	err := c.db.QueryRow(
		`SELECT same_person, confidence, reason, evidence FROM idmatch_verdicts WHERE pair_key=? AND ctx_hash=?`,
		pairKey, ctxHash).Scan(&same, &conf, &reason, &evidence)
	if err == sql.ErrNoRows {
		return idmatch.AdjudicationResult{}, false
	}
	if err != nil {
		c.log.Warn("idmatch cache: get failed", "pair", pairKey, "err", err)
		return idmatch.AdjudicationResult{}, false
	}
	var ev []string
	_ = json.Unmarshal([]byte(evidence), &ev)
	return idmatch.AdjudicationResult{
		SamePerson: same != 0,
		Confidence: conf,
		Reason:     reason,
		Evidence:   ev,
	}, true
}

// Put durably records a verdict (idempotent upsert on the pair+context key).
func (c *SQLiteMatchCache) Put(pairKey, ctxHash string, r idmatch.AdjudicationResult) {
	ev, _ := json.Marshal(r.Evidence)
	same := 0
	if r.SamePerson {
		same = 1
	}
	if _, err := c.db.Exec(`
		INSERT INTO idmatch_verdicts(pair_key, ctx_hash, same_person, confidence, reason, evidence, decided_at)
		VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(pair_key, ctx_hash) DO UPDATE SET
			same_person=excluded.same_person, confidence=excluded.confidence,
			reason=excluded.reason, evidence=excluded.evidence, decided_at=excluded.decided_at`,
		pairKey, ctxHash, same, r.Confidence, r.Reason, string(ev), time.Now().UTC().Format(time.RFC3339)); err != nil {
		c.log.Warn("idmatch cache: put failed", "pair", pairKey, "err", err)
	}
}

// compile-time assertion.
var _ idmatch.AdjudicationCache = (*SQLiteMatchCache)(nil)
