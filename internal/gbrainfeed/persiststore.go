package gbrainfeed

import (
	"database/sql"
	"log/slog"
)

// SQLiteProposalStore is a durable ProposalStore backed by a *sql.DB (the
// switchboard SQLite handle from store.DB()). It makes the human review decisions
// survive a daemon restart: a "Not same" rejection persists and — replayed via
// Projector.LoadRejections at startup — keeps a rejected pair from ever being
// re-proposed. Proposals themselves regenerate from projection, but persisting
// them keeps the pending list stable across restarts too.
//
// Wire it from the daemon BEFORE Start:
//
//	ps, _ := gbrainfeed.NewSQLiteProposalStore(st.DB(), logger)
//	proj.SetProposalStore(ps)
//	proj.LoadRejections()
type SQLiteProposalStore struct {
	db  *sql.DB
	log *slog.Logger
}

// NewSQLiteProposalStore opens/creates the proposal table on the given DB.
func NewSQLiteProposalStore(db *sql.DB, logger *slog.Logger) (*SQLiteProposalStore, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS person_proposals (
		id      TEXT PRIMARY KEY,
		a       TEXT NOT NULL,
		b       TEXT NOT NULL,
		a_name  TEXT,
		b_name  TEXT,
		score   REAL,
		reason  TEXT,
		status  TEXT NOT NULL DEFAULT 'pending'
	)`); err != nil {
		return nil, err
	}
	return &SQLiteProposalStore{db: db, log: logger}, nil
}

// Add inserts a proposal, but never re-opens one the user already resolved
// (status != pending) — matching MemoryProposalStore semantics.
func (s *SQLiteProposalStore) Add(p Proposal) {
	if p.ID == "" {
		p.ID = pairKey(p.A, p.B)
	}
	if p.Status == "" {
		p.Status = ProposalPending
	}
	var existing string
	switch err := s.db.QueryRow(`SELECT status FROM person_proposals WHERE id=?`, p.ID).Scan(&existing); err {
	case nil:
		if existing != string(ProposalPending) {
			return // already resolved — don't re-open
		}
	case sql.ErrNoRows:
		// new proposal
	default:
		s.log.Warn("proposal store: lookup failed", "id", p.ID, "err", err)
		return
	}
	if _, err := s.db.Exec(`
		INSERT INTO person_proposals(id,a,b,a_name,b_name,score,reason,status)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
			a=excluded.a, b=excluded.b, a_name=excluded.a_name, b_name=excluded.b_name,
			score=excluded.score, reason=excluded.reason, status=excluded.status`,
		p.ID, p.A, p.B, p.AName, p.BName, p.Score, p.Reason, string(p.Status)); err != nil {
		s.log.Warn("proposal store: add failed", "id", p.ID, "err", err)
	}
}

func (s *SQLiteProposalStore) Pending() []Proposal {
	return s.query(`SELECT id,a,b,a_name,b_name,score,reason,status FROM person_proposals WHERE status=? ORDER BY id`, string(ProposalPending))
}

func (s *SQLiteProposalStore) Rejected() []Proposal {
	return s.query(`SELECT id,a,b,a_name,b_name,score,reason,status FROM person_proposals WHERE status=? ORDER BY id`, string(ProposalRejected))
}

func (s *SQLiteProposalStore) Approved() []Proposal {
	return s.query(`SELECT id,a,b,a_name,b_name,score,reason,status FROM person_proposals WHERE status=? ORDER BY id`, string(ProposalApproved))
}

func (s *SQLiteProposalStore) Get(id string) (Proposal, bool) {
	rows := s.query(`SELECT id,a,b,a_name,b_name,score,reason,status FROM person_proposals WHERE id=?`, id)
	if len(rows) == 0 {
		return Proposal{}, false
	}
	return rows[0], true
}

func (s *SQLiteProposalStore) SetStatus(id string, status ProposalStatus) {
	if _, err := s.db.Exec(`UPDATE person_proposals SET status=? WHERE id=?`, string(status), id); err != nil {
		s.log.Warn("proposal store: set status failed", "id", id, "err", err)
	}
}

// query runs a proposal SELECT and scans rows.
func (s *SQLiteProposalStore) query(q string, args ...any) []Proposal {
	rows, err := s.db.Query(q, args...)
	if err != nil {
		s.log.Warn("proposal store: query failed", "err", err)
		return nil
	}
	defer rows.Close()
	var out []Proposal
	for rows.Next() {
		var p Proposal
		var status string
		if err := rows.Scan(&p.ID, &p.A, &p.B, &p.AName, &p.BName, &p.Score, &p.Reason, &status); err != nil {
			s.log.Warn("proposal store: scan failed", "err", err)
			continue
		}
		p.Status = ProposalStatus(status)
		out = append(out, p)
	}
	return out
}
