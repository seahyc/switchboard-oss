package idmatch

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
)

// CachingAdjudicator decorates an LLMAdjudicator with an AdjudicationCache so a
// pair is adjudicated at most ONCE — load-bearing on the CPU box (~27s/pair) and
// the reason a crashed multi-hour eval/sweep resumes instead of restarting. Keyed
// by canonical pair + a context-hash (so a changed record re-adjudicates). The
// inner model is only called on a miss.
type CachingAdjudicator struct {
	Inner LLMAdjudicator
	Cache AdjudicationCache
	// Hash yields the context-hash for a pair; nil → key on the pair alone.
	Hash func(a, b PersonRecord) string
}

func (c CachingAdjudicator) Adjudicate(ctx context.Context, req AdjudicationRequest) (AdjudicationResult, error) {
	pk := pairKey(req.A.CanonicalID, req.B.CanonicalID)
	h := ""
	if c.Hash != nil {
		h = c.Hash(req.A, req.B)
	}
	if r, ok := c.Cache.Get(pk, h); ok {
		return r, nil
	}
	r, err := c.Inner.Adjudicate(ctx, req)
	if err != nil {
		return r, err // don't cache transport errors — retry next run
	}
	c.Cache.Put(pk, h, r)
	return r, nil
}

// FileCache is a persistent AdjudicationCache backed by an append-only JSONL file —
// makes a long offline gate-eval / sweep RESUMABLE across crashes with no external
// store. Each Put appends a line + updates memory; New loads prior lines. Safe for
// concurrent use.
type FileCache struct {
	mu sync.Mutex
	m  map[string]AdjudicationResult
	w  *os.File
}

type fileCacheRow struct {
	Key string             `json:"k"`
	Res AdjudicationResult `json:"r"`
}

// NewFileCache opens/creates the cache file and loads any prior verdicts.
func NewFileCache(path string) (*FileCache, error) {
	c := &FileCache{m: map[string]AdjudicationResult{}}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			var row fileCacheRow
			if json.Unmarshal(sc.Bytes(), &row) == nil && row.Key != "" {
				c.m[row.Key] = row.Res
			}
		}
		f.Close()
	}
	w, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	c.w = w
	return c, nil
}

func (c *FileCache) key(pairKey, ctxHash string) string { return pairKey + "@" + ctxHash }

func (c *FileCache) Get(pairKey, ctxHash string) (AdjudicationResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.m[c.key(pairKey, ctxHash)]
	return r, ok
}

func (c *FileCache) Put(pairKey, ctxHash string, r AdjudicationResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := c.key(pairKey, ctxHash)
	c.m[k] = r
	if c.w != nil {
		if b, err := json.Marshal(fileCacheRow{Key: k, Res: r}); err == nil {
			c.w.Write(append(b, '\n'))
			c.w.Sync()
		}
	}
}

// Close flushes + closes the underlying file.
func (c *FileCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.w == nil {
		return nil
	}
	err := c.w.Close()
	c.w = nil
	return err
}

// Len reports cached verdicts (resume progress).
func (c *FileCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}
