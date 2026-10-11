// Command match-eval is the P1 dry-run + eval harness for the LLM identity matcher
// (#19/F2). It runs the PURE pipeline — candidate generation → adjudication → tier
// routing → eval scorecard — over a JSON people set + labeled pairs, with NO model
// and NO live writes. It uses the deterministic HeuristicAdjudicator baseline by
// default (the real local-LLM adjudicator is wired in P2); pass -canned to score a
// fixed verdict set instead.
//
// Usage:
//
//	match-eval -people people.json -labeled labeled.json [-bar 0.99]
//
// Outputs: the candidate-gen recall list + the eval gate scorecard (markdown).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

func main() {
	peoplePath := flag.String("people", "", "JSON array of idmatch.PersonRecord (required)")
	labeledPath := flag.String("labeled", "", "JSON array of idmatch.LabeledPair (required)")
	bar := flag.Float64("bar", 0.99, "auto-merge precision gate")
	flag.Parse()
	if *peoplePath == "" || *labeledPath == "" {
		fmt.Fprintln(os.Stderr, "error: -people and -labeled are required")
		flag.Usage()
		os.Exit(2)
	}

	var people []idmatch.PersonRecord
	must(readJSON(*peoplePath, &people), "read people")
	var labeled []idmatch.LabeledPair
	must(readJSON(*labeledPath, &labeled), "read labeled")

	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)

	// Candidate-gen recall view.
	cands := idmatch.SortedCandidates(gen, people)
	fmt.Printf("# Candidate generation\n\n%d candidate pairs from %d people (bounded, blocking-based):\n\n", len(cands), len(people))
	fmt.Println("| a | b | signals | pre-score |")
	fmt.Println("|---|---|---------|----------:|")
	for _, c := range cands {
		fmt.Printf("| %s | %s | %v | %.2f |\n", c.A, c.B, c.Signals, c.PreScore)
	}
	fmt.Println()

	// Eval gate using the model-free heuristic baseline.
	rep, err := idmatch.Evaluate(context.Background(), people, labeled, gen, idmatch.HeuristicAdjudicator{}, idmatch.DefaultThresholds)
	must(err, "evaluate")
	fmt.Println(rep.Markdown(*bar))
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func must(err error, ctx string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s: %v\n", ctx, err)
		os.Exit(1)
	}
}
