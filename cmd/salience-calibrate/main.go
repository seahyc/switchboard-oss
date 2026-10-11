// Command salience-calibrate renders the people-salience calibration report from
// a cached raw-signal vector + tunable weights/thresholds — the interactive
// re-fit loop. It is PURE and offline: it does NOT touch the live daemon or
// gbrain, so re-running after editing the config is instant.
//
// Usage:
//
//	salience-calibrate -signals people_signals.json [-config weights.json] [-out report.md]
//
// The signals file is a JSON array of gbrainfeed.PersonSignals (dumped once from
// live data). The config file (optional) is {"weights": {...}, "thresholds": {...}}
// overriding the defaults — edit it and re-run to move cut-lines/weights and see
// the ranking shift in sub-second time.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
)

type configFile struct {
	Weights    *gbrainfeed.SalienceWeights    `json:"weights"`
	Thresholds *gbrainfeed.SalienceThresholds `json:"thresholds"`
}

func main() {
	signalsPath := flag.String("signals", "", "path to JSON array of PersonSignals (required)")
	configPath := flag.String("config", "", "path to {weights,thresholds} override JSON (optional)")
	outPath := flag.String("out", "", "write report here (default stdout)")
	nowStr := flag.String("now", "", "RFC3339 'now' for last-seen rendering (default: collection is frozen in signals)")
	flag.Parse()

	if *signalsPath == "" {
		fmt.Fprintln(os.Stderr, "error: -signals is required")
		flag.Usage()
		os.Exit(2)
	}

	var people []gbrainfeed.PersonSignals
	if err := readJSON(*signalsPath, &people); err != nil {
		die("read signals: %v", err)
	}

	weights := gbrainfeed.DefaultSalienceWeights
	thresholds := gbrainfeed.DefaultSalienceThresholds
	if *configPath != "" {
		var cf configFile
		if err := readJSON(*configPath, &cf); err != nil {
			die("read config: %v", err)
		}
		if cf.Weights != nil {
			weights = *cf.Weights
		}
		if cf.Thresholds != nil {
			thresholds = *cf.Thresholds
		}
	}

	now := time.Time{}
	if *nowStr != "" {
		t, err := time.Parse(time.RFC3339, *nowStr)
		if err != nil {
			die("parse -now: %v", err)
		}
		now = t
	}

	report := gbrainfeed.CalibrationReport(people, weights, thresholds, now)

	if *outPath == "" || *outPath == "-" {
		fmt.Print(report)
		return
	}
	if err := os.WriteFile(*outPath, []byte(report), 0o644); err != nil {
		die("write report: %v", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d people)\n", *outPath, len(people))
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
