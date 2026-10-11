package comprehend

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// BackfillDue fills the due date on existing open commitments that have none, by
// deterministically re-parsing each commitment's SOURCE message text (anchored to
// its source ts). No inference; idempotent — only sets a due it can parse, never
// clears one. Returns how many were filled.
func BackfillDue(ctx context.Context, r *Reader, s *Store, loc *time.Location) (int, error) {
	if loc == nil {
		loc = SGT
	}
	missing, err := s.OpenCommitmentsMissingDue(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, c := range missing {
		text, ts, ok, err := r.MessageByID(ctx, c.SourceMsgID)
		if err != nil {
			return n, err
		}
		if !ok || text == "" {
			continue
		}
		anchor := ts
		if anchor.IsZero() {
			anchor = c.SourceTS
		}
		if due := parseDueFromText(text, anchor, loc); due != "" {
			if err := s.SetDue(ctx, c.ID, due); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// Deterministic due-date extraction. The small local model is unreliable at
// pulling a date into the `due` field even when the message states one
// explicitly, so when the model leaves `due` empty we parse the SOURCE TEXT
// ourselves — no inference, fully reproducible. Resolution is anchored to the
// message's own timestamp (so "by Friday" means the Friday after it was sent).
//
// Handles: ISO (2026-06-16), month-name ("by June 16", "16 Jun"), weekdays
// ("by Friday", "next Tuesday"), tomorrow/today/tonight/EOD, and "in N days".
// Returns "YYYY-MM-DD" or "" if no date is found.
func parseDueFromText(text string, anchor time.Time, loc *time.Location) string {
	if loc == nil {
		loc = SGT
	}
	if anchor.IsZero() {
		return ""
	}
	a := time.Date(anchor.In(loc).Year(), anchor.In(loc).Month(), anchor.In(loc).Day(), 0, 0, 0, 0, loc)
	low := strings.ToLower(text)

	// 1. Explicit ISO date wins.
	if m := isoDateRe.FindString(text); m != "" {
		if t, err := time.ParseInLocation("2006-01-02", m, loc); err == nil {
			return t.Format("2006-01-02")
		}
	}
	// 2. Month-name + day, either order.
	if d := parseMonthDay(low, a, loc); d != "" {
		return d
	}
	// 3. Relative day words.
	switch {
	case strings.Contains(low, "tomorrow"):
		return a.AddDate(0, 0, 1).Format("2006-01-02")
	case reEOD.MatchString(low), strings.Contains(low, "tonight"), wordPresent(low, "today"):
		return a.Format("2006-01-02")
	}
	// 4. "in N days".
	if m := reInNDays.FindStringSubmatch(low); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n >= 0 && n < 366 {
			return a.AddDate(0, 0, n).Format("2006-01-02")
		}
	}
	// 5. Weekday name, optionally "next".
	if d := parseWeekday(low, a); d != "" {
		return d
	}
	// 6. End of week → upcoming Friday.
	if reEOW.MatchString(low) {
		return nextWeekday(a, time.Friday, false).Format("2006-01-02")
	}
	return ""
}

var (
	isoDateRe = regexp.MustCompile(`\b(\d{4}-\d{2}-\d{2})\b`)
	reInNDays = regexp.MustCompile(`\bin (\d{1,3}) days?\b`)
	reEOD     = regexp.MustCompile(`\b(eod|end of day|by end of day)\b`)
	reEOW     = regexp.MustCompile(`\b(eow|end of week|by end of week)\b`)
)

var months = map[string]time.Month{
	"january": time.January, "jan": time.January,
	"february": time.February, "feb": time.February,
	"march": time.March, "mar": time.March,
	"april": time.April, "apr": time.April,
	"may":  time.May,
	"june": time.June, "jun": time.June,
	"july": time.July, "jul": time.July,
	"august": time.August, "aug": time.August,
	"september": time.September, "sep": time.September, "sept": time.September,
	"october": time.October, "oct": time.October,
	"november": time.November, "nov": time.November,
	"december": time.December, "dec": time.December,
}

// monthDay matches "june 16", "16 june", "jun 16th", "16th of june".
var reMonthDay = regexp.MustCompile(`\b([a-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?\b|\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?([a-z]{3,9})\b`)

func parseMonthDay(low string, a time.Time, loc *time.Location) string {
	for _, m := range reMonthDay.FindAllStringSubmatch(low, -1) {
		var mon time.Month
		var day int
		if m[1] != "" { // "<month> <day>"
			mo, ok := months[m[1]]
			if !ok {
				continue
			}
			d, err := strconv.Atoi(m[2])
			if err != nil {
				continue
			}
			mon, day = mo, d
		} else { // "<day> <month>"
			mo, ok := months[m[4]]
			if !ok {
				continue
			}
			d, err := strconv.Atoi(m[3])
			if err != nil {
				continue
			}
			mon, day = mo, d
		}
		if day < 1 || day > 31 {
			continue
		}
		year := a.Year()
		cand := time.Date(year, mon, day, 0, 0, 0, 0, loc)
		// A bare month/day in the past (>7d before the message) rolls to next year.
		if cand.Before(a.AddDate(0, 0, -7)) {
			cand = cand.AddDate(1, 0, 0)
		}
		return cand.Format("2006-01-02")
	}
	return ""
}

var weekdays = map[string]time.Weekday{
	"sunday": time.Sunday, "sun": time.Sunday,
	"monday": time.Monday, "mon": time.Monday,
	"tuesday": time.Tuesday, "tue": time.Tuesday, "tues": time.Tuesday,
	"wednesday": time.Wednesday, "wed": time.Wednesday,
	"thursday": time.Thursday, "thu": time.Thursday, "thurs": time.Thursday,
	"friday": time.Friday, "fri": time.Friday,
	"saturday": time.Saturday, "sat": time.Saturday,
}

var reWeekday = regexp.MustCompile(`\b(next\s+|this\s+)?(sunday|monday|tuesday|wednesday|thursday|friday|saturday)\b`)

func parseWeekday(low string, a time.Time) string {
	m := reWeekday.FindStringSubmatch(low)
	if m == nil {
		return ""
	}
	wd := weekdays[m[2]]
	next := strings.TrimSpace(m[1]) == "next"
	return nextWeekday(a, wd, next).Format("2006-01-02")
}

// nextWeekday returns the next date with weekday wd on/after the anchor. A plain
// or "this" weekday resolves to today when it matches; "next" weekday resolves to
// the next occurrence STRICTLY after the anchor (so "next Friday" on a Friday is a
// week out, while "next Tuesday" on a Friday is the upcoming Tuesday).
func nextWeekday(a time.Time, wd time.Weekday, forceNext bool) time.Time {
	delta := (int(wd) - int(a.Weekday()) + 7) % 7
	if forceNext && delta == 0 {
		delta = 7
	}
	return a.AddDate(0, 0, delta)
}

// wordPresent matches a whole word (so "today" doesn't match inside "todays").
func wordPresent(s, word string) bool {
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`)
	return re.MatchString(s)
}
