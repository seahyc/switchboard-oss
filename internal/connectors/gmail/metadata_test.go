package gmail

import (
	"reflect"
	"testing"
)

func TestBuildThreadMeta(t *testing.T) {
	labelNames := map[string]string{
		"INBOX":             "INBOX",
		"STARRED":           "STARRED",
		"IMPORTANT":         "IMPORTANT",
		"UNREAD":            "UNREAD",
		"CATEGORY_PERSONAL": "CATEGORY_PERSONAL",
		"Label_42":          "Work/Clients",
	}

	t.Run("inbox+starred+important+category+userlabel", func(t *testing.T) {
		labels := map[string]bool{
			"INBOX": true, "STARRED": true, "IMPORTANT": true, "UNREAD": true,
			"CATEGORY_PERSONAL": true, "Label_42": true,
		}
		m := buildThreadMeta("gmail:me@x.com:t1", "Lunch Friday?", labels, labelNames)
		if m.ChatID != "gmail:me@x.com:t1" || m.Title != "Lunch Friday?" {
			t.Errorf("id/title = %q/%q", m.ChatID, m.Title)
		}
		if !m.Pinned {
			t.Error("STARRED should set Pinned")
		}
		if m.Archived {
			t.Error("INBOX present => not archived")
		}
		if m.Importance != "high" {
			t.Errorf("IMPORTANT should set importance=high, got %q", m.Importance)
		}
		if m.Muted {
			t.Error("no MUTE => not muted")
		}
		// Signal labels excluded; user-facing labels mapped to display names, sorted.
		want := []string{"CATEGORY_PERSONAL", "Work/Clients"}
		if !reflect.DeepEqual(m.Labels, want) {
			t.Errorf("labels = %v, want %v", m.Labels, want)
		}
	})

	t.Run("archived+muted, no signals", func(t *testing.T) {
		labels := map[string]bool{"MUTE": true, "CATEGORY_UPDATES": true}
		m := buildThreadMeta("gmail:me@x.com:t2", "Receipt", labels, labelNames)
		if !m.Archived {
			t.Error("no INBOX => archived")
		}
		if !m.Muted {
			t.Error("MUTE => muted")
		}
		if m.Pinned || m.Importance != "" {
			t.Errorf("unexpected pinned=%v importance=%q", m.Pinned, m.Importance)
		}
		// Unknown label id falls back to the id itself.
		want := []string{"CATEGORY_UPDATES"}
		if !reflect.DeepEqual(m.Labels, want) {
			t.Errorf("labels = %v, want %v", m.Labels, want)
		}
	})
}
