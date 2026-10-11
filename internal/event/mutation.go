package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// NormalizeMutation gives message changes their own stable event identity.
func NormalizeMutation(m Message) Message {
	if m.Kind != KindEdit && m.Kind != KindDelete && m.EditedTS == nil && !m.Deleted {
		return m
	}
	if m.TargetID != "" {
		return m
	}
	target := m.ID
	if (m.Kind == KindEdit || m.Kind == KindDelete) && m.ReplyTo != "" {
		target = m.ReplyTo
	}
	m.TargetID = target
	if m.Deleted || m.Kind == KindDelete {
		m.Kind = KindDelete
		m.Deleted = true
		m.ID = target + ":delete"
		return m
	}
	m.Kind = KindEdit
	version := m.TS
	if m.EditedTS != nil {
		version = *m.EditedTS
	} else {
		m.EditedTS = &version
	}
	content, _ := json.Marshal(struct {
		Text        string
		Attachments []Attachment
		UI          UI
	}{m.Text, m.Attachments, m.UI})
	hash := sha256.Sum256(content)
	m.ID = target + ":edit:" + version.UTC().Format(time.RFC3339Nano) + ":" + hex.EncodeToString(hash[:8])
	return m
}
