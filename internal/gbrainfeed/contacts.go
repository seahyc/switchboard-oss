package gbrainfeed

import (
	"context"
	"strings"
)

// Contact is one entry from a platform's address book / contact store — the
// authoritative name source. Unlike a message sender (whose display name is
// often blank), a Contact carries the user-assigned name a connector pulls from
// its contact store (whatsmeow FullName/FirstName/PushName/BusinessName,
// Telegram first/last/username, Lark directory, Gmail "From" names).
//
// Projecting the full contact list — not just observed senders — is what gives
// the people graph COVERAGE: everyone the user actually knows gets a person page
// even if their message history never backfilled.
type Contact struct {
	UserID       string
	Phone        string
	Email        string
	Handle       string // @username
	FullName     string // address-book name (user-assigned)
	FirstName    string
	LastName     string
	PushName     string // self-set profile name
	BusinessName string
	IsSelf       bool
}

// names folds a Contact's fields into the priority-ordered NameSet.
func (c Contact) names() NameSet {
	return NameSet{
		AddressBook: c.FullName,
		First:       c.FirstName,
		Last:        c.LastName,
		Business:    c.BusinessName,
		Push:        c.PushName,
		Username:    c.Handle,
	}
}

// toIdentity converts a contact to the projector's internal identity. fromContact
// marks it address-book-sourced so coverage stats and flagging can distinguish a
// known contact from a bare message sender.
func (c Contact) toIdentity(platform string) identity {
	return identity{
		Platform:    platform,
		UserID:      c.UserID,
		Names:       c.names(),
		Handle:      strings.TrimSpace(c.Handle),
		Phone:       normalizePhone(c.Phone),
		Email:       strings.ToLower(strings.TrimSpace(c.Email)),
		IsSelf:      c.IsSelf,
		FromContact: true,
	}
}

// ContactsProvider is the optional connector capability that exposes a platform's
// address book to the projector. It mirrors ChatMetadataProvider: connectors that
// can enumerate contacts implement it, the daemon polls it on an interval and
// feeds the result to Projector.IngestContacts. Connectors that don't implement
// it simply contribute no coverage rows (the projector still sees their senders).
type ContactsProvider interface {
	Contacts(ctx context.Context) ([]Contact, error)
}

// IngestContacts projects an entire platform contact list. Each contact is
// enqueued on the same coalescing worker as message senders, so a contact that
// also messages collapses to one identity (last write wins on the richer name).
// Best-effort + non-blocking, exactly like OnIngest.
func (p *Projector) IngestContacts(platform string, contacts []Contact) {
	for _, c := range contacts {
		if strings.TrimSpace(c.UserID) == "" {
			continue
		}
		if !p.enqueue(c.toIdentity(platform)) {
			return
		}
	}
	p.signal()
}
