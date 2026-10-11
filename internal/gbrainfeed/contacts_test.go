package gbrainfeed

import (
	"context"
	"strings"
	"testing"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func TestIngestContactsProjectsAddressBook(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	ctx := context.Background()
	p.Start(ctx)
	// The user's mom is in the WhatsApp address book but never messaged — she must
	// still get a NAMED person page (coverage), resolved from FullName not display.
	p.IngestContacts(event.PlatformWhatsApp, []Contact{
		{UserID: "6590000001@s.whatsapp.net", FullName: "Mom", Phone: "+6590000001"},
		{UserID: "6590000002@s.whatsapp.net", PushName: "Acme", BusinessName: "Acme Pte Ltd", Phone: "+6590000002"},
		{UserID: "", FullName: "skip-blank-id"},
	})
	p.Close()

	// Mom resolves to a real name from the address book.
	momIdent := f.pages[PersonSlug(event.PlatformWhatsApp, "6590000001@s.whatsapp.net")]
	if !strings.Contains(momIdent, "# Mom") || !strings.Contains(momIdent, "name_source: address_book") {
		t.Fatalf("mom identity page not named from address book:\n%s", momIdent)
	}
	if !strings.Contains(momIdent, "from_contact: true") {
		t.Fatalf("contact-sourced page must be flagged from_contact:\n%s", momIdent)
	}
	// Business name resolves for the business contact.
	biz := f.pages["canon-phone-6590000002"]
	if !strings.Contains(biz, "Acme Pte Ltd") {
		t.Fatalf("business canonical missing business name:\n%s", biz)
	}
	// Blank-id contact skipped: only 2 contacts → 2 identity + 2 canonical pages.
	if f.pageCount() != 4 {
		t.Fatalf("want 4 pages (2 contacts × 2 tiers), got %d: %v", f.pageCount(), keysOf(f.pages))
	}
}

func TestContactAndMessageCoalesceToOnePerson(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	ctx := context.Background()
	p.Start(ctx)
	// A contact (address book name) and a message from the same user (blank
	// display) must collapse to one canonical person carrying the contact name.
	p.IngestContacts(event.PlatformWhatsApp, []Contact{
		{UserID: "u@s.whatsapp.net", FullName: "Grandpa", Phone: "+6591110000"},
	})
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "u@s.whatsapp.net", "", "+6591110000", ""))
	p.Close()

	canon := f.pages["canon-phone-6591110000"]
	if !strings.Contains(canon, "Grandpa") {
		t.Fatalf("canonical lost the address-book name after a blank message:\n%s", canon)
	}
}
