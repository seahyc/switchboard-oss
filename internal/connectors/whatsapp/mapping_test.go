package whatsapp

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/seahyc/switchboard-oss/internal/event"
)

const (
	testAccount = "wa:15555550123"
	selfUser    = "15555550123"
	otherUser   = "15551234567"
)

var fixedTS = time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)

func dmJID(user string) types.JID  { return types.NewJID(user, types.DefaultUserServer) }
func groupJID(id string) types.JID { return types.NewJID(id, types.GroupServer) }

func baseEvent(chat, sender types.JID, isGroup, isFromMe bool, id, push string, msg *waE2E.Message) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   sender,
				IsFromMe: isFromMe,
				IsGroup:  isGroup,
			},
			ID:        id,
			PushName:  push,
			Timestamp: fixedTS,
		},
		Message: msg,
	}
}

func TestLIDPhoneFromRaw(t *testing.T) {
	// @lid-addressed sender whose phone is on SenderAlt — round-trips through the
	// stored Raw (json.Marshal of events.Message) and is recovered.
	ev := events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{
		Sender:    types.JID{User: "123456", Server: types.HiddenUserServer},
		SenderAlt: types.JID{User: "15555550125", Server: types.DefaultUserServer},
	}}}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if phone, ok := LIDPhoneFromRaw(raw); !ok || phone != "15555550125" {
		t.Fatalf("want phone 15555550125, got %q ok=%v", phone, ok)
	}
	// @lid with NO SenderAlt phone → nothing to recover.
	ev2 := events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{
		Sender: types.JID{User: "123456", Server: types.HiddenUserServer}}}}
	raw2, _ := json.Marshal(ev2)
	if _, ok := LIDPhoneFromRaw(raw2); ok {
		t.Fatal("no SenderAlt → must not recover a phone")
	}
	// Garbage / empty raw → safe false.
	if _, ok := LIDPhoneFromRaw(json.RawMessage(`not json`)); ok {
		t.Fatal("garbage raw → false")
	}
	if _, ok := LIDPhoneFromRaw(nil); ok {
		t.Fatal("nil raw → false")
	}
}

func TestMapMessage_DMText(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "ABC123", "Alice",
		&waE2E.Message{Conversation: proto.String("hello world")})

	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)

	if m.ID != "whatsapp:ABC123" {
		t.Errorf("ID = %q, want whatsapp:ABC123", m.ID)
	}
	if m.Platform != event.PlatformWhatsApp {
		t.Errorf("Platform = %q", m.Platform)
	}
	if m.AccountID != testAccount {
		t.Errorf("AccountID = %q", m.AccountID)
	}
	if m.ChatID != "whatsapp:"+chat.String() {
		t.Errorf("ChatID = %q", m.ChatID)
	}
	if m.ChatType != event.ChatDM {
		t.Errorf("ChatType = %q, want dm", m.ChatType)
	}
	if m.Kind != event.KindText {
		t.Errorf("Kind = %q, want text", m.Kind)
	}
	if m.Text != "hello world" {
		t.Errorf("Text = %q", m.Text)
	}
	if m.Source != event.SourceLive {
		t.Errorf("Source = %q", m.Source)
	}
	if m.Sender.Phone != otherUser {
		t.Errorf("Sender.Phone = %q, want %s", m.Sender.Phone, otherUser)
	}
	if m.Sender.DisplayName != "Alice" || m.Sender.Handle != "Alice" {
		t.Errorf("Sender name/handle = %q/%q", m.Sender.DisplayName, m.Sender.Handle)
	}
	if m.Sender.IsSelf {
		t.Error("Sender.IsSelf = true, want false")
	}
	if !m.TS.Equal(fixedTS) {
		t.Errorf("TS = %v", m.TS)
	}
	if len(m.Raw) == 0 {
		t.Error("Raw payload not preserved")
	}
	// Non-nil slices for stable JSON.
	if m.Mentions == nil || m.Attachments == nil || m.Reactions == nil {
		t.Error("expected non-nil empty slices")
	}
}

func TestMapMessage_SelfMessage(t *testing.T) {
	chat := dmJID(otherUser)
	self := dmJID(selfUser)
	evt := baseEvent(chat, self, false, true, "SELF1", "Me",
		&waE2E.Message{Conversation: proto.String("from me")})

	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if !m.Sender.IsSelf {
		t.Error("Sender.IsSelf = false, want true")
	}
	if m.Sender.PlatformUserID != self.ToNonAD().String() {
		t.Errorf("PlatformUserID = %q", m.Sender.PlatformUserID)
	}
}

func TestMapMessage_GroupMentionsAndReply(t *testing.T) {
	chat := groupJID("120363000000000000")
	sender := dmJID(otherUser)
	mentioned := dmJID(selfUser)
	evt := baseEvent(chat, sender, true, false, "GRP1", "Bob",
		&waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("hey @me look"),
				ContextInfo: &waE2E.ContextInfo{
					StanzaID:     proto.String("PARENT99"),
					MentionedJID: []string{mentioned.String()},
				},
			},
		})

	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.ChatType != event.ChatGroup {
		t.Errorf("ChatType = %q, want group", m.ChatType)
	}
	if m.Kind != event.KindText || m.Text != "hey @me look" {
		t.Errorf("Kind/Text = %q/%q", m.Kind, m.Text)
	}
	if m.ReplyTo != "whatsapp:PARENT99" {
		t.Errorf("ReplyTo = %q, want whatsapp:PARENT99", m.ReplyTo)
	}
	if len(m.Mentions) != 1 || m.Mentions[0] != mentioned.ToNonAD().String() {
		t.Errorf("Mentions = %v", m.Mentions)
	}
}

func TestMapMessage_ImageWithCaption(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "IMG1", "Alice",
		&waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:    proto.String("a photo"),
				Mimetype:   proto.String("image/jpeg"),
				DirectPath: proto.String("/v/t62/path"),
				URL:        proto.String("https://enc.example/x"),
				FileLength: proto.Uint64(2048),
			},
		})

	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindMedia {
		t.Errorf("Kind = %q, want media", m.Kind)
	}
	if m.Text != "a photo" {
		t.Errorf("Text(caption) = %q", m.Text)
	}
	if len(m.Attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(m.Attachments))
	}
	a := m.Attachments[0]
	if a.Type != "image" || a.MIME != "image/jpeg" || a.Size != 2048 {
		t.Errorf("attachment = %+v", a)
	}
	// Ref now serializes the full download tuple (path + media type).
	var r mediaRefV1
	if err := json.Unmarshal([]byte(a.Ref), &r); err != nil {
		t.Fatalf("ref not JSON: %q (%v)", a.Ref, err)
	}
	if r.Path != "/v/t62/path" || r.MT != "image" {
		t.Errorf("decoded ref = %+v", r)
	}
	if r.Chat != chat.String() || r.ID != "IMG1" || r.FromMe || r.IsGroup || r.Participant != "" {
		t.Errorf("decoded message key metadata = %+v", r)
	}
}

func TestRefFromRaw_RecoversTuple(t *testing.T) {
	// Simulate a stored raw payload (json.Marshal of the events.Message wrapper)
	// for a historical image whose attachment Ref was path-only.
	inner := &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{
			Mimetype:      proto.String("image/jpeg"),
			DirectPath:    proto.String("/v/t62/histpath"),
			URL:           proto.String("https://enc.example/h"),
			MediaKey:      []byte{1, 2, 3, 4},
			FileEncSHA256: []byte{5, 6, 7, 8},
			FileSHA256:    []byte{9, 10, 11, 12},
		},
	}
	raw, err := json.Marshal(struct {
		Info    types.MessageInfo
		Message *waE2E.Message
	}{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     groupJID("120363000000000002"),
				Sender:   dmJID(otherUser),
				IsGroup:  true,
				IsFromMe: false,
			},
			ID: "HIST1",
		},
		Message: inner,
	})
	if err != nil {
		t.Fatal(err)
	}

	ref := RefFromRaw(raw)
	if ref == "" {
		t.Fatal("RefFromRaw returned empty for an image message")
	}
	var r mediaRefV1
	if err := json.Unmarshal([]byte(ref), &r); err != nil {
		t.Fatalf("recovered ref not JSON: %q (%v)", ref, err)
	}
	if r.Path != "/v/t62/histpath" || r.MT != "image" {
		t.Errorf("recovered ref fields wrong: %+v", r)
	}
	if len(r.Key) != 4 || len(r.Enc) != 4 || len(r.SHA) != 4 {
		t.Errorf("recovered decrypt tuple incomplete: key=%v enc=%v sha=%v", r.Key, r.Enc, r.SHA)
	}
	if r.Chat != groupJID("120363000000000002").String() || r.ID != "HIST1" || !r.IsGroup || r.Participant != dmJID(otherUser).String() {
		t.Errorf("recovered message key metadata wrong: %+v", r)
	}
	// No media → empty.
	if RefFromRaw([]byte(`{"Message":{"conversation":"hi"}}`)) != "" {
		t.Error("text message should yield no media ref")
	}
}

func TestMediaRefMessageInfo(t *testing.T) {
	chat := groupJID("120363000000000003")
	sender := dmJID(otherUser)
	r := mediaRefV1{
		Chat:        chat.String(),
		ID:          "MSG123",
		IsGroup:     true,
		Participant: sender.String(),
	}

	info, err := r.messageInfo()
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != "MSG123" || info.Chat != chat || !info.IsGroup || info.Sender != sender {
		t.Errorf("messageInfo = %+v", info)
	}
}

func TestShouldTryMediaRetry(t *testing.T) {
	ready := mediaRefV1{
		Path: "/v/t62/stale",
		Key:  []byte{1, 2, 3},
		Chat: dmJID(otherUser).String(),
		ID:   "MSG123",
	}
	if !shouldTryMediaRetry(whatsmeow.ErrMediaDownloadFailedWith403, ready) {
		t.Fatal("403 with complete ref should try media retry")
	}
	if !shouldTryMediaRetry(whatsmeow.ErrMediaDownloadFailedWith404, ready) {
		t.Fatal("404 with complete ref should try media retry")
	}
	if shouldTryMediaRetry(errors.New("network"), ready) {
		t.Fatal("non-stale error should not try media retry")
	}
	ready.ID = ""
	if shouldTryMediaRetry(whatsmeow.ErrMediaDownloadFailedWith403, ready) {
		t.Fatal("missing message id should not try media retry")
	}
}

func TestMapMessage_Sticker(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "STK1", "Alice",
		&waE2E.Message{
			StickerMessage: &waE2E.StickerMessage{
				Mimetype:   proto.String("image/webp"),
				URL:        proto.String("https://enc.example/s"),
				FileLength: proto.Uint64(512),
			},
		})
	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindSticker {
		t.Errorf("Kind = %q, want sticker", m.Kind)
	}
	if len(m.Attachments) != 1 || m.Attachments[0].Type != "sticker" {
		t.Errorf("attachments = %+v", m.Attachments)
	}
	// Falls back to URL when no direct path (carried in the serialized ref).
	var r mediaRefV1
	if err := json.Unmarshal([]byte(m.Attachments[0].Ref), &r); err != nil {
		t.Fatalf("ref not JSON: %q (%v)", m.Attachments[0].Ref, err)
	}
	if r.URL != "https://enc.example/s" || r.MT != "sticker" {
		t.Errorf("decoded ref = %+v", r)
	}
}

func TestMapMessage_Reaction(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "RCT1", "Alice",
		&waE2E.Message{
			ReactionMessage: &waE2E.ReactionMessage{
				Key:  &waCommon.MessageKey{ID: proto.String("TARGET7")},
				Text: proto.String("👍"),
			},
		})
	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindReaction {
		t.Errorf("Kind = %q, want reaction", m.Kind)
	}
	if m.Text != "👍" {
		t.Errorf("Text = %q", m.Text)
	}
	if m.ReplyTo != "whatsapp:TARGET7" {
		t.Errorf("ReplyTo = %q, want whatsapp:TARGET7", m.ReplyTo)
	}
	if len(m.Reactions) != 1 || m.Reactions[0].Emoji != "👍" {
		t.Errorf("Reactions = %+v", m.Reactions)
	}
	if m.Reactions[0].By != chat.ToNonAD().String() {
		t.Errorf("Reactions[0].By = %q", m.Reactions[0].By)
	}
}

func TestMapMessage_RevokeIsDelete(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "DEL1", "Alice",
		&waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_REVOKE.Enum(),
				Key:  &waCommon.MessageKey{ID: proto.String("GONE5")},
			},
		})
	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindDelete {
		t.Errorf("Kind = %q, want delete", m.Kind)
	}
	if !m.Deleted {
		t.Error("Deleted = false, want true")
	}
	if m.ReplyTo != "whatsapp:GONE5" {
		t.Errorf("ReplyTo = %q", m.ReplyTo)
	}
}

func TestMapMessage_Edit(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "EDT1", "Alice",
		&waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				Key:  &waCommon.MessageKey{ID: proto.String("ORIG3")},
				EditedMessage: &waE2E.Message{
					Conversation: proto.String("edited text"),
				},
			},
		})
	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindEdit {
		t.Errorf("Kind = %q, want edit", m.Kind)
	}
	if m.Text != "edited text" {
		t.Errorf("Text = %q", m.Text)
	}
	if m.EditedTS == nil || !m.EditedTS.Equal(fixedTS) {
		t.Errorf("EditedTS = %v", m.EditedTS)
	}
	if m.ReplyTo != "whatsapp:ORIG3" {
		t.Errorf("ReplyTo = %q", m.ReplyTo)
	}
}

func TestMapMessage_BackfillSource(t *testing.T) {
	chat := dmJID(otherUser)
	evt := baseEvent(chat, chat, false, false, "BF1", "Alice",
		&waE2E.Message{Conversation: proto.String("old message")})
	m := mapMessage(evt, testAccount, event.SourceBackfill, fixedTS)
	if m.Source != event.SourceBackfill {
		t.Errorf("Source = %q, want backfill", m.Source)
	}
}

func TestMapMessage_Poll(t *testing.T) {
	chat := groupJID("120363000000000001")
	sender := dmJID(otherUser)
	evt := baseEvent(chat, sender, true, false, "POLL1", "Bob",
		&waE2E.Message{
			PollCreationMessage: &waE2E.PollCreationMessage{
				Name: proto.String("Lunch?"),
				Options: []*waE2E.PollCreationMessage_Option{
					{OptionName: proto.String("Pizza")},
					{OptionName: proto.String("Sushi")},
				},
			},
		})
	m := mapMessage(evt, testAccount, event.SourceLive, fixedTS)
	if m.Kind != event.KindPoll {
		t.Errorf("Kind = %q, want poll", m.Kind)
	}
	if m.Text != "Lunch?" {
		t.Errorf("Text = %q", m.Text)
	}
	if len(m.UI.Poll) == 0 {
		t.Error("UI.Poll not populated")
	}
}

func TestLabelRegistry(t *testing.T) {
	c := &Connector{
		labelNames: map[string]string{},
		chatLabels: map[string]map[string]bool{},
	}
	chat := dmJID(otherUser)
	chatID := chatIDForJID(chat)

	// Define a label name, then apply it to a chat.
	c.handleLabelEdit(&events.LabelEdit{
		LabelID: "1",
		Action:  &waSyncAction.LabelEditAction{Name: proto.String("VIP")},
	})
	c.handleLabelAssociation(&events.LabelAssociationChat{
		JID:     chat,
		LabelID: "1",
		Action:  &waSyncAction.LabelAssociationAction{Labeled: proto.Bool(true)},
	})
	if got := c.labelsForChat(chatID); len(got) != 1 || got[0] != "VIP" {
		t.Fatalf("labelsForChat = %v, want [VIP]", got)
	}

	// Unlabel removes it.
	c.handleLabelAssociation(&events.LabelAssociationChat{
		JID:     chat,
		LabelID: "1",
		Action:  &waSyncAction.LabelAssociationAction{Labeled: proto.Bool(false)},
	})
	if got := c.labelsForChat(chatID); got != nil {
		t.Fatalf("after unlabel labelsForChat = %v, want nil", got)
	}

	// Unknown label id falls back to the raw id.
	c.handleLabelAssociation(&events.LabelAssociationChat{
		JID:     chat,
		LabelID: "99",
		Action:  &waSyncAction.LabelAssociationAction{Labeled: proto.Bool(true)},
	})
	if got := c.labelsForChat(chatID); len(got) != 1 || got[0] != "99" {
		t.Fatalf("unknown label labelsForChat = %v, want [99]", got)
	}

	// Deleting a label drops its name (future lookups fall back to id).
	c.handleLabelEdit(&events.LabelEdit{
		LabelID: "1",
		Action:  &waSyncAction.LabelEditAction{Deleted: proto.Bool(true)},
	})
	if _, ok := c.labelNames["1"]; ok {
		t.Error("label 1 name not deleted")
	}
}

func TestContactTitle(t *testing.T) {
	cases := []struct {
		name string
		in   types.ContactInfo
		want string
	}{
		{"full name wins", types.ContactInfo{FullName: "Alice A", PushName: "ally", BusinessName: "Biz"}, "Alice A"},
		{"push name fallback", types.ContactInfo{PushName: "ally", BusinessName: "Biz"}, "ally"},
		{"business name fallback", types.ContactInfo{BusinessName: "Biz"}, "Biz"},
		{"empty", types.ContactInfo{}, ""},
	}
	for _, tc := range cases {
		if got := contactTitle(tc.in); got != tc.want {
			t.Errorf("%s: contactTitle = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestParseChatIDAndStrip(t *testing.T) {
	jid, err := parseChatID("whatsapp:" + dmJID(otherUser).String())
	if err != nil {
		t.Fatalf("parseChatID error: %v", err)
	}
	if jid.User != otherUser {
		t.Errorf("jid.User = %q", jid.User)
	}
	if got := stripPlatformPrefix("whatsapp:XYZ"); got != "XYZ" {
		t.Errorf("stripPlatformPrefix = %q", got)
	}
	if _, err := parseChatID(""); err == nil {
		t.Error("expected error on empty chat id")
	}
}
