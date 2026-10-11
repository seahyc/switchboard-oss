package telegram

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/seahyc/switchboard-oss/internal/event"
)

const (
	selfID  = int64(1000)
	aliceID = int64(2001)
	bobID   = int64(2002)
)

func testIndex() entityIndex {
	return entityIndex{
		selfID: selfID,
		users: map[int64]*tg.User{
			selfID:  {ID: selfID, Self: true, FirstName: "Me", Username: "me"},
			aliceID: {ID: aliceID, FirstName: "Alice", LastName: "Anderson", Username: "alice", Phone: "6591234567"},
			bobID:   {ID: bobID, FirstName: "Bob", Username: "bob"},
		},
		chats: map[int64]*tg.Chat{
			55: {ID: 55, Title: "Project Chat"},
		},
		channels: map[int64]*tg.Channel{
			77: {ID: 77, Title: "Announcements", Broadcast: true},
			88: {ID: 88, Title: "Dev Supergroup", Megagroup: true},
		},
	}
}

var ingestT = time.Date(2026, 6, 9, 8, 0, 1, 0, time.UTC)

func TestMapMessage_IncomingDM(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      456,
		PeerID:  &tg.PeerUser{UserID: aliceID}, // incoming DM: peer is Alice, no FromID, not Out
		Date:    int(time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC).Unix()),
		Message: "hey there",
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)

	if got.ID != "telegram:user:2001:456" {
		t.Errorf("ID = %q, want telegram:user:2001:456", got.ID)
	}
	if got.Platform != event.PlatformTelegram {
		t.Errorf("Platform = %q", got.Platform)
	}
	if got.AccountID != "telegram:1000" {
		t.Errorf("AccountID = %q, want telegram:1000", got.AccountID)
	}
	if got.ChatID != "telegram:user:2001" {
		t.Errorf("ChatID = %q", got.ChatID)
	}
	if got.ChatType != event.ChatDM {
		t.Errorf("ChatType = %q, want dm", got.ChatType)
	}
	if got.Kind != event.KindText {
		t.Errorf("Kind = %q, want text", got.Kind)
	}
	if got.Text != "hey there" {
		t.Errorf("Text = %q", got.Text)
	}
	if got.Sender.PlatformUserID != "2001" {
		t.Errorf("Sender.PlatformUserID = %q, want 2001", got.Sender.PlatformUserID)
	}
	if got.Sender.DisplayName != "Alice Anderson" {
		t.Errorf("Sender.DisplayName = %q", got.Sender.DisplayName)
	}
	if got.Sender.Handle != "alice" || got.Sender.Phone != "6591234567" {
		t.Errorf("alias signals wrong: %+v", got.Sender)
	}
	if got.Sender.IsSelf {
		t.Error("incoming DM should not be is_self")
	}
	if !got.TS.Equal(time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("TS = %v", got.TS)
	}
	if got.Source != event.SourceLive {
		t.Errorf("Source = %q", got.Source)
	}
}

func TestMapMessage_OutgoingDM_IsSelf(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      10,
		Out:     true,
		PeerID:  &tg.PeerUser{UserID: aliceID},
		Date:    int(time.Now().Unix()),
		Message: "sent by me",
	}
	got := mapMessage(m, ix, event.SourceBackfill, ingestT)
	if !got.Sender.IsSelf {
		t.Error("outgoing message should be is_self")
	}
	if got.Sender.PlatformUserID != "1000" {
		t.Errorf("Sender = %q, want self 1000", got.Sender.PlatformUserID)
	}
	if got.Source != event.SourceBackfill {
		t.Errorf("Source = %q, want backfill", got.Source)
	}
}

func TestMapMessage_GroupWithFromAndReply(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      300,
		PeerID:  &tg.PeerChat{ChatID: 55},
		FromID:  &tg.PeerUser{UserID: bobID},
		Date:    int(time.Now().Unix()),
		Message: "replying",
	}
	m.SetReplyTo(&tg.MessageReplyHeader{ReplyToMsgID: 299})
	got := mapMessage(m, ix, event.SourceLive, ingestT)

	if got.ChatID != "telegram:chat:55" {
		t.Errorf("ChatID = %q", got.ChatID)
	}
	if got.ChatType != event.ChatGroup {
		t.Errorf("ChatType = %q, want group", got.ChatType)
	}
	if got.Sender.PlatformUserID != "2002" {
		t.Errorf("Sender = %q, want Bob 2002", got.Sender.PlatformUserID)
	}
	if got.ReplyTo != "telegram:chat:55:299" {
		t.Errorf("ReplyTo = %q, want telegram:chat:55:299", got.ReplyTo)
	}
}

func TestChatType_BroadcastVsMegagroup(t *testing.T) {
	ix := testIndex()
	if got := ix.chatType(&tg.PeerChannel{ChannelID: 77}); got != event.ChatChannel {
		t.Errorf("broadcast chatType = %q, want channel", got)
	}
	if got := ix.chatType(&tg.PeerChannel{ChannelID: 88}); got != event.ChatGroup {
		t.Errorf("megagroup chatType = %q, want group", got)
	}
}

func TestMapMessage_MentionsSelf(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:        7,
		PeerID:    &tg.PeerChat{ChatID: 55},
		FromID:    &tg.PeerUser{UserID: aliceID},
		Mentioned: true, // user was @-mentioned
		Date:      int(time.Now().Unix()),
		Message:   "ping @me and @bob",
		Entities: []tg.MessageEntityClass{
			&tg.MessageEntityMentionName{Offset: 5, Length: 3, UserID: bobID},
		},
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if len(got.Mentions) != 2 {
		t.Fatalf("Mentions = %v, want bob + self", got.Mentions)
	}
	wantSelf, wantBob := false, false
	for _, id := range got.Mentions {
		if id == "1000" {
			wantSelf = true
		}
		if id == "2002" {
			wantBob = true
		}
	}
	if !wantSelf || !wantBob {
		t.Errorf("Mentions = %v, want both self(1000) and bob(2002)", got.Mentions)
	}
}

func TestMapMessage_Edited(t *testing.T) {
	ix := testIndex()
	editUnix := int(time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC).Unix())
	m := &tg.Message{
		ID:       1,
		PeerID:   &tg.PeerUser{UserID: aliceID},
		Date:     int(time.Now().Unix()),
		EditDate: editUnix,
		Message:  "edited text",
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if got.EditedTS == nil {
		t.Fatal("EditedTS should be set")
	}
	if !got.EditedTS.Equal(time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("EditedTS = %v", got.EditedTS)
	}
}

func TestMapMessage_PollKindAndUI(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:     2,
		PeerID: &tg.PeerChat{ChatID: 55},
		FromID: &tg.PeerUser{UserID: aliceID},
		Date:   int(time.Now().Unix()),
		Media: &tg.MessageMediaPoll{
			Poll: tg.Poll{
				ID:       9,
				Question: tg.TextWithEntities{Text: "Lunch?"},
				Answers: []tg.PollAnswerClass{
					&tg.PollAnswer{Text: tg.TextWithEntities{Text: "Yes"}, Option: []byte{0x01}},
					&tg.PollAnswer{Text: tg.TextWithEntities{Text: "No"}, Option: []byte{0x02}},
				},
			},
		},
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if got.Kind != event.KindPoll {
		t.Errorf("Kind = %q, want poll", got.Kind)
	}
	if got.UI.Poll == nil {
		t.Fatal("UI.Poll should be populated")
	}
	var p struct {
		Question string `json:"question"`
		Options  []struct {
			Text string `json:"text"`
		} `json:"options"`
	}
	if err := json.Unmarshal(got.UI.Poll, &p); err != nil {
		t.Fatalf("poll json: %v", err)
	}
	if p.Question != "Lunch?" || len(p.Options) != 2 || p.Options[0].Text != "Yes" {
		t.Errorf("poll = %+v", p)
	}
}

func TestMapMessage_InlineButtons(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      3,
		PeerID:  &tg.PeerUser{UserID: aliceID},
		Date:    int(time.Now().Unix()),
		Message: "confirm?",
		ReplyMarkup: &tg.ReplyInlineMarkup{
			Rows: []tg.KeyboardButtonRow{{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonCallback{Text: "Yes", Data: []byte("confirm_yes")},
					&tg.KeyboardButtonURL{Text: "Docs", URL: "https://x"},
				},
			}},
		},
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if len(got.UI.Buttons) != 2 {
		t.Fatalf("buttons = %d, want 2", len(got.UI.Buttons))
	}
	var b0 struct {
		Text string `json:"text"`
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(got.UI.Buttons[0], &b0); err != nil {
		t.Fatal(err)
	}
	if b0.Text != "Yes" || b0.Type != "callback" || b0.ID == "" {
		t.Errorf("button0 = %+v", b0)
	}
}

func TestMapMessage_Reactions(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:     4,
		PeerID: &tg.PeerUser{UserID: aliceID},
		Date:   int(time.Now().Unix()),
		Reactions: tg.MessageReactions{
			Results: []tg.ReactionCount{
				{Reaction: &tg.ReactionEmoji{Emoticon: "👍"}, Count: 3},
			},
		},
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if len(got.Reactions) != 1 || got.Reactions[0].Emoji != "👍" {
		t.Errorf("Reactions = %+v", got.Reactions)
	}
}

func TestMapMessage_ChannelBroadcastKindMediaAndType(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      5,
		PeerID:  &tg.PeerChannel{ChannelID: 77},
		Post:    true,
		Date:    int(time.Now().Unix()),
		Message: "photo post",
		Media: &tg.MessageMediaPhoto{
			Photo: &tg.Photo{ID: 4242},
		},
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	if got.ChatType != event.ChatChannel {
		t.Errorf("ChatType = %q, want channel", got.ChatType)
	}
	if got.Kind != event.KindMedia {
		t.Errorf("Kind = %q, want media", got.Kind)
	}
	// #48: the ref is now the rich downloadable handle (JSON), not the old thin
	// "telegram-photo:<id>". Decode it and check the location params.
	if len(got.Attachments) != 1 {
		t.Fatalf("Attachments = %+v", got.Attachments)
	}
	r, ok := decodeTGMediaRef(got.Attachments[0].Ref)
	if !ok || r.Kind != "photo" || r.ID != 4242 || r.Chat != "telegram:channel:77" || r.Msg != 5 {
		t.Errorf("media ref = %+v (ok=%v)", r, ok)
	}
}

// TestTGMediaRefRoundtrip locks the #48 download contract: a rich ref encodes →
// decodes → reconstructs the correct InputFileLocation for both photo and doc, and
// the legacy thin ref is rejected (so the downloader fails it cleanly, not silently).
func TestTGMediaRefRoundtrip(t *testing.T) {
	pr := tgMediaRef{Kind: "photo", ID: 10, AccessHash: 99, FileRef: []byte{1, 2, 3}, ThumbSize: "y", Chat: "telegram:user:5", Msg: 7}
	got, ok := decodeTGMediaRef(pr.encode())
	if !ok || got.ID != 10 || got.AccessHash != 99 || string(got.FileRef) != "\x01\x02\x03" || got.ThumbSize != "y" || got.Msg != 7 {
		t.Fatalf("photo roundtrip: %+v ok=%v", got, ok)
	}
	pl, ok := got.location().(*tg.InputPhotoFileLocation)
	if !ok || pl.ID != 10 || pl.AccessHash != 99 || pl.ThumbSize != "y" || string(pl.FileReference) != "\x01\x02\x03" {
		t.Fatalf("photo location: %#v ok=%v", got.location(), ok)
	}
	dr := tgMediaRef{Kind: "doc", ID: 20, AccessHash: 88, FileRef: []byte{4, 5}}
	gd, _ := decodeTGMediaRef(dr.encode())
	dl, ok := gd.location().(*tg.InputDocumentFileLocation)
	if !ok || dl.ID != 20 || dl.AccessHash != 88 {
		t.Fatalf("doc location: %#v ok=%v", gd.location(), ok)
	}
	if _, ok := decodeTGMediaRef("telegram-photo:4242"); ok {
		t.Fatal("legacy thin ref must NOT decode as a rich ref")
	}
}

// TestReextractMediaRef proves the #48 recovery parses the REAL gotd json.Marshal shape
// of a tg.Message (capitalized field names; Media is an interface that does NOT round-
// trip via a plain tg.Message unmarshal) back into a rich downloadable ref, with the
// chat id supplied externally and msg id recovered from the raw.
func TestReextractMediaRef(t *testing.T) {
	c := &Connector{}
	// Marshal a real tg.Message exactly as ingest stores it in Raw.
	msg := &tg.Message{
		ID:     77,
		Media:  &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 4242, AccessHash: 99, FileReference: []byte{1, 2, 3}, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", W: 800}, &tg.PhotoSize{Type: "m", W: 320}}}},
	}
	raw, _ := json.Marshal(msg)

	ref := c.ReextractMediaRef(raw, "telegram:user:5")
	got, ok := decodeTGMediaRef(ref)
	if !ok || got.Kind != "photo" || got.ID != 4242 || got.AccessHash != 99 || got.ThumbSize != "x" || got.Chat != "telegram:user:5" || got.Msg != 77 {
		t.Fatalf("reextracted ref = %+v (ok=%v)", got, ok)
	}
	if string(got.FileRef) != "\x01\x02\x03" {
		t.Fatalf("file_reference lost: %v", got.FileRef)
	}
	// A doc message.
	dmsg := &tg.Message{ID: 88, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 20, AccessHash: 7, FileReference: []byte{9}}}}
	draw, _ := json.Marshal(dmsg)
	dgot, ok := decodeTGMediaRef(c.ReextractMediaRef(draw, "telegram:chat:3"))
	if !ok || dgot.Kind != "doc" || dgot.ID != 20 || dgot.Msg != 88 {
		t.Fatalf("doc reextract = %+v ok=%v", dgot, ok)
	}
	// A non-media message → "".
	if c.ReextractMediaRef([]byte(`{"ID":1,"Message":"hi"}`), "telegram:user:5") != "" {
		t.Fatal("non-media message must yield empty ref")
	}
}

func TestMapDelete(t *testing.T) {
	peer := &tg.PeerChat{ChatID: 55}
	got := mapDelete(peer, []int{11, 12}, selfID, ingestT)
	if len(got) != 2 {
		t.Fatalf("got %d delete events", len(got))
	}
	if got[0].ID != "telegram:chat:55:11" || !got[0].Deleted || got[0].Kind != event.KindDelete {
		t.Errorf("delete[0] = %+v", got[0])
	}
	// #13 ts-bug: a delete event must carry the observation time, never a zero
	// (0001-01-01) timestamp that skews oldest_ts / ordering.
	if got[0].TS.IsZero() || !got[0].TS.Equal(ingestT.UTC()) {
		t.Errorf("delete TS must be the ingest time, got %v", got[0].TS)
	}
}

// TestMapMessage_RoundTripsJSON ensures the canonical event still marshals
// cleanly (raw payload included) after mapping.
func TestMapMessage_RoundTripsJSON(t *testing.T) {
	ix := testIndex()
	m := &tg.Message{
		ID:      9,
		PeerID:  &tg.PeerUser{UserID: aliceID},
		Date:    int(time.Now().Unix()),
		Message: "json check",
	}
	got := mapMessage(m, ix, event.SourceLive, ingestT)
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rt event.Message
	if err := json.Unmarshal(b, &rt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rt.ID != got.ID {
		t.Errorf("round-trip id mismatch")
	}
}
