package meta

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

var (
	testSelf     = SelfSignals{MXID: "@sb:meta.local", Handle: "me_ig", Phone: "+6500000000", Email: "me@example.com"}
	testPrefixes = []string{"meta_", "instagram_", "messenger_"}
	testIngest   = time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
)

func mustMap(t *testing.T, ev MatrixEvent, network, source string) event.Message {
	t.Helper()
	m, ok := mapEvent(ev, network, testSelf, testPrefixes, source, testIngest)
	if !ok {
		t.Fatalf("mapEvent returned ok=false for %+v", ev)
	}
	return m
}

func TestMapText_Incoming(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!room1:meta.local", EventID: "$evt1", Sender: "@meta_17841400:meta.local",
		Type: evTypeMessage, TSMillis: 1717900000000, MsgType: msgText, Body: "hey there",
		RemoteMsgID: "mid.ABC123", RemoteThreadID: "340282366000",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)

	if m.Platform != "instagram" {
		t.Errorf("platform = %q, want instagram", m.Platform)
	}
	if m.ID != "instagram:mid.ABC123" {
		t.Errorf("id = %q, want instagram:mid.ABC123", m.ID)
	}
	if m.ChatID != "instagram:340282366000" {
		t.Errorf("chat_id = %q", m.ChatID)
	}
	if m.ChatType != event.ChatDM {
		t.Errorf("chat_type = %q, want dm", m.ChatType)
	}
	if m.Kind != event.KindText || m.Text != "hey there" {
		t.Errorf("kind/text = %q/%q", m.Kind, m.Text)
	}
	if m.Sender.PlatformUserID != "17841400" {
		t.Errorf("sender id = %q, want 17841400 (parsed from ghost mxid)", m.Sender.PlatformUserID)
	}
	if m.Sender.IsSelf {
		t.Error("incoming message should not be is_self")
	}
	if m.Source != event.SourceLive || !m.IngestedAt.Equal(testIngest) {
		t.Errorf("source/ingested = %q/%v", m.Source, m.IngestedAt)
	}
	if !m.TS.Equal(time.UnixMilli(1717900000000).UTC()) {
		t.Errorf("ts = %v", m.TS)
	}
}

func TestMapText_SelfEnriched(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!room1:meta.local", EventID: "$evt2", Sender: testSelf.MXID,
		Type: evTypeMessage, TSMillis: 1717900001000, MsgType: msgText, Body: "my reply",
		RemoteMsgID: "mid.SELF", RemoteThreadID: "340282366000",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if !m.Sender.IsSelf {
		t.Fatal("expected is_self")
	}
	if m.Sender.Handle != "me_ig" || m.Sender.Phone != "+6500000000" || m.Sender.Email != "me@example.com" {
		t.Errorf("self alias signals not enriched: %+v", m.Sender)
	}
	if m.Sender.PlatformUserID != testSelf.MXID {
		t.Errorf("self platform_user_id = %q, want %q", m.Sender.PlatformUserID, testSelf.MXID)
	}
}

func TestMapMedia_VoiceImageVideo(t *testing.T) {
	cases := []struct {
		name     string
		msgType  string
		voice    bool
		mime     string
		wantType string
	}{
		{"image", msgImage, false, "image/jpeg", "image"},
		{"video", msgVideo, false, "video/mp4", "video"},
		{"voicenote", msgAudio, true, "audio/ogg", "voice"},
		{"audio", msgAudio, false, "audio/mpeg", "audio"},
		{"file", msgFile, false, "application/pdf", "application"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := MatrixEvent{
				RoomID: "!r:meta.local", EventID: "$m", Sender: "@meta_99:meta.local",
				Type: evTypeMessage, TSMillis: 1717900002000, MsgType: c.msgType,
				URL: "mxc://meta.local/abc123", MIME: c.mime, Size: 4096, FileName: "f", IsVoice: c.voice,
				RemoteMsgID: "mid.MED", RemoteThreadID: "T",
			}
			m := mustMap(t, ev, PlatformMessenger, event.SourceLive)
			if m.Kind != event.KindMedia {
				t.Errorf("kind = %q, want media", m.Kind)
			}
			if len(m.Attachments) != 1 {
				t.Fatalf("attachments = %d, want 1", len(m.Attachments))
			}
			a := m.Attachments[0]
			if a.Type != c.wantType {
				t.Errorf("att type = %q, want %q", a.Type, c.wantType)
			}
			if a.Ref != "mxc://meta.local/abc123" {
				t.Errorf("att ref = %q, want the mxc uri (media worker fetches it)", a.Ref)
			}
			if a.MIME != c.mime || a.Size != 4096 {
				t.Errorf("att mime/size = %q/%d", a.MIME, a.Size)
			}
		})
	}
}

func TestMapReaction(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$rx", Sender: "@meta_77:meta.local",
		Type: evTypeReaction, TSMillis: 1717900003000,
		ReactionKey: "❤️", ReactionTarget: "$origEvt", RemoteMsgID: "",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if m.Kind != event.KindReaction {
		t.Fatalf("kind = %q, want reaction", m.Kind)
	}
	if len(m.Reactions) != 1 || m.Reactions[0].Emoji != "❤️" || m.Reactions[0].By != "77" {
		t.Errorf("reactions = %+v", m.Reactions)
	}
	if m.ReplyTo != "instagram:evt:$origEvt" {
		t.Errorf("reply_to(target) = %q, want instagram:evt:$origEvt fallback", m.ReplyTo)
	}
}

func TestMapRedactionDelete(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$rd", Sender: "@meta_77:meta.local",
		Type: evTypeRedaction, TSMillis: 1717900004000, Redacts: "$gone",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if m.Kind != event.KindDelete || !m.Deleted {
		t.Errorf("kind/deleted = %q/%v, want delete/true", m.Kind, m.Deleted)
	}
	if m.ReplyTo != "instagram:evt:$gone" {
		t.Errorf("redaction target = %q", m.ReplyTo)
	}
}

func TestMapEdit(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$ed", Sender: "@meta_77:meta.local",
		Type: evTypeMessage, TSMillis: 1717900005000, MsgType: msgText, Body: "fixed text",
		IsEdit: true, EditTarget: "$orig", RemoteMsgID: "mid.EDIT",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if m.Kind != event.KindEdit {
		t.Fatalf("kind = %q, want edit", m.Kind)
	}
	if m.EditedTS == nil {
		t.Error("edited_ts should be set")
	}
	if m.Text != "fixed text" {
		t.Errorf("text = %q", m.Text)
	}
	if m.ReplyTo != "instagram:evt:$orig" {
		t.Errorf("edit target = %q", m.ReplyTo)
	}
}

func TestMapReply(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$rp", Sender: "@meta_77:meta.local",
		Type: evTypeMessage, TSMillis: 1717900006000, MsgType: msgText,
		Body:         "> <@meta_5:meta.local> original\n\nthis is my answer",
		ReplyToEvent: "$parent",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if m.ReplyTo != "instagram:evt:$parent" {
		t.Errorf("reply_to = %q", m.ReplyTo)
	}
	if m.Text != "this is my answer" {
		t.Errorf("text = %q, want fallback-quote stripped", m.Text)
	}
}

func TestMapRichShare_ToCard(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$sh", Sender: "@meta_77:meta.local",
		Type: evTypeMessage, TSMillis: 1717900007000, MsgType: msgText,
		Body: "check this reel", ExternalURL: "https://instagram.com/reel/XYZ",
		RemoteMsgID: "mid.SHARE", RemoteThreadID: "T",
	}
	m := mustMap(t, ev, PlatformInstagram, event.SourceLive)
	if len(m.UI.Card) == 0 {
		t.Fatal("expected ui.card for rich share")
	}
	var card map[string]string
	if err := json.Unmarshal(m.UI.Card, &card); err != nil {
		t.Fatalf("card not valid JSON: %v", err)
	}
	if card["url"] != "https://instagram.com/reel/XYZ" {
		t.Errorf("card url = %q", card["url"])
	}
}

func TestChatID_RoomFallback(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!noremote:meta.local", EventID: "$x", Sender: "@meta_1:meta.local",
		Type: evTypeMessage, TSMillis: 1, MsgType: msgText, Body: "hi",
		// no RemoteThreadID, no RemoteMsgID → both fall back to Matrix ids
	}
	m := mustMap(t, ev, PlatformMessenger, event.SourceBackfill)
	if m.ChatID != "messenger:room:!noremote:meta.local" {
		t.Errorf("chat_id = %q, want room fallback", m.ChatID)
	}
	if m.ID != "messenger:evt:$x" {
		t.Errorf("id = %q, want evt fallback", m.ID)
	}
	if m.Source != event.SourceBackfill {
		t.Errorf("source = %q, want backfill", m.Source)
	}
}

func TestIdempotency_RemoteIDStableAcrossSources(t *testing.T) {
	base := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$live", Sender: "@meta_1:meta.local",
		Type: evTypeMessage, TSMillis: 10, MsgType: msgText, Body: "dup",
		RemoteMsgID: "mid.SAME", RemoteThreadID: "T",
	}
	live := mustMap(t, base, PlatformInstagram, event.SourceLive)
	// Same remote message arriving via backfill has a DIFFERENT Matrix event id
	// but the SAME remote id → same canonical id → pipeline dedupes.
	base.EventID = "$backfill"
	bf := mustMap(t, base, PlatformInstagram, event.SourceBackfill)
	if live.ID != bf.ID {
		t.Errorf("idempotency broken: live %q != backfill %q", live.ID, bf.ID)
	}
}

func TestMapGroup(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!g:meta.local", EventID: "$g", Sender: "@meta_1:meta.local",
		Type: evTypeMessage, TSMillis: 1, MsgType: msgText, Body: "yo", IsGroup: true,
		RemoteThreadID: "GROUP1", RemoteMsgID: "mid.G",
	}
	m := mustMap(t, ev, PlatformMessenger, event.SourceLive)
	if m.ChatType != event.ChatGroup {
		t.Errorf("chat_type = %q, want group", m.ChatType)
	}
}

func TestMapMember_NotAMessage(t *testing.T) {
	ev := MatrixEvent{
		RoomID: "!r:meta.local", EventID: "$mem", Sender: "@meta_1:meta.local",
		Type: evTypeMember, TSMillis: 1, DisplayName: "Jane",
	}
	if _, ok := mapEvent(ev, PlatformInstagram, testSelf, testPrefixes, event.SourceLive, testIngest); ok {
		t.Error("m.room.member should not map to a firehose message")
	}
}

func TestRemoteUserIDFromMXID(t *testing.T) {
	cases := map[string]string{
		"@meta_17841400:meta.local":       "17841400",
		"@instagram_55:meta.local":        "55",
		"@messenger_99:meta.local":        "99",
		"@sb:meta.local":                  "",               // not a ghost
		"@meta_abc:host:weird:meta.local": "abc:host:weird", // only first colon ends localpart? no — localpart stops at first ':'
	}
	// Note: localpart stops at the FIRST ':' so the last case's localpart is "meta_abc".
	cases["@meta_abc:host:weird:meta.local"] = "abc"
	for mxid, want := range cases {
		if got := remoteUserIDFromMXID(mxid, testPrefixes); got != want {
			t.Errorf("remoteUserIDFromMXID(%q) = %q, want %q", mxid, got, want)
		}
	}
}

func TestCleanBody(t *testing.T) {
	cases := map[string]string{
		"plain text":                    "plain text",
		"> <@u:s> quoted\n\nreal reply": "real reply",
		"> line1\n> line2\n\nbody":      "body",
		"> only quote\n":                "",
	}
	for in, want := range cases {
		if got := cleanBody(in); got != want {
			t.Errorf("cleanBody(%q) = %q, want %q", in, got, want)
		}
	}
}
