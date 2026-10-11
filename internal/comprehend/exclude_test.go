package comprehend

import "testing"

func TestExcludedChatsUseInstallationConfiguration(t *testing.T) {
	t.Setenv("SWITCHBOARD_EXCLUDED_CHAT_IDS", `
 telegram:user:100000002
 telegram:user:100000003
 `)
	if !excludedChat("telegram:user:100000002") || !excludedChat("telegram:user:100000003") {
		t.Fatal("configured bot and test chats were not excluded")
	}
	if excludedChat("telegram:user:100000001") {
		t.Fatal("unconfigured saved messages were excluded")
	}
	t.Setenv("SWITCHBOARD_EXCLUDED_CHAT_IDS", "")
	if excludedChat("telegram:user:100000002") {
		t.Fatal("chat remained excluded after configuration was removed")
	}
}
