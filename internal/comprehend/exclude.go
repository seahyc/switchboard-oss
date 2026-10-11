package comprehend

import (
	"os"
	"strings"
)

// TestMarker identifies operational probes that must not become commitments.
var TestMarker = "[olympus E2E test]"

// excludedChats reads installation-specific bot and test conversations.
func excludedChats() map[string]bool {
	chats := make(map[string]bool)
	for _, id := range strings.Fields(os.Getenv("SWITCHBOARD_EXCLUDED_CHAT_IDS")) {
		chats[id] = true
	}
	return chats
}

func excludedChat(chatID string) bool { return excludedChats()[chatID] }
