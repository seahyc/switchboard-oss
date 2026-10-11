package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// botClient is a dependency-free Telegram Bot API client used ONLY for the
// outbound rich-UI path: a USER account (the userbot) cannot send inline
// keyboards, so rich sends route through @example_bot instead. Normal sends go
// via the userbot. This keeps both identities behind the connector's one Send.
//
// We deliberately avoid a heavy Bot API SDK — a couple of JSON POSTs is enough,
// and it keeps switchboard decoupled from Hermes' bot stack.
type botClient struct {
	token string
	http  *http.Client
	base  string // overridable for tests
}

func newBotClient(token string) *botClient {
	return &botClient{
		token: token,
		http:  &http.Client{Timeout: 30 * time.Second},
		base:  "https://api.telegram.org",
	}
}

type botResponse struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
	Result      json.RawMessage `json:"result"`
}

// inlineKeyboard is the Bot API reply_markup shape for inline buttons.
type inlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
	URL          string `json:"url,omitempty"`
}

type inlineKeyboardMarkup struct {
	InlineKeyboard [][]inlineButton `json:"inline_keyboard"`
}

// sendMessage posts a message, optionally with an inline keyboard. Returns the
// numeric message id assigned by Telegram.
func (b *botClient) sendMessage(ctx context.Context, chatID int64, text string, replyTo int, markup *inlineKeyboardMarkup) (int, error) {
	payload := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	if replyTo != 0 {
		payload["reply_to_message_id"] = replyTo
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	var result struct {
		MessageID int `json:"message_id"`
	}
	if err := b.call(ctx, "sendMessage", payload, &result); err != nil {
		return 0, err
	}
	return result.MessageID, nil
}

func (b *botClient) call(ctx context.Context, method string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	url := b.base + "/bot" + b.token + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.http.Do(req)
	if err != nil {
		return fmt.Errorf("telegram bot %s: %w", method, err)
	}
	defer resp.Body.Close()

	var br botResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		return fmt.Errorf("telegram bot %s: decode: %w", method, err)
	}
	if !br.OK {
		return fmt.Errorf("telegram bot %s: api error %d: %s", method, br.ErrorCode, br.Description)
	}
	if out != nil && len(br.Result) > 0 {
		if err := json.Unmarshal(br.Result, out); err != nil {
			return fmt.Errorf("telegram bot %s: unmarshal result: %w", method, err)
		}
	}
	return nil
}

// buttonsToMarkup converts the connector's neutral SendRequest.Buttons payload
// into a Bot API inline keyboard. Accepted shapes:
//   - *inlineKeyboardMarkup (already built)
//   - [][]inlineButton
//   - the generic [][]map[string]string / []any produced by callers/JSON
func buttonsToMarkup(buttons any) (*inlineKeyboardMarkup, error) {
	switch v := buttons.(type) {
	case nil:
		return nil, nil
	case *inlineKeyboardMarkup:
		return v, nil
	case inlineKeyboardMarkup:
		return &v, nil
	case [][]inlineButton:
		return &inlineKeyboardMarkup{InlineKeyboard: v}, nil
	default:
		// Best-effort: round-trip through JSON into the Bot API shape.
		raw, err := json.Marshal(buttons)
		if err != nil {
			return nil, err
		}
		var mk inlineKeyboardMarkup
		if err := json.Unmarshal(raw, &mk); err == nil && len(mk.InlineKeyboard) > 0 {
			return &mk, nil
		}
		var rows [][]inlineButton
		if err := json.Unmarshal(raw, &rows); err == nil && len(rows) > 0 {
			return &inlineKeyboardMarkup{InlineKeyboard: rows}, nil
		}
		return nil, fmt.Errorf("telegram: unsupported buttons payload %T", buttons)
	}
}

// botSentID builds the canonical event id for a message the bot sent into a chat.
func botSentID(chatID string, messageID int) string {
	kind, id, err := parseChatID(chatID)
	if err != nil {
		return ""
	}
	return "telegram:" + kind + ":" + strconv.FormatInt(id, 10) + ":" + strconv.Itoa(messageID)
}
