package llm

import (
	"context"
	"testing"
)

func TestStandaloneResolveDefaults(t *testing.T) {
	t.Setenv("OLLAMA_URL", "")
	t.Setenv("OLLAMA_CONTAINER", "")
	got, err := ResolveOllamaURL(context.Background(), "")
	if err != nil || got != "http://127.0.0.1:11434" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	t.Setenv("OLLAMA_URL", "http://127.0.0.1:1234/")
	got, err = ResolveOllamaURL(context.Background(), "")
	if err != nil || got != "http://127.0.0.1:1234" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}
