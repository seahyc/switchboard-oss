package whatsapp

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

var versionOnce sync.Once

func updateWAVersion(ctx context.Context, client *http.Client) error {
	version, err := whatsmeow.GetLatestVersion(ctx, client)
	if err != nil {
		return err
	}
	store.SetWAVersion(*version)
	return nil
}

func prepareWAVersion(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return updateWAVersion(bounded, &http.Client{Timeout: 10 * time.Second})
}
