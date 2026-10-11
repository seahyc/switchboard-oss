package whatsapp

import (
	"context"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

func TestNewAdvertisesNamedDesktopForQRPairing(t *testing.T) {
	versionOnce.Do(func() {})
	previousOS, previousPlatform := store.DeviceProps.Os, store.DeviceProps.PlatformType
	t.Cleanup(func() {
		store.DeviceProps.Os = previousOS
		store.DeviceProps.PlatformType = previousPlatform
	})
	c, err := New(context.Background(), filepath.Join(t.TempDir(), "session.db"), waLog.Noop)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.container.Close() })
	if store.DeviceProps.GetOs() != "Switchboard" || store.DeviceProps.GetPlatformType() != waCompanionReg.DeviceProps_DESKTOP {
		t.Fatal("registration must identify the named Switchboard desktop")
	}
	if c.client.QRClientType != whatsmeow.PairClientElectron {
		t.Fatalf("QR must identify a desktop client, got %q", c.client.QRClientType)
	}
}
