package daemon

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
)

type rotatingQRConnector struct {
	fakeConnector
	statusCalls int
}

func (f *rotatingQRConnector) Login(context.Context) (connectors.LoginStatus, error) {
	return connectors.LoginStatus{NeedsAuth: true, QRPNG: []byte("code-A")}, nil
}

func (f *rotatingQRConnector) Status(context.Context) (connectors.LoginStatus, error) {
	f.statusCalls++
	if f.statusCalls == 1 {
		return connectors.LoginStatus{NeedsAuth: true}, nil
	}
	code := "code-B"
	if f.statusCalls > 2 {
		code = "code-C"
	}
	return connectors.LoginStatus{NeedsAuth: true, QRPNG: []byte(code)}, nil
}

func TestDriveLoginRefreshesEqualSizeQR(t *testing.T) {
	dir := t.TempDir()
	d := &Daemon{qrDir: dir, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	c := &rotatingQRConnector{fakeConnector: fakeConnector{name: "whatsapp"}}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	_ = d.driveLogin(ctx, c)
	data, err := os.ReadFile(filepath.Join(dir, "whatsapp-qr.png"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "code-C" {
		t.Fatalf("latest equal-size QR was not persisted: got %q", data)
	}
}
