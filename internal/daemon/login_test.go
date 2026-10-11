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

type qrStatusConnector struct {
	fakeConnector
	connected bool
	checked   chan struct{}
	calls     int
}

func (f *qrStatusConnector) Login(context.Context) (connectors.LoginStatus, error) {
	return connectors.LoginStatus{NeedsAuth: true, QRPNG: []byte("new-code")}, nil
}

func (f *qrStatusConnector) Status(context.Context) (connectors.LoginStatus, error) {
	f.calls++
	if f.checked != nil {
		select {
		case f.checked <- struct{}{}:
		default:
		}
	}
	if f.calls > 1 && f.connected {
		return connectors.LoginStatus{Connected: true}, nil
	}
	return connectors.LoginStatus{NeedsAuth: true}, nil
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.driveLogin(ctx, c) }()

	qrPath := filepath.Join(dir, "whatsapp-qr.png")
	var latest []byte
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(qrPath)
		if err == nil && string(data) == "code-C" {
			latest = data
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("driveLogin unexpectedly returned nil after cancellation")
	}
	if string(latest) != "code-C" {
		t.Fatalf("latest equal-size QR was not observed: got %q", latest)
	}
}

func TestDriveLoginRemovesExpiredQR(t *testing.T) {
	testDriveLoginRemovesQR(t, false)
}

func TestDriveLoginRemovesQROnSuccessfulAuth(t *testing.T) {
	testDriveLoginRemovesQR(t, true)
}

func testDriveLoginRemovesQR(t *testing.T, connected bool) {
	t.Helper()
	dir := t.TempDir()
	qrPath := filepath.Join(dir, "whatsapp-qr.png")
	if err := os.WriteFile(qrPath, []byte("stale-code"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{qrDir: dir, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	c := &qrStatusConnector{
		fakeConnector: fakeConnector{name: "whatsapp"},
		connected:     connected,
		checked:       make(chan struct{}, 2),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.driveLogin(ctx, c) }()

	// The first Status call is the pre-login check. The polling status after
	// Login either reports expiry (no QR) or successful authentication.
	for i := 0; i < 2; i++ {
		select {
		case <-c.checked:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("driveLogin did not poll connector status")
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(qrPath); os.IsNotExist(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(qrPath); !os.IsNotExist(err) {
		cancel()
		t.Fatalf("QR file still exists after status without QR (stat error: %v)", err)
	}
	cancel()
	if err := <-done; connected && err != nil {
		t.Fatalf("driveLogin() error = %v, want nil after successful auth", err)
	} else if !connected && err == nil {
		t.Fatal("driveLogin unexpectedly returned nil after expiry and cancellation")
	}
}
