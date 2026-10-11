package whatsapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func qrTestConnector() *Connector {
	return &Connector{client: whatsmeow.NewClient(&store.Device{}, waLog.Noop), log: waLog.Noop}
}

func TestQRPairingFailureRemainsVisible(t *testing.T) {
	c := qrTestConnector()
	c.qrPNG = []byte("expired")
	ch := make(chan whatsmeow.QRChannelItem, 1)
	ch <- whatsmeow.QRChannelItem{Event: "error", Error: &whatsmeow.PairDatabaseError{Message: "failed to save device store", DBErr: errors.New("sensitive underlying value")}}
	close(ch)
	paired, retry := c.consumeQR(ch, func(connectors.LoginStatus) {})
	if paired || retry {
		t.Fatal("terminal failure must not be silently retried")
	}
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.QRPNG) != 0 || !strings.Contains(st.Detail, "failed to save device store") || strings.Contains(st.Detail, "sensitive") {
		t.Fatalf("failure status must clear QR and expose only the failure stage: %+v", st)
	}
}

func TestQRTimeoutClearsCodeAndRenews(t *testing.T) {
	c := qrTestConnector()
	c.qrPNG = []byte("expired")
	ch := make(chan whatsmeow.QRChannelItem, 1)
	ch <- whatsmeow.QRChannelTimeout
	close(ch)
	paired, retry := c.consumeQR(ch, func(connectors.LoginStatus) {})
	if paired || !retry || len(c.qrPNG) != 0 {
		t.Fatal("timeout must clear QR and request renewal")
	}
}

func TestQRStatusDoesNotExposeExpiredCode(t *testing.T) {
	c := qrTestConnector()
	c.qrPNG = []byte("expired")
	c.qrExpiresAt = time.Now().Add(-time.Second)
	st, _ := c.Status(context.Background())
	if len(st.QRPNG) != 0 {
		t.Fatal("expired QR must not be advertised while waiting for channel event")
	}
}

func TestQRSuccessClearsCode(t *testing.T) {
	c := qrTestConnector()
	c.qrPNG = []byte("used")
	ch := make(chan whatsmeow.QRChannelItem, 1)
	ch <- whatsmeow.QRChannelSuccess
	close(ch)
	paired, retry := c.consumeQR(ch, func(connectors.LoginStatus) {})
	if !paired || retry || len(c.qrPNG) != 0 {
		t.Fatal("pair success must clear the used QR")
	}
}

func TestPairingErrorDetailDoesNotExposeUnknownError(t *testing.T) {
	got := pairingErrorDetail(errors.New("secret pairing payload"))
	if strings.Contains(got, "secret") {
		t.Fatal("unknown error payload must not enter auth status")
	}
}
