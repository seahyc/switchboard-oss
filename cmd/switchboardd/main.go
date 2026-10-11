// Command switchboardd is the always-on capture+index daemon. It opens the
// store, builds the ingestion pipeline + gbrain-feed projector, registers the
// platform connectors, and serves the local API consumed by switchboard-mcp.
//
// Connectors are registered here as the teammates land them (whatsapp/telegram/
// lark). Live auth (QR / phone code / token) is driven separately with the user.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/connectors/gmail"
	"github.com/seahyc/switchboard-oss/internal/connectors/lark"
	"github.com/seahyc/switchboard-oss/internal/connectors/meta"
	"github.com/seahyc/switchboard-oss/internal/connectors/slack"
	"github.com/seahyc/switchboard-oss/internal/connectors/telegram"
	"github.com/seahyc/switchboard-oss/internal/connectors/whatsapp"
	"github.com/seahyc/switchboard-oss/internal/daemon"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
	"github.com/seahyc/switchboard-oss/internal/igweb"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// Version is the build version (git SHA), set at build time via
// -ldflags "-X main.Version=<sha>". "dev" for local/un-stamped builds.
var Version = "dev"

func main() {
	var (
		apiTokenFile   = flag.String("api-token-file", filepath.Join(api.DefaultStateDir(), "api.token"), "private administrative credential file; generated when missing")
		readTokenFile  = flag.String("read-token-file", "", "optional private read-only API credential file")
		allowOutbound  = flag.Bool("allow-outbound", false, "enable outbound actions for administrative clients; callers must approve each send")
		slackTokenFile = flag.String("slack-token-file", "", "private Slack user token file; set to enable polling")
		slackChannels  = flag.String("slack-channels", "", "explicit comma-separated Slack channel ID allowlist")
		slackInterval  = flag.Duration("slack-poll-interval", time.Minute, "Slack polling interval")
		slackSince     = flag.String("slack-since", "", "initial Slack history bound in RFC3339")
		showVersion    = flag.Bool("version", false, "print the build version (git SHA) and exit")
		dbPath         = flag.String("db", filepath.Join(api.DefaultStateDir(), "switchboard.db"), "SQLite database path")
		addr           = flag.String("addr", api.DefaultAddr, "local API listen address")
		selfIDs        = flag.String("self", "", "comma-separated platform_user_ids that are the user (mentions/@me)")
		gbrainBin      = flag.String("gbrain", "", "gbrain binary path; empty disables the gbrain-feed projector")
		gbrainCLI      = flag.Bool("gbrain-cli", false, "use the slow per-call gbrain CLI instead of the persistent MCP server (fallback)")
		qrDir          = flag.String("qr-dir", "", "dir to write <platform>-qr.png for QR-based auth (e.g. ~/.switchboard)")

		// Connectors are opt-in: a connector is only started when its flag is set.
		waDB      = flag.String("whatsapp-db", "", "WhatsApp session SQLite path; set to enable the WhatsApp connector")
		larkDir   = flag.String("lark-config", "", "Lark config dir; set to enable the Lark connector")
		tgAppID   = flag.Int("telegram-app-id", 0, "Telegram userbot app_id; set with -telegram-app-hash to enable Telegram")
		tgAppHash = flag.String("telegram-app-hash", "", "Telegram userbot app_hash")
		tgPhone   = flag.String("telegram-phone", "", "Telegram login phone (optional; interactive otherwise)")
		tgBotTok  = flag.String("telegram-bot-token", "", "Telegram outbound bot token (optional)")
		tgSession = flag.String("telegram-session", "telegram.session", "Telegram session path")

		gmailCredentials = flag.String("gmail-credentials-file", "", "private standalone Gmail OAuth credentials JSON")
		gmailOn          = flag.Bool("gmail", false, "enable the Gmail connector (reuses gw-cli OAuth creds from the keyring)")
		gmailKeyring     = flag.String("gmail-keyring", "", "keyrings.alt PlaintextKeyring path (default: ~/.local/share/python_keyring/keyring_pass.cfg)")
		gmailState       = flag.String("gmail-state", "", "Gmail historyId cursor file (default: ~/.switchboard/gmail-state.json)")
		gmailAccts       = flag.String("gmail-accounts", "", "comma-separated allow-list of Gmail addresses (default: all gmail-scoped accounts)")

		// Meta (Instagram + Messenger) via the self-hosted mautrix-meta bridge +
		// Conduit homeserver on the VM. Opt-in like the others; both the
		// "instagram" and "messenger" facades register from one shared session.
		metaOn       = flag.Bool("meta", false, "enable the Meta connector (Instagram + Messenger via the VM mautrix-meta bridge)")
		metaHS       = flag.String("meta-homeserver", "http://127.0.0.1:6167", "Conduit client-server URL")
		metaUser     = flag.String("meta-user", "@sb:meta.local", "switchboard Matrix user id")
		metaToken    = flag.String("meta-token", "", "access token for the Matrix user (or use -meta-token-file)")
		metaTokenF   = flag.String("meta-token-file", "", "file containing the Matrix access token")
		metaState    = flag.String("meta-state", "", "room↔chat index + watermark file (default ~/.switchboard/meta-state.json)")
		metaNetworks = flag.String("meta-networks", "instagram,messenger", "comma-separated allow-list of meta networks to register")
		metaHandle   = flag.String("meta-self-handle", "", "your IG/FB handle (alias signal for the people-graph self persona)")
		metaEmail    = flag.String("meta-self-email", "", "your IG/FB email (alias signal, optional)")
		metaPhone    = flag.String("meta-self-phone", "", "your IG/FB phone (alias signal, optional)")

		// Instagram browser (#25): READ-ONLY private-web reads (profiles/stories)
		// reusing the bridge session, ALWAYS via the residential proxy + egress-verified.
		igSessionFile = flag.String("ig-session-file", "", "exported IG cookie JSON (enables the read-only IG browser; default ~/.switchboard/ig-session.json when -ig-browser set)")
		igBrowser     = flag.Bool("ig-browser", false, "enable the read-only Instagram browser endpoints (profile/stories)")
		igProxy       = flag.String("ig-proxy", "", "SOCKS5 host:port for ALL IG browser traffic (residential home-IP proxy)")
		igEgress      = flag.String("ig-expected-egress", "", "the home IP the proxy MUST egress; reads fail closed otherwise")

		// X.com (#50): read-only chronological Following timeline + profiles, reusing
		// the exported web session, ALWAYS via the residential proxy + egress-verified.
		xSessionFile = flag.String("x-session-file", "", "exported X cookie JSON (auth_token+ct0; enables the read-only X browser; default ~/.switchboard/x-session.json when -x-browser set)")
		xBrowser     = flag.Bool("x-browser", false, "enable the read-only X.com browser endpoints (following_feed/profile)")
		xProxy       = flag.String("x-proxy", "", "SOCKS5 host:port for ALL X browser traffic (residential home-IP proxy)")
		xEgress      = flag.String("x-expected-egress", "", "the home IP the proxy MUST egress; X reads fail closed otherwise")

		mediaRoot  = flag.String("media-root", "", "media blob store dir; set to enable importance-gated attachment download (e.g. ~/switchboard/media)")
		mediaCapGB = flag.Float64("media-cap-gb", 40, "total media blob cap in GiB (dedup-aware)")
		mediaMaxMB = flag.Int("media-max-file-mb", 512, "per-file size ceiling in MiB; larger files are skipped")
		mediaTypes = flag.String("media-types", "", "comma-separated attachment types to download (empty = all: image,video,audio,voice,document,sticker)")
		mediaRate  = flag.Int("media-rate-per-min", 30, "throttle: max media downloads per minute (0 = unlimited; gentle pace avoids platform anti-abuse)")

		// Identity matcher (#19) — off-ingest LLM adjudication. Off by default so a
		// deploy never auto-starts a multi-hour CPU sweep; flip -enable-matcher when
		// ready (stays PROPOSE-ONLY regardless until the lead gates the eval).
		enableMatcher = flag.Bool("enable-matcher", false, "enable the off-ingest LLM identity-matcher worker (propose-only)")
		ollamaURL     = flag.String("ollama-url", "", "ollama base URL for the matcher (empty → OLLAMA_URL env, optional OLLAMA_CONTAINER, else localhost)")
		ollamaModel   = flag.String("ollama-model", "qwen2.5:3b-instruct", "matcher adjudication model (3b: vm-gov removed the 7b; re-pulling reintroduces the OOM storm)")
		matchInterval = flag.Duration("match-interval", 6*time.Hour, "interval between identity-match sweeps")
		matchPaceSecs = flag.Int("match-pace-seconds", 3, "min gap (s) between adjudication calls so the sweep never starves live ingest")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(Version)
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := api.ValidateListenAddr(*addr); err != nil {
		logger.Error("invalid API address", "err", err)
		os.Exit(1)
	}
	adminToken, err := api.LoadOrCreateToken(*apiTokenFile)
	if err != nil {
		logger.Error("API credential setup failed", "err", err)
		os.Exit(1)
	}
	readToken := ""
	if *readTokenFile != "" {
		readToken, err = api.LoadOrCreateToken(*readTokenFile)
		if err != nil {
			logger.Error("read credential setup failed", "err", err)
			os.Exit(1)
		}
		if readToken == adminToken {
			logger.Error("read and admin credentials must differ")
			os.Exit(1)
		}
	}
	var gb gbrainfeed.Client
	if *gbrainBin != "" {
		if *gbrainCLI {
			gb = gbrainfeed.NewCLIClient(*gbrainBin) // slow per-call spawn (fallback)
		} else {
			gb = gbrainfeed.NewMCPClient(*gbrainBin) // persistent server — ~30x faster
		}
	}

	d, err := daemon.New(daemon.Config{
		DBPath:           *dbPath,
		APIPolicy:        api.Policy{AdminToken: adminToken, ReadToken: readToken, AllowOutbound: *allowOutbound},
		SelfIDs:          splitNonEmpty(*selfIDs),
		GBrain:           gb,
		QRDir:            *qrDir,
		Logger:           logger,
		Version:          Version,
		MediaRoot:        *mediaRoot,
		MediaCapBytes:    int64(*mediaCapGB * 1024 * 1024 * 1024),
		MediaPerFileMax:  int64(*mediaMaxMB) * 1024 * 1024,
		MediaTypes:       splitNonEmpty(*mediaTypes),
		MediaRatePerMin:  *mediaRate,
		EnableMatcher:    *enableMatcher,
		OllamaURL:        *ollamaURL,
		OllamaModel:      *ollamaModel,
		MatchInterval:    *matchInterval,
		MatchPaceSeconds: *matchPaceSecs,
	})
	if err != nil {
		logger.Error("init failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *slackTokenFile != "" {
		since := time.Time{}
		if *slackSince != "" {
			since, err = time.Parse(time.RFC3339, *slackSince)
			if err != nil {
				logger.Error("invalid slack-since", "err", err)
				os.Exit(1)
			}
		}
		sc, err := slack.New(slack.Config{TokenFile: *slackTokenFile, Channels: splitNonEmpty(*slackChannels), PollInterval: *slackInterval, Since: since})
		if err != nil {
			logger.Error("Slack connector setup failed", "err", err)
			os.Exit(1)
		}
		d.Register(sc)
	}
	// Register opt-in connectors. Each is wired only when configured; live auth
	// (QR / phone code / Lark token) is driven separately with the user.
	//
	// A connector that fails to init (e.g. missing/expired Lark token) is logged
	// and SKIPPED — never fatal. An always-on capture daemon must keep serving
	// the MCP/store and its other connectors rather than crash-loop on one bad
	// credential.
	if *waDB != "" {
		if wa, err := whatsapp.New(ctx, *waDB, waLog.Stdout("whatsapp", "INFO", true)); err != nil {
			logger.Error("whatsapp connector init failed — skipping", "err", err)
		} else {
			d.Register(wa)
			logger.Info("registered connector", "platform", "whatsapp", "db", *waDB)
		}
	}
	if *larkDir != "" {
		if lk, err := lark.New(*larkDir); err != nil {
			logger.Error("lark connector init failed — skipping", "err", err)
		} else {
			d.Register(lk)
			logger.Info("registered connector", "platform", "lark", "config", *larkDir)
		}
	}
	if *gmailOn {
		gm, err := gmail.New(ctx, gmail.Config{
			CredentialsFile: *gmailCredentials,
			KeyringPath:     *gmailKeyring,
			StatePath:       *gmailState,
			Accounts:        splitNonEmpty(*gmailAccts),
			Logger:          logger,
		})
		if err != nil {
			logger.Error("gmail connector init failed — skipping", "err", err)
		} else {
			d.Register(gm)
			logger.Info("registered connector", "platform", "gmail")
		}
	}
	if *tgAppID != 0 && *tgAppHash != "" {
		tg := telegram.New(telegram.Config{
			AppID:       *tgAppID,
			AppHash:     *tgAppHash,
			Phone:       *tgPhone,
			BotToken:    *tgBotTok,
			SessionPath: *tgSession,
		})
		d.Register(tg)
		logger.Info("registered connector", "platform", "telegram", "app_id", strconv.Itoa(*tgAppID))
	}
	if *metaOn {
		token := *metaToken
		if token == "" && *metaTokenF != "" {
			if b, err := privatefs.ReadSecret(*metaTokenF); err == nil {
				token = strings.TrimSpace(string(b))
			} else {
				logger.Error("meta token file read failed — skipping", "err", err)
			}
		}
		statePath := *metaState
		if statePath == "" {
			if home, err := os.UserHomeDir(); err == nil {
				statePath = home + "/.switchboard/meta-state.json"
			}
		}
		backend, err := meta.NewMautrixBackend(meta.Config{
			Homeserver: *metaHS, UserID: *metaUser, Token: token,
		})
		if err != nil {
			logger.Error("meta connector init failed — skipping", "err", err)
		} else {
			mc := meta.NewClient(meta.Config{
				Homeserver: *metaHS, UserID: *metaUser, Token: token,
				StatePath:  statePath,
				SelfHandle: *metaHandle, SelfEmail: *metaEmail, SelfPhone: *metaPhone,
				Logger: logger,
			}, backend)
			if err := mc.Load(); err != nil {
				logger.Warn("meta state load failed (continuing fresh)", "err", err)
			}
			allow := map[string]bool{}
			for _, n := range splitNonEmpty(*metaNetworks) {
				allow[n] = true
			}
			if allow[meta.PlatformInstagram] {
				d.Register(mc.Instagram())
			}
			if allow[meta.PlatformMessenger] {
				d.Register(mc.Messenger())
			}
			logger.Info("registered connector", "platform", "meta", "networks", *metaNetworks)
		}
	}
	if *igBrowser {
		sessFile := *igSessionFile
		if sessFile == "" {
			if home, err := os.UserHomeDir(); err == nil {
				sessFile = home + "/.switchboard/ig-session.json"
			}
		}
		igc, err := igweb.New(igweb.Config{
			SessionPath:    sessFile,
			ProxyAddr:      *igProxy,
			ExpectedEgress: *igEgress,
			EgressTTL:      60 * time.Second,
		})
		if err != nil {
			logger.Error("ig-browser init failed — skipping", "err", err)
		} else {
			d.SetIGBrowser(igc)
			logger.Info("enabled instagram browser (read-only)", "proxy", *igProxy, "egress", *igEgress, "session", sessFile)
		}
	}
	if *xBrowser {
		sessFile := *xSessionFile
		if sessFile == "" {
			if home, err := os.UserHomeDir(); err == nil {
				sessFile = home + "/.switchboard/x-session.json"
			}
		}
		xc, err := xweb.New(xweb.Config{
			SessionPath:    sessFile,
			ProxyAddr:      *xProxy,
			ExpectedEgress: *xEgress,
			EgressTTL:      60 * time.Second,
		})
		if err != nil {
			logger.Error("x-browser init failed — skipping", "err", err)
		} else {
			d.SetXBrowser(xc)
			logger.Info("enabled x.com browser (read-only)", "proxy", *xProxy, "egress", *xEgress, "session", sessFile)
		}
	}

	if err := d.Run(ctx, *addr); err != nil {
		logger.Error("daemon exited", "err", err)
		os.Exit(1)
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
