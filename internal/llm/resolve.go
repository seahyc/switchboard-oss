package llm

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// DefaultOllamaContainer is the honcho-managed ollama container on the docker net.
// It is NOT host-published, so the daemon must resolve its current docker-network
// IP at startup (the IP changes across container restarts).
const DefaultOllamaContainer = "honcho-ollama-1"

// OllamaPort is the in-container ollama API port.
const OllamaPort = "11434"

// ResolveOllamaURL uses an explicit URL, an optional Docker container, or localhost.
// Container lookup errors accompany the localhost fallback.
func ResolveOllamaURL(ctx context.Context, container string) (string, error) {
	if env := strings.TrimSpace(os.Getenv("OLLAMA_URL")); env != "" {
		return strings.TrimRight(env, "/"), nil
	}
	if container == "" {
		container = strings.TrimSpace(os.Getenv("OLLAMA_CONTAINER"))
		if container == "" {
			return DefaultBaseURL, nil
		}
	}
	ip, err := dockerContainerIP(ctx, container)
	if err != nil {
		return DefaultBaseURL, fmt.Errorf("resolve ollama container %q: %w (using fallback %s)", container, err, DefaultBaseURL)
	}
	return "http://" + net.JoinHostPort(ip, OllamaPort), nil
}

// dockerContainerIP runs `docker inspect` to read the container's first network IP.
func dockerContainerIP(ctx context.Context, container string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", "inspect",
		"-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	// A container on multiple networks concatenates IPs; take the first valid one.
	for _, f := range strings.Fields(ip) {
		if net.ParseIP(f) != nil {
			return f, nil
		}
	}
	if net.ParseIP(ip) != nil {
		return ip, nil
	}
	return "", fmt.Errorf("no IP for container %q (got %q)", container, ip)
}
