package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotReloadAndImmutability(t *testing.T) {
	t.Cleanup(func() { Reload() })
	t.Setenv("AGY_GW_HOME", t.TempDir())
	t.Setenv("AGY_MAX_LIVE_WORKERS", "12")
	t.Setenv("AGY_MAX_PROBE_WORKERS", "2")
	t.Setenv("AGY_CORS_ORIGINS", "http://localhost:3000, https://app.example.com")

	snap1 := Reload()
	if snap1.MaxLiveWorkers != 12 || snap1.MaxProbeWorkers != 2 {
		t.Fatalf("unexpected workers in snap1: live=%d probe=%d", snap1.MaxLiveWorkers, snap1.MaxProbeWorkers)
	}
	if !snap1.AllowedCorsOrigin("http://localhost:3000") || !snap1.AllowedCorsOrigin("https://app.example.com") {
		t.Fatalf("expected CORS origins populated cleanly")
	}

	// Mutating a cloned map must not affect snap1.
	cloned := snap1.CloneCorsOrigins()
	cloned["http://evil.example"] = true
	if snap1.AllowedCorsOrigin("http://evil.example") {
		t.Fatal("mutating cloned map affected Snapshot")
	}

	// Loaded() stays pinned to snap1 until Reload() even when env changes.
	t.Setenv("AGY_MAX_LIVE_WORKERS", "4")
	if Loaded().MaxLiveWorkers != 12 {
		t.Fatalf("Loaded() mutated before Reload(): got %d want 12", Loaded().MaxLiveWorkers)
	}
	snap2 := Reload()
	if snap2.MaxLiveWorkers != 4 {
		t.Fatalf("Reload() did not pick up updated env: got %d want 4", snap2.MaxLiveWorkers)
	}
	if snap1.MaxLiveWorkers != 12 {
		t.Fatalf("previously returned snap1 changed after Reload(): got %d want 12", snap1.MaxLiveWorkers)
	}
}

func TestSecretRedactionAndOpenPrivateLog(t *testing.T) {
	in := "Authorization: Bearer sk-secret-123 /mcp/secret-token /v1beta/models/key=gem-secret:generateContent?key=qsecret"
	out := RedactSecretsInString(in)
	for _, secret := range []string{"sk-secret-123", "secret-token", "gem-secret", "qsecret"} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q leaked in redacted output: %q", secret, out)
		}
	}

	rawJSON, err := RedactLogJSON(map[string]any{
		"headers": []any{map[string]any{"name": "x-api-key", "value": "top-secret-key"}},
		"count":   1234567890123,
	})
	if err != nil {
		t.Fatalf("RedactLogJSON error: %v", err)
	}
	if strings.Contains(string(rawJSON), "top-secret-key") || !strings.Contains(string(rawJSON), "1234567890123") {
		t.Fatalf("unexpected RedactLogJSON output: %s", string(rawJSON))
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "sub", "gateway.log")
	f, err := OpenPrivateLog(logPath, dir)
	if err != nil {
		t.Fatalf("OpenPrivateLog error: %v", err)
	}
	_ = f.Close()
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions, got %#o", info.Mode().Perm())
	}
}
