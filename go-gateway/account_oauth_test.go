package main

import (
	"runtime"
	"strings"
	"testing"
)

func TestOpenLinuxPTY_AllocatesBidirectionalTTY(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("PTY ioctl path only applies on linux/darwin, got %s", runtime.GOOS)
	}
	master, slave, err := openLinuxPTY()
	if err != nil {
		t.Fatalf("openLinuxPTY failed on %s: %v", runtime.GOOS, err)
	}
	defer master.Close()
	defer slave.Close()

	const payload = "oauth-test-code\n"
	if _, err := master.WriteString(payload); err != nil {
		t.Fatalf("write to PTY master failed: %v", err)
	}
	buf := make([]byte, 64)
	n, err := slave.Read(buf)
	if err != nil {
		t.Fatalf("read from PTY slave failed: %v", err)
	}
	if got := string(buf[:n]); !strings.Contains(got, "oauth-test-code") {
		t.Fatalf("expected PTY slave to receive payload, got %q", got)
	}
}

func TestSummarizeOAuthFailureOutput(t *testing.T) {
	raw := strings.Join([]string{
		"Authentication required. Please visit the URL to log in:",
		"  https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=123456&code_challenge=abcdef&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&scope=openid&state=xyz",
		"",
		"Waiting for authentication (timeout 60s)...",
		"Or, paste the authorization code here and press Enter:",
		`Error: authentication failed: token exchange failed: Post "https://oauth2.googleapis.com/token": proxyconnect tcp: dial tcp 127.0.0.1:7890: connect: connection refused`,
		"error: authentication failed or timed out",
	}, "\n")

	got := summarizeOAuthFailureOutput(raw)
	if strings.Contains(got, "https://accounts.google.com/o/oauth2/auth") {
		t.Fatalf("expected OAuth URL boilerplate to be stripped, got %q", got)
	}
	if !strings.Contains(got, "connection refused") {
		t.Fatalf("expected real error reason to be preserved, got %q", got)
	}

	bannerOnly := "Authentication required. Please visit the URL to log in:\n  https://accounts.google.com/o/oauth2/auth?state=1"
	if got := summarizeOAuthFailureOutput(bannerOnly); got != bannerOnly {
		t.Fatalf("expected fallback to trimmed original output when no error lines remain, got %q", got)
	}
}
