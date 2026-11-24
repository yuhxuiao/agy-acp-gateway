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

func TestBuildOAuthSubprocessEnv_DarwinKeychainIsolationAndSecretStripping(t *testing.T) {
	base := []string{
		"PATH=/usr/bin:/bin",
		"HOME=/Users/hostuser",
		"BROWSER=/usr/bin/open",
		"AGY_API_KEYS=sk-master-secret",
		"GEMINI_API_KEY=leaked-gemini",
		"AGY_RESIN_PROXY_TOKEN=resin-tok",
	}
	darwinEnv := buildAccountSubprocessEnvForGOOS(base, "darwin", "/tmp/acc-home", "http://127.0.0.1:7890", "BROWSER=/bin/false")
	envMap := make(map[string]string, len(darwinEnv))
	for _, kv := range darwinEnv {
		if k, v, ok := strings.Cut(kv, "="); ok {
			envMap[k] = v
		}
	}
	if envMap["SSH_CONNECTION"] != "1 1 1 1" {
		t.Fatalf("expected darwin OAuth subprocess env to inject SSH_CONNECTION='1 1 1 1' for Keychain isolation, got %q", envMap["SSH_CONNECTION"])
	}
	if envMap["HOME"] != "/tmp/acc-home" {
		t.Fatalf("expected isolated HOME=/tmp/acc-home, got %q", envMap["HOME"])
	}
	if envMap["BROWSER"] != "/bin/false" {
		t.Fatalf("expected BROWSER=/bin/false, got %q", envMap["BROWSER"])
	}
	if envMap["HTTP_PROXY"] != "http://127.0.0.1:7890" || envMap["HTTPS_PROXY"] != "http://127.0.0.1:7890" {
		t.Fatalf("expected account proxy injected, got HTTP_PROXY=%q HTTPS_PROXY=%q", envMap["HTTP_PROXY"], envMap["HTTPS_PROXY"])
	}
	for _, forbidden := range []string{"AGY_API_KEYS", "GEMINI_API_KEY", "AGY_RESIN_PROXY_TOKEN"} {
		if _, leaked := envMap[forbidden]; leaked {
			t.Fatalf("expected %s to be stripped from OAuth subprocess env", forbidden)
		}
	}

	// Preserve real SSH_CONNECTION when already present on darwin, and do not inject fake SSH_CONNECTION on linux.
	withSSH := buildAccountSubprocessEnvForGOOS(append(base, "SSH_CONNECTION=10.0.0.1 5555 10.0.0.2 22"), "darwin", "/tmp/acc-home", "http://127.0.0.1:7890", "BROWSER=/bin/false")
	sshCount := 0
	for _, kv := range withSSH {
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			sshCount++
			if kv != "SSH_CONNECTION=10.0.0.1 5555 10.0.0.2 22" {
				t.Fatalf("expected existing SSH_CONNECTION preserved, got %q", kv)
			}
		}
	}
	if sshCount != 1 {
		t.Fatalf("expected exactly 1 SSH_CONNECTION entry, got %d", sshCount)
	}

	linuxEnv := buildAccountSubprocessEnvForGOOS(base, "linux", "/tmp/acc-home", "http://127.0.0.1:7890", "BROWSER=/bin/false")
	for _, kv := range linuxEnv {
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			t.Fatalf("did not expect synthetic SSH_CONNECTION on linux, got %q", kv)
		}
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
