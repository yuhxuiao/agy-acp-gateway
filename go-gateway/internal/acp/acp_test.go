package acp

import (
	"net/http"
	"testing"
)

func TestParseCapabilitiesAndNoticeClassification(t *testing.T) {
	raw := []byte(`{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{}},"mcpCapabilities":{"http":true}}}`)
	caps, err := ParseCapabilities(raw)
	if err != nil {
		t.Fatalf("ParseCapabilities failed: %v", err)
	}
	if !caps.Load || !caps.Resume || !caps.McpHTTP {
		t.Fatalf("unexpected caps: %+v", caps)
	}

	v, notice, rest := ClassifyAgyNotice("⚠ "+NoticeNetwork+"\nrecovered ok", false)
	if v != NoticeDrop || notice != NoticeNetwork || rest != "recovered ok" {
		t.Fatalf("unexpected verdict=%v notice=%q rest=%q", v, notice, rest)
	}
}

func TestDetectAntigravitySoftErrorAndRetryAfter(t *testing.T) {
	tosCalled := false
	se := DetectAntigravitySoftError("This service has been disabled in this account", func(string) {
		tosCalled = true
	})
	if !tosCalled || se == nil || se.StatusCode != http.StatusServiceUnavailable || se.ErrorType != "tos_circuit_breaker" {
		t.Fatalf("expected ToS soft error, got %+v tosCalled=%v", se, tosCalled)
	}
	if got := ExtractRetryAfterHeaderValue("retry in 2m 15s"); got != "135" {
		t.Fatalf("expected 135s retry-after, got %q", got)
	}
}
