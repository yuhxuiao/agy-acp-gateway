package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Exercises the actual gateway/official-argv-shaped fake ACP boundary in mode,
// including its model ACK path, rather than only calling the placement helper.
func TestPrivatePlacementGateway(t *testing.T) {
	t.Setenv("AGY_PROMPT_PLACEMENT", "first-prompt")
	base, cleanup := startTestGateway(t)
	defer cleanup()
	messages := []map[string]any{{"role": "user", "content": "hello placement"}}
	for turn := 0; turn < 2; turn++ {
		body, _ := json.Marshal(map[string]any{"model": "gemini-3.8-flash", "max_tokens": 64, "system": "FULL  system\n    indent: 中文\n", "messages": messages})
		req, _ := http.NewRequest("POST", base+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", "private-placement-two-turns")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(raw), "FAKE_OK") {
			t.Fatalf("turn %d: status %d %s", turn, resp.StatusCode, raw)
		}
		messages = append(messages, map[string]any{"role": "assistant", "content": "FAKE_OK"}, map[string]any{"role": "user", "content": "follow up"})
	}
}
