package config

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
)

const (
	McpBridgeName       = "ws-bridge"
	LegacyMcpBridgeName = "dsh-bridge"
	AwaitToolName       = "ws_await_result"
	LegacyAwaitToolName = "dsh_await_result"
	CallToolName        = "ws_call_tool"
	LegacyCallToolName  = "dsh_call_tool"
)

// IsInternalBridgeTool reports whether name matches one of the gateway's internal MCP bridge tool identifiers.
func IsInternalBridgeTool(name string) bool {
	return name == AwaitToolName || name == CallToolName || name == LegacyAwaitToolName || name == LegacyCallToolName
}

// ShortSHA256 returns the first 16 hex characters of the SHA-256 digest of s.
func ShortSHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:16]
}

// TruncateRunes truncates s to at most maxRunes Unicode code points.
func TruncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || len(s) <= maxRunes {
		return s
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes])
}

func isLiteralCDrivePath(clean string) bool {
	if clean == "/mnt/c" || strings.HasPrefix(clean, "/mnt/c/") {
		return true
	}
	if len(clean) >= 2 && (clean[0] == 'C' || clean[0] == 'c') && clean[1] == ':' {
		return true
	}
	return false
}

// IsWindowsCDrivePath reports whether p (or its resolved symlink target) points inside a Windows C: drive mount.
func IsWindowsCDrivePath(p string) bool {
	if strings.TrimSpace(p) == "" {
		return false
	}
	clean := filepath.Clean(p)
	if isLiteralCDrivePath(clean) {
		return true
	}
	cur := clean
	for {
		if _, err := os.Lstat(cur); err == nil {
			if resolved, rErr := filepath.EvalSymlinks(cur); rErr == nil {
				resolvedClean := filepath.Clean(resolved)
				if isLiteralCDrivePath(resolvedClean) {
					return true
				}
				if rel, relErr := filepath.Rel(cur, clean); relErr == nil && rel != "." {
					if isLiteralCDrivePath(filepath.Clean(filepath.Join(resolvedClean, rel))) {
						return true
					}
				}
			}
			break
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return false
}
