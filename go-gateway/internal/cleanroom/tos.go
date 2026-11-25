package cleanroom

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

var (
	tosPhraseRe = regexp.MustCompile(`(?i)violation of Terms of Service|This service has been disabled in this account|account has been (?:suspended|disabled|terminated) for violating`)
	tos403Re    = regexp.MustCompile(`(?i)(?:403|PERMISSION_DENIED).*?(?:Terms of Service|disabled in this account|suspended)|(?:Terms of Service|disabled in this account).*?(?:403|PERMISSION_DENIED)`)
	tosSoftRe   = regexp.MustCompile(`(?i)^\s*(?:(?:error[: ]+|403[: ]+|PERMISSION_DENIED[: ]+)\s*)?This service has been disabled in this account(?: for violation of Terms of Service)?[.!]?\s*$`)
)

// IsTosViolationError detects upstream ToS suspension/account-disabled errors from RPC error strings.
func IsTosViolationError(text string) bool {
	return tosPhraseRe.MatchString(text) || tos403Re.MatchString(text)
}

// IsTosSoftNotice detects inline assistant notice messages reporting an account ToS restriction.
func IsTosSoftNotice(text string) bool {
	return tosSoftRe.MatchString(text)
}

// WriteTosMarker atomically writes the persistent ToS circuit marker file and returns any I/O error.
func WriteTosMarker(target, reason string) (sanitizedReason string, err error) {
	sanitizedReason = gwcfg.TruncateRunes(gwcfg.RedactSecretsInString(strings.TrimSpace(reason)), 500)
	if target == "" {
		return sanitizedReason, errors.New("marker path unavailable")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return sanitizedReason, err
	}
	payload, _ := json.Marshal(map[string]any{
		"tripped":   true,
		"trippedAt": time.Now().UTC().Format(time.RFC3339),
		"reason":    sanitizedReason,
	})
	f, err := os.CreateTemp(filepath.Dir(target), ".tos-tripped-*")
	if err != nil {
		return sanitizedReason, err
	}
	tmp := f.Name()
	_, wErr := f.Write(payload)
	if wErr == nil {
		wErr = f.Chmod(0600)
	}
	if wErr == nil {
		wErr = f.Sync()
	}
	closeErr := f.Close()
	if wErr == nil {
		wErr = closeErr
	}
	if wErr != nil {
		_ = os.Remove(tmp)
		return sanitizedReason, wErr
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return sanitizedReason, err
	}
	return sanitizedReason, nil
}

// ReadTosMarker checks a ToS marker file on disk.
func ReadTosMarker(target string) (present bool, unreadable bool, reason string) {
	if target == "" {
		return false, false, ""
	}
	raw, err := os.ReadFile(target)
	if err == nil {
		var p struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(raw, &p)
		if p.Reason != "" {
			reason = gwcfg.RedactSecretsInString(p.Reason)
		}
		return true, false, reason
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, false, ""
	}
	return false, true, "Circuit marker cannot be read"
}

// TosBreaker encapsulates a thread-safe persistent ToS circuit breaker without global engine coupling.
type TosBreaker struct {
	mu         sync.Mutex
	trippedMem atomic.Bool
	reasonMem  string
	persisted  bool
	tosFile    string
	onTrip     func()
}

// NewTosBreaker constructs a standalone TosBreaker bound to tosFile and an optional onTrip shutdown callback.
func NewTosBreaker(tosFile string, onTrip func()) *TosBreaker {
	return &TosBreaker{
		tosFile: tosFile,
		onTrip:  onTrip,
	}
}

// Trip latches the breaker in memory, attempts atomic marker persistence, and fires onTrip outside the lock.
func (b *TosBreaker) Trip(reason string) error {
	b.mu.Lock()
	b.trippedMem.Store(true)
	sanitized, persistErr := WriteTosMarker(b.tosFile, reason)
	b.reasonMem = sanitized
	if persistErr == nil {
		b.persisted = true
	}
	b.mu.Unlock()
	if b.onTrip != nil {
		b.onTrip()
	}
	return persistErr
}

// IsTripped returns whether the breaker is currently tripped along with the redacted reason.
func (b *TosBreaker) IsTripped() (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tosFile == "" {
		return b.trippedMem.Load(), b.reasonMem
	}
	present, unreadable, reason := ReadTosMarker(b.tosFile)
	if present {
		b.trippedMem.Store(true)
		b.persisted = true
		if reason != "" {
			b.reasonMem = reason
		}
		return true, b.reasonMem
	}
	if unreadable {
		b.trippedMem.Store(true)
		b.reasonMem = reason
		return true, b.reasonMem
	}
	if b.persisted {
		b.trippedMem.Store(false)
		b.reasonMem = ""
		b.persisted = false
	}
	return b.trippedMem.Load(), b.reasonMem
}
