package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"agy-acp-gateway-go/internal/acp"
	"agy-acp-gateway-go/internal/protocol"
)

const (
	imageGenOuterModel     = "gemini-3.8-flash"
	imageGenOuterTier      = "low"
	imageGenServedModel    = "gemini-3.1-flash-image"
	maxImageGenCount       = 4
	maxImageEditInputs     = 3
	maxImageGenPromptChars = 32768
	maxGeneratedImageBytes = 32 * 1024 * 1024
)

var (
	imageGenIdleTTL         = 45 * time.Second
	imageGenWarnIdleTTL     = 10 * time.Second
	imageGenMaxGenerations  = 40
	imageGenMaxEngineAge    = 20 * time.Minute
	imageGenMaxConsecErrors = 2
	imageGenMaxConcurrency  = 2
	imageGenIdleTimeout     = 180 * time.Second
	imageGenMaxDuration     = 300 * time.Second
	imageTranscodeMu        sync.Mutex
)

type aspectRatioSpec struct {
	name  string
	ratio float64
}

var allowedImageAspectRatios = []aspectRatioSpec{
	{name: "1:1", ratio: 1.0},
	{name: "2:3", ratio: 2.0 / 3.0},
	{name: "3:2", ratio: 3.0 / 2.0},
	{name: "3:4", ratio: 3.0 / 4.0},
	{name: "4:3", ratio: 4.0 / 3.0},
	{name: "9:16", ratio: 9.0 / 16.0},
	{name: "16:9", ratio: 16.0 / 9.0},
}

var allowedImageAspectRatioSet = map[string]bool{
	"1:1":  true,
	"2:3":  true,
	"3:2":  true,
	"3:4":  true,
	"4:3":  true,
	"9:16": true,
	"16:9": true,
}

var exactImageSizeToAspectRatio = map[string]string{
	"auto":      "1:1",
	"256x256":   "1:1",
	"512x512":   "1:1",
	"1024x1024": "1:1",
	"1536x1024": "3:2",
	"1024x1536": "2:3",
	"1792x1024": "16:9",
	"1920x1080": "16:9",
	"1280x720":  "16:9",
	"1024x1792": "9:16",
	"1080x1920": "9:16",
	"720x1280":  "9:16",
	"1280x960":  "4:3",
	"1024x768":  "4:3",
	"800x600":   "4:3",
	"960x1280":  "3:4",
	"768x1024":  "3:4",
	"600x800":   "3:4",
}

func mapImageAspectRatio(aspectRatio, size string) (string, error) {
	ar := strings.TrimSpace(aspectRatio)
	if ar != "" {
		if !allowedImageAspectRatioSet[ar] {
			return "", fmt.Errorf("unsupported aspect_ratio %q; allowed values: 1:1, 2:3, 3:2, 3:4, 4:3, 9:16, 16:9", ar)
		}
		return ar, nil
	}
	sz := strings.ToLower(strings.TrimSpace(size))
	if sz == "" {
		return "1:1", nil
	}
	if mapped, ok := exactImageSizeToAspectRatio[sz]; ok {
		return mapped, nil
	}
	parts := strings.Split(sz, "x")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid size %q; expected WxH (e.g. 1024x1024) or auto", size)
	}
	w, errW := strconv.Atoi(strings.TrimSpace(parts[0]))
	h, errH := strconv.Atoi(strings.TrimSpace(parts[1]))
	if errW != nil || errH != nil || w <= 0 || h <= 0 {
		return "", fmt.Errorf("invalid size %q; width and height must be positive integers", size)
	}
	target := float64(w) / float64(h)
	best := "1:1"
	bestDist := math.Inf(1)
	for _, spec := range allowedImageAspectRatios {
		dist := math.Abs(math.Log(target / spec.ratio))
		if dist < bestDist {
			bestDist = dist
			best = spec.name
		}
	}
	return best, nil
}

func imageExtForSniffedMime(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "image/heic":
		return ".heic"
	case "image/heif":
		return ".heif"
	default:
		return ".png"
	}
}

func isGeneratedImageExt(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".heif":
		return true
	default:
		return false
	}
}

type imageGenManager struct {
	mu                sync.Mutex
	notifyCh          chan struct{}
	eng               *SharedAcpEngine
	inFlight          int
	maxConcurrency    int
	transitioning     bool
	idleTimer         *time.Timer
	genCount          int
	engineStartedAt   time.Time
	consecutiveErrors int
	engineGen         uint64
}

var GlobalImageGen = newImageGenManager()

func newImageGenManager() *imageGenManager {
	return &imageGenManager{
		eng: newAuxiliaryAcpEngine(ImageGenAgentName),
	}
}

func (m *imageGenManager) broadcastLocked() {
	if m.notifyCh != nil {
		close(m.notifyCh)
		m.notifyCh = nil
	}
}

func (m *imageGenManager) waitChanLocked() <-chan struct{} {
	if m.notifyCh == nil {
		m.notifyCh = make(chan struct{})
	}
	return m.notifyCh
}

// effectiveMaxConcurrency computes the process-wide concurrency ceiling for single-process ws-imagegen
// (default 2, dynamically tightened to 1 when cgroup usage >= warnPct) outside m.mu to avoid lock inversion.
func (m *imageGenManager) effectiveMaxConcurrency() int {
	capVal := imageGenMaxConcurrency
	m.mu.Lock()
	if m.maxConcurrency > 0 {
		capVal = m.maxConcurrency
	}
	m.mu.Unlock()
	if capVal < 1 {
		capVal = 1
	}
	mem := readCgroupMemoryStatus()
	warnPct, _ := effectiveMemPressureThresholds()
	if mem.HasLiveUsage && mem.LimitBytes > 0 && mem.UsagePct >= float64(warnPct) {
		if capVal > 1 {
			capVal = 1
		}
	}
	return capVal
}

func currentResolvedAgyBin() string {
	resolved, err := filepath.EvalSymlinks(PatchedAgyBin)
	if err != nil || resolved == "" {
		return PatchedAgyBin
	}
	return resolved
}

func (m *imageGenManager) binaryChangedLocked() bool {
	startedBin, startedHome, alive := m.eng.StartedBinaryAndHome()
	if !alive {
		return false
	}
	if startedBin != "" && currentResolvedAgyBin() != startedBin {
		return true
	}
	if startedHome != "" && PureApiHome != "" && startedHome != PureApiHome {
		return true
	}
	return false
}

func (m *imageGenManager) shouldRecycleLocked() (bool, string) {
	if imageGenMaxGenerations > 0 && m.genCount >= imageGenMaxGenerations {
		return true, "gen_count"
	}
	if imageGenMaxEngineAge > 0 && !m.engineStartedAt.IsZero() && time.Since(m.engineStartedAt) >= imageGenMaxEngineAge {
		return true, "engine_age"
	}
	if imageGenMaxConsecErrors > 0 && m.consecutiveErrors >= imageGenMaxConsecErrors {
		return true, "consecutive_errors"
	}
	return false, ""
}

func effectiveImageGenIdleTTL() time.Duration {
	base := imageGenIdleTTL
	if base <= 0 {
		return 0
	}
	mem := readCgroupMemoryStatus()
	if mem.HasLiveUsage && mem.LimitBytes > 0 {
		warnPct, highPct := effectiveMemPressureThresholds()
		if mem.UsagePct >= float64(highPct) {
			return 0
		}
		if mem.UsagePct >= float64(warnPct) {
			warnTTL := imageGenWarnIdleTTL
			if warnTTL < base {
				return warnTTL
			}
		}
	}
	return base
}

// cleanupImageGenConversationsDir removes historical one-shot SQLite conversation files under
// <PureApiHome>/antigravity-cli-imagegen/conversations/ once ws-imagegen has completely exited.
func cleanupImageGenConversationsDir() {
	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		return
	}
	appDir := filepath.Join(PureApiHome, ImageGenAppDataDirName)
	evalAppDir, err := filepath.EvalSymlinks(appDir)
	if err != nil || evalAppDir == "" || isWindowsCDrivePath(evalAppDir) {
		return
	}
	convDir := filepath.Join(evalAppDir, "conversations")
	fi, err := os.Lstat(convDir)
	if err != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return
	}
	evalConvDir, err := filepath.EvalSymlinks(convDir)
	if err != nil || evalConvDir == "" || isWindowsCDrivePath(evalConvDir) {
		return
	}
	rel, err := filepath.Rel(evalAppDir, evalConvDir)
	if err != nil || rel != "conversations" {
		return
	}
	entries, err := os.ReadDir(evalConvDir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		name := ent.Name()
		low := strings.ToLower(name)
		if !strings.HasSuffix(low, ".db") &&
			!strings.HasSuffix(low, ".db-wal") &&
			!strings.HasSuffix(low, ".db-shm") &&
			!strings.HasSuffix(low, ".db-journal") {
			continue
		}
		child := filepath.Join(evalConvDir, name)
		cfi, cErr := os.Lstat(child)
		if cErr != nil || cfi.Mode()&os.ModeSymlink != 0 || !cfi.Mode().IsRegular() {
			continue
		}
		_ = os.Remove(child)
	}
}

// stopEngineLocked stops the ws-imagegen process, cleans historical conversations/*.db* files once
// exited, resets slow-leak recycle counters, and broadcasts to waiters.
// Caller MUST hold m.mu and ensure !m.transitioning. Returns with m.mu unlocked.
func (m *imageGenManager) stopEngineLocked(logCat, logMsg string, logMeta map[string]any) {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.transitioning = true
	eng := m.eng
	m.mu.Unlock()

	eng.StopEngine()
	cleanupImageGenConversationsDir()
	if logCat != "" {
		LogEvent(logCat, logMsg, logMeta)
	}

	m.mu.Lock()
	m.genCount = 0
	m.engineStartedAt = time.Time{}
	m.consecutiveErrors = 0
	m.engineGen = 0
	m.transitioning = false
	m.broadcastLocked()
	m.mu.Unlock()
}

func (m *imageGenManager) IsWarm() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	warm := m.eng != nil && m.eng.IsAlive()
	m.mu.Unlock()
	return warm
}

func (m *imageGenManager) acquire(ctx context.Context) (*SharedAcpEngine, func(bool), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := circuitError(); err != nil {
		return nil, nil, err
	}
	maxConc := m.effectiveMaxConcurrency()
	m.mu.Lock()
	for {
		for {
			if err := ctx.Err(); err != nil {
				m.mu.Unlock()
				return nil, nil, err
			}
			waitNeeded := false
			if m.transitioning {
				waitNeeded = true
			} else if m.inFlight > 0 {
				needRecycle, _ := m.shouldRecycleLocked()
				if needRecycle || m.binaryChangedLocked() || m.inFlight >= maxConc {
					waitNeeded = true
				}
			}
			if !waitNeeded {
				break
			}
			ch := m.waitChanLocked()
			m.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			maxConc = m.effectiveMaxConcurrency()
			m.mu.Lock()
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return nil, nil, err
		}
		if err := circuitError(); err != nil {
			m.mu.Unlock()
			return nil, nil, err
		}
		if m.idleTimer != nil {
			m.idleTimer.Stop()
			m.idleTimer = nil
		}
		if m.inFlight == 0 && m.binaryChangedLocked() {
			m.stopEngineLocked("ACP_IMAGEGEN_BINARY_SWITCH", "Restarting ws-imagegen engine for updated agy binary", nil)
			maxConc = m.effectiveMaxConcurrency()
			m.mu.Lock()
			continue
		}
		if m.inFlight == 0 {
			if needRecycle, reason := m.shouldRecycleLocked(); needRecycle {
				if m.eng.IsAlive() {
					meta := map[string]any{
						"reason":            reason,
						"genCount":          m.genCount,
						"consecutiveErrors": m.consecutiveErrors,
					}
					m.stopEngineLocked("ACP_IMAGEGEN_RECYCLE_STOP", "Recycling ws-imagegen engine before next generation", meta)
					maxConc = m.effectiveMaxConcurrency()
					m.mu.Lock()
					continue
				}
				m.genCount = 0
				m.engineStartedAt = time.Time{}
				m.consecutiveErrors = 0
				m.engineGen = 0
			}
		}
		m.inFlight++
		eng := m.eng
		m.mu.Unlock()

		if err := eng.EnsureStarted(); err != nil {
			m.release(false, false)
			return nil, nil, err
		}
		if err := ctx.Err(); err != nil {
			m.release(false, false)
			return nil, nil, err
		}

		curGen := eng.CurrentGen()
		m.mu.Lock()
		if m.engineGen != curGen || m.engineStartedAt.IsZero() {
			m.engineGen = curGen
			m.engineStartedAt = time.Now()
			m.genCount = 0
			m.consecutiveErrors = 0
		}
		m.mu.Unlock()

		var once sync.Once
		return eng, func(succeeded bool) { once.Do(func() { m.release(succeeded, true) }) }, nil
	}
}

func (m *imageGenManager) release(succeeded bool, ranAttempt bool) {
	m.mu.Lock()
	if m.inFlight > 0 {
		m.inFlight--
	}
	if ranAttempt {
		m.genCount++
		if succeeded {
			m.consecutiveErrors = 0
		} else {
			m.consecutiveErrors++
		}
	}
	if m.inFlight == 0 {
		if !m.transitioning && m.binaryChangedLocked() {
			m.stopEngineLocked("ACP_IMAGEGEN_BINARY_SWITCH", "Stopped drained ws-imagegen engine for new agy binary", nil)
			return
		}
		if !m.transitioning {
			if needRecycle, reason := m.shouldRecycleLocked(); needRecycle {
				if m.eng.IsAlive() {
					meta := map[string]any{
						"reason":            reason,
						"genCount":          m.genCount,
						"consecutiveErrors": m.consecutiveErrors,
					}
					m.stopEngineLocked("ACP_IMAGEGEN_RECYCLE_STOP", "Drained and stopped ws-imagegen engine to reclaim single-process memory", meta)
					return
				}
				m.genCount = 0
				m.engineStartedAt = time.Time{}
				m.consecutiveErrors = 0
				m.engineGen = 0
			}
		}
		if !m.transitioning && m.eng.IsAlive() {
			ttl := effectiveImageGenIdleTTL()
			if ttl <= 0 {
				m.stopEngineLocked("ACP_IMAGEGEN_PRESSURE_STOP", "Stopped ws-imagegen engine immediately under cgroup memory pressure", nil)
				return
			}
			if m.idleTimer != nil {
				m.idleTimer.Stop()
			}
			m.idleTimer = time.AfterFunc(ttl, func() { _ = m.StopIfIdle() })
		}
	}
	m.broadcastLocked()
	m.mu.Unlock()
}

func (m *imageGenManager) StopIfIdle() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	if m.transitioning || m.inFlight > 0 {
		m.mu.Unlock()
		return false
	}
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	if !m.eng.IsAlive() {
		m.mu.Unlock()
		return false
	}
	m.stopEngineLocked("ACP_IMAGEGEN_IDLE_STOP", "Stopped idle ws-imagegen engine", nil)
	return true
}

func (m *imageGenManager) StopIfBinaryChanged() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.transitioning || m.inFlight > 0 || !m.binaryChangedLocked() {
		m.mu.Unlock()
		return
	}
	m.stopEngineLocked("ACP_IMAGEGEN_BINARY_SWITCH", "Stopped idle ws-imagegen engine for new agy binary", nil)
}

func (m *imageGenManager) Stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	for m.transitioning {
		ch := m.waitChanLocked()
		m.mu.Unlock()
		<-ch
		m.mu.Lock()
	}
	m.stopEngineLocked("", "", nil)
}

// StopImageGenEngine shuts down the dedicated lazy ws-imagegen ACP engine cleanly.
func StopImageGenEngine() {
	if GlobalImageGen != nil {
		GlobalImageGen.Stop()
	}
}

// StopImageGenEngineIfIdle stops the dedicated ws-imagegen ACP engine only when idle.
func StopImageGenEngineIfIdle() bool {
	if GlobalImageGen != nil {
		return GlobalImageGen.StopIfIdle()
	}
	return false
}

// StopImageGenEngineIfBinaryChanged stops an idle ws-imagegen engine when AGY_ACP_BIN symlink changed.
func StopImageGenEngineIfBinaryChanged() {
	if GlobalImageGen != nil {
		GlobalImageGen.StopIfBinaryChanged()
	}
}

type imageGenSessionListener struct {
	eng            *SharedAcpEngine
	start          time.Time
	lastActivityNs atomic.Int64

	mu           sync.Mutex
	gen          uint64
	savedPath    string
	toolFailed   bool
	toolErrors   []string
	agentTexts   []string
	engineExited bool
}

var _ acp.SessionListener = (*imageGenSessionListener)(nil)

func (l *imageGenSessionListener) touch() {
	if l == nil || l.start.IsZero() {
		return
	}
	l.lastActivityNs.Store(int64(time.Since(l.start)))
}

func (l *imageGenSessionListener) SetEngineGen(gen uint64) {
	l.mu.Lock()
	l.gen = gen
	l.mu.Unlock()
}

func extractStringsFromAny(v any, out *[]string) {
	switch x := v.(type) {
	case string:
		if s := strings.TrimSpace(x); s != "" {
			*out = append(*out, s)
		}
	case []any:
		for _, item := range x {
			extractStringsFromAny(item, out)
		}
	case map[string]any:
		for _, key := range []string{"text", "content", "output", "rawOutput", "message", "error", "title"} {
			if sub, ok := x[key]; ok {
				extractStringsFromAny(sub, out)
			}
		}
	}
}

var savedImagePathRe = regexp.MustCompile(`(?i)generated image is saved at[ \t]+([^\r\n]+)`)

func extractSavedImagePathFromText(text string) string {
	matches := savedImagePathRe.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return ""
	}
	rest := matches[len(matches)-1][1]
	if dotSpace := strings.Index(rest, ". "); dotSpace >= 0 {
		rest = rest[:dotSpace]
	}
	rest = strings.TrimSpace(rest)
	rest = strings.TrimRight(rest, ".`\"'")
	rest = strings.TrimLeft(rest, "`\"'")
	return strings.TrimSpace(rest)
}

func (l *imageGenSessionListener) HandleSessionUpdate(u map[string]any) {
	l.touch()
	updType, _ := u["sessionUpdate"].(string)
	switch updType {
	case "tool_call_update", "tool_call":
		status, _ := u["status"].(string)
		statusLow := strings.ToLower(strings.TrimSpace(status))
		var texts []string
		extractStringsFromAny(u["content"], &texts)
		extractStringsFromAny(u["rawOutput"], &texts)
		extractStringsFromAny(u["error"], &texts)
		l.mu.Lock()
		for _, t := range texts {
			if p := extractSavedImagePathFromText(t); p != "" {
				l.savedPath = p
			}
			if statusLow == "failed" || statusLow == "error" || statusLow == "cancelled" {
				l.toolFailed = true
				l.toolErrors = append(l.toolErrors, t)
			}
		}
		if (statusLow == "failed" || statusLow == "error") && len(texts) == 0 {
			l.toolFailed = true
		}
		l.mu.Unlock()
	case "agent_message_chunk":
		if c, ok := u["content"].(map[string]any); ok {
			if t, okT := c["text"].(string); okT && t != "" {
				l.mu.Lock()
				l.agentTexts = append(l.agentTexts, t)
				l.mu.Unlock()
			}
		}
	}
}

func (l *imageGenSessionListener) HandlePermissionRequest(rpcID any, options []acp.PermissionOption, tc map[string]any) {
	// generate_image requires no permission; reject any unexpected permission prompt safely.
	replyPermissionOutcomeWithEngine(l.eng, rpcID, options, false)
}

func (l *imageGenSessionListener) OnEngineExited(sid string, exitedGen uint64) {
	l.mu.Lock()
	l.engineExited = true
	l.mu.Unlock()
}

type imageGenError struct {
	StatusCode int
	ErrType    string
	ErrCode    string
	Message    string
}

func (e *imageGenError) Error() string { return e.Message }

func classifyImageGenError(rpcErr error, toolErrors []string, agentText string) *imageGenError {
	var parts []string
	if rpcErr != nil {
		parts = append(parts, redactSecretsInString(rpcErr.Error()))
	}
	for _, te := range toolErrors {
		if s := strings.TrimSpace(redactSecretsInString(te)); s != "" {
			parts = append(parts, s)
		}
	}
	if s := strings.TrimSpace(redactSecretsInString(agentText)); s != "" && len(parts) == 0 {
		parts = append(parts, s)
	}
	combined := strings.Join(parts, "; ")
	if combined == "" {
		combined = "image generation failed without producing an image artifact"
	}
	low := strings.ToLower(combined)

	if IsTosViolationError(combined) {
		TripTosCircuitBreaker(combined)
		return &imageGenError{
			StatusCode: http.StatusServiceUnavailable,
			ErrType:    "tos_circuit_breaker",
			ErrCode:    "tos_circuit_breaker",
			Message:    "Antigravity ToS circuit breaker tripped: " + combined,
		}
	}
	if strings.Contains(low, "image generation quota exceeded") ||
		strings.Contains(low, "resource_exhausted") ||
		strings.Contains(low, "resourceexhausted") ||
		strings.Contains(low, "429") ||
		strings.Contains(low, "rate limit") ||
		strings.Contains(low, "rate_limit") ||
		strings.Contains(low, "quota") {
		return &imageGenError{
			StatusCode: http.StatusTooManyRequests,
			ErrType:    "rate_limit_error",
			ErrCode:    "rate_limit_error",
			Message:    combined,
		}
	}
	if isUpstreamPolicyOrSafetyBlock(combined, true) ||
		strings.Contains(low, "safety") ||
		strings.Contains(low, "content policy") ||
		strings.Contains(low, "content_policy") ||
		strings.Contains(low, "prohibited") ||
		strings.Contains(low, "policy violation") ||
		strings.Contains(low, "blocked by") ||
		strings.Contains(low, "unsafe") ||
		strings.Contains(low, "cannot generate") ||
		strings.Contains(low, "can't generate") {
		return &imageGenError{
			StatusCode: http.StatusBadRequest,
			ErrType:    "content_policy_violation",
			ErrCode:    "content_policy_violation",
			Message:    combined,
		}
	}
	if strings.Contains(low, "model_capacity_exhausted") ||
		strings.Contains(low, "no capacity") ||
		strings.Contains(low, "overloaded") ||
		strings.Contains(low, "503") {
		return &imageGenError{
			StatusCode: http.StatusServiceUnavailable,
			ErrType:    "overloaded_error",
			ErrCode:    "overloaded_error",
			Message:    combined,
		}
	}
	if errors.Is(rpcErr, context.Canceled) {
		return &imageGenError{
			StatusCode: 499,
			ErrType:    "request_cancelled",
			ErrCode:    "request_cancelled",
			Message:    "request cancelled by client",
		}
	}
	if errors.Is(rpcErr, context.DeadlineExceeded) {
		return &imageGenError{
			StatusCode: http.StatusGatewayTimeout,
			ErrType:    "timeout_error",
			ErrCode:    "timeout_error",
			Message:    "image generation timed out",
		}
	}
	return &imageGenError{
		StatusCode: http.StatusBadGateway,
		ErrType:    "api_error",
		ErrCode:    "image_generation_failed",
		Message:    combined,
	}
}

func validateBrainImagePath(evalBrainDir, candidate string) (string, error) {
	if !filepath.IsAbs(candidate) {
		return "", errors.New("generated image path is not absolute")
	}
	clean := filepath.Clean(candidate)
	if isWindowsCDrivePath(clean) {
		return "", errors.New("generated image path on C drive rejected")
	}
	fi, err := os.Lstat(clean)
	if err != nil {
		return "", err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("generated image symlink rejected")
	}
	if !fi.Mode().IsRegular() {
		return "", errors.New("generated image path is not a regular file")
	}
	if fi.Size() <= 0 {
		return "", errors.New("generated image file is empty")
	}
	if fi.Size() > maxGeneratedImageBytes {
		return "", fmt.Errorf("generated image (%d bytes) exceeds %d byte limit", fi.Size(), maxGeneratedImageBytes)
	}
	evalPath, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", err
	}
	if isWindowsCDrivePath(evalPath) {
		return "", errors.New("resolved generated image path on C drive rejected")
	}
	rel, err := filepath.Rel(evalBrainDir, evalPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", errors.New("generated image path escapes session brain directory")
	}
	return evalPath, nil
}

func isSafeBrainSessionID(sid string) bool {
	return sid != "" && sid != "." && sid != ".." && filepath.Base(sid) == sid && !strings.ContainsAny(sid, `/\`)
}

// resolveVerifiedSessionBrainDir resolves <PureApiHome>/antigravity-cli-imagegen/brain first and verifies
// via os.Lstat that <brainRoot>/<sid> is a real non-symlink directory strictly inside brainRoot.
func resolveVerifiedSessionBrainDir(sid string) (string, error) {
	if !isSafeBrainSessionID(sid) {
		return "", errors.New("invalid ACP session id for image artifact resolution")
	}
	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		return "", errors.New("PureApiHome is unset or points to restricted path")
	}
	brainRoot := filepath.Join(PureApiHome, ImageGenAppDataDirName, "brain")
	evalBrainRoot, err := filepath.EvalSymlinks(brainRoot)
	if err != nil {
		return "", fmt.Errorf("brain root directory unavailable: %w", err)
	}
	if isWindowsCDrivePath(evalBrainRoot) {
		return "", errors.New("brain root resolved to restricted C drive path")
	}
	sidDir := filepath.Join(evalBrainRoot, sid)
	fi, err := os.Lstat(sidDir)
	if err != nil {
		return "", fmt.Errorf("session brain directory unavailable: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("session brain directory symlink rejected")
	}
	if !fi.IsDir() {
		return "", errors.New("session brain path is not a directory")
	}
	evalBrainDir, err := filepath.EvalSymlinks(sidDir)
	if err != nil {
		return "", fmt.Errorf("session brain directory eval failed: %w", err)
	}
	if isWindowsCDrivePath(evalBrainDir) {
		return "", errors.New("session brain directory resolved to restricted C drive path")
	}
	rel, err := filepath.Rel(evalBrainRoot, evalBrainDir)
	if err != nil || rel != sid {
		return "", errors.New("session brain directory escapes brain root")
	}
	return evalBrainDir, nil
}

// removeTreeNoFollow removes regular files, symlinks (unlinking only the link itself, never
// descending into symlink targets), and real subdirectories up to maxDepth inside a verified dir.
func removeTreeNoFollow(dir string, maxDepth int) {
	if dir == "" || isWindowsCDrivePath(dir) || maxDepth < 0 {
		return
	}
	fi, err := os.Lstat(dir)
	if err != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return
	}
	entries, err := os.ReadDir(dir)
	if err == nil {
		for _, ent := range entries {
			child := filepath.Join(dir, ent.Name())
			cfi, cErr := os.Lstat(child)
			if cErr != nil {
				continue
			}
			if cfi.Mode()&os.ModeSymlink != 0 || !cfi.IsDir() {
				_ = os.Remove(child)
				continue
			}
			if maxDepth > 0 {
				removeTreeNoFollow(child, maxDepth-1)
			}
		}
	}
	_ = os.Remove(dir)
}

func cleanupSessionBrainDir(sid string) {
	evalBrainDir, err := resolveVerifiedSessionBrainDir(sid)
	if err != nil || evalBrainDir == "" {
		return
	}
	removeTreeNoFollow(evalBrainDir, 3)
}

func readVerifiedBrainImageFile(resolvedPath string) ([]byte, error) {
	if isWindowsCDrivePath(resolvedPath) {
		return nil, errors.New("generated image path on C drive rejected")
	}
	f, err := os.OpenFile(resolvedPath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("opened generated image fd is not a regular file")
	}
	if fi.Size() <= 0 {
		return nil, errors.New("generated image file is empty")
	}
	if fi.Size() > maxGeneratedImageBytes {
		return nil, fmt.Errorf("generated image (%d bytes) exceeds %d byte limit", fi.Size(), maxGeneratedImageBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxGeneratedImageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > maxGeneratedImageBytes {
		return nil, errors.New("generated image artifact has invalid size")
	}
	return raw, nil
}

func resolveGeneratedImageInBrainDir(sid, reportedPath string, turnStart time.Time) (evalBrainDir, resolvedPath string, err error) {
	evalBrainDir, err = resolveVerifiedSessionBrainDir(sid)
	if err != nil {
		if reportedPath != "" {
			LogEvent("IMAGEGEN_PATH_REJECTED", "Rejected generated image path: session brain directory invalid", map[string]any{
				"sessionId": sid,
				"reason":    err.Error(),
			})
		}
		return "", "", err
	}
	if reportedPath != "" {
		validPath, vErr := validateBrainImagePath(evalBrainDir, reportedPath)
		if vErr == nil {
			return evalBrainDir, validPath, nil
		}
		// Reject security violations (symlink escape / traversal) immediately without falling back.
		if !errors.Is(vErr, os.ErrNotExist) {
			LogEvent("IMAGEGEN_PATH_REJECTED", "Rejected unsafe generated image path", map[string]any{
				"sessionId": sid,
				"reason":    vErr.Error(),
			})
			return evalBrainDir, "", fmt.Errorf("unsafe generated image path rejected: %w", vErr)
		}
	}

	// Fallback scan of <PureApiHome>/antigravity-cli/brain/<sessionId>/ for newest image file (mtime >= turnStart).
	entries, rErr := os.ReadDir(evalBrainDir)
	if rErr != nil {
		return evalBrainDir, "", rErr
	}
	cutoff := turnStart.Add(-2 * time.Second)
	var bestPath string
	var bestMod time.Time
	for _, ent := range entries {
		if ent.IsDir() || ent.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !isGeneratedImageExt(ent.Name()) {
			continue
		}
		cand := filepath.Join(evalBrainDir, ent.Name())
		fi, sErr := os.Lstat(cand)
		if sErr != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsRegular() {
			continue
		}
		if fi.ModTime().Before(cutoff) {
			continue
		}
		validPath, vErr := validateBrainImagePath(evalBrainDir, cand)
		if vErr != nil {
			continue
		}
		if bestPath == "" || fi.ModTime().After(bestMod) || (fi.ModTime().Equal(bestMod) && validPath > bestPath) {
			bestPath = validPath
			bestMod = fi.ModTime()
		}
	}
	if bestPath == "" {
		return evalBrainDir, "", errors.New("no generated image artifact found in session brain directory")
	}
	return evalBrainDir, bestPath, nil
}

func buildImageGenPrompt(prompt, imageName, aspectRatio string, inputPaths []string) string {
	quotedPrompt, _ := json.Marshal(prompt)
	var sb strings.Builder
	sb.WriteString("Call the `generate_image` tool exactly once with these parameters:\n")
	sb.WriteString("- Prompt: ")
	sb.Write(quotedPrompt)
	sb.WriteString("\n- ImageName: ")
	sb.WriteString(strconv.Quote(imageName))
	sb.WriteString("\n- AspectRatio: ")
	sb.WriteString(strconv.Quote(aspectRatio))
	if len(inputPaths) > 0 {
		quotedPaths, _ := json.Marshal(inputPaths)
		sb.WriteString("\n- ImagePaths: ")
		sb.Write(quotedPaths)
		sb.WriteString("\nUse the exact local absolute file paths listed in ImagePaths as reference/input images for editing.")
	}
	sb.WriteString("\nExecute generate_image now without asking questions or adding commentary.")
	return sb.String()
}

type generatedImageResult struct {
	B64  string
	Mime string
}

func normalizeImageOutputFormat(raw string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "", "jpeg", "jpg":
		return "jpeg", nil
	case "png":
		return "png", nil
	default:
		return "", fmt.Errorf("unsupported output_format %q; allowed values: jpeg, png", raw)
	}
}

func transcodeToPNGSerial(raw []byte, sniffedMime string) ([]byte, error) {
	if sniffedMime == "image/png" {
		return raw, nil
	}
	imageTranscodeMu.Lock()
	defer imageTranscodeMu.Unlock()

	var (
		img image.Image
		err error
	)
	if sniffedMime == "image/jpeg" {
		img, err = jpeg.Decode(bytes.NewReader(raw))
	} else {
		img, _, err = image.Decode(bytes.NewReader(raw))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to decode generated %s for png transcode: %w", sniffedMime, err)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		img = nil
		return nil, fmt.Errorf("failed to encode generated image as png: %w", err)
	}
	img = nil
	return buf.Bytes(), nil
}

func (m *imageGenManager) generateOne(ctx context.Context, prompt, imageName, aspectRatio string, inputPaths []string, outputFormat string) (generatedImageResult, *imageGenError) {
	eng, release, err := m.acquire(ctx)
	if err != nil {
		return generatedImageResult{}, classifyImageGenError(err, nil, "")
	}
	genOK := false
	defer func() {
		release(genOK)
	}()

	rawNew, err := eng.RPC("session/new", map[string]any{
		"cwd":        DefaultCwd,
		"mcpServers": []any{},
	}, 30*time.Second)
	if err != nil {
		return generatedImageResult{}, classifyImageGenError(err, nil, "")
	}
	var newResp struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(rawNew, &newResp); err != nil || strings.TrimSpace(newResp.SessionID) == "" {
		return generatedImageResult{}, &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "session_init_failed",
			Message:    "ws-imagegen session/new did not return a sessionId",
		}
	}
	sid := strings.TrimSpace(newResp.SessionID)
	if !isSafeBrainSessionID(sid) {
		_ = eng.CloseSession(sid)
		return generatedImageResult{}, &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "session_init_failed",
			Message:    "ws-imagegen session/new returned invalid sessionId",
		}
	}

	turnStart := time.Now()
	listener := &imageGenSessionListener{eng: eng, start: turnStart}
	eng.RegisterListener(sid, listener)
	defer func() {
		_ = eng.CloseSession(sid)
		eng.UnregisterSession(sid)
		cleanupSessionBrainDir(sid)
	}()

	// Pin cheap outer chat model gemini-3.8-flash (low) so built-in generate_image runs with 0 monitor calls.
	curOpts := parseConfigOptionValues(rawNew)
	if curOpts == nil || curOpts["model"] != imageGenOuterModel {
		ackRaw, errM := setAndVerifySessionConfigOptionWithRaw(eng, sid, "model", imageGenOuterModel, configOptionTimeout)
		if errM != nil {
			return generatedImageResult{}, classifyImageGenError(errM, nil, "")
		}
		curOpts = parseConfigOptionValues(ackRaw)
	}
	if curOpts == nil || curOpts["thought_level"] != imageGenOuterTier {
		if errT := setAndVerifySessionConfigOptionWith(eng, sid, "thought_level", imageGenOuterTier, configOptionTimeout); errT != nil {
			return generatedImageResult{}, classifyImageGenError(errT, nil, "")
		}
	}

	promptText := buildImageGenPrompt(prompt, imageName, aspectRatio, inputPaths)
	_, rpcErr := eng.RPCWithActivity(ctx, "session/prompt", map[string]any{
		"sessionId": sid,
		"prompt": []map[string]any{
			{"type": "text", "text": promptText},
		},
	}, imageGenIdleTimeout, imageGenMaxDuration, &listener.lastActivityNs, turnStart)
	if rpcErr != nil {
		eng.CancelSession(sid)
		listener.mu.Lock()
		toolErrs := append([]string(nil), listener.toolErrors...)
		agentTxt := strings.Join(listener.agentTexts, "")
		listener.mu.Unlock()
		return generatedImageResult{}, classifyImageGenError(rpcErr, toolErrs, agentTxt)
	}

	listener.mu.Lock()
	reportedPath := listener.savedPath
	toolFailed := listener.toolFailed
	toolErrs := append([]string(nil), listener.toolErrors...)
	agentTxt := strings.Join(listener.agentTexts, "")
	listener.mu.Unlock()

	if toolFailed && reportedPath == "" {
		return generatedImageResult{}, classifyImageGenError(nil, toolErrs, agentTxt)
	}

	_, resolvedPath, resErr := resolveGeneratedImageInBrainDir(sid, reportedPath, turnStart)
	if resErr != nil {
		if len(toolErrs) > 0 || strings.TrimSpace(agentTxt) != "" {
			return generatedImageResult{}, classifyImageGenError(resErr, toolErrs, agentTxt)
		}
		return generatedImageResult{}, &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_artifact_error",
			Message:    resErr.Error(),
		}
	}

	rawBytes, readErr := readVerifiedBrainImageFile(resolvedPath)
	if readErr != nil {
		return generatedImageResult{}, &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_read_failed",
			Message:    "failed to read generated image artifact: " + readErr.Error(),
		}
	}
	sniffedMime, ok := sniffAllowedImageMime(rawBytes)
	if !ok {
		return generatedImageResult{}, &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_invalid_format",
			Message:    "generated image artifact failed image magic-byte verification",
		}
	}
	if outputFormat == "png" && sniffedMime != "image/png" {
		pngBytes, tcErr := transcodeToPNGSerial(rawBytes, sniffedMime)
		rawBytes = nil
		if tcErr != nil {
			return generatedImageResult{}, &imageGenError{
				StatusCode: http.StatusBadGateway,
				ErrType:    "api_error",
				ErrCode:    "image_transcode_failed",
				Message:    tcErr.Error(),
			}
		}
		rawBytes = pngBytes
		sniffedMime = "image/png"
	}
	b64 := base64.StdEncoding.EncodeToString(rawBytes)
	rawBytes = nil
	genOK = true
	return generatedImageResult{B64: b64, Mime: sniffedMime}, nil
}

func writeImageAPIError(w http.ResponseWriter, r *http.Request, statusCode int, errType, errCode, msg string) {
	if r != nil {
		recordPrimaryDirectOutcome(r, statusCode, errType, "", msg)
	}
	if statusCode == 499 {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    errType,
			"param":   nil,
			"code":    errCode,
		},
	})
}

type validatedImageGenParams struct {
	Prompt       string
	Model        string
	N            int
	AspectRatio  string
	OutputFormat string
}

func parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, outputFormat string, nVal int, hasN bool) (validatedImageGenParams, error) {
	p := strings.TrimSpace(prompt)
	if p == "" {
		return validatedImageGenParams{}, errors.New("prompt is required and must be non-empty")
	}
	if len(p) > maxImageGenPromptChars {
		return validatedImageGenParams{}, fmt.Errorf("prompt length (%d) exceeds %d character limit", len(p), maxImageGenPromptChars)
	}
	n := 1
	if hasN {
		if nVal < 1 || nVal > maxImageGenCount {
			return validatedImageGenParams{}, fmt.Errorf("n must be an integer between 1 and %d", maxImageGenCount)
		}
		n = nVal
	}
	rf := strings.ToLower(strings.TrimSpace(responseFormat))
	if rf != "" && rf != "b64_json" {
		return validatedImageGenParams{}, fmt.Errorf("unsupported response_format %q; only \"b64_json\" is supported", responseFormat)
	}
	outFmt, errFmt := normalizeImageOutputFormat(outputFormat)
	if errFmt != nil {
		return validatedImageGenParams{}, errFmt
	}
	ar, err := mapImageAspectRatio(aspectRatio, size)
	if err != nil {
		return validatedImageGenParams{}, err
	}
	m := strings.TrimSpace(model)
	if m == "" {
		m = imageGenServedModel
	}
	return validatedImageGenParams{
		Prompt:       p,
		Model:        m,
		N:            n,
		AspectRatio:  ar,
		OutputFormat: outFmt,
	}, nil
}

func parseOptionalJSONString(body map[string]any, key string) (string, error) {
	raw, exists := body[key]
	if !exists || raw == nil {
		return "", nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return s, nil
}

func parseOptionalJSONInt(body map[string]any, key string) (int, bool, error) {
	raw, exists := body[key]
	if !exists || raw == nil {
		return 0, false, nil
	}
	switch v := raw.(type) {
	case json.Number:
		i, err := v.Int64()
		if err != nil {
			return 0, true, fmt.Errorf("%s must be an integer", key)
		}
		return int(i), true, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, true, fmt.Errorf("%s must be an integer", key)
		}
		return int(v), true, nil
	case int:
		return v, true, nil
	case int64:
		return int(v), true, nil
	default:
		return 0, true, fmt.Errorf("%s must be an integer", key)
	}
}

func readGatewayJSONBody(w http.ResponseWriter, r *http.Request) ([]byte, int, string) {
	if preRead, ok := preReadBodyFromContext(r); ok {
		return preRead, 0, ""
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return nil, http.StatusRequestEntityTooLarge, "Request body exceeds maximum size"
		}
		return nil, http.StatusBadRequest, "Failed to read request body"
	}
	return raw, 0, ""
}

func effectiveImageGenBatchConcurrency(n int) int {
	if n <= 1 {
		return 1
	}
	conc := 2
	mem := readCgroupMemoryStatus()
	warnPct, _ := effectiveMemPressureThresholds()
	if mem.HasLiveUsage && mem.LimitBytes > 0 && mem.UsagePct >= float64(warnPct) {
		conc = 1
	}
	if conc > n {
		conc = n
	}
	return conc
}

// generateImageBatch runs n image generation turns with bounded concurrency (max 2, auto-serial under
// cgroup memory pressure), independent per-branch acquirePromptSlot, strict 0..n-1 ordering, and
// fail-fast sibling cancellation on first error or caller disconnect.
func generateImageBatch(ctx context.Context, reqID int64, params validatedImageGenParams, inputPaths []string) ([]generatedImageResult, *imageGenError) {
	if params.N <= 0 {
		return nil, &imageGenError{
			StatusCode: http.StatusBadRequest,
			ErrType:    "invalid_request_error",
			ErrCode:    "invalid_n",
			Message:    "n must be >= 1",
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, &imageGenError{
			StatusCode: 499,
			ErrType:    "request_cancelled",
			ErrCode:    "request_cancelled",
			Message:    "request cancelled",
		}
	}

	concurrency := effectiveImageGenBatchConcurrency(params.N)
	reqTag := fmt.Sprintf("r%d_%s", reqID, randomHex(4))
	batchCtx, cancelBatch := context.WithCancel(ctx)
	defer cancelBatch()

	results := make([]generatedImageResult, params.N)
	var (
		errMu    sync.Mutex
		firstErr *imageGenError
		nextIdx  atomic.Int32
	)
	recordErr := func(e *imageGenError) {
		if e == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil || (firstErr.StatusCode == 499 && e.StatusCode != 499) {
			firstErr = e
		}
		errMu.Unlock()
		cancelBatch()
	}

	worker := func() {
		for {
			if batchCtx.Err() != nil {
				return
			}
			idx := int(nextIdx.Add(1) - 1)
			if idx >= params.N {
				return
			}
			releaseSlot, slotErr := acquirePromptSlot(batchCtx)
			if slotErr != nil {
				recordErr(&imageGenError{
					StatusCode: 499,
					ErrType:    "request_cancelled",
					ErrCode:    "request_cancelled",
					Message:    "request cancelled while waiting for prompt slot",
				})
				return
			}
			imgName := fmt.Sprintf("img_%s_%d", reqTag, idx+1)
			res, imgErr := GlobalImageGen.generateOne(batchCtx, params.Prompt, imgName, params.AspectRatio, inputPaths, params.OutputFormat)
			releaseSlot()
			if imgErr != nil {
				recordErr(imgErr)
				return
			}
			results[idx] = res
		}
	}

	if concurrency == 1 {
		worker()
	} else {
		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker()
			}()
		}
		wg.Wait()
	}

	if firstErr != nil {
		clear(results)
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		clear(results)
		return nil, &imageGenError{
			StatusCode: 499,
			ErrType:    "request_cancelled",
			ErrCode:    "request_cancelled",
			Message:    "request cancelled",
		}
	}
	return results, nil
}

func (gs *GatewayServer) executeImageGenerationRequest(w http.ResponseWriter, r *http.Request, reqID int64, params validatedImageGenParams, inputPaths []string) {
	results, imgErr := generateImageBatch(r.Context(), reqID, params, inputPaths)
	if imgErr != nil {
		writeImageAPIError(w, r, imgErr.StatusCode, imgErr.ErrType, imgErr.ErrCode, imgErr.Message)
		return
	}

	data := make([]map[string]any, len(results))
	for i, res := range results {
		data[i] = map[string]any{
			"b64_json": res.B64,
		}
		results[i].B64 = ""
	}
	results = nil

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Agy-Served-Model", imageGenServedModel)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"created":       time.Now().Unix(),
		"output_format": params.OutputFormat,
		"data":          data,
	})
	clear(data)
}

func (gs *GatewayServer) handleImagesGenerations(w http.ResponseWriter, r *http.Request, reqID int64) {
	if tripped, reason := IsTosTripped(); tripped {
		writeImageAPIError(w, r, http.StatusServiceUnavailable, "tos_circuit_breaker", "tos_circuit_breaker",
			"Gateway halted by ToS circuit breaker (delete runtime/tos-tripped to reset): "+reason)
		return
	}
	rawBody, readStatus, readMsg := readGatewayJSONBody(w, r)
	if readStatus != 0 {
		clearPreReadBody(r)
		writeImageAPIError(w, r, readStatus, "invalid_request_error", "invalid_request", readMsg)
		return
	}
	if len(rawBody) == 0 {
		clearPreReadBody(r)
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", "Empty JSON request body")
		return
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		clearPreReadBody(r)
		rawBody = nil
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_json", "Malformed JSON request body: "+fmt.Sprint(err))
		return
	}
	clearPreReadBody(r)
	rawBody = nil
	prompt, _ := body["prompt"].(string)
	model, _ := body["model"].(string)
	size, _ := body["size"].(string)
	aspectRatio, _ := body["aspect_ratio"].(string)
	responseFormat, _ := body["response_format"].(string)
	outputFormat, errFmt := parseOptionalJSONString(body, "output_format")
	if errFmt != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_output_format", errFmt.Error())
		return
	}
	nVal, hasN, errN := parseOptionalJSONInt(body, "n")
	if errN != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_n", errN.Error())
		return
	}
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, outputFormat, nVal, hasN)
	if err != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	body = nil
	gs.executeImageGenerationRequest(w, r, reqID, params, nil)
}

type validatedEditImage struct {
	raw  []byte
	mime string
}

func decodeEditImageString(rawStr, fieldPath string) (validatedEditImage, error) {
	s := strings.TrimSpace(rawStr)
	if s == "" {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, fieldPath, "image entry is empty")
	}
	b64Payload := s
	if hasPrefixFold(s, "data:") {
		mime, isB64, payload, ok := parseDataURI(s)
		if !ok {
			return validatedEditImage{}, newMMError(http.StatusBadRequest, fieldPath, "malformed image data: URI")
		}
		if mime == "image/svg+xml" {
			return validatedEditImage{}, newMMError(http.StatusBadRequest, fieldPath, "image/svg+xml attachments are not supported")
		}
		if !strings.HasPrefix(mime, "image/") {
			return validatedEditImage{}, rejectUnsupportedMedia(mime, fieldPath)
		}
		if !isB64 {
			return validatedEditImage{}, newMMError(http.StatusBadRequest, fieldPath, "image data: URI must use ;base64 encoding")
		}
		b64Payload = payload
	}
	raw, _, err := decodeBoundedBase64(b64Payload, MaxImageBytes, http.StatusRequestEntityTooLarge, fieldPath, "image")
	if err != nil {
		return validatedEditImage{}, err
	}
	sniffed, ok := sniffAllowedImageMime(raw)
	if !ok || !allowedImageMimeMap[sniffed] {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, fieldPath, "unsupported or malformed image bytes; only PNG, JPEG, WEBP, GIF, HEIC, and HEIF magic headers are supported")
	}
	return validatedEditImage{raw: raw, mime: sniffed}, nil
}

func parseImageEditsMultipart(w http.ResponseWriter, r *http.Request) (validatedImageGenParams, []validatedEditImage, int, string) {
	if _, hasPre := preReadBodyFromContext(r); !hasPre {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		clearPreReadBody(r)
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return validatedImageGenParams{}, nil, http.StatusRequestEntityTooLarge, "Request body exceeds maximum size"
		}
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Failed to parse multipart form: " + err.Error()
	}
	clearPreReadBody(r)
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	prompt := r.FormValue("prompt")
	model := r.FormValue("model")
	size := r.FormValue("size")
	aspectRatio := r.FormValue("aspect_ratio")
	responseFormat := r.FormValue("response_format")
	outputFormat := r.FormValue("output_format")
	nStr := strings.TrimSpace(r.FormValue("n"))
	nVal := 1
	hasN := false
	if nStr != "" {
		v, err := strconv.Atoi(nStr)
		if err != nil {
			return validatedImageGenParams{}, nil, http.StatusBadRequest, "n must be an integer"
		}
		nVal = v
		hasN = true
	}
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, outputFormat, nVal, hasN)
	if err != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, err.Error()
	}

	var fhs []*multipart.FileHeader
	if r.MultipartForm != nil {
		for _, key := range []string{"image", "image[]"} {
			fhs = append(fhs, r.MultipartForm.File[key]...)
		}
	}
	if len(fhs) == 0 {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "at least one image file is required in 'image' or 'image[]'"
	}
	if len(fhs) > maxImageEditInputs {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("at most %d input images are supported", maxImageEditInputs)
	}

	limit := MaxImageBytes
	if limit <= 0 {
		limit = 10 * 1024 * 1024
	}
	imgs := make([]validatedEditImage, 0, len(fhs))
	for i, fh := range fhs {
		f, errO := fh.Open()
		if errO != nil {
			return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("failed to open uploaded image[%d]", i)
		}
		raw, errR := io.ReadAll(io.LimitReader(f, int64(limit)+1))
		_ = f.Close()
		if errR != nil {
			return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("failed to read uploaded image[%d]", i)
		}
		if len(raw) == 0 {
			return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("uploaded image[%d] is empty", i)
		}
		if len(raw) > limit {
			return validatedImageGenParams{}, nil, http.StatusRequestEntityTooLarge, fmt.Sprintf("uploaded image[%d] exceeds %d byte limit", i, limit)
		}
		sniffed, ok := sniffAllowedImageMime(raw)
		if !ok || !allowedImageMimeMap[sniffed] {
			return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("uploaded image[%d] failed PNG/JPEG/WEBP/GIF/HEIC/HEIF magic header verification", i)
		}
		imgs = append(imgs, validatedEditImage{raw: raw, mime: sniffed})
	}
	return params, imgs, 0, ""
}

func parseImageEditsJSON(w http.ResponseWriter, r *http.Request) (validatedImageGenParams, []validatedEditImage, int, string) {
	rawBody, readStatus, readMsg := readGatewayJSONBody(w, r)
	if readStatus != 0 {
		clearPreReadBody(r)
		return validatedImageGenParams{}, nil, readStatus, readMsg
	}
	if len(rawBody) == 0 {
		clearPreReadBody(r)
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Empty JSON request body"
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		clearPreReadBody(r)
		rawBody = nil
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Malformed JSON request body: " + fmt.Sprint(err)
	}
	clearPreReadBody(r)
	rawBody = nil
	prompt, _ := body["prompt"].(string)
	model, _ := body["model"].(string)
	size, _ := body["size"].(string)
	aspectRatio, _ := body["aspect_ratio"].(string)
	responseFormat, _ := body["response_format"].(string)
	outputFormat, errFmt := parseOptionalJSONString(body, "output_format")
	if errFmt != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, errFmt.Error()
	}
	nVal, hasN, errN := parseOptionalJSONInt(body, "n")
	if errN != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, errN.Error()
	}
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, outputFormat, nVal, hasN)
	if err != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, err.Error()
	}

	var rawImgs []string
	collectImgField := func(v any, field string) error {
		switch x := v.(type) {
		case nil:
			return nil
		case string:
			rawImgs = append(rawImgs, x)
			return nil
		case []any:
			for i, item := range x {
				s, ok := item.(string)
				if !ok {
					return fmt.Errorf("%s[%d] must be a base64 string or data URI", field, i)
				}
				rawImgs = append(rawImgs, s)
			}
			return nil
		default:
			return fmt.Errorf("%s must be a base64 string, data URI, or array of strings", field)
		}
	}
	if err := collectImgField(body["image"], "image"); err != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, err.Error()
	}
	if err := collectImgField(body["images"], "images"); err != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, err.Error()
	}
	if len(rawImgs) == 0 {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "at least one input image is required in 'image' or 'images'"
	}
	if len(rawImgs) > maxImageEditInputs {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, fmt.Sprintf("at most %d input images are supported", maxImageEditInputs)
	}
	imgs := make([]validatedEditImage, 0, len(rawImgs))
	for i, s := range rawImgs {
		vi, dErr := decodeEditImageString(s, fmt.Sprintf("images[%d]", i))
		if dErr != nil {
			status := http.StatusBadRequest
			var mmErr *MultimodalValidationError
			if errors.As(dErr, &mmErr) && mmErr != nil && mmErr.StatusCode > 0 {
				status = mmErr.StatusCode
			}
			return validatedImageGenParams{}, nil, status, dErr.Error()
		}
		imgs = append(imgs, vi)
	}
	return params, imgs, 0, ""
}

func stageValidatedEditImages(reqID int64, imgs []validatedEditImage) (tmpDir string, absPaths []string, err error) {
	if len(imgs) == 0 {
		return "", nil, nil
	}
	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		return "", nil, errors.New("PureApiHome is unset or points to restricted path")
	}
	tmpDir = filepath.Join(PureApiHome, "tmp", fmt.Sprintf("imagegen-%d-%s", reqID, randomHex(4)))
	if isWindowsCDrivePath(tmpDir) {
		return "", nil, errors.New("restricted temp path")
	}
	if mkErr := os.MkdirAll(tmpDir, 0700); mkErr != nil {
		return "", nil, fmt.Errorf("failed to prepare image edit workspace: %w", mkErr)
	}
	absPaths = make([]string, 0, len(imgs))
	for i := range imgs {
		ext := imageExtForSniffedMime(imgs[i].mime)
		p := filepath.Join(tmpDir, fmt.Sprintf("input_%d%s", i+1, ext))
		if wErr := os.WriteFile(p, imgs[i].raw, 0600); wErr != nil {
			for j := range imgs {
				imgs[j].raw = nil
			}
			_ = os.RemoveAll(tmpDir)
			return "", nil, fmt.Errorf("failed to stage input image: %w", wErr)
		}
		imgs[i].raw = nil
		absP, errA := filepath.Abs(p)
		if errA != nil {
			absP = p
		}
		absPaths = append(absPaths, absP)
	}
	return tmpDir, absPaths, nil
}

func (gs *GatewayServer) handleImagesEdits(w http.ResponseWriter, r *http.Request, reqID int64) {
	if tripped, reason := IsTosTripped(); tripped {
		writeImageAPIError(w, r, http.StatusServiceUnavailable, "tos_circuit_breaker", "tos_circuit_breaker",
			"Gateway halted by ToS circuit breaker (delete runtime/tos-tripped to reset): "+reason)
		return
	}
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	var (
		params  validatedImageGenParams
		imgs    []validatedEditImage
		errCode int
		errMsg  string
	)
	if strings.HasPrefix(ct, "multipart/form-data") {
		params, imgs, errCode, errMsg = parseImageEditsMultipart(w, r)
	} else {
		params, imgs, errCode, errMsg = parseImageEditsJSON(w, r)
	}
	if errCode != 0 {
		for i := range imgs {
			imgs[i].raw = nil
		}
		imgs = nil
		writeImageAPIError(w, r, errCode, "invalid_request_error", "invalid_request", errMsg)
		return
	}

	tmpDir, absPaths, stageErr := stageValidatedEditImages(reqID, imgs)
	for i := range imgs {
		imgs[i].raw = nil
	}
	imgs = nil
	if stageErr != nil {
		writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "temp_write_error", stageErr.Error())
		return
	}
	if tmpDir != "" {
		defer func() {
			_ = os.RemoveAll(tmpDir)
		}()
	}

	gs.executeImageGenerationRequest(w, r, reqID, params, absPaths)
}

type imagePromptTurn struct {
	Role string
	Text string
}

var markdownDataImageRe = regexp.MustCompile(`!\[[^\]]*\]\((data:image/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/=\s]+)\)`)

// extractAndStripMarkdownDataImages pulls embedded Markdown data:image/... URIs into refs
// and replaces them with a compact placeholder so multi-turn chat echoes don't flood the text prompt.
func extractAndStripMarkdownDataImages(text string, fieldPath string, refs *[]chatImageRef) string {
	if !strings.Contains(text, "data:image/") || !strings.Contains(text, "![") {
		return text
	}
	return markdownDataImageRe.ReplaceAllStringFunc(text, func(match string) string {
		sub := markdownDataImageRe.FindStringSubmatch(match)
		if len(sub) == 2 {
			*refs = append(*refs, chatImageRef{url: strings.TrimSpace(sub[1]), fieldPath: fieldPath})
		}
		return "[image]"
	})
}

func buildMultiTurnImagePrompt(sysText string, turns []imagePromptTurn) string {
	var cleaned []imagePromptTurn
	for _, t := range turns {
		txt := strings.TrimSpace(t.Text)
		if txt == "" {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(t.Role))
		if role == "" {
			role = "user"
		}
		cleaned = append(cleaned, imagePromptTurn{Role: role, Text: txt})
	}
	sys := strings.TrimSpace(sysText)
	if len(cleaned) == 0 {
		return ""
	}
	if len(cleaned) == 1 && sys == "" {
		return boundPeerStringUTF8(cleaned[0].Text, maxImageGenPromptChars)
	}

	lastIdx := len(cleaned) - 1
	for i := len(cleaned) - 1; i >= 0; i-- {
		if cleaned[i].Role == "user" {
			lastIdx = i
			break
		}
	}
	latest := cleaned[lastIdx].Text

	var priorLines []string
	for i := 0; i < lastIdx; i++ {
		lbl := "User"
		if cleaned[i].Role == "assistant" || cleaned[i].Role == "model" {
			lbl = "Assistant"
		}
		snippet := strings.Join(strings.Fields(cleaned[i].Text), " ")
		if len(snippet) > 600 {
			snippet = boundPeerStringUTF8(snippet, 597) + "..."
		}
		if snippet != "" {
			priorLines = append(priorLines, lbl+": "+snippet)
		}
	}

	// Reserve budget so the latest user instruction always stays intact up to maxImageGenPromptChars.
	const headerBudget = 128
	latestBounded := boundPeerStringUTF8(latest, maxImageGenPromptChars)
	remaining := maxImageGenPromptChars - len(latestBounded) - headerBudget

	var sb strings.Builder
	if sys != "" && remaining > 64 {
		sysCap := remaining / 3
		if sysCap > 2048 {
			sysCap = 2048
		}
		sysClip := boundPeerStringUTF8(sys, sysCap)
		sb.WriteString("System guidance:\n")
		sb.WriteString(sysClip)
		sb.WriteString("\n\n")
		remaining -= len(sysClip) + 20
	}
	if len(priorLines) > 0 && remaining > 64 {
		// Keep the most recent prior turns within remaining budget.
		kept := make([]string, 0, len(priorLines))
		used := 0
		for i := len(priorLines) - 1; i >= 0; i-- {
			line := priorLines[i]
			if used+len(line)+1 > remaining {
				break
			}
			kept = append(kept, line)
			used += len(line) + 1
		}
		for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
			kept[i], kept[j] = kept[j], kept[i]
		}
		if len(kept) > 0 {
			sb.WriteString("Context from previous turns:\n")
			sb.WriteString(strings.Join(kept, "\n"))
			sb.WriteString("\n\n")
		}
	}
	if sb.Len() > 0 {
		sb.WriteString("Current instruction:\n")
	}
	sb.WriteString(latestBounded)
	return boundPeerStringUTF8(strings.TrimSpace(sb.String()), maxImageGenPromptChars)
}

type chatImageRef struct {
	url       string
	fieldPath string
}

func resolveChatImageRef(ctx context.Context, ref chatImageRef) (validatedEditImage, error) {
	u := strings.TrimSpace(ref.url)
	if u == "" {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, ref.fieldPath, "image_url is empty")
	}
	if hasPrefixFold(u, "data:") {
		return decodeEditImageString(u, ref.fieldPath)
	}
	if !hasPrefixFold(u, "http://") && !hasPrefixFold(u, "https://") {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, ref.fieldPath, "unsupported image_url scheme; expected data:, http://, or https://")
	}
	cached, err := GlobalImageFetcher.Fetch(ctx, u, ref.fieldPath)
	if err != nil {
		return validatedEditImage{}, err
	}
	raw, decErr := base64.StdEncoding.DecodeString(cached.b64)
	if decErr != nil {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, ref.fieldPath, "failed to decode fetched remote image")
	}
	sniffed, ok := sniffAllowedImageMime(raw)
	if !ok || !allowedImageMimeMap[sniffed] {
		return validatedEditImage{}, newMMError(http.StatusBadRequest, ref.fieldPath, "remote image failed PNG/JPEG/WEBP/GIF/HEIC/HEIF magic header verification")
	}
	return validatedEditImage{raw: raw, mime: sniffed}, nil
}

func extractChatImageURLFromPart(part map[string]any) string {
	switch u := part["image_url"].(type) {
	case string:
		return strings.TrimSpace(u)
	case map[string]any:
		if s, ok := u["url"].(string); ok {
			return strings.TrimSpace(s)
		}
	}
	if s, ok := part["url"].(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func writeChatImageSSEChunks(sw *protocol.StreamWriter, cmplID string, createdTs int64, model string, results []generatedImageResult, promptTok int) {
	writeChunk := func(delta map[string]any, finish any, usage map[string]any) {
		payload := map[string]any{
			"id":      cmplID,
			"object":  "chat.completion.chunk",
			"created": createdTs,
			"model":   model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		}
		if usage != nil {
			payload["usage"] = usage
		}
		sw.WriteSSEData(payload)
	}

	writeChunk(map[string]any{"role": "assistant", "content": ""}, nil, nil)

	const chunkSize = 64 * 1024
	for i := range results {
		prefix := fmt.Sprintf("![image](data:%s;base64,", results[i].Mime)
		if i > 0 {
			prefix = "\n\n" + prefix
		}
		writeChunk(map[string]any{"content": prefix}, nil, nil)
		b64 := results[i].B64
		for off := 0; off < len(b64); off += chunkSize {
			end := off + chunkSize
			if end > len(b64) {
				end = len(b64)
			}
			writeChunk(map[string]any{"content": b64[off:end]}, nil, nil)
		}
		writeChunk(map[string]any{"content": ")"}, nil, nil)
		results[i].B64 = ""
	}

	compTok := len(results) * 256
	writeChunk(map[string]any{}, "stop", map[string]any{
		"prompt_tokens":     promptTok,
		"completion_tokens": compTok,
		"total_tokens":      promptTok + compTok,
	})
	sw.WriteRaw("data: [DONE]\n\n")
}

func (gs *GatewayServer) handleChatCompletionsImageGen(w http.ResponseWriter, r *http.Request, reqID int64, reqModel string, body map[string]any) {
	isStream, _ := body["stream"].(bool)
	size, errSz := parseOptionalJSONString(body, "size")
	if errSz != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_size", errSz.Error())
		return
	}
	aspectRatio, errAR := parseOptionalJSONString(body, "aspect_ratio")
	if errAR != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_aspect_ratio", errAR.Error())
		return
	}
	if aspectRatio == "" {
		aspectRatio, errAR = parseOptionalJSONString(body, "aspectRatio")
		if errAR != nil {
			writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_aspect_ratio", errAR.Error())
			return
		}
	}
	outputFormat, errFmt := parseOptionalJSONString(body, "output_format")
	if errFmt != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_output_format", errFmt.Error())
		return
	}
	nVal, hasN, errN := parseOptionalJSONInt(body, "n")
	if errN != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_n", errN.Error())
		return
	}

	rawMsgs, ok := body["messages"].([]any)
	if !ok || len(rawMsgs) == 0 {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", "messages must be a non-empty array")
		return
	}

	var (
		sysParts []string
		turns    []imagePromptTurn
		imgRefs  []chatImageRef
	)
	for mi, rm := range rawMsgs {
		m, okM := rm.(map[string]any)
		if !okM {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(stringField(m, "role", "")))
		if role == "" {
			role = "user"
		}
		var textParts []string
		switch cv := m["content"].(type) {
		case string:
			cleaned := extractAndStripMarkdownDataImages(cv, fmt.Sprintf("messages[%d].content", mi), &imgRefs)
			if strings.TrimSpace(cleaned) != "" {
				textParts = append(textParts, cleaned)
			}
		case []any:
			for pi, rp := range cv {
				switch part := rp.(type) {
				case string:
					cleaned := extractAndStripMarkdownDataImages(part, fmt.Sprintf("messages[%d].content[%d]", mi, pi), &imgRefs)
					if strings.TrimSpace(cleaned) != "" {
						textParts = append(textParts, cleaned)
					}
				case map[string]any:
					pType := strings.ToLower(strings.TrimSpace(stringField(part, "type", "")))
					switch pType {
					case "image_url", "input_image":
						if u := extractChatImageURLFromPart(part); u != "" {
							imgRefs = append(imgRefs, chatImageRef{
								url:       u,
								fieldPath: fmt.Sprintf("messages[%d].content[%d].image_url", mi, pi),
							})
						}
					case "text", "input_text", "":
						if t, okT := part["text"].(string); okT {
							cleaned := extractAndStripMarkdownDataImages(t, fmt.Sprintf("messages[%d].content[%d].text", mi, pi), &imgRefs)
							if strings.TrimSpace(cleaned) != "" {
								textParts = append(textParts, cleaned)
							}
						}
					}
				}
			}
		}
		joined := strings.TrimSpace(strings.Join(textParts, "\n"))
		if role == "system" || role == "developer" {
			if joined != "" {
				sysParts = append(sysParts, joined)
			}
		} else if joined != "" {
			turns = append(turns, imagePromptTurn{Role: role, Text: joined})
		}
	}

	prompt := buildMultiTurnImagePrompt(strings.Join(sysParts, "\n"), turns)
	if prompt == "" {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", "messages must contain a non-empty text prompt for image generation")
		return
	}
	params, valErr := parseAndValidateCommonImageParams(prompt, reqModel, size, aspectRatio, "b64_json", outputFormat, nVal, hasN)
	if valErr != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", valErr.Error())
		return
	}

	// Keep only the last maxImageEditInputs (3) images in chronological order.
	if len(imgRefs) > maxImageEditInputs {
		imgRefs = imgRefs[len(imgRefs)-maxImageEditInputs:]
	}
	var imgs []validatedEditImage
	if len(imgRefs) > 0 {
		imgs = make([]validatedEditImage, 0, len(imgRefs))
		for _, ref := range imgRefs {
			vi, rErr := resolveChatImageRef(r.Context(), ref)
			if rErr != nil {
				for j := range imgs {
					imgs[j].raw = nil
				}
				imgs = nil
				status := http.StatusBadRequest
				var mmErr *MultimodalValidationError
				if errors.As(rErr, &mmErr) && mmErr != nil && mmErr.StatusCode > 0 {
					status = mmErr.StatusCode
				}
				writeImageAPIError(w, r, status, "invalid_request_error", "invalid_image", rErr.Error())
				return
			}
			imgs = append(imgs, vi)
		}
	}
	imgRefs = nil
	body = nil

	tmpDir, absPaths, stageErr := stageValidatedEditImages(reqID, imgs)
	for i := range imgs {
		imgs[i].raw = nil
	}
	imgs = nil
	if stageErr != nil {
		writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "temp_write_error", stageErr.Error())
		return
	}
	if tmpDir != "" {
		defer func() {
			_ = os.RemoveAll(tmpDir)
		}()
	}

	promptTok := int(math.Ceil(float64(len(params.Prompt)) / 4.0))
	if promptTok < 1 {
		promptTok = 1
	}
	createdTs := time.Now().Unix()
	cmplID := "chatcmpl-img_" + randomHex(12)

	LogEvent("HTTP_REQ", fmt.Sprintf("REQ #%d POST /v1/chat/completions (image-gen)", reqID), map[string]any{
		"requestedModel": reqModel,
		"servedModel":    imageGenServedModel,
		"stream":         isStream,
		"n":              params.N,
		"aspectRatio":    params.AspectRatio,
		"outputFormat":   params.OutputFormat,
		"refImages":      len(absPaths),
	})

	if !isStream {
		results, imgErr := generateImageBatch(r.Context(), reqID, params, absPaths)
		if imgErr != nil {
			writeImageAPIError(w, r, imgErr.StatusCode, imgErr.ErrType, imgErr.ErrCode, imgErr.Message)
			return
		}
		var sb strings.Builder
		for i := range results {
			if i > 0 {
				sb.WriteString("\n\n")
			}
			sb.WriteString("![image](data:")
			sb.WriteString(results[i].Mime)
			sb.WriteString(";base64,")
			sb.WriteString(results[i].B64)
			sb.WriteByte(')')
			results[i].B64 = ""
		}
		compTok := len(results) * 256
		results = nil
		contentStr := sb.String()
		sb.Reset()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Agy-Served-Model", imageGenServedModel)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      cmplID,
			"object":  "chat.completion",
			"created": createdTs,
			"model":   imageGenServedModel,
			"choices": []map[string]any{
				{
					"index": 0,
					"message": map[string]any{
						"role":    "assistant",
						"content": contentStr,
					},
					"finish_reason": "stop",
				},
			},
			"usage": map[string]any{
				"prompt_tokens":     promptTok,
				"completion_tokens": compTok,
				"total_tokens":      promptTok + compTok,
			},
		})
		return
	}

	stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
		HeartbeatInterval: sseHeartbeatInterval,
		WriteTimeout:      sseWriteTimeout,
	})
	defer stream.ClearDeadline()
	stream.SetPreHeader("X-Agy-Served-Model", imageGenServedModel)
	stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
		sw.WriteRaw(": ping\n\n")
	})

	results, imgErr := generateImageBatch(r.Context(), reqID, params, absPaths)
	stream.StopHeartbeat()
	if imgErr != nil {
		recordPrimaryDirectOutcome(r, imgErr.StatusCode, imgErr.ErrType, "", imgErr.Message)
		if imgErr.StatusCode == 499 {
			return
		}
		stream.DoLocked(func(sw *protocol.StreamWriter) {
			b, _ := json.Marshal(map[string]any{
				"error": map[string]any{"type": imgErr.ErrType, "code": imgErr.ErrCode, "message": imgErr.Message},
			})
			if !sw.HeadersCommitted() {
				sw.CommitErrorStatus(imgErr.StatusCode, nil, "application/json")
				sw.WriteRaw(string(b) + "\n")
				return
			}
			sw.WriteRaw(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", string(b)))
		})
		return
	}

	stream.DoLocked(func(sw *protocol.StreamWriter) {
		writeChatImageSSEChunks(sw, cmplID, createdTs, imageGenServedModel, results, promptTok)
	})
	results = nil
}
