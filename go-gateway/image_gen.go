package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	imageGenIdleTTL     = 60 * time.Second
	imageGenIdleTimeout = 180 * time.Second
	imageGenMaxDuration = 300 * time.Second
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
	mu            sync.Mutex
	cond          *sync.Cond
	eng           *SharedAcpEngine
	inFlight      int
	transitioning bool
	idleTimer     *time.Timer
}

var GlobalImageGen = newImageGenManager()

func newImageGenManager() *imageGenManager {
	m := &imageGenManager{
		eng: newAuxiliaryAcpEngine(ImageGenAgentName),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
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

func (m *imageGenManager) acquire() (*SharedAcpEngine, func(), error) {
	if err := circuitError(); err != nil {
		return nil, nil, err
	}
	m.mu.Lock()
	for {
		for m.transitioning {
			m.cond.Wait()
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
			m.transitioning = true
			eng := m.eng
			m.mu.Unlock()
			LogEvent("ACP_IMAGEGEN_BINARY_SWITCH", "Restarting ws-imagegen engine for updated agy binary", nil)
			eng.StopEngine()
			m.mu.Lock()
			m.transitioning = false
			m.cond.Broadcast()
			continue
		}
		m.inFlight++
		eng := m.eng
		m.mu.Unlock()

		if err := eng.EnsureStarted(); err != nil {
			m.release()
			return nil, nil, err
		}

		var once sync.Once
		return eng, func() { once.Do(m.release) }, nil
	}
}

func (m *imageGenManager) release() {
	m.mu.Lock()
	if m.inFlight > 0 {
		m.inFlight--
	}
	if m.inFlight == 0 {
		if !m.transitioning && m.binaryChangedLocked() {
			if m.idleTimer != nil {
				m.idleTimer.Stop()
				m.idleTimer = nil
			}
			m.transitioning = true
			eng := m.eng
			m.mu.Unlock()
			eng.StopEngine()
			LogEvent("ACP_IMAGEGEN_BINARY_SWITCH", "Stopped drained ws-imagegen engine for new agy binary", nil)
			m.mu.Lock()
			m.transitioning = false
			m.cond.Broadcast()
			m.mu.Unlock()
			return
		}
		if !m.transitioning && m.eng.IsAlive() {
			ttl := imageGenIdleTTL
			if ttl <= 0 {
				if m.idleTimer != nil {
					m.idleTimer.Stop()
					m.idleTimer = nil
				}
				m.transitioning = true
				eng := m.eng
				m.mu.Unlock()
				eng.StopEngine()
				m.mu.Lock()
				m.transitioning = false
				m.cond.Broadcast()
				m.mu.Unlock()
				return
			}
			if m.idleTimer != nil {
				m.idleTimer.Stop()
			}
			m.idleTimer = time.AfterFunc(ttl, m.StopIfIdle)
		}
	}
	m.mu.Unlock()
}

func (m *imageGenManager) StopIfIdle() {
	m.mu.Lock()
	if m.transitioning || m.inFlight > 0 {
		m.mu.Unlock()
		return
	}
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	if !m.eng.IsAlive() {
		m.mu.Unlock()
		return
	}
	m.transitioning = true
	eng := m.eng
	m.mu.Unlock()
	eng.StopEngine()
	LogEvent("ACP_IMAGEGEN_IDLE_STOP", "Stopped idle ws-imagegen engine", nil)
	m.mu.Lock()
	m.transitioning = false
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *imageGenManager) StopIfBinaryChanged() {
	m.mu.Lock()
	if m.transitioning || m.inFlight > 0 || !m.binaryChangedLocked() {
		m.mu.Unlock()
		return
	}
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.transitioning = true
	eng := m.eng
	m.mu.Unlock()
	eng.StopEngine()
	LogEvent("ACP_IMAGEGEN_BINARY_SWITCH", "Stopped idle ws-imagegen engine for new agy binary", nil)
	m.mu.Lock()
	m.transitioning = false
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *imageGenManager) Stop() {
	m.mu.Lock()
	for m.transitioning {
		m.cond.Wait()
	}
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.transitioning = true
	eng := m.eng
	m.mu.Unlock()
	eng.StopEngine()
	m.mu.Lock()
	m.transitioning = false
	m.cond.Broadcast()
	m.mu.Unlock()
}

// StopImageGenEngine shuts down the dedicated lazy ws-imagegen ACP engine cleanly.
func StopImageGenEngine() {
	if GlobalImageGen != nil {
		GlobalImageGen.Stop()
	}
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

// resolveVerifiedSessionBrainDir resolves <PureApiHome>/antigravity-cli/brain first and verifies
// via os.Lstat that <brainRoot>/<sid> is a real non-symlink directory strictly inside brainRoot.
func resolveVerifiedSessionBrainDir(sid string) (string, error) {
	if !isSafeBrainSessionID(sid) {
		return "", errors.New("invalid ACP session id for image artifact resolution")
	}
	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		return "", errors.New("PureApiHome is unset or points to restricted path")
	}
	brainRoot := filepath.Join(PureApiHome, "antigravity-cli", "brain")
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

func (m *imageGenManager) generateOne(ctx context.Context, prompt, imageName, aspectRatio string, inputPaths []string) (string, *imageGenError) {
	eng, release, err := m.acquire()
	if err != nil {
		return "", classifyImageGenError(err, nil, "")
	}
	defer release()

	rawNew, err := eng.RPC("session/new", map[string]any{
		"cwd":        DefaultCwd,
		"mcpServers": []any{},
	}, 30*time.Second)
	if err != nil {
		return "", classifyImageGenError(err, nil, "")
	}
	var newResp struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(rawNew, &newResp); err != nil || strings.TrimSpace(newResp.SessionID) == "" {
		return "", &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "session_init_failed",
			Message:    "ws-imagegen session/new did not return a sessionId",
		}
	}
	sid := strings.TrimSpace(newResp.SessionID)
	if !isSafeBrainSessionID(sid) {
		_ = eng.CloseSession(sid)
		return "", &imageGenError{
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
			return "", classifyImageGenError(errM, nil, "")
		}
		curOpts = parseConfigOptionValues(ackRaw)
	}
	if curOpts == nil || curOpts["thought_level"] != imageGenOuterTier {
		if errT := setAndVerifySessionConfigOptionWith(eng, sid, "thought_level", imageGenOuterTier, configOptionTimeout); errT != nil {
			return "", classifyImageGenError(errT, nil, "")
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
		return "", classifyImageGenError(rpcErr, toolErrs, agentTxt)
	}

	listener.mu.Lock()
	reportedPath := listener.savedPath
	toolFailed := listener.toolFailed
	toolErrs := append([]string(nil), listener.toolErrors...)
	agentTxt := strings.Join(listener.agentTexts, "")
	listener.mu.Unlock()

	if toolFailed && reportedPath == "" {
		return "", classifyImageGenError(nil, toolErrs, agentTxt)
	}

	_, resolvedPath, resErr := resolveGeneratedImageInBrainDir(sid, reportedPath, turnStart)
	if resErr != nil {
		if len(toolErrs) > 0 || strings.TrimSpace(agentTxt) != "" {
			return "", classifyImageGenError(resErr, toolErrs, agentTxt)
		}
		return "", &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_artifact_error",
			Message:    resErr.Error(),
		}
	}

	rawBytes, readErr := readVerifiedBrainImageFile(resolvedPath)
	if readErr != nil {
		return "", &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_read_failed",
			Message:    "failed to read generated image artifact: " + readErr.Error(),
		}
	}
	if _, ok := sniffAllowedImageMime(rawBytes); !ok {
		return "", &imageGenError{
			StatusCode: http.StatusBadGateway,
			ErrType:    "api_error",
			ErrCode:    "image_invalid_format",
			Message:    "generated image artifact failed image magic-byte verification",
		}
	}
	return base64.StdEncoding.EncodeToString(rawBytes), nil
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
	Prompt      string
	Model       string
	N           int
	AspectRatio string
}

func parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat string, nVal int, hasN bool) (validatedImageGenParams, error) {
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
	ar, err := mapImageAspectRatio(aspectRatio, size)
	if err != nil {
		return validatedImageGenParams{}, err
	}
	m := strings.TrimSpace(model)
	if m == "" {
		m = imageGenServedModel
	}
	return validatedImageGenParams{
		Prompt:      p,
		Model:       m,
		N:           n,
		AspectRatio: ar,
	}, nil
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

func (gs *GatewayServer) executeImageGenerationRequest(w http.ResponseWriter, r *http.Request, reqID int64, params validatedImageGenParams, inputPaths []string) {
	releaseSlot, err := acquirePromptSlot(r.Context())
	if err != nil {
		writeImageAPIError(w, r, 499, "request_cancelled", "request_cancelled", "request cancelled while waiting for prompt slot")
		return
	}
	defer releaseSlot()

	reqTag := fmt.Sprintf("r%d_%s", reqID, randomHex(4))
	data := make([]map[string]any, 0, params.N)
	for i := 0; i < params.N; i++ {
		if err := r.Context().Err(); err != nil {
			writeImageAPIError(w, r, 499, "request_cancelled", "request_cancelled", "request cancelled")
			return
		}
		imgName := fmt.Sprintf("img_%s_%d", reqTag, i+1)
		b64, imgErr := GlobalImageGen.generateOne(r.Context(), params.Prompt, imgName, params.AspectRatio, inputPaths)
		if imgErr != nil {
			writeImageAPIError(w, r, imgErr.StatusCode, imgErr.ErrType, imgErr.ErrCode, imgErr.Message)
			return
		}
		data = append(data, map[string]any{
			"b64_json": b64,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Agy-Served-Model", imageGenServedModel)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"created": time.Now().Unix(),
		"data":    data,
	})
}

func (gs *GatewayServer) handleImagesGenerations(w http.ResponseWriter, r *http.Request, reqID int64) {
	if tripped, reason := IsTosTripped(); tripped {
		writeImageAPIError(w, r, http.StatusServiceUnavailable, "tos_circuit_breaker", "tos_circuit_breaker",
			"Gateway halted by ToS circuit breaker (delete runtime/tos-tripped to reset): "+reason)
		return
	}
	rawBody, readStatus, readMsg := readGatewayJSONBody(w, r)
	if readStatus != 0 {
		writeImageAPIError(w, r, readStatus, "invalid_request_error", "invalid_request", readMsg)
		return
	}
	if len(rawBody) == 0 {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", "Empty JSON request body")
		return
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_json", "Malformed JSON request body: "+fmt.Sprint(err))
		return
	}
	prompt, _ := body["prompt"].(string)
	model, _ := body["model"].(string)
	size, _ := body["size"].(string)
	aspectRatio, _ := body["aspect_ratio"].(string)
	responseFormat, _ := body["response_format"].(string)
	nVal, hasN, errN := parseOptionalJSONInt(body, "n")
	if errN != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_n", errN.Error())
		return
	}
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, nVal, hasN)
	if err != nil {
		writeImageAPIError(w, r, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
		return
	}
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
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return validatedImageGenParams{}, nil, http.StatusRequestEntityTooLarge, "Request body exceeds maximum size"
		}
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Failed to parse multipart form: " + err.Error()
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	prompt := r.FormValue("prompt")
	model := r.FormValue("model")
	size := r.FormValue("size")
	aspectRatio := r.FormValue("aspect_ratio")
	responseFormat := r.FormValue("response_format")
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
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, nVal, hasN)
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
		return validatedImageGenParams{}, nil, readStatus, readMsg
	}
	if len(rawBody) == 0 {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Empty JSON request body"
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, "Malformed JSON request body: " + fmt.Sprint(err)
	}
	prompt, _ := body["prompt"].(string)
	model, _ := body["model"].(string)
	size, _ := body["size"].(string)
	aspectRatio, _ := body["aspect_ratio"].(string)
	responseFormat, _ := body["response_format"].(string)
	nVal, hasN, errN := parseOptionalJSONInt(body, "n")
	if errN != nil {
		return validatedImageGenParams{}, nil, http.StatusBadRequest, errN.Error()
	}
	params, err := parseAndValidateCommonImageParams(prompt, model, size, aspectRatio, responseFormat, nVal, hasN)
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
		writeImageAPIError(w, r, errCode, "invalid_request_error", "invalid_request", errMsg)
		return
	}

	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "storage_unavailable", "PureApiHome is unset or points to restricted path")
		return
	}
	tmpDir := filepath.Join(PureApiHome, "tmp", fmt.Sprintf("imagegen-%d-%s", reqID, randomHex(4)))
	if isWindowsCDrivePath(tmpDir) {
		writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "storage_unavailable", "restricted temp path")
		return
	}
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "temp_dir_error", "failed to prepare image edit workspace")
		return
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	absPaths := make([]string, 0, len(imgs))
	for i, img := range imgs {
		ext := imageExtForSniffedMime(img.mime)
		p := filepath.Join(tmpDir, fmt.Sprintf("input_%d%s", i+1, ext))
		if err := os.WriteFile(p, img.raw, 0600); err != nil {
			writeImageAPIError(w, r, http.StatusInternalServerError, "api_error", "temp_write_error", "failed to stage input image")
			return
		}
		absP, errA := filepath.Abs(p)
		if errA != nil {
			absP = p
		}
		absPaths = append(absPaths, absP)
	}

	gs.executeImageGenerationRequest(w, r, reqID, params, absPaths)
}
