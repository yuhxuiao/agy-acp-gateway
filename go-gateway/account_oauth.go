package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

const antigravityCodeRedirectURI = "https://antigravity.google/oauth-callback"

var (
	oauthSessionTTL    = 5 * time.Minute
	oauthStartTimeout  = 12 * time.Second
	oauthSubmitTimeout = 30 * time.Second
	oauthVerifyTimeout = 15 * time.Second
	oauthVerifyInTest  = false
	validRawAuthCodeRe = regexp.MustCompile(`^[A-Za-z0-9._/~%+=:-]+$`)
)

func redactAccountErrorString(s string, proxyStr string) string {
	if proxyStr != "" {
		s = strings.ReplaceAll(s, proxyStr, maskProxyURL(proxyStr))
	}
	s = gwcfg.RedactURLSecretsForError(s)
	s = gwcfg.RedactSecretsInString(s)
	return s
}

func isStrippedSubprocessEnv(kv string) bool {
	if isStrippedAgyEngineEnv(kv) {
		return true
	}
	for _, prefix := range []string{
		"HOME=", "BROWSER=",
		"DISPLAY=", "WAYLAND_DISPLAY=",
		"AGY_ACP_ENABLE_OAUTH=",
		"SSH_CONNECTION=", "SSH_TTY=", "SSH_CLIENT=",
		"AGY_API_KEYS=", "AGY_API_KEYS_FILE=",
		"AGY_RESIN_PROXY_TOKEN=", "AGY_RESIN_HOST=",
		"AGY_PROXY_PRESETS_FILE=", "AGY_UPSTREAM_PROXY=",
		"AGY_DASHBOARD_PEERS=",
	} {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
}

func oauthSubprocessEnv(homeDir, geminiDir, proxy string) []string {
	env := make([]string, 0, len(os.Environ())+16)
	for _, kv := range os.Environ() {
		if isStrippedSubprocessEnv(kv) {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"HOME="+homeDir,
		"GEMINI_HOME="+geminiDir,
		"AGY_ACP_FORCE_FILE_STORAGE=1",
		"AGY_CLI_DISABLE_AUTO_UPDATE=1",
		"SSH_CONNECTION=127.0.0.1 0 127.0.0.1 0",
		"BROWSER=/bin/false",
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null",
		"HTTP_PROXY="+proxy,
		"HTTPS_PROXY="+proxy,
		"http_proxy="+proxy,
		"https_proxy="+proxy,
		"ALL_PROXY=",
		"all_proxy=",
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	)
	return env
}

func newOAuthFlowID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("flow-%d", time.Now().UnixNano())
}

func resetPrimaryQuotaCache() {
	quotaMu.Lock()
	cachedQuota = nil
	cachedQuotaAt = time.Time{}
	lastQuotaErr = nil
	lastQuotaAttemptAt = time.Time{}
	quotaBackoffSteps = 0
	quotaMu.Unlock()
}

// ManagedPeersForDashboard returns the peer configs for all enabled, authenticated non-primary accounts,
// plus synthetic views for accounts awaiting web OAuth login or still starting their child worker.
// It never blocks /api/dashboard waiting for cold worker startup.
func (am *AccountManager) ManagedPeersForDashboard() (peers []gwcfg.DashboardPeerConfig, unauthInstances []map[string]any) {
	if isChildAccountWorker() {
		return nil, nil
	}
	am.ensureInitialized()
	accs := am.ListAccounts()
	for _, acc := range accs {
		if acc.Primary || !acc.Enabled {
			continue
		}
		if !am.HasOAuthToken(acc.ID) {
			unauthInstances = append(unauthInstances, map[string]any{
				"id":              acc.ID,
				"label":           acc.Label,
				"self":            false,
				"reachable":       false,
				"stale":           false,
				"unauthenticated": true,
				"proxyPreset":     acc.ProxyPreset,
				"boundKeys":       maskBoundKeysForDashboard(acc.BoundKeys),
				"boundKeyCount":   len(acc.BoundKeys),
				"allowedModels":   acc.AllowedModels,
				"defaultModels":   acc.DefaultModels,
				"error":           "待 Google 登录或导入凭据",
				"fetchedAt":       "",
				"gateway": map[string]any{
					"instanceId":          acc.ID,
					"instanceLabel":       acc.Label,
					"activeSessionsCount": 0,
					"maxLiveWorkers":      acc.MaxLiveWorkers,
				},
				"quota": QuotaSnapshot{
					Status: "not_enabled",
					Error:  "待 Google 登录",
				},
				"windows":       map[string]WindowAggregate{},
				"canaryRoutes":  []map[string]any{},
				"cacheWarnings": []map[string]any{},
			})
			continue
		}
		if running, _, dashPort := am.IsWorkerRunning(acc.ID); running && dashPort > 0 {
			peers = append(peers, gwcfg.DashboardPeerConfig{
				ID:      acc.ID,
				Label:   acc.Label,
				BaseURL: fmt.Sprintf("http://127.0.0.1:%d", dashPort),
			})
			continue
		}
		if !testing.Testing() && am.shouldTriggerWorkerWarmup(acc.ID, 10*time.Second) {
			go func(id string) {
				_ = am.EnsureAccountWorkerRunning(id)
			}(acc.ID)
		}
		unauthInstances = append(unauthInstances, map[string]any{
			"id":            acc.ID,
			"label":         acc.Label,
			"self":          false,
			"reachable":     false,
			"stale":         false,
			"starting":      true,
			"proxyPreset":   acc.ProxyPreset,
			"boundKeys":     maskBoundKeysForDashboard(acc.BoundKeys),
			"boundKeyCount": len(acc.BoundKeys),
			"allowedModels": acc.AllowedModels,
			"defaultModels": acc.DefaultModels,
			"error":         "独立子进程启动中，请稍候自动刷新",
			"fetchedAt":     "",
			"gateway": map[string]any{
				"instanceId":          acc.ID,
				"instanceLabel":       acc.Label,
				"activeSessionsCount": 0,
				"maxLiveWorkers":      acc.MaxLiveWorkers,
			},
			"quota": QuotaSnapshot{
				Status: "loading",
				Error:  "独立子进程启动中",
			},
			"windows":       map[string]WindowAggregate{},
			"canaryRoutes":  []map[string]any{},
			"cacheWarnings": []map[string]any{},
		})
	}
	return peers, unauthInstances
}

// UpsertAccount creates or updates an AccountProfile and persists accounts.json.
func (am *AccountManager) UpsertAccount(in AccountProfile) (AccountProfile, error) {
	am.ensureInitialized()
	in.ID = strings.ToLower(strings.TrimSpace(in.ID))
	if !validAccountIDRe.MatchString(in.ID) {
		return AccountProfile{}, errors.New("账号 ID 仅允许小写字母、数字、连字符或下划线 (1-32 位)")
	}
	in.Label = strings.TrimSpace(in.Label)
	if in.Label == "" {
		in.Label = in.ID
	}
	if in.MaxLiveWorkers <= 0 {
		in.MaxLiveWorkers = 3
	}
	if in.MaxLiveWorkers > 16 {
		in.MaxLiveWorkers = 16
	}
	if in.Priority <= 0 {
		in.Priority = defaultAccountPriority
	} else if in.Priority > 1000 {
		in.Priority = 1000
	}
	if in.Weight <= 0 {
		in.Weight = defaultAccountWeight
	} else if in.Weight > 100 {
		in.Weight = 100
	}
	if in.MaxConcurrency < 0 {
		in.MaxConcurrency = 0
	} else if in.MaxConcurrency > 64 {
		in.MaxConcurrency = 64
	}
	in.Groups = normalizeGroupIDSlice(in.Groups)
	in.Notes = truncateRunes(strings.TrimSpace(in.Notes), 240)

	in.ProxyPreset = strings.ToLower(strings.TrimSpace(in.ProxyPreset))
	if in.ProxyPreset == "" {
		in.ProxyPreset = "direct"
	}
	if err := validateProxyPreset(in.ProxyPreset); err != nil {
		return AccountProfile{}, err
	}
	in.CustomProxy = strings.TrimSpace(in.CustomProxy)

	seenBound := make(map[string]bool, len(in.BoundKeys))
	var cleanKeys []string
	for _, k := range in.BoundKeys {
		k = strings.TrimSpace(k)
		if k == "" || seenBound[k] {
			continue
		}
		if rawMasterKeyMatches(k) {
			return AccountProfile{}, errors.New("渠道专属 Key 不可与网关主管理 Key 相同（主 Key 默认具备全局池权限，指定账号可用 <主Key>@<账号ID>）")
		}
		seenBound[k] = true
		cleanKeys = append(cleanKeys, k)
	}
	if cleanKeys == nil {
		cleanKeys = []string{}
	}
	in.BoundKeys = cleanKeys

	if in.AllowedModels != nil {
		in.AllowedModels = normalizeCanonicalModelList(in.AllowedModels)
	} else if in.DefaultModels != nil {
		in.AllowedModels = normalizeCanonicalModelList(in.DefaultModels)
	} else {
		in.AllowedModels = []string{}
	}
	in.DefaultModels = append([]string{}, in.AllowedModels...)

	opMu := am.accountOpLock(in.ID)
	opMu.Lock()
	defer opMu.Unlock()

	am.mu.Lock()
	foundIdx := -1
	var prevAcc AccountProfile
	for i, a := range am.accounts {
		if a.ID == in.ID {
			foundIdx = i
			prevAcc = a
			in.Primary = a.Primary
			// Preserve stored credentials when UI submits back the masked representation unchanged.
			if in.CustomProxy != "" && a.CustomProxy != "" && in.CustomProxy == maskProxyURL(a.CustomProxy) {
				in.CustomProxy = a.CustomProxy
			}
			continue
		}
		for _, existingKey := range a.BoundKeys {
			if seenBound[strings.TrimSpace(existingKey)] {
				am.mu.Unlock()
				return AccountProfile{}, fmt.Errorf("渠道专属 Key 已绑定至账号 %q，请勿跨账号重复绑定同一 Key", a.ID)
			}
		}
	}
	for _, g := range am.groups {
		for _, gk := range g.BoundKeys {
			if seenBound[strings.TrimSpace(gk)] {
				am.mu.Unlock()
				return AccountProfile{}, fmt.Errorf("渠道专属 Key 已绑定至分组池 %q，请勿重复绑定", g.ID)
			}
		}
	}
	if in.ProxyPreset == "custom" && in.CustomProxy == "" {
		am.mu.Unlock()
		return AccountProfile{}, errors.New("选择自定义代理时必须填写代理 URL")
	}
	if err := validateCustomProxyURL(in.CustomProxy); err != nil {
		am.mu.Unlock()
		return AccountProfile{}, err
	}
	if foundIdx >= 0 {
		if in.Primary {
			in.Enabled = true
			if strings.EqualFold(strings.TrimSpace(in.SchedState), SchedStateDraining) {
				in.SchedState = SchedStateDraining
			} else {
				in.SchedState = SchedStateActive
			}
		} else {
			in.SchedState, in.Enabled = normalizeSchedState(in.SchedState, in.Enabled)
		}
		am.accounts[foundIdx] = in
	} else {
		in.Primary = false
		in.SchedState, in.Enabled = normalizeSchedState(in.SchedState, in.Enabled)
		am.accounts = append(am.accounts, in)
	}
	err := am.saveToDiskLocked()
	am.mu.Unlock()
	if err != nil {
		return AccountProfile{}, err
	}

	am.InvalidateTokenCache(in.ID)
	if !in.Enabled {
		am.ClearStickyForAccount(in.ID)
	} else {
		am.ClearStickyDisallowedModels(in.ID, in.AllowedModels)
	}

	workerCfgChanged := foundIdx < 0 ||
		prevAcc.ProxyPreset != in.ProxyPreset ||
		prevAcc.CustomProxy != in.CustomProxy ||
		prevAcc.MaxLiveWorkers != in.MaxLiveWorkers ||
		prevAcc.Enabled != in.Enabled
	if !in.Primary && workerCfgChanged {
		am.StopAccountWorker(in.ID)
		if cur, ok := am.GetAccount(in.ID); ok && cur.Enabled && am.HasOAuthToken(in.ID) {
			_ = am.EnsureAccountWorkerRunning(in.ID)
		}
	}
	am.ResetRoutingWeights()
	InvalidatePeerDashboardCache()
	return in, nil
}

// DeleteAccount removes a non-primary account and moves its data directory to a recoverable trash folder.
func (am *AccountManager) DeleteAccount(accountID string) error {
	am.ensureInitialized()
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	opMu := am.accountOpLock(accountID)
	opMu.Lock()
	defer opMu.Unlock()

	am.mu.Lock()
	idx := -1
	for i, a := range am.accounts {
		if a.ID == accountID {
			if a.Primary {
				am.mu.Unlock()
				return errors.New("主账号不可删除")
			}
			idx = i
			break
		}
	}
	if idx < 0 {
		am.mu.Unlock()
		return errors.New("未找到该账号")
	}
	am.accounts = append(am.accounts[:idx], am.accounts[idx+1:]...)
	if am.routing.ActiveAccountID == accountID {
		am.routing.ActiveAccountID = ""
	}
	for i := range am.groups {
		if am.groups[i].ActiveAccountID == accountID {
			am.groups[i].ActiveAccountID = ""
		}
	}
	err := am.saveToDiskLocked()
	baseDir := am.baseDataDir
	am.mu.Unlock()
	if err != nil {
		return err
	}

	var oldSess *pendingOAuthSession
	am.oauthMu.Lock()
	if old, exists := am.oauthActive[accountID]; exists && old != nil {
		oldSess = old
		am.cancelOAuthSessionLocked(old)
	}
	if am.oauthState != nil {
		delete(am.oauthState, accountID)
	}
	am.oauthMu.Unlock()
	if oldSess != nil {
		waitOAuthProcExit(oldSess, 3*time.Second)
		cleanupOAuthSessionArtifacts(oldSess)
	}

	am.InvalidateTokenCache(accountID)
	am.ClearStickyForAccount(accountID)
	am.ClearAccountCooldown(accountID)
	am.ResetRoutingWeights()
	am.StopAccountWorker(accountID)
	srcDir := filepath.Join(baseDir, "accounts", accountID)
	if fileExists(srcDir) {
		trashDir := filepath.Join(baseDir, "accounts", fmt.Sprintf(".trash-%s-%d", accountID, time.Now().Unix()))
		_ = os.Rename(srcDir, trashDir)
	}
	InvalidatePeerDashboardCache()
	return nil
}

func (am *AccountManager) ensureOAuthMapsLocked() {
	if am.oauthActive == nil {
		am.oauthActive = make(map[string]*pendingOAuthSession)
	}
	if am.oauthState == nil {
		am.oauthState = make(map[string]OAuthFlowSnapshot)
	}
}

func (am *AccountManager) recordOAuthSnapshotLocked(snap OAuthFlowSnapshot) {
	am.ensureOAuthMapsLocked()
	am.oauthState[snap.AccountID] = snap
}

func terminateOAuthProcGroup(s *pendingOAuthSession) {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	pid := s.cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	go func(done <-chan struct{}, pgid int) {
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		if done != nil {
			select {
			case <-done:
				return
			case <-timer.C:
			}
		} else {
			<-timer.C
		}
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}(s.done, pid)
}

func waitOAuthProcExit(s *pendingOAuthSession, waitFor time.Duration) {
	if s == nil || s.done == nil {
		return
	}
	timer := time.NewTimer(waitFor)
	defer timer.Stop()
	select {
	case <-s.done:
	case <-timer.C:
		if s.cmd != nil && s.cmd.Process != nil {
			_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		}
		<-s.done
	}
}

func cleanupOAuthSessionArtifacts(s *pendingOAuthSession) {
	if s == nil {
		return
	}
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
}

func (am *AccountManager) cancelOAuthSessionLocked(s *pendingOAuthSession) {
	if s == nil {
		return
	}
	if am.oauthActive[s.accountID] == s {
		delete(am.oauthActive, s.accountID)
	}
	s.closeStdin()
	terminateOAuthProcGroup(s)
}

func sanitizeOAuthCodeString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func extractAuthCodeFromInput(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") || strings.Contains(s, "code=") {
		if u, err := url.Parse(s); err == nil {
			if c := sanitizeOAuthCodeString(u.Query().Get("code")); c != "" {
				return c
			}
		}
		if idx := strings.Index(s, "code="); idx >= 0 {
			rest := s[idx+5:]
			if amp := strings.IndexByte(rest, '&'); amp >= 0 {
				rest = rest[:amp]
			}
			if hash := strings.IndexByte(rest, '#'); hash >= 0 {
				rest = rest[:hash]
			}
			if dec, err := url.QueryUnescape(rest); err == nil {
				if cleaned := sanitizeOAuthCodeString(dec); cleaned != "" {
					return cleaned
				}
			}
			return sanitizeOAuthCodeString(rest)
		}
	}
	return sanitizeOAuthCodeString(s)
}

// parseAntigravityCodeAuthURL strictly validates an OAuth URL returned by `agy acp --login`.
func parseAntigravityCodeAuthURL(rawURL string) (authURL, redirectURI, state string, err error) {
	if rawURL == "" || len(rawURL) > 16384 || strings.ContainsAny(rawURL, " \t\r\n") {
		return "", "", "", errors.New("Antigravity 返回了无效的 Google 登录链接")
	}
	if strings.Contains(rawURL, "#") {
		return "", "", "", errors.New("Antigravity 登录链接包含非法片段")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", errors.New("Antigravity 返回了无法解析的 Google 登录链接")
	}
	if u.Scheme != "https" || !strings.EqualFold(u.Host, "accounts.google.com") || u.User != nil {
		return "", "", "", errors.New("Antigravity 返回了非法的授权域名")
	}
	if u.Path != "/o/oauth2/auth" && u.Path != "/o/oauth2/v2/auth" {
		return "", "", "", errors.New("Antigravity 返回了非预期的授权路径")
	}
	q := u.Query()
	if len(q["response_type"]) != 1 || q.Get("response_type") != "code" {
		return "", "", "", errors.New("Antigravity 授权链接缺少 response_type=code")
	}
	if len(q["state"]) != 1 {
		return "", "", "", errors.New("Antigravity 授权链接缺少唯一 state 参数")
	}
	state = q.Get("state")
	if state == "" || len(state) > 512 || strings.ContainsAny(state, " \t\r\n") {
		return "", "", "", errors.New("Antigravity 授权链接 state 参数格式无效")
	}
	if len(q["redirect_uri"]) != 1 {
		return "", "", "", errors.New("Antigravity 授权链接缺少唯一 redirect_uri")
	}
	redirectURI = q.Get("redirect_uri")
	if redirectURI != antigravityCodeRedirectURI {
		return "", "", "", errors.New("Antigravity 授权链接使用了非预期的 redirect_uri")
	}
	return rawURL, redirectURI, state, nil
}

// validateAntigravityLoginResponse validates either a raw authorization code or the full
// Antigravity callback URL against the active login session's redirect_uri and state.
func validateAntigravityLoginResponse(pendingRedirectURI, pendingState, rawInput string) (string, error) {
	v := strings.TrimSpace(rawInput)
	if v == "" {
		return "", errors.New("授权码不能为空")
	}
	if len(v) > 16384 || strings.ContainsAny(v, " \t\r\n") {
		return "", errors.New("输入包含空白或换行，请仅粘贴授权码或完整回调 URL")
	}
	low := strings.ToLower(v)
	if strings.HasPrefix(low, "http://") || strings.HasPrefix(low, "https://") {
		if strings.Contains(v, "#") {
			return "", errors.New("回调 URL 不可包含 # 片段")
		}
		u, err := url.Parse(v)
		if err != nil || u.Host == "" || u.User != nil {
			return "", errors.New("回调 URL 格式无效，请粘贴完整的授权跳转 URL")
		}
		originPath := strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) + u.Path
		expected := pendingRedirectURI
		if expected == "" {
			expected = antigravityCodeRedirectURI
		}
		if originPath != expected {
			return "", errors.New("回调 URL 与当前登录会话不匹配，请使用本次生成的 Google 登录链接")
		}
		q := u.Query()
		if len(q["state"]) != 1 || (pendingState != "" && q.Get("state") != pendingState) {
			return "", errors.New("回调 URL 的 state 与当前登录会话不匹配，请重新生成登录链接")
		}
		if errVals := q["error"]; len(errVals) > 0 && strings.TrimSpace(errVals[0]) != "" {
			return "", fmt.Errorf("Google 登录未获批准 (%s)", strings.TrimSpace(errVals[0]))
		}
		if len(q["code"]) != 1 || strings.TrimSpace(q.Get("code")) == "" {
			return "", errors.New("回调 URL 中缺少有效的授权码 code 参数")
		}
		return v, nil
	}
	if strings.Contains(v, "code=") || strings.Contains(v, "&") || strings.Contains(v, "?") {
		return "", errors.New("请粘贴完整回调 URL (https://...) 或页面显示的纯授权码")
	}
	if len(v) < 4 || len(v) > 2048 || !validRawAuthCodeRe.MatchString(v) {
		return "", errors.New("授权码格式无效，请检查是否完整复制了 Antigravity 页面上的授权码")
	}
	return v, nil
}

// StartOAuthLogin spawns `agy acp --gemini-dir <dir> --app-data-dir antigravity-cli --login`
// and returns the Google OAuth URL.
func (am *AccountManager) StartOAuthLogin(accountID string) (string, error) {
	snap, err := am.StartOAuthLoginFlow(accountID)
	if err != nil {
		return "", err
	}
	return snap.AuthorizationURL, nil
}

// StartOAuthLoginFlow launches T3-style `agy acp ... --login` inside an isolated staging
// directory so existing live credentials and workers stay untouched until the new login is verified.
func (am *AccountManager) StartOAuthLoginFlow(accountID string) (OAuthFlowSnapshot, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return OAuthFlowSnapshot{}, fmt.Errorf("未找到账号 %q", accountID)
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	proxy, proxyErr := am.ResolveAccountProxyChecked(acc)
	if proxyErr != nil {
		return OAuthFlowSnapshot{}, proxyErr
	}

	var prevSess *pendingOAuthSession
	flowID := newOAuthFlowID()
	meta := am.ReadAccountTokenMeta(acc.ID)
	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	if old, exists := am.oauthActive[acc.ID]; exists && old != nil {
		prevSess = old
		am.cancelOAuthSessionLocked(old)
	}
	am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "starting",
		FlowID:        flowID,
		Mode:          "acp_code",
		Message:       "正在通过该账号代理启动 agy acp OAuth 握手...",
		Authenticated: meta.Authenticated,
		Email:         meta.Email,
	})
	am.oauthMu.Unlock()

	if prevSess != nil {
		waitOAuthProcExit(prevSess, 3*time.Second)
		cleanupOAuthSessionArtifacts(prevSess)
	}

	tokPath := am.AccountTokenPath(acc.ID)
	tempDir, err := os.MkdirTemp("", "agy-oauth-login-*")
	if err != nil {
		return OAuthFlowSnapshot{}, err
	}
	stagingGeminiDir := filepath.Join(tempDir, "gemini")
	stagingTokPath := filepath.Join(stagingGeminiDir, "antigravity-cli", "antigravity-oauth-token")
	if err := os.MkdirAll(filepath.Dir(stagingTokPath), 0o700); err != nil {
		_ = os.RemoveAll(tempDir)
		return OAuthFlowSnapshot{}, err
	}

	cmd := exec.Command(PatchedAgyBin, "acp", "--gemini-dir", stagingGeminiDir, "--app-data-dir", "antigravity-cli", "--login")
	cmd.Dir = tempDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = oauthSubprocessEnv(tempDir, stagingGeminiDir, proxy)

	failStart := func(msg string) {
		_ = os.RemoveAll(tempDir)
		curMeta := am.ReadAccountTokenMeta(acc.ID)
		am.oauthMu.Lock()
		am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
			AccountID:     acc.ID,
			Phase:         "failed",
			FlowID:        flowID,
			Mode:          "acp_code",
			Message:       msg,
			Email:         curMeta.Email,
			Authenticated: curMeta.Authenticated,
		})
		am.oauthMu.Unlock()
	}

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		failStart(err.Error())
		return OAuthFlowSnapshot{}, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		failStart(err.Error())
		return OAuthFlowSnapshot{}, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		failStart(err.Error())
		return OAuthFlowSnapshot{}, err
	}

	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		failStart(err.Error())
		return OAuthFlowSnapshot{}, err
	}

	sess := &pendingOAuthSession{
		accountID:        acc.ID,
		flowID:           flowID,
		tempDir:          tempDir,
		stagingGeminiDir: stagingGeminiDir,
		stagingTokPath:   stagingTokPath,
		cmd:              cmd,
		stdin:            stdinPipe,
		output:           &bytes.Buffer{},
		tokenPath:        tokPath,
		startedAt:        time.Now(),
		done:             make(chan struct{}),
		doneCh:           make(chan error, 1),
	}

	urlCh := make(chan string, 1)
	var urlOnce sync.Once
	scanPipe := func(r io.Reader) {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64*1024), 512*1024)
		for sc.Scan() {
			line := sc.Text()
			sess.outMu.Lock()
			if sess.output.Len() < 64*1024 {
				sess.output.WriteString(line + "\n")
			}
			sess.outMu.Unlock()
			if m := oauthURLRe.FindString(line); m != "" {
				urlOnce.Do(func() {
					urlCh <- m
				})
			}
		}
		_, _ = io.Copy(io.Discard, r)
	}

	var ioWG sync.WaitGroup
	ioWG.Add(2)
	go func() {
		defer ioWG.Done()
		scanPipe(stdoutPipe)
	}()
	go func() {
		defer ioWG.Done()
		scanPipe(stderrPipe)
	}()
	go func() {
		ioWG.Wait()
		waitErr := cmd.Wait()
		sess.doneCh <- waitErr
		close(sess.done)
	}()

	select {
	case rawAuthURL := <-urlCh:
		authURL, redirectURI, state, valErr := parseAntigravityCodeAuthURL(rawAuthURL)
		if valErr != nil {
			sess.closeStdin()
			terminateOAuthProcGroup(sess)
			waitOAuthProcExit(sess, 3*time.Second)
			failStart(valErr.Error())
			return OAuthFlowSnapshot{}, valErr
		}
		expiresAt := time.Now().Add(oauthSessionTTL)
		sess.authURL = authURL
		sess.redirectURI = redirectURI
		sess.state = state
		sess.expiresAt = expiresAt

		curMeta := am.ReadAccountTokenMeta(acc.ID)
		snap := OAuthFlowSnapshot{
			AccountID:        acc.ID,
			Phase:            "waiting",
			FlowID:           flowID,
			Mode:             "acp_code",
			AuthorizationURL: authURL,
			ExpiresAt:        expiresAt.UTC().Format(time.RFC3339),
			Message:          "请打开 Google 登录页完成授权，再将页面显示的授权码或完整回调 URL 粘贴到下方完成验证",
			Email:            curMeta.Email,
			Authenticated:    curMeta.Authenticated,
		}

		var replaced *pendingOAuthSession
		am.oauthMu.Lock()
		am.ensureOAuthMapsLocked()
		if old, exists := am.oauthActive[acc.ID]; exists && old != nil && old != sess {
			replaced = old
			am.cancelOAuthSessionLocked(old)
		}
		am.oauthActive[acc.ID] = sess
		am.recordOAuthSnapshotLocked(snap)
		am.oauthMu.Unlock()
		if replaced != nil {
			waitOAuthProcExit(replaced, 2*time.Second)
			cleanupOAuthSessionArtifacts(replaced)
		}

		go func(s *pendingOAuthSession) {
			ttl := time.Until(s.expiresAt)
			if ttl <= 0 {
				ttl = time.Millisecond
			}
			timer := time.NewTimer(ttl)
			defer timer.Stop()
			var earlyExit bool
			select {
			case <-s.done:
				earlyExit = true
			case <-timer.C:
			}

			opLock := am.accountOpLock(s.accountID)
			opLock.Lock()
			am.oauthMu.Lock()
			cur := am.oauthActive[s.accountID]
			if cur != s || s.submitted {
				am.oauthMu.Unlock()
				opLock.Unlock()
				return
			}
			am.cancelOAuthSessionLocked(s)
			liveMeta := am.ReadAccountTokenMeta(s.accountID)
			msg := "Google 登录链接已超时，请重新生成授权链接"
			if earlyExit {
				msg = "agy acp 登录进程提前退出，请重新生成 Google 登录链接"
			}
			am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
				AccountID:     s.accountID,
				Phase:         "failed",
				FlowID:        s.flowID,
				Mode:          "acp_code",
				Message:       msg,
				Email:         liveMeta.Email,
				Authenticated: liveMeta.Authenticated,
			})
			am.oauthMu.Unlock()
			if !earlyExit {
				waitOAuthProcExit(s, 3*time.Second)
			}
			cleanupOAuthSessionArtifacts(s)
			opLock.Unlock()
		}(sess)

		return snap, nil

	case waitErr := <-sess.doneCh:
		sess.closeStdin()
		<-sess.done
		sess.outMu.Lock()
		outStr := strings.TrimSpace(sess.output.String())
		sess.outMu.Unlock()
		errMsg := fmt.Sprintf("未能获取 OAuth 授权链接 (%v): %s", waitErr, redactAccountErrorString(truncateRunes(outStr, 200), proxy))
		failStart(errMsg)
		return OAuthFlowSnapshot{}, errors.New(errMsg)

	case <-time.After(oauthStartTimeout):
		sess.closeStdin()
		terminateOAuthProcGroup(sess)
		waitOAuthProcExit(sess, 3*time.Second)
		errMsg := fmt.Sprintf("启动 agy acp OAuth 握手超时 (%s)", oauthStartTimeout)
		failStart(errMsg)
		return OAuthFlowSnapshot{}, errors.New(errMsg)
	}
}

// CancelOAuthLogin aborts any active OAuth handshake for accountID without touching live credentials.
func (am *AccountManager) CancelOAuthLogin(accountID, flowID string) (OAuthFlowSnapshot, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return OAuthFlowSnapshot{}, fmt.Errorf("未找到账号 %q", accountID)
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	var sess *pendingOAuthSession
	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	if cur := am.oauthActive[acc.ID]; cur != nil && (flowID == "" || cur.flowID == flowID) {
		sess = cur
		am.cancelOAuthSessionLocked(cur)
	}
	am.oauthMu.Unlock()

	if sess != nil {
		waitOAuthProcExit(sess, 3*time.Second)
		cleanupOAuthSessionArtifacts(sess)
	}
	meta := am.ReadAccountTokenMeta(acc.ID)
	snap := OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "cancelled",
		FlowID:        flowID,
		Mode:          "acp_code",
		Message:       "已取消本次 Google 登录",
		Email:         meta.Email,
		Authenticated: meta.Authenticated,
	}
	am.oauthMu.Lock()
	am.recordOAuthSnapshotLocked(snap)
	am.oauthMu.Unlock()
	return snap, nil
}

// GetOAuthStatus returns the current OAuth flow state and persisted credential metadata for accountID.
func (am *AccountManager) GetOAuthStatus(accountID string) (OAuthFlowSnapshot, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return OAuthFlowSnapshot{}, fmt.Errorf("未找到账号 %q", accountID)
	}
	meta := am.ReadAccountTokenMeta(acc.ID)
	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	snap, exists := am.oauthState[acc.ID]
	active := am.oauthActive[acc.ID]
	if active != nil && active.expiresAt.After(time.Now()) && !active.submitted {
		snap = OAuthFlowSnapshot{
			AccountID:        acc.ID,
			Phase:            "waiting",
			FlowID:           active.flowID,
			Mode:             "acp_code",
			AuthorizationURL: active.authURL,
			ExpiresAt:        active.expiresAt.UTC().Format(time.RFC3339),
			Message:          "请打开 Google 登录页完成授权，再将页面显示的授权码或完整回调 URL 粘贴到下方完成验证",
			Email:            meta.Email,
			Authenticated:    meta.Authenticated,
		}
	} else if !exists {
		snap = OAuthFlowSnapshot{
			AccountID:     acc.ID,
			Phase:         "idle",
			Mode:          "acp_code",
			Email:         meta.Email,
			Authenticated: meta.Authenticated,
		}
	} else {
		snap.Authenticated = meta.Authenticated
		if meta.Email != "" {
			snap.Email = meta.Email
		} else if !meta.Authenticated {
			snap.Email = ""
		}
	}
	am.oauthMu.Unlock()
	return snap, nil
}

// SubmitOAuthCode feeds the user's authorization code into the waiting agy login session and verifies token creation.
func (am *AccountManager) SubmitOAuthCode(accountID, rawCode string) (tierLabel string, modelsCount int, err error) {
	snap, err := am.SubmitOAuthCodeFlow(accountID, "", rawCode)
	if err != nil {
		return "", 0, err
	}
	return snap.Tier, snap.ModelsCount, nil
}

// SubmitOAuthCodeFlow validates the pasted code/URL against the active flow's state and redirect_uri,
// exchanges it via `agy acp --login`, verifies the resulting credential via ACP `authenticate`, and
// reloads the target runtime.
func (am *AccountManager) SubmitOAuthCodeFlow(accountID, flowID, rawInput string) (OAuthFlowSnapshot, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return OAuthFlowSnapshot{}, fmt.Errorf("未找到账号 %q", accountID)
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	proxy, proxyErr := am.ResolveAccountProxyChecked(acc)
	if proxyErr != nil {
		return OAuthFlowSnapshot{}, proxyErr
	}

	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	sess := am.oauthActive[acc.ID]
	if sess == nil {
		am.oauthMu.Unlock()
		return OAuthFlowSnapshot{}, errors.New("当前无进行中的 OAuth 登录会话，请先生成 Google 授权链接")
	}
	if flowID != "" && sess.flowID != flowID {
		am.oauthMu.Unlock()
		return OAuthFlowSnapshot{}, errors.New("登录会话已过期或已被新会话替换，请重新生成 Google 授权链接")
	}
	if !sess.expiresAt.IsZero() && time.Now().After(sess.expiresAt) {
		am.cancelOAuthSessionLocked(sess)
		liveMeta := am.ReadAccountTokenMeta(acc.ID)
		am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
			AccountID:     acc.ID,
			Phase:         "failed",
			FlowID:        sess.flowID,
			Mode:          "acp_code",
			Message:       "Google 登录链接已过期，请重新生成授权链接",
			Authenticated: liveMeta.Authenticated,
			Email:         liveMeta.Email,
		})
		am.oauthMu.Unlock()
		waitOAuthProcExit(sess, 3*time.Second)
		cleanupOAuthSessionArtifacts(sess)
		return OAuthFlowSnapshot{}, errors.New("Google 登录链接已过期，请重新生成授权链接")
	}
	if sess.submitted {
		am.oauthMu.Unlock()
		return OAuthFlowSnapshot{}, errors.New("授权码已提交，正在等待 Google 验证完成")
	}

	validated, valErr := validateAntigravityLoginResponse(sess.redirectURI, sess.state, rawInput)
	if valErr != nil {
		// Keep sess active so the user can fix a typo or wrong-tab URL without starting over.
		am.oauthMu.Unlock()
		return OAuthFlowSnapshot{}, valErr
	}

	sess.submitted = true
	liveMeta := am.ReadAccountTokenMeta(acc.ID)
	am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "verifying",
		FlowID:        sess.flowID,
		Mode:          "acp_code",
		Message:       "正在经该账号专属代理向 Google 换取凭据并校验 Antigravity 访问权限...",
		Authenticated: liveMeta.Authenticated,
		Email:         liveMeta.Email,
	})
	am.oauthMu.Unlock()

	_, writeErr := io.WriteString(sess.stdin, validated+"\n")
	sess.closeStdin()

	var waitErr error
	timedOut := false
	if writeErr != nil {
		waitErr = writeErr
		terminateOAuthProcGroup(sess)
		waitOAuthProcExit(sess, 3*time.Second)
	} else {
		timer := time.NewTimer(oauthSubmitTimeout)
		select {
		case waitErr = <-sess.doneCh:
			timer.Stop()
			<-sess.done
		case <-timer.C:
			timedOut = true
			terminateOAuthProcGroup(sess)
			waitOAuthProcExit(sess, 3*time.Second)
		}
	}

	am.oauthMu.Lock()
	if am.oauthActive[acc.ID] == sess {
		delete(am.oauthActive, acc.ID)
	}
	am.oauthMu.Unlock()
	defer cleanupOAuthSessionArtifacts(sess)

	failStaging := func(errMsg string) (OAuthFlowSnapshot, error) {
		curMeta := am.ReadAccountTokenMeta(acc.ID)
		snap := OAuthFlowSnapshot{
			AccountID:     acc.ID,
			Phase:         "failed",
			FlowID:        sess.flowID,
			Mode:          "acp_code",
			Message:       errMsg,
			Email:         curMeta.Email,
			Authenticated: curMeta.Authenticated,
		}
		am.oauthMu.Lock()
		am.recordOAuthSnapshotLocked(snap)
		am.oauthMu.Unlock()
		return OAuthFlowSnapshot{}, errors.New(errMsg)
	}

	if timedOut {
		return failStaging(fmt.Sprintf("向 Google 换取凭据超时 (%s)，请检查代理连通性后重试", oauthSubmitTimeout))
	}
	stInfo, statErr := os.Stat(sess.stagingTokPath)
	if waitErr != nil || statErr != nil || stInfo.IsDir() || stInfo.Size() <= 16 {
		sess.outMu.Lock()
		outStr := strings.TrimSpace(sess.output.String())
		sess.outMu.Unlock()
		detail := redactAccountErrorString(truncateRunes(outStr, 240), proxy)
		if detail == "" && waitErr != nil {
			detail = redactAccountErrorString(waitErr.Error(), proxy)
		}
		return failStaging(fmt.Sprintf("OAuth 换取凭据未成功: %s", detail))
	}
	_ = os.Chmod(sess.stagingTokPath, 0o600)
	stagingTokBytes, readErr := os.ReadFile(sess.stagingTokPath)
	if readErr != nil || len(bytes.TrimSpace(stagingTokBytes)) <= 16 {
		return failStaging("未能读取暂存的 OAuth 凭据文件")
	}

	tier, cnt, verErr := am.verifyAccountViaACPInDir(acc, sess.tempDir, sess.stagingGeminiDir)
	if verErr != nil {
		return failStaging(verErr.Error())
	}

	if !acc.Primary {
		am.StopAccountWorker(acc.ID)
	}
	if err := os.MkdirAll(filepath.Dir(sess.tokenPath), 0o700); err != nil {
		return failStaging(err.Error())
	}
	_ = am.EnsureAccountSandboxSeeded(acc.ID)
	tmpTok := sess.tokenPath + ".tmp"
	if err := os.WriteFile(tmpTok, stagingTokBytes, 0o600); err != nil {
		return failStaging(err.Error())
	}
	if err := os.Rename(tmpTok, sess.tokenPath); err != nil {
		_ = os.Remove(tmpTok)
		return failStaging(err.Error())
	}
	_ = os.Chmod(sess.tokenPath, 0o600)
	_ = os.Remove(sess.tokenPath + ".oauth-bak")
	_ = os.Remove(filepath.Join(filepath.Dir(sess.tokenPath), "acp_token.json"))
	am.InvalidateTokenCache(acc.ID)

	meta := am.ReadAccountTokenMeta(acc.ID)
	am.ClearStickyForAccount(acc.ID)
	am.ClearAccountCooldown(acc.ID)
	am.ClearWorkerBackoff(acc.ID)
	am.ResetRoutingWeights()
	if acc.Primary {
		if !testing.Testing() {
			GlobalEngine.StopEngine()
		}
		resetPrimaryQuotaCache()
	} else if acc.Enabled {
		_ = am.EnsureAccountWorkerRunning(acc.ID)
	}
	InvalidatePeerDashboardCache()

	snap := OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "succeeded",
		FlowID:        sess.flowID,
		Mode:          "acp_code",
		Message:       "Google 登录并验证成功",
		Email:         meta.Email,
		Tier:          tier,
		ModelsCount:   cnt,
		Authenticated: true,
	}
	am.oauthMu.Lock()
	am.recordOAuthSnapshotLocked(snap)
	am.oauthMu.Unlock()
	return snap, nil
}

// ImportOAuthToken verifies a pasted antigravity-oauth-token in a staging directory first,
// then atomically installs it into the account's isolated directory and reloads the runtime.
func (am *AccountManager) ImportOAuthToken(accountID, rawToken string) (tierLabel string, modelsCount int, err error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return "", 0, fmt.Errorf("未找到账号 %q", accountID)
	}
	trimmed := strings.TrimSpace(rawToken)
	if len(trimmed) < 20 {
		return "", 0, errors.New("凭据内容过短，请粘贴完整的 antigravity-oauth-token / jetski-standalone-oauth-token 文件内容")
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	if _, err := am.ResolveAccountProxyChecked(acc); err != nil {
		return "", 0, err
	}

	stageDir, err := os.MkdirTemp("", "agy-oauth-import-*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		_ = os.RemoveAll(stageDir)
	}()
	stageGem := filepath.Join(stageDir, "gemini")
	stageTok := filepath.Join(stageGem, "antigravity-cli", "antigravity-oauth-token")
	if err := os.MkdirAll(filepath.Dir(stageTok), 0o700); err != nil {
		return "", 0, err
	}
	if err := os.WriteFile(stageTok, []byte(trimmed+"\n"), 0o600); err != nil {
		return "", 0, err
	}

	tier, cnt, verErr := am.verifyAccountViaACPInDir(acc, stageDir, stageGem)
	if verErr != nil {
		return "", 0, verErr
	}

	var prevSess *pendingOAuthSession
	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	if old, exists := am.oauthActive[acc.ID]; exists && old != nil {
		prevSess = old
		am.cancelOAuthSessionLocked(old)
	}
	am.oauthMu.Unlock()
	if prevSess != nil {
		waitOAuthProcExit(prevSess, 3*time.Second)
		cleanupOAuthSessionArtifacts(prevSess)
	}

	if !acc.Primary {
		am.StopAccountWorker(acc.ID)
	}
	tokPath := am.AccountTokenPath(acc.ID)
	if err := os.MkdirAll(filepath.Dir(tokPath), 0o700); err != nil {
		return "", 0, err
	}
	_ = am.EnsureAccountSandboxSeeded(acc.ID)
	tmp := tokPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(trimmed+"\n"), 0o600); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, tokPath); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	_ = os.Chmod(tokPath, 0o600)
	_ = os.Remove(tokPath + ".oauth-bak")
	_ = os.Remove(filepath.Join(filepath.Dir(tokPath), "acp_token.json"))
	am.InvalidateTokenCache(acc.ID)

	meta := am.ReadAccountTokenMeta(acc.ID)
	am.ClearStickyForAccount(acc.ID)
	am.ClearAccountCooldown(acc.ID)
	am.ClearWorkerBackoff(acc.ID)
	am.ResetRoutingWeights()
	if acc.Primary {
		if !testing.Testing() {
			GlobalEngine.StopEngine()
		}
		resetPrimaryQuotaCache()
	} else if acc.Enabled {
		_ = am.EnsureAccountWorkerRunning(acc.ID)
	}
	InvalidatePeerDashboardCache()

	am.oauthMu.Lock()
	am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "succeeded",
		Mode:          "import",
		Message:       "凭据导入并验证成功",
		Email:         meta.Email,
		Tier:          tier,
		ModelsCount:   cnt,
		Authenticated: true,
	})
	am.oauthMu.Unlock()
	return tier, cnt, nil
}

// LogoutAccount revokes/removes the account's saved OAuth credentials and stops its active runtime.
func (am *AccountManager) LogoutAccount(accountID string) error {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("未找到账号 %q", accountID)
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	var oldSess *pendingOAuthSession
	am.oauthMu.Lock()
	am.ensureOAuthMapsLocked()
	if cur := am.oauthActive[acc.ID]; cur != nil {
		oldSess = cur
		am.cancelOAuthSessionLocked(cur)
	}
	am.oauthMu.Unlock()
	if oldSess != nil {
		waitOAuthProcExit(oldSess, 3*time.Second)
		cleanupOAuthSessionArtifacts(oldSess)
	}

	if acc.Primary {
		if !testing.Testing() {
			GlobalEngine.StopEngine()
		}
	} else {
		am.StopAccountWorker(acc.ID)
	}

	_ = am.logoutAccountViaACP(acc)

	tokPath := am.AccountTokenPath(acc.ID)
	_ = os.Remove(tokPath)
	_ = os.Remove(tokPath + ".oauth-bak")
	_ = os.Remove(filepath.Join(filepath.Dir(tokPath), "acp_token.json"))
	am.InvalidateTokenCache(acc.ID)
	am.ClearStickyForAccount(acc.ID)
	am.ClearAccountCooldown(acc.ID)
	am.ClearWorkerBackoff(acc.ID)
	am.ResetRoutingWeights()
	if acc.Primary {
		resetPrimaryQuotaCache()
	}
	InvalidatePeerDashboardCache()

	am.oauthMu.Lock()
	am.recordOAuthSnapshotLocked(OAuthFlowSnapshot{
		AccountID:     acc.ID,
		Phase:         "idle",
		Mode:          "acp_code",
		Message:       "已退出 Google 登录",
		Authenticated: false,
	})
	am.oauthMu.Unlock()
	return nil
}

func stopDisposableACPProcess(cmd *exec.Cmd, stdin io.WriteCloser, done <-chan struct{}) {
	if stdin != nil {
		_ = stdin.Close()
	}
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	if done != nil {
		timer := time.NewTimer(600 * time.Millisecond)
		select {
		case <-done:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	if done != nil {
		<-done
	}
}

func (am *AccountManager) verifyAccountViaACP(acc AccountProfile) (tierLabel string, modelsCount int, err error) {
	return am.verifyAccountViaACPInDir(acc, am.AccountHomeDir(acc.ID), am.AccountGeminiDir(acc.ID))
}

func (am *AccountManager) verifyAccountViaACPInDir(acc AccountProfile, homeDir, geminiDir string) (tierLabel string, modelsCount int, err error) {
	if testing.Testing() && !oauthVerifyInTest {
		return "测试环境已验证", 0, nil
	}

	proxy, proxyErr := am.ResolveAccountProxyChecked(acc)
	if proxyErr != nil {
		return "", 0, proxyErr
	}

	verifyDir, err := os.MkdirTemp("", "agy-acp-verify-*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		_ = os.RemoveAll(verifyDir)
	}()

	cmd := exec.Command(PatchedAgyBin, "acp", "--gemini-dir", geminiDir, "--app-data-dir", "antigravity-cli", "--disable-telemetry")
	cmd.Dir = verifyDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = oauthSubprocessEnv(homeDir, geminiDir, proxy)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return "", 0, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		return "", 0, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return "", 0, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		return "", 0, err
	}

	type rpcMsg struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	msgCh := make(chan rpcMsg, 8)
	done := make(chan struct{})
	var errBuf bytes.Buffer
	var errMu sync.Mutex
	var ioWG sync.WaitGroup
	ioWG.Add(2)
	go func() {
		defer ioWG.Done()
		sc := bufio.NewScanner(stdoutPipe)
		sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 || line[0] != '{' {
				continue
			}
			var m rpcMsg
			if json.Unmarshal(line, &m) == nil && m.ID > 0 {
				msgCh <- m
			}
		}
		_, _ = io.Copy(io.Discard, stdoutPipe)
	}()
	go func() {
		defer ioWG.Done()
		sc := bufio.NewScanner(stderrPipe)
		sc.Buffer(make([]byte, 32*1024), 256*1024)
		for sc.Scan() {
			errMu.Lock()
			if errBuf.Len() < 8192 {
				errBuf.WriteString(sc.Text() + "\n")
			}
			errMu.Unlock()
		}
		_, _ = io.Copy(io.Discard, stderrPipe)
	}()
	go func() {
		ioWG.Wait()
		_ = cmd.Wait()
		close(done)
	}()

	defer stopDisposableACPProcess(cmd, stdinPipe, done)

	initReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": 1,
			"clientCapabilities": map[string]any{
				"fs":       map[string]any{"readTextFile": false, "writeTextFile": false},
				"terminal": false,
			},
			"clientInfo": map[string]any{"name": "antigravity", "version": detectAgySemver(PatchedAgyBin)},
		},
	}
	authReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "authenticate",
		"params":  map[string]any{"methodId": "oauth-personal"},
	}
	b1, _ := json.Marshal(initReq)
	b2, _ := json.Marshal(authReq)
	if _, err := stdinPipe.Write(append(append(b1, '\n'), append(b2, '\n')...)); err != nil {
		return "", 0, err
	}

	timer := time.NewTimer(oauthVerifyTimeout)
	defer timer.Stop()
	for {
		select {
		case m := <-msgCh:
			if m.ID == 1 {
				if m.Error != nil {
					return "", 0, fmt.Errorf("Antigravity ACP 初始化失败: %s", redactAccountErrorString(truncateRunes(m.Error.Message, 200), proxy))
				}
				continue
			}
			if m.ID == 2 {
				if m.Error != nil {
					em := m.Error.Message
					low := strings.ToLower(em)
					switch {
					case strings.Contains(em, "SUBSCRIPTION_REQUIRED"):
						return "", 0, errors.New("该 Google 账号尚未开通有效的 Antigravity 订阅资格")
					case strings.Contains(low, "access_denied") || strings.Contains(low, "denied access") || strings.Contains(low, "cancelled"):
						return "", 0, errors.New("Google 登录授权未批准，请重新登录")
					case strings.Contains(em, "No browser is available"):
						return "", 0, errors.New("凭据未被 Antigravity 接受或已失效，请重新完成 Google 登录")
					default:
						return "", 0, fmt.Errorf("Antigravity 验证失败: %s", redactAccountErrorString(truncateRunes(em, 200), proxy))
					}
				}
				var authRes struct {
					Meta *struct {
						Antigravity *struct {
							Tier *struct {
								Name string `json:"name"`
								ID   string `json:"id"`
							} `json:"tier"`
						} `json:"antigravity"`
					} `json:"_meta"`
				}
				tier := ""
				if json.Unmarshal(m.Result, &authRes) == nil && authRes.Meta != nil && authRes.Meta.Antigravity != nil && authRes.Meta.Antigravity.Tier != nil {
					tier = strings.TrimSpace(authRes.Meta.Antigravity.Tier.Name)
				}
				if tier == "" {
					tier = "Google 账号已验证"
				}
				return tier, 0, nil
			}
		case <-done:
			errMu.Lock()
			es := strings.TrimSpace(errBuf.String())
			errMu.Unlock()
			if es == "" {
				es = "agy acp 验证进程意外退出"
			}
			return "", 0, fmt.Errorf("Antigravity 验证未完成: %s", redactAccountErrorString(truncateRunes(es, 200), proxy))
		case <-timer.C:
			return "", 0, fmt.Errorf("Antigravity 凭据验证超时 (%s)，请检查代理连通性", oauthVerifyTimeout)
		}
	}
}

func (am *AccountManager) logoutAccountViaACP(acc AccountProfile) error {
	if testing.Testing() && !oauthVerifyInTest {
		return nil
	}
	homeDir := am.AccountHomeDir(acc.ID)
	geminiDir := am.AccountGeminiDir(acc.ID)
	proxy := am.ResolveAccountProxy(acc)

	tmpDir, err := os.MkdirTemp("", "agy-acp-logout-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	cmd := exec.Command(PatchedAgyBin, "acp", "--gemini-dir", geminiDir, "--app-data-dir", "antigravity-cli", "--disable-telemetry")
	cmd.Dir = tmpDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = oauthSubprocessEnv(homeDir, geminiDir, proxy)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		return err
	}

	type rpcMsg struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	msgCh := make(chan rpcMsg, 4)
	done := make(chan struct{})
	var ioWG sync.WaitGroup
	ioWG.Add(2)
	go func() {
		defer ioWG.Done()
		sc := bufio.NewScanner(stdoutPipe)
		sc.Buffer(make([]byte, 64*1024), 2*1024*1024)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 || line[0] != '{' {
				continue
			}
			var m rpcMsg
			if json.Unmarshal(line, &m) == nil && m.ID > 0 {
				msgCh <- m
			}
		}
		_, _ = io.Copy(io.Discard, stdoutPipe)
	}()
	go func() {
		defer ioWG.Done()
		_, _ = io.Copy(io.Discard, stderrPipe)
	}()
	go func() {
		ioWG.Wait()
		_ = cmd.Wait()
		close(done)
	}()
	defer stopDisposableACPProcess(cmd, stdinPipe, done)

	initReq, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": 1,
			"clientInfo":      map[string]any{"name": "antigravity", "version": detectAgySemver(PatchedAgyBin)},
		},
	})
	if _, err := stdinPipe.Write(append(initReq, '\n')); err != nil {
		return err
	}

	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	sentLogout := false
	for {
		select {
		case m := <-msgCh:
			if m.ID == 1 && !sentLogout {
				var initRes struct {
					AgentCapabilities *struct {
						Auth *struct {
							Logout any `json:"logout"`
						} `json:"auth"`
					} `json:"agentCapabilities"`
				}
				if json.Unmarshal(m.Result, &initRes) != nil || initRes.AgentCapabilities == nil || initRes.AgentCapabilities.Auth == nil || initRes.AgentCapabilities.Auth.Logout == nil {
					return nil
				}
				sentLogout = true
				logoutReq, _ := json.Marshal(map[string]any{
					"jsonrpc": "2.0",
					"id":      2,
					"method":  "logout",
					"params":  map[string]any{},
				})
				_, _ = stdinPipe.Write(append(logoutReq, '\n'))
				continue
			}
			if m.ID == 2 {
				return nil
			}
		case <-done:
			return nil
		case <-timer.C:
			return context.DeadlineExceeded
		}
	}
}

func (am *AccountManager) buildAccountSubprocessEnv(homeDir, proxy string) []string {
	env := make([]string, 0, len(os.Environ())+16)
	hasSSH := false
	for _, kv := range os.Environ() {
		if isStrippedSubprocessEnv(kv) {
			continue
		}
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			hasSSH = true
		}
		env = append(env, kv)
	}
	if runtime.GOOS == "darwin" && !hasSSH {
		env = append(env, "SSH_CONNECTION=1 1 1 1")
	}
	env = append(env,
		"HOME="+homeDir,
		"AGY_ACP_FORCE_FILE_STORAGE=1",
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null",
		"AGY_CLI_DISABLE_AUTO_UPDATE=1",
		"HTTP_PROXY="+proxy,
		"HTTPS_PROXY="+proxy,
		"http_proxy="+proxy,
		"https_proxy="+proxy,
		"ALL_PROXY=",
		"all_proxy=",
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	)
	return env
}

func (am *AccountManager) probeAccountModelsTier(acc AccountProfile) (tierLabel string, modelsCount int) {
	if testing.Testing() {
		return "测试环境已验证", 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = am.EnsureAccountSandboxSeeded(acc.ID)
	homeDir := am.AccountHomeDir(acc.ID)
	proxy, pErr := am.ResolveAccountProxyChecked(acc)
	if pErr != nil {
		return "代理未就绪", 0
	}
	cmd := exec.CommandContext(ctx, PatchedAgyBin, "models")
	cmd.Dir = homeDir
	cmd.Env = am.buildAccountSubprocessEnv(homeDir, proxy)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "已写入凭据", 0
	}
	s := string(out)
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "gemini-") || strings.HasPrefix(line, "claude-") || strings.HasPrefix(line, "gpt-") {
			modelsCount++
		}
	}
	if strings.Contains(s, "gemini-pro-orchestrator-g1-pro") {
		return fmt.Sprintf("Google One AI Pro (%d 款模型)", modelsCount), modelsCount
	}
	if strings.Contains(s, "gemini-4-carbon") {
		return fmt.Sprintf("20x / Ultra 池 (%d 款模型)", modelsCount), modelsCount
	}
	return fmt.Sprintf("已验证 (%d 款模型)", modelsCount), modelsCount
}

// ProbeAccountEgress verifies the account's resolved proxy egress IP via ipinfo.io and checks Google
// reachability without ever sending raw Go HTTP packets to private Cloud Code Assist CCPA endpoints (C1):
// authenticated accounts run official `agy models` inside the account sandbox; unauthenticated accounts
// test generic public `https://www.google.com/generate_204`.
func (am *AccountManager) ProbeAccountEgress(accountID string) (map[string]any, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return nil, fmt.Errorf("未找到账号 %q", accountID)
	}
	proxyStr, err := am.ResolveAccountProxyChecked(acc)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		Proxy: nil,
	}
	defer tr.CloseIdleConnections()
	if proxyStr != "" {
		u, err := url.Parse(proxyStr)
		if err != nil {
			return nil, fmt.Errorf("代理地址格式错误: %s", redactAccountErrorString(err.Error(), proxyStr))
		}
		tr.Proxy = http.ProxyURL(u)
	}
	cli := &http.Client{Transport: tr, Timeout: 12 * time.Second}

	maskedIP := "直连/未知"
	country := ""
	city := ""
	org := ""
	reqIP, _ := http.NewRequest(http.MethodGet, "https://ipinfo.io/json", nil)
	if resp, err := cli.Do(reqIP); err == nil {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()
		var d map[string]any
		if json.Unmarshal(body, &d) == nil {
			rawIP, _ := d["ip"].(string)
			parts := strings.Split(rawIP, ".")
			if len(parts) == 4 {
				maskedIP = parts[0] + "." + parts[1] + ".x.x"
			} else if rawIP != "" {
				maskedIP = "IPv6"
			}
			country, _ = d["country"].(string)
			city, _ = d["city"].(string)
			org, _ = d["org"].(string)
		}
	}

	t0 := time.Now()
	ccStatus := 0
	if !testing.Testing() && am.HasOAuthToken(acc.ID) && fileExists(PatchedAgyBin) {
		_ = am.EnsureAccountSandboxSeeded(acc.ID)
		homeDir := am.AccountHomeDir(acc.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		cmd := exec.CommandContext(ctx, PatchedAgyBin, "models")
		cmd.Dir = homeDir
		cmd.Env = am.buildAccountSubprocessEnv(homeDir, proxyStr)
		out, cmdErr := cmd.CombinedOutput()
		cancel()
		if cmdErr != nil {
			detail := strings.TrimSpace(string(out))
			if detail == "" {
				detail = cmdErr.Error()
			}
			return nil, fmt.Errorf("通过官方 agy 校验 Google 连通失败: %s", redactAccountErrorString(truncateRunes(detail, 200), proxyStr))
		}
		ccStatus = http.StatusOK
	} else {
		reqG, _ := http.NewRequest(http.MethodHead, "https://www.google.com/generate_204", nil)
		if resp, err := cli.Do(reqG); err == nil {
			ccStatus = resp.StatusCode
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		} else {
			return nil, fmt.Errorf("连接 Google 网络失败: %s", redactAccountErrorString(err.Error(), proxyStr))
		}
	}
	ccMs := int(time.Since(t0).Milliseconds())

	return map[string]any{
		"ok":              ccStatus > 0,
		"accountId":       acc.ID,
		"proxyPreset":     acc.ProxyPreset,
		"maskedIp":        maskedIP,
		"country":         country,
		"city":            city,
		"org":             org,
		"cloudcodeStatus": ccStatus,
		"cloudcodeMs":     ccMs,
	}, nil
}

// requireDashboardMutationGuard blocks cross-site browser CSRF requests against mutating /api/accounts*
// endpoints while preserving non-browser API / curl automation without Origin or Sec-Fetch-Site headers.
func requireDashboardMutationGuard(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	secFetchSite := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")))
	if origin == "" && secFetchSite == "" {
		return true
	}
	if strings.TrimSpace(r.Header.Get("X-Agy-Requested-With")) != "dashboard" {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "forbidden: missing dashboard CSRF header"})
		return false
	}
	if secFetchSite != "" && secFetchSite != "same-origin" && secFetchSite != "none" {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "forbidden: cross-site dashboard mutation rejected"})
		return false
	}
	// Reuse same-origin check behind authenticated /acp/ reverse proxy; require direct loopback Host==Origin when unauthenticated.
	if origin != "" {
		okOrigin := false
		if !authEnabled() {
			okOrigin = isDirectLoopbackSameOrigin(r)
		} else {
			okOrigin = sameOrigin(r)
		}
		if !okOrigin {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "forbidden: origin mismatch on dashboard mutation"})
			return false
		}
	}
	return true
}

// HandleAccountsAPI dispatches /api/accounts* requests from the dashboard.
func HandleAccountsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.Path

	if r.Method == http.MethodGet && path == "/api/accounts" {
		now := time.Now()
		accs := GlobalAccountManager.ListAccounts()
		routingCfg := GlobalAccountManager.GetRoutingConfig()
		groups := GlobalAccountManager.ListGroups()
		ttl := GlobalAccountManager.stickyTTL()
		GlobalAccountManager.stickyMu.Lock()
		totalSticky, stickyByAcc := GlobalAccountManager.stickyLRU.snapshotCounts(now, ttl)
		GlobalAccountManager.stickyMu.Unlock()

		var totalInFlight int64
		healthyCount := 0
		enabledCount := 0
		views := make([]map[string]any, 0, len(accs))
		for _, a := range accs {
			meta := GlobalAccountManager.ReadAccountTokenMeta(a.ID)
			hasTok := meta.Authenticated
			st, _ := GlobalAccountManager.GetOAuthStatus(a.ID)
			running, gwPort, dashPort := GlobalAccountManager.IsWorkerRunning(a.ID)
			if a.Primary {
				running = true
				gwPort = GatewayPort
				dashPort = DashboardPort
			}
			hView := GlobalAccountManager.BuildAccountHealthView(a, hasTok, stickyByAcc[a.ID], now)
			totalInFlight += hView.InFlight
			if a.Enabled {
				enabledCount++
				if hView.State == "healthy" {
					healthyCount++
				}
			}
			views = append(views, map[string]any{
				"id":                      a.ID,
				"label":                   a.Label,
				"primary":                 a.Primary,
				"enabled":                 a.Enabled,
				"schedState":              a.EffectiveSchedState(),
				"isActiveTarget":          routingCfg.ActiveAccountID != "" && routingCfg.ActiveAccountID == a.ID,
				"priority":                a.EffectivePriority(),
				"weight":                  a.EffectiveWeight(),
				"maxConcurrency":          a.MaxConcurrency,
				"effectiveMaxConcurrency": a.EffectiveMaxConcurrency(),
				"groups":                  a.Groups,
				"notes":                   a.Notes,
				"authenticated":           hasTok,
				"email":                   meta.Email,
				"oauthPhase":              st.Phase,
				"running":                 running,
				"gwPort":                  gwPort,
				"dashPort":                dashPort,
				"proxyPreset":             a.ProxyPreset,
				"customProxy":             maskProxyURL(a.CustomProxy),
				"maxLiveWorkers":          a.MaxLiveWorkers,
				"boundKeys":               a.BoundKeys,
				"allowedModels":           a.AllowedModels,
				"defaultModels":           a.DefaultModels,
				"keySuffixHint":           "@" + a.ID,
				"health":                  hView,
			})
		}
		availModels := make([]map[string]any, 0, len(AgyModels))
		for _, m := range AgyModels {
			availModels = append(availModels, map[string]any{
				"id":     m.ID,
				"label":  m.Label,
				"family": m.Family,
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"routing": routingCfg,
			"groups":  groups,
			"poolStats": map[string]any{
				"totalAccounts":    len(accs),
				"enabledAccounts":  enabledCount,
				"healthyAccounts":  healthyCount,
				"totalInFlight":    totalInFlight,
				"stickySessions":   totalSticky,
				"stickyMaxEntries": routingCfg.StickyMaxEntries,
				"stickyTtlSec":     int(ttl.Seconds()),
			},
			"accounts":        views,
			"proxyPresets":    LoadProxyPresets(),
			"availableModels": availModels,
		})
		return
	}

	if r.Method == http.MethodGet && path == "/api/accounts/oauth/status" {
		accountID := r.URL.Query().Get("accountId")
		if accountID == "" {
			accountID = r.URL.Query().Get("id")
		}
		snap, err := GlobalAccountManager.GetOAuthStatus(accountID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": snap})
		return
	}

	if r.Method == http.MethodPost || r.Method == http.MethodDelete {
		if !requireDashboardMutationGuard(w, r) {
			return
		}
	}

	if r.Method == http.MethodPost && path == "/api/accounts" {
		var in AccountProfile
		if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&in); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		saved, err := GlobalAccountManager.UpsertAccount(in)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		saved.CustomProxy = maskProxyURL(saved.CustomProxy)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "account": saved})
		return
	}
	if r.Method == http.MethodDelete && path == "/api/accounts" {
		id := r.URL.Query().Get("id")
		if err := GlobalAccountManager.DeleteAccount(id); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/oauth/start" {
		var req struct {
			AccountID string `json:"accountId"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		snap, err := GlobalAccountManager.StartOAuthLoginFlow(req.AccountID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"accountId": snap.AccountID,
			"authUrl":   snap.AuthorizationURL,
			"flowId":    snap.FlowID,
			"expiresAt": snap.ExpiresAt,
			"phase":     snap.Phase,
			"mode":      snap.Mode,
			"message":   snap.Message,
			"status":    snap,
		})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/oauth/submit" {
		var req struct {
			AccountID string `json:"accountId"`
			FlowID    string `json:"flowId"`
			Code      string `json:"code"`
			Response  string `json:"response"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		rawInput := req.Code
		if strings.TrimSpace(rawInput) == "" {
			rawInput = req.Response
		}
		snap, err := GlobalAccountManager.SubmitOAuthCodeFlow(req.AccountID, req.FlowID, rawInput)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"accountId":   snap.AccountID,
			"tier":        snap.Tier,
			"modelsCount": snap.ModelsCount,
			"email":       snap.Email,
			"status":      snap,
		})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/oauth/cancel" {
		var req struct {
			AccountID string `json:"accountId"`
			FlowID    string `json:"flowId"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		snap, err := GlobalAccountManager.CancelOAuthLogin(req.AccountID, req.FlowID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": snap})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/oauth/logout" {
		var req struct {
			AccountID string `json:"accountId"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		if err := GlobalAccountManager.LogoutAccount(req.AccountID); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"accountId": strings.ToLower(strings.TrimSpace(req.AccountID)),
		})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/token" {
		var req struct {
			AccountID string `json:"accountId"`
			Token     string `json:"token"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		tier, cnt, err := GlobalAccountManager.ImportOAuthToken(req.AccountID, req.Token)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		meta := GlobalAccountManager.ReadAccountTokenMeta(strings.ToLower(strings.TrimSpace(req.AccountID)))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"accountId":   strings.ToLower(strings.TrimSpace(req.AccountID)),
			"tier":        tier,
			"modelsCount": cnt,
			"email":       meta.Email,
		})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/probe-ip" {
		var req struct {
			AccountID string `json:"accountId"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		res, err := GlobalAccountManager.ProbeAccountEgress(req.AccountID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(res)
		return
	}
	if r.Method == http.MethodPost && path == "/api/accounts/routing" {
		var req AccountPoolConfig
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		saved, err := GlobalAccountManager.UpdateRoutingConfig(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "routing": saved})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/groups" {
		var req AccountGroup
		if err := json.NewDecoder(io.LimitReader(r.Body, 128*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		saved, err := GlobalAccountManager.UpsertGroup(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "group": saved})
		return
	}

	if r.Method == http.MethodDelete && path == "/api/accounts/groups" {
		gid := r.URL.Query().Get("id")
		if err := GlobalAccountManager.DeleteGroup(gid); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/action" {
		var req AccountActionRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		res, err := GlobalAccountManager.PerformAccountActionEx(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(res)
		return
	}

	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "unknown accounts endpoint"})
}
