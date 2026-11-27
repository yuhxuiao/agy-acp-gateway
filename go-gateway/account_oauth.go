package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func openLinuxPTY() (master *os.File, slave *os.File, err error) {
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	var unlock int32
	const tiocsptlck = 0x40045431
	const tiocgptn = 0x80045430
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), tiocsptlck, uintptr(unsafe.Pointer(&unlock))); errno != 0 {
		_ = ptmx.Close()
		return nil, nil, errno
	}
	var ptn uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), tiocgptn, uintptr(unsafe.Pointer(&ptn))); errno != 0 {
		_ = ptmx.Close()
		return nil, nil, errno
	}
	pts, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", ptn), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		_ = ptmx.Close()
		return nil, nil, err
	}
	return ptmx, pts, nil
}

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
		"AGY_API_KEYS=", "AGY_API_KEYS_FILE=",
		"AGY_RESIN_PROXY_TOKEN=", "AGY_RESIN_HOST=",
		"AGY_PROXY_PRESETS_FILE=",
	} {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
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
				"role":            acc.EffectiveRole(),
				"self":            false,
				"reachable":       false,
				"stale":           false,
				"unauthenticated": true,
				"proxyPreset":     acc.ProxyPreset,
				"boundKeys":       maskBoundKeysForDashboard(acc.BoundKeys),
				"boundKeyCount":   len(acc.BoundKeys),
				"allowedModels":   acc.AllowedModels,
				"defaultModels":   acc.DefaultModels,
				"error":           "待网页 OAuth 授权或导入凭据",
				"fetchedAt":       "",
				"gateway": map[string]any{
					"instanceId":          acc.ID,
					"instanceLabel":       acc.Label,
					"activeSessionsCount": 0,
					"maxLiveWorkers":      acc.MaxLiveWorkers,
				},
				"quota": QuotaSnapshot{
					Status: "not_enabled",
					Error:  "待网页 OAuth 授权",
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
			"role":          acc.EffectiveRole(),
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
	return am.upsertAccountWithMask(in, false, true)
}

func (am *AccountManager) upsertAccount(in AccountProfile, hasPlanTierOverride bool) (AccountProfile, error) {
	return am.upsertAccountWithMask(in, hasPlanTierOverride, true)
}

func (am *AccountManager) upsertAccountWithMask(in AccountProfile, hasPlanTierOverride, hasIgnoreBurnAnomaly bool) (AccountProfile, error) {
	am.ensureInitialized()
	in.ID = strings.ToLower(strings.TrimSpace(in.ID))
	if !validAccountIDRe.MatchString(in.ID) {
		return AccountProfile{}, errors.New("账号 ID 仅允许小写字母、数字、连字符或下划线 (1-32 位)")
	}
	in.Label = strings.TrimSpace(in.Label)
	if in.Label == "" {
		in.Label = in.ID
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

	hasTok := am.snapshotAuthMap()

	am.mu.Lock()
	foundIdx := -1
	var prevAcc AccountProfile
	for i, a := range am.accounts {
		if a.ID == in.ID {
			foundIdx = i
			prevAcc = a
			in.Primary = a.Primary
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
	if foundIdx < 0 {
		in.Primary = false
	}
	in.Role = normalizeAccountRole(in.Role)
	in.PlanTierHint = normalizePlanTierHint(in.PlanTierHint)
	in.PlanTierOverride = normalizePlanTierHint(in.PlanTierOverride)
	if in.MaxLiveWorkers <= 0 {
		if foundIdx >= 0 && prevAcc.MaxLiveWorkers > 0 {
			in.MaxLiveWorkers = prevAcc.MaxLiveWorkers
		} else if in.Primary {
			if bootW := gwcfg.Current().MaxLiveWorkers; bootW > 0 {
				in.MaxLiveWorkers = bootW
			} else {
				in.MaxLiveWorkers = 8
			}
		} else {
			in.MaxLiveWorkers = 3
		}
	}
	if maxCap := maxAllowedAccountLiveWorkers(); in.MaxLiveWorkers > maxCap {
		in.MaxLiveWorkers = maxCap
	}
	prevRouting := am.routing
	prevGroups := append([]AccountGroup(nil), am.groups...)
	if foundIdx >= 0 {
		if in.PlanTierHint == "" {
			in.PlanTierHint = normalizePlanTierHint(prevAcc.PlanTierHint)
		}
		if !hasPlanTierOverride && in.PlanTierOverride == "" {
			in.PlanTierOverride = normalizePlanTierHint(prevAcc.PlanTierOverride)
		}
		if !hasIgnoreBurnAnomaly {
			in.IgnoreBurnAnomaly = prevAcc.IgnoreBurnAnomaly
		}
		in.SchedState, in.Enabled = normalizeSchedState(in.SchedState, in.Enabled)
		am.accounts[foundIdx] = in
	} else {
		in.Primary = false
		in.SchedState, in.Enabled = normalizeSchedState(in.SchedState, in.Enabled)
		am.accounts = append(am.accounts, in)
	}
	if !in.Enabled {
		am.advanceActiveOnDisableLocked(in.ID, hasTok)
	}
	err := am.saveToDiskLocked()
	if err != nil {
		if foundIdx >= 0 {
			am.accounts[foundIdx] = prevAcc
		} else {
			am.accounts = am.accounts[:len(am.accounts)-1]
		}
		am.routing = prevRouting
		am.groups = prevGroups
	} else if in.Primary {
		am.syncPrimaryMaxLiveWorkersLocked()
	}
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

	if in.Primary {
		primaryDisabledTransition := !in.Enabled && (foundIdx < 0 || prevAcc.Enabled)
		primaryProxyChanged := foundIdx >= 0 && (prevAcc.ProxyPreset != in.ProxyPreset || prevAcc.CustomProxy != in.CustomProxy)
		if (primaryDisabledTransition || primaryProxyChanged) && GlobalEngine != nil {
			GlobalEngine.StopEngine()
			StopImageGenEngine()
			StopMediaRelayEngine()
		}
	}

	planTierOverrideChanged := foundIdx >= 0 && prevAcc.PlanTierOverride != in.PlanTierOverride
	if in.Primary && planTierOverrideChanged {
		InvalidatePrimaryQuotaTierCache(in.PlanTierOverride)
	}
	workerCfgChanged := foundIdx < 0 ||
		prevAcc.ProxyPreset != in.ProxyPreset ||
		prevAcc.CustomProxy != in.CustomProxy ||
		prevAcc.MaxLiveWorkers != in.MaxLiveWorkers ||
		prevAcc.Enabled != in.Enabled ||
		prevAcc.PlanTierHint != in.PlanTierHint ||
		planTierOverrideChanged
	if !in.Primary && workerCfgChanged {
		am.StopAccountWorker(in.ID)
		if cur, ok := am.GetAccount(in.ID); ok && cur.Enabled && am.HasOAuthToken(in.ID) {
			_ = am.EnsureAccountWorkerRunning(in.ID)
		}
	} else if !in.Primary && foundIdx >= 0 && prevAcc.IgnoreBurnAnomaly != in.IgnoreBurnAnomaly {
		am.mu.RLock()
		dashPort := 0
		if w := am.workers[in.ID]; w != nil && !w.stopped && w.dashPort > 0 {
			dashPort = w.dashPort
		}
		rCfg := am.routing
		am.mu.RUnlock()
		if dashPort > 0 {
			ign := in.IgnoreBurnAnomaly
			am.pushMonitorPolicyToChild(in.ID, dashPort, rCfg, &ign)
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
	am.oauthMu.Lock()
	if old, exists := am.oauthActive[accountID]; exists && old != nil {
		am.cancelOAuthSessionLocked(old, false)
	}
	am.oauthMu.Unlock()
	am.InvalidateTokenCache(accountID)
	am.ClearStickyForAccount(accountID)
	am.ClearAccountCooldown(accountID)
	ResetAccountMonitorState(accountID, true)
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

// maybeRestartRestoredAccountWorker restarts a non-primary enabled account worker in the background
// after an aborted or failed OAuth login restores its previous token backup.
func (am *AccountManager) maybeRestartRestoredAccountWorker(accountID string) {
	if testing.Testing() || isChildAccountWorker() {
		return
	}
	acc, ok := am.GetAccount(accountID)
	if !ok || acc.Primary || !acc.Enabled || !am.HasOAuthToken(acc.ID) {
		return
	}
	go func(id string) {
		_ = am.EnsureAccountWorkerRunning(id)
	}(acc.ID)
}

// StartOAuthLogin spawns `agy -p /login` with the account's isolated HOME and resolved proxy,
// capturing the Google OAuth URL and waiting on stdin for the user's authorization code.
func (am *AccountManager) StartOAuthLogin(accountID string) (string, error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return "", fmt.Errorf("未找到账号 %q", accountID)
	}
	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	if !acc.Primary {
		am.StopAccountWorker(acc.ID)
	}

	am.oauthMu.Lock()
	if old, exists := am.oauthActive[acc.ID]; exists && old != nil {
		am.cancelOAuthSessionLocked(old, true)
	}
	am.oauthMu.Unlock()

	proxy, proxyErr := am.ResolveAccountProxyChecked(acc)
	if proxyErr != nil {
		am.maybeRestartRestoredAccountWorker(acc.ID)
		return "", proxyErr
	}
	homeDir := am.AccountHomeDir(acc.ID)
	tokPath := am.AccountTokenPath(acc.ID)
	if err := os.MkdirAll(filepath.Dir(tokPath), 0o700); err != nil {
		am.maybeRestartRestoredAccountWorker(acc.ID)
		return "", err
	}
	_ = am.EnsureAccountSandboxSeeded(acc.ID)
	bakPath := tokPath + ".oauth-bak"
	if fileExists(tokPath) && !fileExists(bakPath) {
		_ = os.Rename(tokPath, bakPath)
	} else if fileExists(tokPath) && fileExists(bakPath) {
		_ = os.Remove(bakPath)
		_ = os.Rename(tokPath, bakPath)
	}
	am.InvalidateTokenCache(acc.ID)

	restoreOnStartFailure := func() {
		if fileExists(bakPath) && !fileExists(tokPath) {
			_ = os.Rename(bakPath, tokPath)
			am.InvalidateTokenCache(acc.ID)
		}
		am.maybeRestartRestoredAccountWorker(acc.ID)
	}

	cmd := exec.Command(PatchedAgyBin, "-p", "/login")
	cmd.Dir = homeDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	env := make([]string, 0, len(os.Environ())+14)
	for _, kv := range os.Environ() {
		if isStrippedSubprocessEnv(kv) {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"HOME="+homeDir,
		"BROWSER=/bin/false",
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
	cmd.Env = env

	var stdinPipe io.WriteCloser
	var ptySlave *os.File
	if ptmx, pts, ptyErr := openLinuxPTY(); ptyErr == nil {
		cmd.Stdin = pts
		stdinPipe = ptmx
		ptySlave = pts
	} else {
		p, err := cmd.StdinPipe()
		if err != nil {
			restoreOnStartFailure()
			return "", err
		}
		stdinPipe = p
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		if ptySlave != nil {
			_ = ptySlave.Close()
		}
		if stdinPipe != nil {
			_ = stdinPipe.Close()
		}
		restoreOnStartFailure()
		return "", err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		if ptySlave != nil {
			_ = ptySlave.Close()
		}
		if stdinPipe != nil {
			_ = stdinPipe.Close()
		}
		restoreOnStartFailure()
		return "", err
	}
	if ptySlave != nil {
		_ = ptySlave.Close()
	}

	sess := &pendingOAuthSession{
		accountID: acc.ID,
		cmd:       cmd,
		stdin:     stdinPipe,
		output:    &bytes.Buffer{},
		tokenPath: tokPath,
		bakPath:   bakPath,
		startedAt: time.Now(),
		done:      make(chan struct{}),
		doneCh:    make(chan error, 1),
	}

	urlCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdoutPipe)
		sc.Buffer(make([]byte, 64*1024), 512*1024)
		sentURL := false
		for sc.Scan() {
			line := sc.Text()
			sess.outMu.Lock()
			if sess.output.Len() < 64*1024 {
				sess.output.WriteString(line + "\n")
			}
			sess.outMu.Unlock()
			if !sentURL {
				if m := oauthURLRe.FindString(line); m != "" {
					sentURL = true
					urlCh <- m
				}
			}
		}
		// Drain any unread trailing stdout (e.g. if scanner hit >512KB line) so child never deadlocks before Wait().
		_, _ = io.Copy(io.Discard, stdoutPipe)
		waitErr := cmd.Wait()
		close(sess.done)
		sess.doneCh <- waitErr
	}()

	select {
	case authURL := <-urlCh:
		sess.authURL = authURL
		am.oauthMu.Lock()
		if old, exists := am.oauthActive[acc.ID]; exists && old != nil && old != sess {
			am.cancelOAuthSessionLocked(old, false)
		}
		am.oauthActive[acc.ID] = sess
		am.oauthMu.Unlock()
		// Auto-reap on 70s abandonment OR unexpected early process exit before code submission.
		go func(s *pendingOAuthSession) {
			timer := time.NewTimer(70 * time.Second)
			defer timer.Stop()
			select {
			case <-s.done:
			case <-timer.C:
			}
			opLock := am.accountOpLock(s.accountID)
			opLock.Lock()
			am.oauthMu.Lock()
			reaped := false
			if cur := am.oauthActive[s.accountID]; cur == s {
				am.cancelOAuthSessionLocked(s, false)
				reaped = true
			}
			am.oauthMu.Unlock()
			if reaped {
				am.restoreOAuthBackupAfterExit(s)
				am.maybeRestartRestoredAccountWorker(s.accountID)
			}
			opLock.Unlock()
		}(sess)
		return authURL, nil
	case waitErr := <-sess.doneCh:
		sess.closeStdin()
		restoreOnStartFailure()
		sess.outMu.Lock()
		outStr := strings.TrimSpace(sess.output.String())
		sess.outMu.Unlock()
		return "", fmt.Errorf("未能获取 OAuth 授权链接 (%v): %s", waitErr, redactAccountErrorString(truncateRunes(outStr, 200), proxy))
	case <-time.After(8 * time.Second):
		am.oauthMu.Lock()
		am.cancelOAuthSessionLocked(sess, false)
		am.oauthMu.Unlock()
		am.restoreOAuthBackupAfterExit(sess)
		am.maybeRestartRestoredAccountWorker(acc.ID)
		return "", errors.New("启动 agy OAuth 握手超时 (8s)")
	}
}

func terminateOAuthProcGroup(s *pendingOAuthSession) {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	pid := s.cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	go func(done <-chan struct{}, pgid int) {
		timer := time.NewTimer(2 * time.Second)
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

func (am *AccountManager) restoreOAuthBackupAfterExit(s *pendingOAuthSession) {
	if s == nil {
		return
	}
	if s.done != nil {
		select {
		case <-s.done:
		case <-time.After(1500 * time.Millisecond):
		}
	}
	if s.bakPath != "" && fileExists(s.bakPath) && !fileExists(s.tokenPath) {
		_ = os.Rename(s.bakPath, s.tokenPath)
		am.InvalidateTokenCache(s.accountID)
	}
}

func (am *AccountManager) cancelOAuthSessionLocked(s *pendingOAuthSession, restoreBackup bool) {
	if s == nil {
		return
	}
	if am.oauthActive[s.accountID] == s {
		delete(am.oauthActive, s.accountID)
	}
	s.closeStdin()
	terminateOAuthProcGroup(s)
	if restoreBackup && s.bakPath != "" && fileExists(s.bakPath) && !fileExists(s.tokenPath) {
		_ = os.Rename(s.bakPath, s.tokenPath)
		am.InvalidateTokenCache(s.accountID)
	}
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

// SubmitOAuthCode feeds the user's authorization code into the waiting agy login session and verifies token creation.
func (am *AccountManager) SubmitOAuthCode(accountID, rawCode string) (tierLabel string, modelsCount int, err error) {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	acc, ok := am.GetAccount(accountID)
	if !ok {
		return "", 0, fmt.Errorf("未找到账号 %q", accountID)
	}
	code := extractAuthCodeFromInput(rawCode)
	if code == "" {
		return "", 0, errors.New("授权码不能为空")
	}

	opMu := am.accountOpLock(acc.ID)
	opMu.Lock()
	defer opMu.Unlock()

	am.oauthMu.Lock()
	sess := am.oauthActive[acc.ID]
	delete(am.oauthActive, acc.ID)
	am.oauthMu.Unlock()
	if sess == nil {
		return "", 0, errors.New("当前无进行中的 OAuth 授权会话，请先点击“获取 Google 授权链接”")
	}
	defer sess.closeStdin()

	proxy, proxyErr := am.ResolveAccountProxyChecked(acc)
	if proxyErr != nil {
		am.restoreOAuthBackupAfterExit(sess)
		am.maybeRestartRestoredAccountWorker(acc.ID)
		return "", 0, proxyErr
	}
	_, _ = io.WriteString(sess.stdin, code+"\n")

	// Wait up to 25s for token exchange to write antigravity-oauth-token, then give agy a brief
	// grace window to finish flushing files before sending SIGTERM.
	deadline := time.Now().Add(25 * time.Second)
waitLoop:
	for time.Now().Before(deadline) {
		am.InvalidateTokenCache(acc.ID)
		if am.HasOAuthToken(acc.ID) {
			select {
			case <-sess.done:
			case <-time.After(500 * time.Millisecond):
			}
			break waitLoop
		}
		select {
		case <-sess.done:
			break waitLoop
		case <-time.After(150 * time.Millisecond):
		}
	}
	terminateOAuthProcGroup(sess)

	am.InvalidateTokenCache(acc.ID)
	if !am.HasOAuthToken(acc.ID) {
		if sess.bakPath != "" && fileExists(sess.bakPath) {
			_ = os.Rename(sess.bakPath, sess.tokenPath)
			am.InvalidateTokenCache(acc.ID)
		}
		am.maybeRestartRestoredAccountWorker(acc.ID)
		sess.outMu.Lock()
		outStr := strings.TrimSpace(sess.output.String())
		sess.outMu.Unlock()
		return "", 0, fmt.Errorf("OAuth 换取凭据未成功: %s", redactAccountErrorString(truncateRunes(outStr, 240), proxy))
	}

	_ = os.Chmod(sess.tokenPath, 0o600)
	if sess.bakPath != "" {
		_ = os.Remove(sess.bakPath)
	}
	am.InvalidateTokenCache(acc.ID)
	InvalidateCreditsTierCache(am.AccountGeminiDir(acc.ID))
	am.ClearAccountCooldown(acc.ID)
	ResetAccountMonitorState(acc.ID, false)

	tier, cnt := am.probeAccountModelsTier(acc)
	if !acc.Primary && acc.Enabled {
		_ = am.EnsureAccountWorkerRunning(acc.ID)
	}
	InvalidatePeerDashboardCache()
	return tier, cnt, nil
}

// ImportOAuthToken writes a pasted antigravity-oauth-token file content directly into the account's isolated directory.
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

	am.oauthMu.Lock()
	if old, exists := am.oauthActive[acc.ID]; exists && old != nil {
		am.cancelOAuthSessionLocked(old, false)
	}
	am.oauthMu.Unlock()

	if _, err := am.ResolveAccountProxyChecked(acc); err != nil {
		return "", 0, err
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
		return "", 0, err
	}
	_ = os.Remove(tokPath + ".oauth-bak")
	am.InvalidateTokenCache(acc.ID)
	InvalidateCreditsTierCache(am.AccountGeminiDir(acc.ID))
	am.ClearAccountCooldown(acc.ID)
	ResetAccountMonitorState(acc.ID, false)

	tier, cnt := am.probeAccountModelsTier(acc)
	if !acc.Primary && acc.Enabled {
		_ = am.EnsureAccountWorkerRunning(acc.ID)
	}
	InvalidatePeerDashboardCache()
	return tier, cnt, nil
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

// accountModelsCmdArgs builds `<agy> [--gemini_dir <dir> --app_data_dir antigravity-cli-usage] models`
// so auxiliary `models` probes never scan the main ws-bridge conversations directory (#1825).
func (am *AccountManager) accountModelsCmdArgs(accountID string) []string {
	gemDir := strings.TrimSpace(am.AccountGeminiDir(accountID))
	if gemDir != "" && !isWindowsCDrivePath(gemDir) {
		if absGem, err := filepath.Abs(gemDir); err == nil && absGem != "" && !isWindowsCDrivePath(absGem) {
			if ensureIsolatedAppDataDir(absGem, UsageAppDataDirName) != "" {
				return []string{"--gemini_dir", absGem, "--app_data_dir", UsageAppDataDirName, "models"}
			}
		}
	}
	return []string{"models"}
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
	cmd := exec.CommandContext(ctx, PatchedAgyBin, am.accountModelsCmdArgs(acc.ID)...)
	cmd.Dir = homeDir
	cmd.Env = am.buildAccountSubprocessEnv(homeDir, proxy)
	configureQuotaSubprocess(cmd)
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
	if ov := normalizePlanTierHint(acc.PlanTierOverride); ov != "" {
		return fmt.Sprintf("%s (%d 款模型)", planLabelForTier(ov), modelsCount), modelsCount
	}
	if strings.Contains(s, "gemini-pro-orchestrator-g1-pro") {
		am.SetAccountPlanTierHint(acc.ID, "pro")
		return fmt.Sprintf("Google One AI Pro (%d 款模型)", modelsCount), modelsCount
	}
	if strings.Contains(s, "gemini-4-carbon") {
		if normalizePlanTierHint(acc.PlanTierHint) == "ultra_5x" {
			return fmt.Sprintf("Ultra 5x (%d 款模型)", modelsCount), modelsCount
		}
		am.SetAccountPlanTierHint(acc.ID, "ultra")
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
		cmd := exec.CommandContext(ctx, PatchedAgyBin, am.accountModelsCmdArgs(acc.ID)...)
		cmd.Dir = homeDir
		cmd.Env = am.buildAccountSubprocessEnv(homeDir, proxyStr)
		configureQuotaSubprocess(cmd)
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
			hasTok := GlobalAccountManager.HasOAuthToken(a.ID)
			running, gwPort, dashPort := GlobalAccountManager.IsWorkerRunning(a.ID)
			if a.Primary {
				running = a.Enabled
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
				"role":                    a.EffectiveRole(),
				"schedState":              a.EffectiveSchedState(),
				"isActiveTarget":          routingCfg.ActiveAccountID != "" && routingCfg.ActiveAccountID == a.ID,
				"priority":                a.EffectivePriority(),
				"weight":                  a.EffectiveWeight(),
				"maxConcurrency":          a.MaxConcurrency,
				"effectiveMaxConcurrency": a.EffectiveMaxConcurrency(),
				"groups":                  a.Groups,
				"notes":                   a.Notes,
				"ignoreBurnAnomaly":       a.IgnoreBurnAnomaly,
				"planTierHint":            a.PlanTierHint,
				"planTierOverride":        a.PlanTierOverride,
				"effectivePlanTier":       a.EffectivePlanTier(),
				"authenticated":           hasTok,
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
		availModels := make([]map[string]any, 0, len(AgyModels)+1)
		for _, m := range AgyModels {
			availModels = append(availModels, map[string]any{
				"id":     m.ID,
				"label":  m.Label,
				"family": m.Family,
			})
		}
		availModels = append(availModels, map[string]any{
			"id":     imageGenServedModel,
			"label":  "Gemini 3.1 Flash Image",
			"family": "gemini",
			"kind":   "image",
		})
		gMax := GlobalAccountManager.EffectiveGlobalIdleTarget()
		gBurst := GlobalAccountManager.EffectiveGlobalBurstMaxLiveWorkers()
		totalMaxLiveWorkers, effectiveMaxLiveWorkers, estPeakRssMB, hostMemLimitMB := computeEffectivePoolMemoryBudget(accs, gMax)
		memStat := readCgroupMemoryStatus()
		govSnap := GlobalAccountManager.SnapshotGlobalMemory()
		effGMax := gMax
		if effGMax <= 0 {
			effGMax = totalMaxLiveWorkers
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"routing": routingCfg.ForAPI(),
			"groups":  groups,
			"poolStats": map[string]any{
				"totalAccounts":             len(accs),
				"enabledAccounts":           enabledCount,
				"healthyAccounts":           healthyCount,
				"totalInFlight":             totalInFlight,
				"stickySessions":            totalSticky,
				"stickyMaxEntries":          routingCfg.StickyMaxEntries,
				"stickyTtlSec":              int(ttl.Seconds()),
				"totalMaxLiveWorkers":       totalMaxLiveWorkers,
				"effectiveMaxLiveWorkers":   effectiveMaxLiveWorkers,
				"globalMaxLiveWorkers":      effGMax,
				"globalIdleTarget":          gMax,
				"globalBurstMaxLiveWorkers": gBurst,
				"globalLiveWorkers":         govSnap.GlobalLiveWorkers,
				"globalLogicalSessions":     govSnap.GlobalLogicalSessions,
				"globalHibernatedSessions":  govSnap.GlobalHibernatedSessions,
				"estPeakRssMB":              estPeakRssMB,
				"hostMemLimitMB":            hostMemLimitMB,
				"cgroupWorkingSetBytes":     memStat.WorkingSetBytes,
				"cgroupAnonBytes":           memStat.AnonBytes,
				"cgroupFileBytes":           memStat.FileBytes,
				"cgroupLimitBytes":          memStat.LimitBytes,
				"cgroupUsagePct":            roundTo(memStat.UsagePct, 2),
				"monitorTrips":              MonitorTripSnapshot(now),
				"monitorBudgets":            SnapshotMonitorBudgetsForAccounts(accs, now),
			},
			"accounts":        views,
			"proxyPresets":    LoadProxyPresets(),
			"availableModels": availModels,
		})
		return
	}

	if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
		if !requireDashboardMutationGuard(w, r) {
			return
		}
	}

	if r.Method == http.MethodPost && path == "/api/accounts" {
		bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, 256*1024))
		if readErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "读取请求体失败: " + readErr.Error()})
			return
		}
		var in AccountProfile
		if err := json.Unmarshal(bodyBytes, &in); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		var rawKeys map[string]json.RawMessage
		_ = json.Unmarshal(bodyBytes, &rawKeys)
		_, hasPlanTierOverride := rawKeys["planTierOverride"]
		_, hasIgnoreBurnAnomaly := rawKeys["ignoreBurnAnomaly"]
		saved, err := GlobalAccountManager.upsertAccountWithMask(in, hasPlanTierOverride, hasIgnoreBurnAnomaly)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		saved.CustomProxy = maskProxyURL(saved.CustomProxy)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "account": saved})
		return
	}
	if r.Method == http.MethodPut && strings.HasPrefix(path, "/api/accounts/") {
		rawID := strings.TrimPrefix(path, "/api/accounts/")
		if rawID != "" && !strings.Contains(rawID, "/") {
			bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, 256*1024))
			if readErr != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "读取请求体失败: " + readErr.Error()})
				return
			}
			var in AccountProfile
			if err := json.Unmarshal(bodyBytes, &in); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
				return
			}
			normPathID := strings.ToLower(strings.TrimSpace(rawID))
			if strings.TrimSpace(in.ID) == "" {
				in.ID = normPathID
			} else if strings.ToLower(strings.TrimSpace(in.ID)) != normPathID {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "路径账号 ID 与请求体不一致"})
				return
			}
			var rawKeys map[string]json.RawMessage
			_ = json.Unmarshal(bodyBytes, &rawKeys)
			_, hasPlanTierOverride := rawKeys["planTierOverride"]
			_, hasIgnoreBurnAnomaly := rawKeys["ignoreBurnAnomaly"]
			saved, err := GlobalAccountManager.upsertAccountWithMask(in, hasPlanTierOverride, hasIgnoreBurnAnomaly)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
				return
			}
			saved.CustomProxy = maskProxyURL(saved.CustomProxy)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "account": saved})
			return
		}
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
		authURL, err := GlobalAccountManager.StartOAuthLogin(req.AccountID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "accountId": strings.ToLower(strings.TrimSpace(req.AccountID)), "authUrl": authURL})
		return
	}

	if r.Method == http.MethodPost && path == "/api/accounts/oauth/submit" {
		var req struct {
			AccountID string `json:"accountId"`
			Code      string `json:"code"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "JSON 格式错误: " + err.Error()})
			return
		}
		tier, cnt, err := GlobalAccountManager.SubmitOAuthCode(req.AccountID, req.Code)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "accountId": strings.ToLower(strings.TrimSpace(req.AccountID)), "tier": tier, "modelsCount": cnt})
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
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "accountId": strings.ToLower(strings.TrimSpace(req.AccountID)), "tier": tier, "modelsCount": cnt})
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
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "routing": saved.ForAPI()})
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
