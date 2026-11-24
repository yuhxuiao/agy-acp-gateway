package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// Client API keys are dashboard-issued credentials that only unlock the :18890 data plane.
// They are persisted as SHA-256 digests plus a short display preview; the plaintext is
// returned exactly once at creation time and never stored or logged.

const (
	ClientKeyPrefix       = "agy-ck-"
	ClientKeyScopeGlobal  = "global"
	ClientKeyScopeGroup   = "group"
	ClientKeyScopeAccount = "account"
	clientKeyRandomBytes  = 32
	clientKeyMaxNameRunes = 64
	clientKeyMaxNoteRunes = 512
	clientKeyMaxCount     = 512
)

// ClientAPIKey is the persisted (secret-free) record of a dashboard-issued client key.
type ClientAPIKey struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Notes         string    `json:"notes,omitempty"`
	Hash          string    `json:"hash"`
	Preview       string    `json:"preview"`
	Scope         string    `json:"scope"`
	ScopeID       string    `json:"scopeId,omitempty"`
	AllowedModels []string  `json:"allowedModels,omitempty"`
	Enabled       bool      `json:"enabled"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// AllowsModel mirrors AccountGroup.AllowsModel: empty list or "*" means unrestricted.
func (k ClientAPIKey) AllowsModel(canonicalModelID string) bool {
	if canonicalModelID == "" || len(k.AllowedModels) == 0 {
		return true
	}
	for _, m := range k.AllowedModels {
		if m == "*" || strings.EqualFold(strings.TrimSpace(m), canonicalModelID) {
			return true
		}
	}
	return false
}

// PublicView is the only representation handed to the dashboard (no hash, no plaintext).
func (k ClientAPIKey) PublicView(lastUsed time.Time) map[string]any {
	v := map[string]any{
		"id":            k.ID,
		"name":          k.Name,
		"notes":         k.Notes,
		"preview":       k.Preview,
		"scope":         k.Scope,
		"scopeId":       k.ScopeID,
		"allowedModels": append([]string{}, k.AllowedModels...),
		"enabled":       k.Enabled,
		"createdAt":     k.CreatedAt.UTC().Format(time.RFC3339),
		"updatedAt":     k.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if !lastUsed.IsZero() {
		v["lastUsedAt"] = lastUsed.UTC().Format(time.RFC3339)
	}
	return v
}

type clientKeysFileSchema struct {
	Version int            `json:"version"`
	Keys    []ClientAPIKey `json:"keys"`
}

// ClientKeyStore holds dashboard-issued client keys, indexed by digest.
type ClientKeyStore struct {
	mu       sync.RWMutex
	path     string
	loaded   bool
	loadErr  error
	keys     []ClientAPIKey
	byHash   map[string]int
	lastUsed sync.Map // id -> time.Time (in-memory only)
}

var GlobalClientKeys = &ClientKeyStore{}

func resolveClientKeysFilePath() string {
	if v := gwcfg.Current().ClientKeysFile; v != "" {
		return v
	}
	_, base := resolveAccountsFilePath()
	return filepath.Join(base, "client_keys.json")
}

func clientKeyDigest(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}

func looksLikeClientKey(candidate string) bool {
	return strings.HasPrefix(candidate, ClientKeyPrefix) && len(candidate) > len(ClientKeyPrefix)+16
}

func clientKeyPreview(plain string) string {
	if len(plain) <= len(ClientKeyPrefix)+8 {
		return ClientKeyPrefix + "…"
	}
	return plain[:len(ClientKeyPrefix)+4] + "…" + plain[len(plain)-4:]
}

func generateClientKeyPlaintext() (string, error) {
	buf := make([]byte, clientKeyRandomBytes)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return ClientKeyPrefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

// ensureLoaded (re)loads the store when the resolved file path changes (tests swap env per case).
// Child account workers never honour client keys: the parent authenticates and forwards over loopback.
func (s *ClientKeyStore) ensureLoaded() {
	if isChildAccountWorker() {
		return
	}
	p := resolveClientKeysFilePath()
	s.mu.RLock()
	ok := s.loaded && s.path == p
	s.mu.RUnlock()
	if ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded && s.path == p {
		return
	}
	s.path = p
	s.loaded = true
	s.loadErr = nil
	s.keys = nil
	s.byHash = map[string]int{}
	b, err := os.ReadFile(p)
	if err != nil {
		if !os.IsNotExist(err) {
			s.loadErr = errors.New("client key store is unreadable")
		}
		return
	}
	var f clientKeysFileSchema
	if err := json.Unmarshal(b, &f); err != nil {
		s.loadErr = errors.New("client key store is malformed")
		return
	}
	for _, k := range f.Keys {
		k.Hash = strings.ToLower(strings.TrimSpace(k.Hash))
		if len(k.Hash) != sha256.Size*2 || k.ID == "" {
			continue
		}
		if _, dup := s.byHash[k.Hash]; dup {
			continue
		}
		k.Scope, k.ScopeID = normalizeClientKeyScope(k.Scope, k.ScopeID)
		if len(k.AllowedModels) > 0 {
			k.AllowedModels = normalizeCanonicalModelList(k.AllowedModels)
		}
		s.byHash[k.Hash] = len(s.keys)
		s.keys = append(s.keys, k)
	}
}

func (s *ClientKeyStore) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(clientKeysFileSchema{Version: 1, Keys: s.keys}, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *ClientKeyStore) reindexLocked() {
	s.byHash = make(map[string]int, len(s.keys))
	for i, k := range s.keys {
		s.byHash[k.Hash] = i
	}
}

// LoadError reports a fail-closed condition (unreadable or corrupt store file).
func (s *ClientKeyStore) LoadError() error {
	s.ensureLoaded()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadErr
}

// HasAny reports whether any client key record exists (enabled or not). Issuing a key turns
// data-plane auth on even without AGY_API_KEYS, and disabling a key must not silently reopen it.
func (s *ClientKeyStore) HasAny() bool {
	s.ensureLoaded()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys) > 0 || s.loadErr != nil
}

// Lookup returns the record for candidate regardless of enabled state.
func (s *ClientKeyStore) Lookup(candidate string) (ClientAPIKey, bool) {
	if !looksLikeClientKey(candidate) {
		return ClientAPIKey{}, false
	}
	s.ensureLoaded()
	d := clientKeyDigest(candidate)
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.byHash[d]
	if !ok || idx >= len(s.keys) {
		return ClientAPIKey{}, false
	}
	k := s.keys[idx]
	if subtle.ConstantTimeCompare([]byte(k.Hash), []byte(d)) != 1 {
		return ClientAPIKey{}, false
	}
	return k, true
}

// Match returns an enabled client key for candidate and records its last use.
func (s *ClientKeyStore) Match(candidate string) (ClientAPIKey, bool) {
	k, ok := s.Lookup(candidate)
	if !ok || !k.Enabled {
		return ClientAPIKey{}, false
	}
	s.lastUsed.Store(k.ID, time.Now())
	return k, true
}

func (s *ClientKeyStore) List() []ClientAPIKey {
	s.ensureLoaded()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ClientAPIKey, len(s.keys))
	copy(out, s.keys)
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *ClientKeyStore) LastUsed(id string) time.Time {
	if v, ok := s.lastUsed.Load(id); ok {
		if t, ok := v.(time.Time); ok {
			return t
		}
	}
	return time.Time{}
}

// ClientKeyInput is the mutable part of a client key accepted from the dashboard.
type ClientKeyInput struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Notes         string   `json:"notes"`
	Scope         string   `json:"scope"`
	ScopeID       string   `json:"scopeId"`
	AllowedModels []string `json:"allowedModels"`
	Enabled       *bool    `json:"enabled"`
}

func normalizeClientKeyScope(scope, scopeID string) (string, string) {
	scope = strings.ToLower(strings.TrimSpace(scope))
	scopeID = strings.ToLower(strings.TrimSpace(scopeID))
	switch scope {
	case ClientKeyScopeGroup, ClientKeyScopeAccount:
		return scope, scopeID
	default:
		return ClientKeyScopeGlobal, ""
	}
}

func truncateRunesClean(s string, n int) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}

func validateClientKeyInput(in *ClientKeyInput) error {
	in.Name = truncateRunesClean(in.Name, clientKeyMaxNameRunes)
	in.Notes = truncateRunesClean(in.Notes, clientKeyMaxNoteRunes)
	rawScope := strings.ToLower(strings.TrimSpace(in.Scope))
	if rawScope != "" && rawScope != ClientKeyScopeGlobal && rawScope != ClientKeyScopeGroup && rawScope != ClientKeyScopeAccount {
		return fmt.Errorf("不支持的作用域 %q (可选 global / group / account)", in.Scope)
	}
	in.Scope, in.ScopeID = normalizeClientKeyScope(in.Scope, in.ScopeID)
	switch in.Scope {
	case ClientKeyScopeGroup:
		if !validAccountIDRe.MatchString(in.ScopeID) {
			return errors.New("分组作用域需要合法的分组 ID")
		}
		if _, ok := GlobalAccountManager.GetGroup(in.ScopeID); !ok {
			return fmt.Errorf("分组 %q 不存在", in.ScopeID)
		}
	case ClientKeyScopeAccount:
		if !validAccountIDRe.MatchString(in.ScopeID) {
			return errors.New("账号作用域需要合法的账号 ID")
		}
		if _, ok := GlobalAccountManager.GetAccount(in.ScopeID); !ok {
			return fmt.Errorf("账号 %q 不存在", in.ScopeID)
		}
	}
	if len(in.AllowedModels) > 0 {
		in.AllowedModels = normalizeCanonicalModelList(in.AllowedModels)
	} else {
		in.AllowedModels = nil
	}
	return nil
}

// Create issues a new key and returns the record plus the one-time plaintext.
func (s *ClientKeyStore) Create(in ClientKeyInput) (ClientAPIKey, string, error) {
	if err := validateClientKeyInput(&in); err != nil {
		return ClientAPIKey{}, "", err
	}
	s.ensureLoaded()
	plain, err := generateClientKeyPlaintext()
	if err != nil {
		return ClientAPIKey{}, "", errors.New("无法生成随机密钥")
	}
	now := time.Now().UTC()
	rec := ClientAPIKey{
		ID:            "ck-" + randomHex(6),
		Name:          in.Name,
		Notes:         in.Notes,
		Hash:          clientKeyDigest(plain),
		Preview:       clientKeyPreview(plain),
		Scope:         in.Scope,
		ScopeID:       in.ScopeID,
		AllowedModels: in.AllowedModels,
		Enabled:       in.Enabled == nil || *in.Enabled,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if rec.Name == "" {
		rec.Name = rec.ID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return ClientAPIKey{}, "", s.loadErr
	}
	if len(s.keys) >= clientKeyMaxCount {
		return ClientAPIKey{}, "", fmt.Errorf("客户端 Key 数量已达上限 %d", clientKeyMaxCount)
	}
	prev := s.keys
	s.keys = append(append([]ClientAPIKey{}, s.keys...), rec)
	if err := s.saveLocked(); err != nil {
		s.keys = prev
		return ClientAPIKey{}, "", errors.New("保存客户端 Key 失败")
	}
	s.reindexLocked()
	return rec, plain, nil
}

// Update edits metadata/scope/models/enabled state; the secret itself never changes.
func (s *ClientKeyStore) Update(in ClientKeyInput) (ClientAPIKey, error) {
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return ClientAPIKey{}, errors.New("缺少 Key ID")
	}
	if err := validateClientKeyInput(&in); err != nil {
		return ClientAPIKey{}, err
	}
	s.ensureLoaded()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return ClientAPIKey{}, s.loadErr
	}
	for i := range s.keys {
		if s.keys[i].ID != id {
			continue
		}
		prev := append([]ClientAPIKey{}, s.keys...)
		k := s.keys[i]
		if in.Name != "" {
			k.Name = in.Name
		}
		k.Notes = in.Notes
		k.Scope, k.ScopeID = in.Scope, in.ScopeID
		k.AllowedModels = in.AllowedModels
		if in.Enabled != nil {
			k.Enabled = *in.Enabled
		}
		k.UpdatedAt = time.Now().UTC()
		s.keys[i] = k
		if err := s.saveLocked(); err != nil {
			s.keys = prev
			return ClientAPIKey{}, errors.New("保存客户端 Key 失败")
		}
		return k, nil
	}
	return ClientAPIKey{}, fmt.Errorf("客户端 Key %q 不存在", id)
}

// SetEnabled toggles a key without touching other fields.
func (s *ClientKeyStore) SetEnabled(id string, enabled bool) (ClientAPIKey, error) {
	s.ensureLoaded()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return ClientAPIKey{}, s.loadErr
	}
	for i := range s.keys {
		if s.keys[i].ID == id {
			prev := s.keys[i]
			s.keys[i].Enabled = enabled
			s.keys[i].UpdatedAt = time.Now().UTC()
			if err := s.saveLocked(); err != nil {
				s.keys[i] = prev
				return ClientAPIKey{}, errors.New("保存客户端 Key 失败")
			}
			return s.keys[i], nil
		}
	}
	return ClientAPIKey{}, fmt.Errorf("客户端 Key %q 不存在", id)
}

// Delete revokes a key permanently.
func (s *ClientKeyStore) Delete(id string) error {
	id = strings.TrimSpace(id)
	s.ensureLoaded()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	for i := range s.keys {
		if s.keys[i].ID == id {
			prev := s.keys
			s.keys = append(append([]ClientAPIKey{}, s.keys[:i]...), s.keys[i+1:]...)
			if err := s.saveLocked(); err != nil {
				s.keys = prev
				return errors.New("保存客户端 Key 失败")
			}
			s.reindexLocked()
			s.lastUsed.Delete(id)
			return nil
		}
	}
	return fmt.Errorf("客户端 Key %q 不存在", id)
}

// --- Request integration ---

// clientKeyForRequest returns the enabled client key presented by r, if any.
func clientKeyForRequest(r *http.Request) (ClientAPIKey, bool) {
	if r == nil {
		return ClientAPIKey{}, false
	}
	return GlobalClientKeys.Match(requestKey(r))
}

// ResetForTest clears in-memory cached state so tests can cleanly reinitialize.
func (s *ClientKeyStore) ResetForTest() {
	s.mu.Lock()
	s.path = ""
	s.loaded = false
	s.loadErr = nil
	s.keys = nil
	s.byHash = nil
	s.mu.Unlock()
	s.lastUsed.Range(func(k, _ any) bool {
		s.lastUsed.Delete(k)
		return true
	})
}

func isClientKeyModelGatedPath(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	p := cleanGeminiV1BetaPath(r.URL.Path)
	if r.Method == http.MethodGet {
		return strings.HasPrefix(p, "/v1beta/models/") && strings.Trim(strings.TrimPrefix(p, "/v1beta/models/"), "/") != ""
	}
	if r.Method != http.MethodPost {
		return false
	}
	switch p {
	case "/v1/messages", "/v1/chat/completions", "/v1/responses", "/v1/messages/count_tokens":
		return true
	}
	return strings.HasPrefix(p, "/v1beta/models/")
}

// enforceClientKeyModelAllowlist rejects data-plane requests whose model is outside the
// presenting client key's allowlist. Returns false after writing an error response.
func enforceClientKeyModelAllowlist(w http.ResponseWriter, r *http.Request) bool {
	ck, ok := clientKeyForRequest(r)
	if !ok || len(ck.AllowedModels) == 0 || !isClientKeyModelGatedPath(r) {
		return true
	}
	p := cleanGeminiV1BetaPath(r.URL.Path)
	modelID, _, _, _, invalidPathModel, _, malformedJSON, tooLarge, readErr := extractRequestRoutingHintsFull(r)
	if tooLarge {
		writeAccountRouteError(w, p, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large", "Request body exceeds maximum size")
		return false
	}
	if readErr != nil {
		writeAccountRouteError(w, p, http.StatusBadRequest, "invalid_request_error", "body_read_error", "Failed to read request body")
		return false
	}
	if invalidPathModel {
		writeAccountRouteError(w, p, http.StatusBadRequest, "invalid_request_error", "invalid_model_path", "Invalid model identifier in path")
		return false
	}
	if r.Method == http.MethodPost && malformedJSON {
		return true
	}
	if modelID == "" {
		if r.Method == http.MethodGet {
			return true
		}
		modelID = ResolveRequestedModel("", nil).Model.ID
	}
	if !ck.AllowsModel(modelID) {
		writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "model_not_allowed",
			fmt.Sprintf("model %q is not allowed for this API key", modelID))
		return false
	}
	return true
}

// filterModelsByClientKey trims /v1/models and /v1beta/models listings to the client key allowlist.
func filterModelsByClientKey(r *http.Request, items []map[string]any, idField string) []map[string]any {
	ck, ok := clientKeyForRequest(r)
	if !ok || len(ck.AllowedModels) == 0 {
		return items
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		idStr, _ := item[idField].(string)
		idStr = strings.TrimPrefix(idStr, "models/")
		if ck.AllowsModel(ResolveRequestedModel(idStr, nil).Model.ID) {
			out = append(out, item)
		}
	}
	return out
}

// --- Dashboard management API (/api/client-keys*) ---

func writeClientKeysJSON(w http.ResponseWriter, code int, v any) {
	if code != http.StatusOK {
		w.WriteHeader(code)
	}
	_ = json.NewEncoder(w).Encode(v)
}

// HandleClientKeysAPI serves the dashboard-only client key lifecycle endpoints.
func HandleClientKeysAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.Path

	if r.Method == http.MethodGet && path == "/api/client-keys" {
		keys := GlobalClientKeys.List()
		views := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			views = append(views, k.PublicView(GlobalClientKeys.LastUsed(k.ID)))
		}
		resp := map[string]any{
			"ok":                  true,
			"keys":                views,
			"dashboardAuth":       dashboardAuthMode(),
			"dataPlaneAuthActive": dataPlaneAuthEnabled(),
		}
		if err := GlobalClientKeys.LoadError(); err != nil {
			resp["storeError"] = err.Error()
		}
		writeClientKeysJSON(w, http.StatusOK, resp)
		return
	}

	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeClientKeysJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method not allowed"})
		return
	}
	if !requireDashboardMutationGuard(w, r) {
		return
	}
	if !dashboardAuthEnabled() {
		writeClientKeysJSON(w, http.StatusForbidden, map[string]any{"ok": false,
			"error": "请先配置 AGY_DASHBOARD_PASSWORD（或 AGY_API_KEYS）保护管理后台，再签发客户端 Key"})
		return
	}

	switch {
	case r.Method == http.MethodPost && path == "/api/client-keys":
		var in ClientKeyInput
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&in); err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "JSON 格式错误"})
			return
		}
		rec, plain, err := GlobalClientKeys.Create(in)
		if err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		LogEvent("CLIENT_KEY_CREATED", "Client API key issued", map[string]any{"id": rec.ID, "scope": rec.Scope, "scopeId": rec.ScopeID})
		writeClientKeysJSON(w, http.StatusOK, map[string]any{"ok": true, "key": rec.PublicView(time.Time{}), "secret": plain})
	case r.Method == http.MethodPost && path == "/api/client-keys/update":
		var in ClientKeyInput
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&in); err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "JSON 格式错误"})
			return
		}
		rec, err := GlobalClientKeys.Update(in)
		if err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		LogEvent("CLIENT_KEY_UPDATED", "Client API key updated", map[string]any{"id": rec.ID, "enabled": rec.Enabled})
		writeClientKeysJSON(w, http.StatusOK, map[string]any{"ok": true, "key": rec.PublicView(GlobalClientKeys.LastUsed(rec.ID))})
	case r.Method == http.MethodPost && path == "/api/client-keys/toggle":
		var in struct {
			ID      string `json:"id"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&in); err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "JSON 格式错误"})
			return
		}
		rec, err := GlobalClientKeys.SetEnabled(strings.TrimSpace(in.ID), in.Enabled)
		if err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		LogEvent("CLIENT_KEY_UPDATED", "Client API key toggled", map[string]any{"id": rec.ID, "enabled": rec.Enabled})
		writeClientKeysJSON(w, http.StatusOK, map[string]any{"ok": true, "key": rec.PublicView(GlobalClientKeys.LastUsed(rec.ID))})
	case r.Method == http.MethodDelete && path == "/api/client-keys":
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if err := GlobalClientKeys.Delete(id); err != nil {
			writeClientKeysJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		LogEvent("CLIENT_KEY_REVOKED", "Client API key revoked", map[string]any{"id": id})
		writeClientKeysJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeClientKeysJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
	}
}
