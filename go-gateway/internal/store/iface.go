package store

// TurnUsage holds token and latency counters accumulated across one HTTP turn.
type TurnUsage struct {
	InputTokens                int                     `json:"inputTokens"`
	CachedReadTokens           int                     `json:"cachedReadTokens"`
	OutputTokens               int                     `json:"outputTokens"`
	ThinkingTokens             int                     `json:"thinkingTokens,omitempty"`
	RawUncachedInputTokens     int                     `json:"rawUncachedInputTokens,omitempty"`
	RawTpuCachedTokens         int                     `json:"rawTpuCachedTokens,omitempty"`
	ContextInputTokens         int                     `json:"contextInputTokens,omitempty"`
	ContextCachedTokens        int                     `json:"contextCachedTokens,omitempty"`
	CumulativeInputTokens      int                     `json:"cumulativeInputTokens,omitempty"`
	CumulativeCachedReadTokens int                     `json:"cumulativeCachedReadTokens,omitempty"`
	StepsInPrompt              int                     `json:"stepsInPrompt"`
	UsageSource                string                  `json:"usageSource,omitempty"`
	Degradation                *ArgonDegradationReport `json:"degradation,omitempty"`
	Generations                int                     `json:"generations,omitempty"`
	ServedModel                string                  `json:"servedModel,omitempty"`
	ActualTier                 string                  `json:"actualTier,omitempty"`
	CacheWarning               string                  `json:"cacheWarning,omitempty"`
	GenTtftMs                  int                     `json:"genTtftMs,omitempty"`
	StreamMs                   int                     `json:"streamMs,omitempty"`
	DecodeTps                  float64                 `json:"decodeTps,omitempty"`
}

// ContextTotal returns the session's actual single-generation prompt context size after this turn.
func (u *TurnUsage) ContextTotal() int {
	if u == nil {
		return 0
	}
	if u.ContextInputTokens+u.ContextCachedTokens > 0 {
		return u.ContextInputTokens + u.ContextCachedTokens
	}
	return u.InputTokens + u.CachedReadTokens
}

// ClientTokens returns the (input, cachedRead, output) tuple emitted in client-facing HTTP usage blocks.
func (u *TurnUsage) ClientTokens() (inTok, crTok, outTok int) {
	if u == nil {
		return 0, 0, 16
	}
	if u.Generations > 1 && (u.ContextInputTokens > 0 || u.ContextCachedTokens > 0) {
		return u.ContextInputTokens, u.ContextCachedTokens, u.OutputTokens
	}
	return u.InputTokens, u.CachedReadTokens, u.OutputTokens
}

// TurnHistoryEntry captures one persisted completed turn in gateway.db.
type TurnHistoryEntry struct {
	TurnIndex      int        `json:"turnIndex"`
	CompletedAtMs  int64      `json:"completedAtMs"`
	Ts             string     `json:"ts"`
	SessionID      string     `json:"sessionId"`
	Mode           string     `json:"mode"`
	ElapsedMs      int        `json:"elapsedMs"`
	TtftMs         int        `json:"ttftMs"`
	UpstreamTtftMs int        `json:"upstreamTtftMs,omitempty"`
	ThinkingMs     int        `json:"thinkingMs,omitempty"`
	StreamMs       int        `json:"streamMs,omitempty"`
	RetryCount     int        `json:"retryCount,omitempty"`
	Tps            float64    `json:"tps"`
	DecodeTps      float64    `json:"decodeTps,omitempty"`
	HasThinking    bool       `json:"hasThinking"`
	ThinkingTokens int        `json:"thinkingTokens"`
	Usage          *TurnUsage `json:"usage"`
	CacheHitRate   string     `json:"cacheHitRate"`
	StopReason     string     `json:"stopReason"`
	Model          string     `json:"model"`
	ThoughtLevel   string     `json:"thoughtLevel"`
	ServedModel    string     `json:"servedModel,omitempty"`
	CacheMiss      string     `json:"cacheMiss,omitempty"`
	ClientID       string     `json:"clientId,omitempty"`
	ClientLabel    string     `json:"clientLabel,omitempty"`
	Protocol       string     `json:"protocol,omitempty"`
}

// SessionSnapshot captures an immutable point-in-time snapshot of a conversation row for SQLite persistence.
type SessionSnapshot struct {
	ConvKey          string
	SessionID        string
	TenantKey        string
	Cwd              string
	ModelID          string
	ThoughtLevel     string
	ToolFingerprint  string
	InitToolsJSON    string
	CurToolsJSON     string
	SystemText       string
	InitSystemText   string
	InitAppendPrompt string
	MsgCount         int
	HistoryFp        string
	LastCachedTok    int
	LastDbStepIdx    int
	LastGenMetaIdx   int
	ClientID         string
	ClientLabel      string
	Protocol         string
	SessionReuses    int
	Rollovers        int
	State            string
	CreatedAtMs      int64
	UpdatedAtMs      int64
	BlobGen          uint64
	SnapSeq          uint64
}

// TurnMissKey indexes a persisted turn's cache-miss classification by session + token signature.
type TurnMissKey struct {
	SessionID string
	InTok     int
	CrTok     int
	OutTok    int
}

// UsageAllTimeRollup aggregates all-time token and cost counters from usage_steps.
type UsageAllTimeRollup struct {
	StepsCount   int     `json:"stepsCount"`
	ClaudeGwStep int     `json:"claudeGwSteps"`
	WslHostSteps int     `json:"wslHostSteps"`
	RawIn        int64   `json:"rawIn"`
	RawCr        int64   `json:"rawCr"`
	OutTok       int64   `json:"outTok"`
	ThTok        int64   `json:"thTok"`
	CwTok        int64   `json:"cwTok"`
	CostUsd      float64 `json:"costUsd"`
	SavedUsd     float64 `json:"savedUsd"`
}

// SessionClientMeta maps a persisted session ID to its attributed client metadata.
type SessionClientMeta struct {
	ConvKey     string
	ClientID    string
	ClientLabel string
	Protocol    string
}

// SessionStore abstracts durable SQLite session/turn storage for GatewayServer & AcpProjectSession DI.
type SessionStore interface {
	IsReady() bool
	Version() uint64
	DBPath() string
	Flush()
	UpsertSessionSnapshot(snap SessionSnapshot)
	UpsertSessionSnapshotAsync(snap SessionSnapshot)
	InsertTurn(convKey string, entry TurnHistoryEntry)
	InsertTurnWithSnapshot(snap SessionSnapshot, hasSnap bool, convKey string, entry TurnHistoryEntry)
	LoadKnownGatewaySids(dest map[string]bool)
	IsCursorUpToDate(sid string, dbSize, dbMtimeNs, walSize, walMtimeNs int64) bool
	ClearCleanedConversationSid(sid string)
	SyncUsageStepsFromDir(convDir string, gwSids map[string]bool) int
	SyncUsageStepsFromSourceDir(convDir, defaultSourceSide, defaultSourceCat, defaultSourceLabel string, gwSids map[string]bool) int
	QueryTurnCacheMissIndex() (byStep map[TurnMissKey]string, byFirstSession map[string]string)
	QueryActiveUsageSteps(gwSids map[string]bool, windowSec int64) ([]ParsedModelStep, bool)
	QueryUsageAllTimeRollup(gwSids map[string]bool) (UsageAllTimeRollup, bool)
	QueryAllUsageSteps(gwSids map[string]bool) ([]ParsedModelStep, bool)
	LookupSessionClientMap() map[string]SessionClientMeta
}
