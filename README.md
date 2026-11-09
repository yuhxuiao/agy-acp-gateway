# agy-acp-gateway

<p align="center">
  <strong>High-Performance, Pure-Go Multi-Protocol API Gateway & Real-Time Observability Suite for Google Antigravity CLI (<code>agy acp</code>)</strong><br/>
  <strong>基于 Google 官方 <code>agy acp</code> 的纯 Go 高性能多协议网关与实时可观测面板</strong>
</p>

<p align="center">
  <a href="#english">English</a> •
  <a href="#简体中文">简体中文</a> •
  <a href="#architecture--core-capabilities">Architecture</a> •
  <a href="#quick-start--快速开始">Quick Start</a> •
  <a href="#api--client-configuration--客户端接入">Client Config</a> •
  <a href="#upstream-maintenance--safe-upgrade--上游安全升级">Safe Upgrader</a>
</p>

---

<a name="english"></a>

## English

`agy-acp-gateway` is a zero-CGO (`CGO_ENABLED=0`), single-binary Go gateway that turns the official Google Antigravity CLI (`agy acp`, JSON-RPC 2.0 over stdio) into standard **Anthropic Messages (`/v1/messages`)**, **OpenAI Chat Completions (`/v1/chat/completions`)**, and **OpenAI Responses (`/v1/responses`)** HTTP/SSE endpoints.

Unlike naive HTTP/OAuth proxies that reverse-engineer private Google protobuf endpoints and risk immediate account suspension, `agy-acp-gateway` communicates **exclusively through the official `agy acp` process**. All TLS fingerprints, OAuth token refreshes, checkpointing, and context management remain 100% inside the official binary.

### Key Highlights

1. **Tri-Protocol Native Compatibility**:
   - **Anthropic `/v1/messages` & `/v1/messages/count_tokens`**: Full support for Claude Code, including streaming `thinking` + `signature` blocks, `tool_use` / `tool_result` parking & resumption, and prompt cache hit accounting.
   - **OpenAI `/v1/chat/completions`**: Full support for Cursor, Cline, Continue, and standard OpenAI SDKs (`reasoning_content`, `tool_calls`, streaming `include_usage`).
   - **OpenAI `/v1/responses`**: Native Codex CLI / Responses API compatibility (`response.reasoning_summary_text.delta`, `response.function_call_arguments.delta`, `previous_response_id` stateful resumption).
2. **Real-Time SQLite `gen_metadata` Accounting & Dual Watermarks**:
   - Directly decodes the `gen_metadata` and `steps` protobuf blobs from `agy`'s per-session SQLite database (`modernc.org/sqlite` read-only fast path, **0.44 ms/op**).
   - Tracks `LastSeenGenMetaIdx` independently of `LastSeenDbStepIdx`, delivering exact input/cached/output token billing even on `tool_use` turns while external tools are parked.
3. **5-Dimensional Argon Shadow-Degradation Detection & Transparent In-Turn Recovery**:
   - Real-time detection of upstream `gemini-4-argon` silent backend fallback / context truncation across 5 signals:
     - **F1 `context_truncated`**: `cwmMaxTokens > 0 && cwmMaxTokens <= 262,144` (+5)
     - **F2 `cache_cliff`**: Previous turn had `>= 30,000` cached tokens and current turn drops by `> 80%` to `< 5,000` (+4)
     - **F3 `fast_decode`**: Output `>= 120` tokens with decode TPS `> 115 tok/s` (+3) or `> 95 tok/s` (+2), including sub-second `google.protobuf.Duration` (`nanos`-only) precision
     - **F4 `block_cache`**: `cachedTokens > 0 && cachedTokens % 32768 == 0` (+2~+3, corroborating signal)
     - **F5 `shallow_thought`**: High/medium thinking tier producing disproportionately brief reasoning (+1~+3)
   - **Zero-Disruption Self-Healing**: If detected before any SSE delta is emitted to the client (`EmittedDeltas == 0`), the gateway transparently closes the poisoned ACP session, boots a fresh session, and replays the turn once. If streaming has already started, it finishes the stream cleanly and schedules `ForceRolloverNext` for the next turn.
4. **900K Context Window Unlock & Native Eager MCP Tool Bridge**:
   - Includes a deterministic 7-point binary patcher (`scripts/agy_upgrade.py`) that unlocks the 900K context window (`900,000` input tokens vs. the default `256,000` cap) while preserving binary section sizes.
   - Bridges external client tools (`Bash`, `Read`, `Edit`, `Write`, `Grep`, `Glob`, `Agent`, etc.) into `agy acp` via an ephemeral local MCP HTTP server with `_meta: {"antigravity/alwaysLoad": true}` so all client tools are loaded eagerly on turn 1 without deferred tool-search roundtrips.
5. **Post-Compaction Session Continuity & Warm Hibernation (`session/resume`)**:
   - Dual-key identity routing (`sid:` / `fp:` + tenant isolation) with full 64-bit historical message digest matching: when clients like Claude Code compact middle turns (`messages` length shrinks while `x-claude-code-session-id` stays constant), the gateway seamlessly retains the canonical session and triggers a single clean context rollover.
   - Idle workers hibernate via ACP `session/close` and wake in **~0.25s** via `session/resume` without replaying historical stdout chunks.
6. **Dual-Port Isolation & Real-Time Dashboard**:
   - **Port `18890` (API Gateway)**: `/v1/messages`, `/v1/chat/completions`, `/v1/responses`, `/v1/models`, `/health`, `/healthz`.
   - **Port `18891` (Dashboard & Prometheus Metrics)**: Interactive dark-mode telemetry UI (`/`, `/api/dashboard`) and Prometheus metrics (`/metrics`) with live worker pool status, per-model quota gauges, cache hit rates, and Argon recovery counters.

---

<a name="简体中文"></a>

## 简体中文

`agy-acp-gateway` 是一个**零 CGO 依赖 (`CGO_ENABLED=0`)、单二进制**的高性能 Go 网关，将 Google 官方 Antigravity CLI (`agy acp`，基于 stdio 的 JSON-RPC 2.0 协议) 无缝转换为标准 **Anthropic Messages (`/v1/messages`)**、**OpenAI Chat Completions (`/v1/chat/completions`)** 与 **OpenAI Responses (`/v1/responses`)** 接口，并内置实时监控面板与 Prometheus 指标端点。

与市面上直接逆向 Google OAuth 内部接口裸发 HTTP/Protobuf 请求的方案不同，本网关**100% 严格通过官方 `agy acp` 进程通信**。所有 TLS 指纹、OAuth 令牌刷新、会话快照和上下文状态机完全由官方二进制托管，从根源上杜绝协议指纹异常导致的封号风险。

### 核心特性

1. **三协议原生兼容与外部工具闭环 (MCP Bridge)**：
   - **Anthropic `/v1/messages` & `/v1/messages/count_tokens`**：完整支持 Claude Code（流式 `thinking` + `signature` 签名块、`tool_use` / `tool_result` 挂起与恢复、缓存命中精准统计、Outbound Cleanroom 提示词脱敏）。
   - **OpenAI `/v1/chat/completions`**：完整支持 Cursor、Cline、Continue 与标准 OpenAI SDK（`reasoning_content`、`tool_calls`、流式 `include_usage`）。
   - **OpenAI `/v1/responses`**：原生支持 Codex CLI / Responses API（`response.reasoning_summary_text.delta`、`response.function_call_arguments.delta`、`previous_response_id` 跨轮状态恢复）。
   - **全量 Eager 工具注入**：内置轻量级 MCP HTTP 桥接器，自动在 `tools/list` 注入 `_meta: {"antigravity/alwaysLoad": true}` 并放宽 Schema 描述限制，无需客户端拆包或二次 `ToolSearch` 即可在首轮全量激活客户端工具。
2. **SQLite `gen_metadata` 实时双游标精准计费**：
   - 基于纯 Go `modernc.org/sqlite` 只读快速路径（单次查询仅 **0.44 ms**，零锁竞争），直接解析 `agy` 会话库中的 `gen_metadata` 与 `steps` Protobuf 载荷。
   - 独立维护 `LastSeenGenMetaIdx` 与 `LastSeenDbStepIdx` 双游标：即便在 `tool_use` 工具挂起尚未落盘 `steps` 时，也能实时从 `gen_metadata` 提取精确的 `input_tokens`、`cache_read_input_tokens`、`output_tokens` 与真实 TTFT/流式耗时。
3. **Argon (`gemini-4-argon`) 5 维暗降级检测与当轮透明自愈**：
   - 针对上游偶发的 `gemini-4-argon` 静默路由降级（上下文锁死 256K、TPU 缓存悬崖、Flash 级异常高解码流速等），网关在每次响应后基于 5 个正交维度实时评分：
     - **F1 `context_truncated` (+5)**：`cwmMaxTokens > 0 && cwmMaxTokens <= 262,144`（上下文窗口被截断至 256K）
     - **F2 `cache_cliff` (+4)**：前轮缓存 `>= 30,000` Token，本轮骤降 `> 80%` 且 `< 5,000` Token
     - **F3 `fast_decode` (+2/+3)**：输出 `>= 120` Token 且解码速度 `> 115 tok/s` (+3) 或 `> 95 tok/s` (+2)，支持亚秒级 `google.protobuf.Duration` (`nanos`-only) 纳秒精度解析
     - **F4 `block_cache` (+2/+3，辅助信号)**：`cachedTokens > 0 && cachedTokens % 32768 == 0`
     - **F5 `shallow_thought` (+1~+3)**：`high`/`medium` 思考档位下思考长度与输出严重失衡
   - **零感知透明自愈**：当判定暗降级（`score >= 3` 且命中任一主信号 F1/F2/F3/F5）且尚未向客户端发送流式分片（`EmittedDeltas == 0`）时，网关自动销毁污染会话、拉起新会话并在同一次 HTTP 请求内透明重放；若已发送流式分片，则正常结束当前响应并标记 `ForceRolloverNext = true` 于下一轮自动重建。
4. **900K 上下文窗口解锁与四门禁安全升级器 (`scripts/agy-upgrade.sh`)**：
   - 内置确定性二进制补丁流水线，自动解除 `agy` 在 ACP 模式下的 256K 窗口硬编码限制，解锁 **900K (900,000 Tokens)** 上下文窗口。
   - 提供包含 `offline`（静态与补丁门禁）、`fake_e2e`（三协议 + 工具闭环 + UI 零报错）、`contract`（16 项 ACP 协议契约零额度校验）、`live_flash`（单次低成本真实探针）的四门禁原子升级与一键回滚工具。
5. **压缩后会话连续性 (Post-Compaction Continuity) 与 0.25s 极速热唤醒**：
   - 当客户端（如 Claude Code `/compact`）压缩中间历史导致消息数组变短时，网关通过完整 64 位历史消息摘要匹配自动识别同源会话，仅执行一次平滑 Rollover 并继承累计 Token 统计。
   - 空闲会话自动休眠释放内存，再次请求时优先通过 ACP `session/resume` 在 **0.25 秒**内热恢复，避免 `session/load` 重放数千行历史块。

---

<a name="architecture--core-capabilities"></a>

## Architecture / 架构概览

```text
  Claude Code / Cursor / Codex CLI / OpenAI & Anthropic SDKs
         │                           │                         │
   POST /v1/messages      POST /v1/chat/completions     POST /v1/responses
         └───────────────────────────┼─────────────────────────┘
                                     ▼
                    ┌─────────────────────────────────┐
                    │   agy-acp-gateway (Port 18890)  │
                    │  • Auth / CORS / ToS Circuit    │
                    │  • Outbound Prompt Cleanroom    │
                    │  • Session Continuity & Pool    │
                    │  • Ephemeral MCP HTTP Bridge    │
                    │  • 5D Argon Degradation Guard   │
                    └───────────┬──────────┬──────────┘
         JSON-RPC 2.0 (stdio)   │          │  Read-Only SQLite Fast Path
         + Local MCP Callbacks  ▼          ▼  (gen_metadata & steps blobs)
                    ┌─────────────────┐  ┌──────────────────────────┐
                    │ bin/agy-acp-900k│  │ ~/.gemini/antigravity-cli│
                    │  (Official CLI) │─▶│   conversations/*.db     │
                    └─────────────────┘  └──────────────────────────┘
```

### Repository Layout / 目录结构

```text
agy-acp-gateway/
├── go-gateway/                 # Pure-Go gateway source code (CGO_ENABLED=0)
│   ├── main.go                 # HTTP servers, tri-protocol handlers, MCP bridge, SSE sinks
│   ├── acp_engine.go           # ACP stdio engine, worker pool, session lifecycle & Argon recovery
│   ├── sqlite_protobuf.go      # Zero-copy protobuf decoder for SQLite steps & gen_metadata
│   ├── models.go               # Model catalog, aliases, thinking tier mapping & pricing
│   ├── store.go                # Persistent gateway state DB (conversations, turns, usage_steps)
│   ├── security.go             # Outbound prompt cleanroom, credential redaction & ToS circuit breaker
│   ├── dashboard.go            # Embedded real-time monitoring UI & /api/dashboard payload
│   ├── *_test.go               # Unit, race, continuity, remediation & Argon 5D test suites
│   └── testdata/fakeacp/       # Deterministic fake ACP server for offline/E2E testing
├── scripts/
│   ├── gw.sh                   # Multi-instance process supervisor (start / stop / restart / status / logs)
│   ├── agy-upgrade.sh          # Safe upstream agy binary upgrader & rollback entrypoint
│   ├── agy_upgrade.py          # 7-gate 900K binary patcher & 4-stage candidate validator
│   ├── agy_acp_smoke.py        # 16-point zero-quota ACP protocol contract verifier
│   ├── upstream_watch.py       # Automated upstream version watcher (--check / --prepare)
│   └── e2e_*.py                # Isolated E2E test suites (tool call loop, multi-model, thinking tiers)
├── deploy/
│   ├── Dockerfile              # Production container image definition
│   ├── docker-compose.yml      # Docker Compose deployment stack
│   └── openresty-acp.conf      # Optional Nginx/OpenResty reverse-proxy snippet
├── .env.example                # Environment variable template
└── LICENSE                     # MIT License
```

---

<a name="quick-start--快速开始"></a>

## Quick Start / 快速开始

### Prerequisites / 前置条件

1. **Go 1.22+** (for building `go-gateway` with `CGO_ENABLED=0`).
2. **Python 3.10+** (for running the `agy` 900K binary patcher and verification scripts).
3. **Google Antigravity CLI (`agy`)**: Installed and logged in (`agy auth login` or existing `~/.gemini/antigravity-cli/antigravity-oauth-token`).

### 1. Build the Gateway & Prepare `bin/agy-acp-900k` / 编译网关与生成 900K 补丁二进制

```bash
# 1. Clone the repository
git clone https://github.com/yuhxuiao/agy-acp-gateway.git
cd agy-acp-gateway

# 2. Build the pure-Go gateway binary
mkdir -p bin backups logs runtime
cd go-gateway
gofmt -l . && go vet ./... && go test -race -count=1 ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway .
cd ..

# 3. Prepare and validate the 900K-unlocked agy binary from your local ~/.local/bin/agy
#    (Runs offline static checks + fake E2E + 16-point ACP contract check without consuming quota)
scripts/agy-upgrade.sh --prepare --from-binary ~/.local/bin/agy --skip-live
# Or install directly after candidate verification:
# scripts/agy-upgrade.sh --install --candidate runtime/upgrades/candidates/<candidate-id>
```

### 2. Configure & Start Locally / 配置与启动服务

```bash
# Copy the environment template if you want API key authentication
cp .env.example .env

# Start the gateway (18890) and dashboard (18891)
scripts/gw.sh start

# Check instance health and listening ports
scripts/gw.sh status

# Tail live gateway logs
scripts/gw.sh logs
```

- **API Gateway Health**: `http://127.0.0.1:18890/health`
- **Live Dashboard UI**: `http://127.0.0.1:18891/`
- **Prometheus Metrics**: `http://127.0.0.1:18891/metrics`

### 3. Docker Compose Deployment / Docker 容器化部署

```bash
# Ensure bin/agy-acp-gateway and bin/agy-acp-900k are built and present in ./bin/
cp .env.example deploy/.env
docker compose -f deploy/docker-compose.yml up -d --build

# Check container health
docker ps --filter name=agy-acp-gateway
curl -fsS http://127.0.0.1:18890/healthz
```

---

<a name="api--client-configuration--客户端接入"></a>

## API & Client Configuration / 客户端接入指南

### Supported Models / 支持的模型与别名

| Canonical Model ID | Supported Aliases (Examples) | Thinking Tiers | Max Context |
| :--- | :--- | :--- | :--- |
| `gemini-4-argon` | `gemini-3.1-pro-high`, `gemini-2.5-pro`, `gemini-pro`, `argon` | `low`, `medium`, `high` | **900,000** |
| `gemini-3.8-flash` | `gemini-3-flash`, `gemini-2.5-flash`, `gemini-flash`, `flash` | `low`, `medium`, `high` | **900,000** |
| `claude-opus-5-5` | `claude-opus-4-6`, `claude-opus-4-6-thinking`, `opus` | `low`, `medium`, `high` | **900,000** |
| `claude-sonnet-5-5` | `claude-sonnet-4-6`, `claude-sonnet-4-6-thinking`, `sonnet` | `low`, `medium`, `high` | **900,000** |
| `gpt-oss-120b-medium` | `gpt-oss-120b`, `gpt-4o`, `gpt-4.1`, `o3` | `medium` | **900,000** |

> **Tip**: Append `-low`, `-medium`, or `-high` to any model ID (e.g., `gemini-4-argon-high`, `gemini-3.8-flash-low`), or use standard protocol fields (`thinking.budget_tokens` in Anthropic, `reasoning_effort` in OpenAI Chat, `reasoning.effort` in OpenAI Responses).

### 1. Claude Code (`/v1/messages`)

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
export ANTHROPIC_API_KEY="sk-agy-replace-with-your-secret-key"
export ANTHROPIC_MODEL="gemini-4-argon-high"
export ANTHROPIC_SMALL_FAST_MODEL="gemini-3.8-flash-low"

claude
```

### 2. Anthropic Messages API (`curl`)

```bash
curl -sS http://127.0.0.1:18890/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: $AGY_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "gemini-3.8-flash-low",
    "max_tokens": 1024,
    "messages": [
      {"role": "user", "content": "Explain how ACP session/resume works in two sentences."}
    ]
  }'
```

### 3. OpenAI Chat Completions (`/v1/chat/completions`)

```bash
curl -sS http://127.0.0.1:18890/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AGY_API_KEY" \
  -d '{
    "model": "gemini-4-argon",
    "reasoning_effort": "high",
    "stream": true,
    "messages": [
      {"role": "user", "content": "Write a concurrent worker pool in Go."}
    ]
  }'
```

### 4. OpenAI Responses API (`/v1/responses`)

```bash
curl -sS http://127.0.0.1:18890/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AGY_API_KEY" \
  -d '{
    "model": "gemini-3.8-flash",
    "reasoning": {"effort": "low"},
    "input": "Summarize the benefits of pure-Go SQLite readers."
  }'
```

---

<a name="upstream-maintenance--safe-upgrade--上游安全升级"></a>

## Upstream Maintenance & Safe Upgrade / 上游维护与安全升级

When Google releases a new version of `agy`, use the built-in 4-gate upgrade pipeline to patch, validate in complete isolation, and atomically switch with instant rollback support:

```bash
# 1. Check upstream APT registry vs local installed versions (read-only, zero quota)
python3 scripts/upstream_watch.py --check

# 2. Prepare and validate a candidate in an isolated directory (Ports 18892/18893)
scripts/agy-upgrade.sh --prepare

# 3. Atomically install a verified candidate (automatically backs up current binaries)
scripts/agy-upgrade.sh --install --candidate runtime/upgrades/candidates/<candidate-id>

# 4. One-command rollback if ever needed
scripts/agy-upgrade.sh --rollback backups/<backup-dir>
```

### Running Tests / 运行测试套件

All unit tests, race detector suites, and multi-protocol E2E tests run against the deterministic `fakeacp` harness with **zero model quota consumption**:

```bash
# 1. Go static analysis, race detector, and coverage suite
cd go-gateway
gofmt -l .
go vet ./...
go test -race -count=1 -cover ./...
cd ..

# 2. Upgrader isolation & manifest binding unit tests
python3 -m unittest discover -s scripts -p "test_*.py" -v
```

---

## Security & Privacy Design / 安全与隐私设计

- **Official Transport Only**: Never makes raw HTTP calls to Google internal endpoints; all model traffic flows through `agy acp`.
- **Outbound Prompt Cleanroom**: Automatically strips third-party client attribution headers (`x-anthropic-billing-header`, client harness boilerplate, `<system-reminder>` wrappers) before forwarding to `agy acp`, while preserving user project instructions (`CLAUDE.md`, `AGENTS.md`, workspace paths, and skills).
- **Structured Credential Redaction**: Automatically scrubs API keys, Bearer tokens, and OAuth secrets from gateway logs and optional debug recordings (`0600` file permissions).
- **ToS Circuit Breaker**: Immediately latches into a fail-closed state if upstream ever reports an account-level Terms of Service restriction, preventing retry storms.

---

## License / 开源协议

Released under the [MIT License](LICENSE).
