<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/banner-dark.svg" />
    <img src="assets/banner-light.svg" alt="agy-acp-gateway" width="760" />
  </picture>
</p>

<h3 align="center">Zero-CGO, quad-protocol AI gateway for Google's official Antigravity CLI (<code>agy acp</code>) — with isolated multi-account pools, ban-safe sticky routing, Carbon KV cache defense, and warm-paper live telemetry.</h3>

<p align="center">
  <a href="./README.md"><strong>English</strong></a> &nbsp;·&nbsp;
  <a href="./README_ZH.md"><strong>简体中文</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>Quick Start</strong></a> &nbsp;·&nbsp;
  <a href="#client-setup"><strong>Client Setup</strong></a> &nbsp;·&nbsp;
  <a href="#multi-account"><strong>Multi-Account &amp; Routing</strong></a> &nbsp;·&nbsp;
  <a href="#models"><strong>Models &amp; Tiers</strong></a> &nbsp;·&nbsp;
  <a href="#engineering-pillars"><strong>Architecture</strong></a> &nbsp;·&nbsp;
  <a href="#troubleshooting"><strong>Troubleshooting</strong></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26%2B_(CGO__ENABLED%3D0)-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+ Zero CGO" />
  <img src="https://img.shields.io/badge/Protocols-Anthropic_%7C_OpenAI_%7C_Responses_%7C_Gemini_v1beta-0856F5?style=flat-square" alt="Quad Protocol" />
  <img src="https://img.shields.io/badge/Multi--Account-5_Strategies_%2B_U8_Sticky_Guard-167c52?style=flat-square" alt="Multi-Account Pool" />
  <img src="https://img.shields.io/badge/Context-900K_Unlocked-0D9488?style=flat-square" alt="900K Context" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-22c55e?style=flat-square" alt="MIT License" /></a>
</p>

---

**`agy-acp-gateway`** turns Google's official Antigravity CLI (`agy acp`, JSON-RPC 2.0 over stdio) into a production-hardened, single-binary multi-protocol gateway (`CGO_ENABLED=0`) and real-time control console.

Unlike reverse-engineered proxies that forge private OAuth/Protobuf HTTP calls and expose accounts to TLS/header fingerprint bans, **100% of upstream traffic runs through the official `agy` binary**. Google's own binary handles TLS BoringSSL handshakes, OAuth token refreshes, trajectory checkpoints, and upstream framing — while `agy-acp-gateway` provides:

- **Quad-Protocol Native Endpoints (`:18890`)**: `Anthropic` `/v1/messages`, OpenAI `/v1/chat/completions`, OpenAI `/v1/responses`, and native Google Gemini REST `/v1beta`.
- **Multi-Account Pool & Channel Groups (`v2.3`)**: Single-container isolated account sandboxes (`HOME` + SQLite + egress proxy), WebUI PKCE OAuth onboarding, 5 scheduling strategies, pre-stream 1st-turn failover, family-scoped cooldowns, graceful draining, and **Decision U8 ban-safe sticky continuity**.
- **5-Layer KV Cache Survival**: Native `agy 1.5.14+` trajectory `SessionId` verification + Gate 5 binary unlock (`900K` context), Turn-1 prefix freezing, in-place dynamic tool updates (`TOOLS_UPDATED_IN_PLACE`), and Prefill Soft-Detach (`45s` grace + Singleflight re-attach).
- **Authoritative SQLite Telemetry & Federated Console (`:18891`)**: Zero-lock `.1.19` protobuf step/token/TTFT/TPS extraction, automatic AI Pro vs. Ultra `20x` quota pool detection, `step_type=17` silent-retry sensing, Prometheus `/metrics`, and multi-node dashboard federation.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/arch-overview-dark.png" />
    <img src="assets/arch-overview-light.png" alt="agy-acp-gateway quad-protocol architecture diagram" width="960" />
  </picture>
</p>

---

## Why `agy-acp-gateway`?

| Production Pain Point | Thin ACP Wrapper / Spoofed Proxy | `agy-acp-gateway` Engineering Solution |
| :--- | :--- | :--- |
| **Upstream ban & fingerprint risk** | Spoofs internal OAuth/Protobuf HTTP APIs or replays mid-turn history across accounts | **100% Official `agy acp` Engine + Strict U8 Guard**: zero raw OAuth packets; multi-turn threads stay pinned to one account; per-account proxy/sandbox isolation |
| **Multi-account rate limits & manual rotation** | Single global `~/.gemini` home; manual container restarts on `429`/`529` | **v3 Pool & Channel Groups**: 5 strategies (`rr`, `fill-first`, `weighted-rr`, `least-load`, `manual-active`), Turn-1 pre-stream failover, family-scoped cooldowns, 1-click **Drain & Rotate** |
| **Gemini 4 Carbon concurrent 0% KV cache** | Shares empty trajectory IDs or mutates `system`/tool prefixes mid-conversation | **Trajectory `SessionId` Isolation + Prefix Freeze**: per-session `SessionId`, frozen Turn-1 system/tool catalog, and `TOOLS_UPDATED_IN_PLACE` |
| **120s+ long-context prefill disconnect storms** | Sends `session/cancel` on client timeout, destroying 150K+ in-flight prefill | **Prefill Soft-Detach (`30s`/`45s`) + Singleflight**: keeps upstream compute alive so retrying clients re-attach seamlessly |
| **Client harness tags trigger 400 WAF blocks** | Fails outright or strips user instructions via lossy regexes | **3-Stage Cleanroom + Session HMAC Codec + Flash Auto-Bisect**: losslessly encodes harness tags/paths and auto-isolates novel WAF triggers |
| **Opaque usage, silent retries & quota blindness** | Estimates tokens blindly; hides upstream `429` backoffs and Argon shadow routing | **Zero-Lock `.1.19` SQLite Reader**: decodes exact `cached`/`thinking`/`output` tokens, `step_type=17` retry telemetry, and Pro/Ultra `5h` + Weekly pools |

---

<a id="quick-start"></a>

## 5-Minute Quick Start

### Prerequisites

- **OS**: Linux (`x86_64` / `arm64`), macOS (`arm64` Apple Silicon), or Windows via **WSL2 Ubuntu**.
- **Official Antigravity CLI (`agy`)**: Installed at `~/.local/bin/agy` (`1.5.14+` recommended; never modified in place).
- **Toolchain** *(when building from source)*: Go `1.26+` and Python `3.10+`.

---

### Option A: Docker Compose Deployment (Recommended for Servers & Multi-Account Pools)

```bash
# 1. Clone repository
git clone https://github.com/yuhxuiao/agy-acp-gateway.git
cd agy-acp-gateway

# 2. Build zero-CGO gateway binary & prepare patched 900K agy binary
cd go-gateway && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway . && cd ..
# Build & verify candidate in isolated sandbox, then atomically install <candidate_dir> printed above
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. Configure environment & start container
cp .env.example deploy/.env
# Edit deploy/.env -> set a strong AGY_API_KEYS value (docker-compose pins AGY_LISTEN_HOST=0.0.0.0 inside the container;
# leave AGY_UPSTREAM_PROXY commented out unless using a reachable proxy)
docker compose -f deploy/docker-compose.yml up -d --build

# 4. Verify liveness
curl -fsS http://127.0.0.1:18890/healthz
# -> {"ok":true}
```

> **Multi-Account Memory Sizing**: `deploy/docker-compose.yml` defaults to `mem_limit: 3200m` and `stop_grace_period: 25s`. Each active secondary account spawns a lightweight child gateway (`128MiB` cap) plus `1..N` official `agy acp` workers (~`250-350MB` RSS each when warm). Budget roughly `+512MB` to `+1GB` of container memory per concurrently active account.

---

### Option B: Native Linux / macOS / WSL2 (`./scripts/gw.sh`)

```bash
# 1. Run race-enabled test suite & build zero-CGO binary
cd go-gateway
gofmt -l . && go vet ./... && go test -race -count=1 ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway .
cd ..

# 2. Generate & verify Gate 1-5 patched `./bin/agy-acp-900k` (leaves ~/.local/bin/agy untouched)
# Build & verify candidate in isolated sandbox, then atomically install <candidate_dir> printed above
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. Configure .env & start local service
cp .env.example .env
# Edit .env -> set AGY_API_KEYS=sk-your-master-key and AGY_UPSTREAM_PROXY
./scripts/gw.sh start
./scripts/gw.sh status
```

---

### Step 3: Onboarding & OAuth Account Authorization (3 Methods)

Open the **Warm-Paper Console** at `http://127.0.0.1:18891/` (enter any username and your `AGY_API_KEYS` master key as the password), then click **Account Control Center (`账号控制中心`)** in the top bar:

1. **Method 1 — Cross-Platform WebUI ACP OAuth (Linux / macOS / WSL2 / Docker; Recommended for All Accounts)**:
   - Works natively on **macOS, Linux, WSL2, and Docker** without Linux PTYs by driving `agy acp --gemini-dir <staging> --app-data-dir antigravity-cli --login` with `AGY_ACP_FORCE_FILE_STORAGE=1` and each slot's resolved egress proxy inside an isolated staging directory (`5m` TTL, zero downtime to existing live tokens/workers until verified).
   - Click **Add Account (`新增子账号`)** (or **Google 登录 / 换号** on an existing card), choose an egress proxy (`Direct`, named preset, or custom `http://` / `socks5://` proxy), and save.
   - Opening the OAuth panel automatically resumes any active handshake or starts a fresh one and displays the `https://accounts.google.com/...` authorization URL with a live countdown. Sign in from **any** browser; after Google lands on `https://antigravity.google/oauth-callback?...`, copy the displayed authorization code (or full callback URL) and submit it in the form.
   - The gateway validates `state`/`redirect_uri`, exchanges and verifies the credential via disposable `agy acp` `initialize` + `authenticate("oauth-personal")` RPC, atomically installs the token (`0600`), cleans up legacy `acp_token.json`, displays the authenticated account email, and reloads the active runtime (or supports **Cancel** / **Sign Out** via `logout` RPC).
2. **Method 2 — Offline Token JSON Import**:
   - In the same OAuth panel, paste an existing `antigravity-oauth-token` JSON payload (`{"access_token":...,"refresh_token":...}`) and click **Import Token JSON**.
3. **Method 3 — Host/Container Interactive CLI *(Primary Account Only)***:
   - Native host: `./bin/agy-acp-900k login`
   - Docker: `docker exec -it agy-acp-gateway /app/bin/agy-acp-900k login`

---

### Step 4: Verify Your First API Call

```bash
curl -sS http://127.0.0.1:18890/v1/chat/completions \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.8-flash",
    "reasoning_effort": "low",
    "messages": [{"role": "user", "content": "Reply with READY in one word."}]
  }'
```

---

<a id="client-setup"></a>

## Client Setup Guide (Copy-Paste Ready)

| Client / SDK | Protocol Endpoint | Base URL | Auth Header / Env | Session Continuity & Tool Bridge |
| :--- | :--- | :--- | :--- | :--- |
| **`Claude Code`** | `Anthropic` `/v1/messages` | `http://127.0.0.1:18890` | `x-api-key` / `ANTHROPIC_API_KEY` | `metadata.user_id` + `ws-bridge` MCP |
| **`Codex` CLI** | OpenAI `/v1/responses` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | `prompt_cache_key` / Turn-1 fingerprint + stateless history + MCP |
| **Cursor / Continue / `Cline` / `Roo Code` / `OpenCode`** | OpenAI `/v1/chat/completions` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | Turn-1 `(system + first_user)` fingerprint + Sibling Tool Batching |
| **Gemini CLI / `google-genai` SDK** | Gemini `/v1beta` | `http://127.0.0.1:18890` | `x-goog-api-key` / `?key=` | Native `functionCall` / `thoughtSignature` + MCP |
| **Pi Coding Agent** | `/v1/messages` or `/v1/responses` | `http://127.0.0.1:18890` | `x-api-key` / `Bearer` | `x-pi-session-id` + `TOOLS_UPDATED_IN_PLACE` |

### 1. `Claude Code` (`/v1/messages`)

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
# Tip: append @acct:<id> or @group:<id> to pin this IDE window to a specific account or channel group!
export ANTHROPIC_API_KEY="sk-your-master-key"
export ANTHROPIC_MODEL="gemini-4-carbon-medium"
export ANTHROPIC_SMALL_FAST_MODEL="gemini-3.8-flash-low"

claude
```

### 2. `Codex` CLI / OpenAI Responses API (`/v1/responses`)

Add to `~/.codex/config.toml`:

```toml
model = "gemini-4-carbon"
model_reasoning_effort = "medium"
model_provider = "agy"

[model_providers.agy]
name = "Antigravity ACP Gateway"
base_url = "http://127.0.0.1:18890/v1"
env_key = "AGY_API_KEY"
wire_api = "responses"
```

Or via `curl`:

```bash
curl -N http://127.0.0.1:18890/v1/responses \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-4-neon",
    "reasoning": {"effort": "high"},
    "input": "Explain how Go sync.Pool reduces GC pressure.",
    "stream": true
  }'
```

### 3. OpenAI SDK / Cursor / `Cline` (`/v1/chat/completions`)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:18890/v1",
    api_key="sk-your-master-key@group:team-pro",  # or plain master/bound key
)

resp = client.chat.completions.create(
    model="gemini-4-neon",
    reasoning_effort="high",
    messages=[
        {"role": "system", "content": "You are a concise systems architect."},
        {"role": "user", "content": "Design a lock-free ring buffer in Go."},
    ],
    stream=True,
)
for chunk in resp:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

### 4. Google Gemini CLI & `google-genai` SDK (`/v1beta`)

```bash
export GOOGLE_GEMINI_BASE_URL="http://127.0.0.1:18890"
export GEMINI_API_KEY="sk-your-master-key"

curl -sS "http://127.0.0.1:18890/v1beta/models/gemini-4-hydrogen:generateContent" \
  -H "x-goog-api-key: ${GEMINI_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [{"role": "user", "parts": [{"text": "Summarize Raft leader election in 3 bullets."}]}],
    "generationConfig": {"thinkingConfig": {"thinkingLevel": "XHIGH"}}
  }'
```

---

<a id="multi-account"></a>

## Multi-Account Pool, Channel Groups & Ban-Safe Routing (`v2.3`)

`agy-acp-gateway` embeds a multi-account scheduler and sandbox supervisor designed specifically for official `agy acp` subprocesses — combining channel-pool ergonomics (inspired by `sub2api` & `CLIProxyAPI`) with strict personal-account anti-ban invariants.

### 1. Physical Sandbox & Per-Account Proxy Isolation

Every secondary account (`acct-b`, `acct-c`, ...) added in `:18891` receives a hermetically isolated runtime directory (`<data_dir>/accounts/<id>/`) and loopback child gateway (`127.0.0.1:28902+`):

- **Zero Shared State**: Isolated `HOME`, `.gemini/antigravity-cli/antigravity-oauth-token` (`0600`), `workspace`, `gateway.db`, `tos-tripped`, and `logs/gateway.log`.
- **Per-Account Egress Proxy**: Each account independently selects `direct`, a named preset (`AGY_PROXY_PRESETS_FILE` + `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN`, failing closed if the token is missing), or a custom `http://`, `https://`, `socks5://`, or `socks5h://` proxy.
- **C1-Compliant Egress Probe (`POST /api/accounts/probe-ip`)**: Verifies masked exit IP/region and tests Google reachability through official `agy models` (when authenticated) or `generate_204` — **never** sending raw OAuth tokens outside `agy`.

### 2. Five Pool & Group Routing Strategies

Configure globally on the dashboard overview bar (`#pool-ctrl-bar`) or override per **Channel Group (`AccountGroup`)**:

| Strategy ID | Aliases | Scheduling Behavior | Best For |
| :--- | :--- | :--- | :--- |
| **`round-robin`** *(default)* | `rr` | Cycles evenly across all active, healthy, non-cooling accounts in the pool/group | Equal-tier accounts with uniform traffic |
| **`fill-first`** | `priority` | Sorts by lowest `priority` (`1..1000`), breaking ties by discrete `2%` remaining-quota buckets, `inFlight`, and config order | Burning expiring/promo accounts first while keeping backups warm |
| **`weighted-rr`** | `weighted`, `wrr` | Clamped Nginx Smooth Weighted Round-Robin (`weight` `1..100`, e.g. Ultra `20x` = `10`, Pro = `1`) without burst starvation | Mixing Google AI Ultra `20x` and AI Pro accounts in one pool |
| **`least-load`** | `smart`, `quota-aware` | Minimizes `inFlight / maxConcurrency - 0.35 * remainingQuota` across healthy candidates | High-concurrency team coding workloads with uneven turn durations |
| **`manual-active`** | `manual`, `active` | Pins new sessions to `activeAccountId`; falls back by priority when `manualAutoFailover: true` (default) or fails closed (`429`/`503`) when `false` | Solo developers who want deterministic active account control + 1-click rotation |

### 3. Key Suffixes, Channel Groups & Routing Precedence

You can target a specific account, target a **Channel Group**, or let the global pool schedule automatically — without restarting clients:

| Targeting Mode | Client Credential / Header | Admin Access (`:18891`) | Model Allowlist Enforcement |
| :--- | :--- | :--- | :--- |
| **Account Bound Key** | `Authorization: Bearer sk-acct-only-key` | **Denied (`401`)** | Account `allowedModels` |
| **Group Bound Key** | `Authorization: Bearer sk-team-group-key` | **Denied (`401`)** | Group `allowedModels` ∩ Account `allowedModels` |
| **Master Key + Account Suffix** | `<master_key>@acct:<id>` or `<master_key>@<id>` *(also `#`)* | API plane only | Account `allowedModels` |
| **Master Key + Group Suffix** | `<master_key>@group:<group_id>` *(also `#group:<group_id>`)* | API plane only | Group `allowedModels` ∩ Account `allowedModels` |
| **Explicit Request Header** | `X-Agy-Account: <id>` or `X-Agy-Group: <group_id>` *(with master key)* | Master key | Conflicting `X-Agy-Account` outside `X-Agy-Group` rejected (`403`) |
| **Default Global Pool** | `<master_key>` | Master key | Scheduled account's `allowedModels` |

Every API response exposes routing telemetry headers (CORS-exposed):
`X-Agy-Account`, `X-Agy-Group`, `X-Agy-Route-Mode`, `X-Agy-Failover-Count`, `X-Agy-Served-Model`, and `X-Agy-Cache-Warning`.

### 4. Pre-Stream 1st-Turn Failover vs. Decision U8 Ban-Safe Sticky Continuity

```text
  [Brand-New Turn 1 Request] (!isContinuation, e.g. [system, user_1])
         │
         ├──► Pick Account A ──► Pre-header 429 / 529 / 502 / 503 / 401(#3501)?
         │                           │ YES (up to MaxFailoverHops = 2)
         │                           ▼
         │                   Record cooldown on A + transparent hop to Account B
         │                           │
         └──► Account B accepts ──► Commit Sticky(callerScope | sessionKey -> B)
                                    + emit X-Agy-Account: B, X-Agy-Failover-Count: 1

  [Active Multi-Turn Continuation] (isContinuation, e.g. Turn 2+ / tool_result / function_call_output)
         │
         └──► Strict U8 Guard (`strictU8NoMidTurnHop = true`):
              ALWAYS stays pinned to Account B (even if B is `draining` or in transient cooldown).
              NEVER replays mid-turn conversation history onto Account C (returns 503 if B is disabled/revoked).
```

- **Why U8 Matters**: Replaying a 50-turn coding transcript with mid-session tool calls from Account A onto Account B creates an immediate cross-account trajectory anomaly upstream and destroys KV cache (`0%` hit). By restricting load-balancing and automatic failover strictly to **Turn 1** while keeping **Turn 2+** sticky-pinned via caller-scoped LRU (`session_id` / `prompt_cache_key` / `(system + first_user)` SHA-256 fingerprint), you get seamless high availability **without** cross-account history leakage.
- **Turn-1 System Prompt Disambiguation**: Stateless OpenAI `/v1/chat/completions` requests carrying `[system, user]` are recognized as Turn 1 (`isContinuation = false`) and fingerprinted over `(system + first_user)` — so brand-new threads from the same IDE distribute cleanly across the pool while Turn 2+ (`[system, user, assistant, user_2]`) hashes to the exact same Turn-1 fingerprint and stays pinned!

### 5. Family-Scoped Cooldowns, Graceful Draining & One-Click Rotation

- **Model-Family Cooldown Isolation**: Rate-limit (`429`) and overload (`529`) cooldowns are tracked independently per model family (`gemini`, `claude`, `argon`). Hitting a `429` on `Claude Opus 5.5` cools down only that account's `claude` family while keeping `Gemini 4 Neon` / `Carbon` `100%` routable!
  - `429`: Honors upstream `Retry-After` (capped at `6h`) or exponential strikes (`30s -> 60s -> 2m -> 5m -> 10m`).
  - `529`: Scoped `15s -> 30s -> 60s` backoff (never shortens an existing longer `429` cooldown).
  - `401` / `403 (#3501)`: `30m` `auth_error` quarantine without tripping the global ToS circuit breaker.
  - Non-upstream probes (`count_tokens`, `:countTokens`, `GET /v1/models`) are health-isolated (`skipHealth`) so local `200 OK` responses never wipe active upstream cooldowns.
- **One-Click WebUI / API Actions (`POST /api/accounts/action`)**:
  - **Rotate Next (`rotate_next`)**: Advances `activeAccountId` to the next healthy authenticated account in the pool or group.
  - **Drain & Rotate (`drain_and_rotate`)**: Marks the current account as **`draining`** (stops accepting new Turn-1 sessions while letting all active U8 sticky conversations finish without interruption) and promotes the next healthy account in one atomic click.

---

<a id="models"></a>

## Supported Models, Quota Pools & Thinking Tiers

### Canonical Model Matrix (`agy 1.5.14+`)

| Canonical Model ID | Upstream Display Name | Quota Pool | Context | Max Images | Thinking Tiers | `xhigh` |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`gemini-4-carbon`** *(default)* | Gemini 4 Carbon | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` | Clamps to `high` |
| **`gemini-4-neon`** | Gemini 4 Neon | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high`, **`xhigh`** | **Native** |
| **`gemini-4-hydrogen`** | Gemini 4 Hydrogen | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high`, **`xhigh`** | **Native** |
| **`gemini-4-argon`** | Gemini 4 Argon | Argon / Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` | Clamps to `high` |
| **`gemini-3.8-flash`** | Gemini 3.8 Flash | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` | Clamps to `high` |
| **`claude-opus-5-5`** | `Claude` Opus 5.5 | 3P (`Claude`/GPT) Pool | `1,048,576` (`900K` safe) | `100` | `low`, `medium`, `high` | Clamps to `high` |
| **`claude-sonnet-5-5`** | `Claude` Sonnet 5.5 | 3P (`Claude`/GPT) Pool | `1,048,576` (`900K` safe) | `100` | `low`, `medium`, `high` | Clamps to `high` |

- **Automatic Pro vs. Ultra `20x` Quota Cap Detection**: The console inspects `<agy> -p /usage` bucket topology and automatically renders exact pool caps — **Google AI Pro** (`$10` 5h / `$35` Weekly Gemini + `$2.50` / `$5` 3P + dedicated Weekly Argon pool) vs. **Google AI Ultra `20x`** (`$200` 5h / `$800` Weekly Gemini + `$25` / `$50` 3P).
- **Smart Aliases**: Strips `models/`, `[1m]`, and tier suffixes (`-low`, `-medium`, `-high`, `-xhigh`). Short aliases (`carbon`, `neon`, `hydrogen`, `argon`, `gemini-flash`, `agy-opus`, `agy-sonnet`) work out of the box; set `AGY_ROUTE_CLAUDE_NAMES=1` to map generic `*opus*` / `*sonnet*` / `*haiku*` names automatically.

### Cross-Protocol Thinking Tier Mapping

Explicit body fields take precedence over model-name suffixes (`<model>-high`, `<model>-xhigh`); default tier is `medium`:

| Gateway Tier | Model Suffix | OpenAI (`reasoning_effort` / `reasoning.effort`) | `Anthropic` (`thinking.budget_tokens`) | Gemini `/v1beta` (`thinkingLevel` / `thinkingBudget`) |
| :--- | :--- | :--- | :--- | :--- |
| **`low`** | `-minimal`, `-low` | `none`, `minimal`, `low` | `type: "disabled"` or `<= 4096` | `NONE`, `MINIMAL`, `LOW` or `0..4096` |
| **`medium`** *(default)* | `-medium` | `medium` *(or omitted)* | `4097 .. 16384` | `MEDIUM` or `4097..16384` |
| **`high`** | `-high` | `high` | `16385 .. 32768` | `HIGH` or `16385..32768` |
| **`xhigh`** *(Neon / Hydrogen)* | `-xhigh`, `-max`, `-ultra` | `xhigh`, `max` | `> 32768` | `XHIGH` or `> 32768` |

---

<a id="engineering-pillars"></a>

## Core Engineering Pillars
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/lifecycle-cache-dark.png" />
    <img src="assets/lifecycle-cache-light.png" alt="Worker lifecycle and Carbon KV cache preservation" width="48%" />
  </picture>
  &nbsp;
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/seq-prefill-dark.png" />
    <img src="assets/seq-prefill-light.png" alt="Slow prefill soft-detach and Singleflight sequence" width="48%" />
  </picture>
</p>

1. **5-Layer Carbon KV Cache Defense & Disconnect Rescue**:
   - Verifies native `agy 1.5.14+` CCPA `SessionId` propagation (`SPEC_VERSION=3`) alongside Gate 5 binary trajectory `sessionID` stamping so concurrent sessions never collide on empty upstream cache keys.
   - Freezes Turn-1 `system`/`developer` prompts and initial tool declarations across the session lifetime; refreshes deferred MCP tools (`ToolSearch`) in place via `TOOLS_UPDATED_IN_PLACE`.
   - **Prefill Soft-Detach + Singleflight**: Holds long-context (`>=30s`) in-flight prefills alive for `45s` on client disconnect so retries re-attach without `0%` cache resets.
2. **Dual-Channel MCP Tool Bridge (`ws-bridge`) & Sibling Batching**:
   - Serves an ephemeral loopback-only Streamable HTTP MCP bridge (`/mcp/{tenantKey}`) that suspends upstream tool execution cleanly, returning standard `tool_use` / `tool_calls` / `function_call` / `functionCall` payloads to the caller and resuming the parked worker on the next HTTP turn.
   - Coalesces parallel tool calls from `.db` `PlannerResponse` steps in a single round-trip and caps empty `0/0` orphan tool relays (`AGY_MAX_TOOL_RELAYS=4`).
3. **3-Stage Semantic Cleanroom & Self-Healing WAF Bisection**:
   - Deterministically neutralizes client harness metadata (`<system-reminder>`, workspace paths, reserved tool names) via session-scoped `HMAC-SHA256` reversible tokens (`reversible_codec.go`) restored transparently on outbound SSE streams.
   - On unexpected upstream `400` WAF rejections, bisects prompt segments via `gemini-3.8-flash` probes to isolate and persist minimal rewrite rules (`AGY_WAF_AUTOBISECT=true`).
4. **Zero-Lock `.1.19` SQLite Telemetry & Silent-Retry Sensing**:
   - Parses `agy`'s SQLite B-Tree + `.db-wal` frames in pure Go (`internal/store/sqlite_protobuf.go`) without opening competing SQLite write locks — extracting authoritative cached/thinking/output tokens, `step_type=17` upstream silent-retry counts/delays (`agy 1.5.13+`), Argon `.1.19` shadow-routing anomalies, and read-only Desktop App (`AGY_APP`) usage.
5. **Strict Dual-Port Isolation, Structured Output Parity & SSRF-Guarded Multimodal v2**:
   - `:18890` serves **only** AI generation endpoints + `/healthz` (`404` on `/`, `/api/*`, `/metrics`); `:18891` serves **only** master-key-authenticated console/admin/metrics endpoints (`404` on `/v1/*`, `/v1beta/*`, `/mcp/*`).
   - Unifies structured JSON schema output (`output_config.format`, `response_format`, `text.format`, `responseJsonSchema`), exposes accurate `max_input_tokens` metadata, and enforces strict `/v1/responses` continuation validation (`previous_response_not_found`, `thread_not_found`).
   - Remote image URLs use `16`-byte magic MIME sniffing, `64MB/15m` unseen-delta caching, and kernel-level `net.Dialer.Control` private/link-local IP blocking against DNS-rebinding SSRF.

---

<a id="configuration-reference"></a>

## HTTP Endpoints & Prometheus Metrics Reference

### Surface & Port Matrix

| Surface | Method & Path | Authentication | Description |
| :--- | :--- | :--- | :--- |
| **Gateway `:18890`** | `GET /healthz` | None | Container/load-balancer liveness probe (`{"ok":true}`) |
| **Gateway `:18890`** | `GET /v1/models`, `GET /v1beta/models` | API Key (master / bound / `?key=`) | Model discovery filtered by caller's account/group allowlist |
| **Gateway `:18890`** | `POST /v1/messages`, `POST /v1/messages/count_tokens` | `x-api-key` or `Bearer` | `Anthropic` Messages API (SSE + JSON + tools + vision + thinking) |
| **Gateway `:18890`** | `POST /v1/chat/completions` | `Authorization: Bearer` | OpenAI Chat Completions API |
| **Gateway `:18890`** | `POST /v1/responses` | `Authorization: Bearer` | OpenAI Responses API (`Codex` CLI) |
| **Gateway `:18890`** | `POST /v1beta/models/{m}:generateContent` / `:streamGenerateContent` / `:countTokens` | `x-goog-api-key`, `Bearer`, or `?key=` | Native Google Gemini REST v1beta API |
| **Console `:18891`** | `GET /`, `GET /api/dashboard`, `GET /api/quota` | **Master Key Only** (Basic/Bearer/`x-api-key`) | Warm-Paper live console, federated instances, and quota snapshots |
| **Console `:18891`** | `GET /metrics` | **Master Key Only** | Prometheus `0.0.4` gateway & per-account metrics |
| **Console `:18891`** | `GET/POST/DELETE /api/accounts`, `/routing`, `/groups`, `/action`, `/probe-ip`, `/oauth/*` | **Master Key + CSRF Guard** (`X-Agy-Requested-With: dashboard`) | Multi-account sandbox, group, routing, rotation & PKCE OAuth APIs |
| **Console `:18891`** | `GET /api/upgrade`, `POST /api/upgrade/approve` | **Master Key + CSRF Guard** | `G1-G4` upstream `agy` upgrade status & 1-click hot-swap approval |

### Key Environment Variables (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `AGY_API_KEYS` | *(empty on loopback)* | Comma-separated master keys for `:18890` & `:18891` (supports `@acct:<id>` / `@group:<id>`) |
| `AGY_GW_PORT` / `AGY_DASH_PORT` | `18890` / `18891` | Strict data-plane API port vs. admin/console/metrics port |
| `AGY_ACCOUNTS_FILE` | `<data>/accounts.json` | Multi-account sandboxes, `AccountGroup` pools, and routing configuration |
| `AGY_PROXY_PRESETS_FILE` | *(optional)* | External JSON array of named egress proxy presets |
| `AGY_DASHBOARD_PEERS` | *(optional)* | Federated remote console nodes (`id\|label\|url\|key;...`) |
| `AGY_UPSTREAM_PROXY` | *(empty / `127.0.0.1:7890`)* | Default HTTP/SOCKS5 upstream proxy for primary `agy` engine |
| `AGY_MAX_LIVE_WORKERS` | `8` | Maximum live `agy acp` sessions in the primary worker pool |
| `AGY_IDLE_WORKER_TTL_SEC` | `1200` | Idle ACP session hibernation timeout in seconds (`20m`) |
| `AGY_PARKED_TTL_SEC` | `300` | Parked MCP tool-call resume TTL in seconds (`5m`) |
| `AGY_PREFILL_CANCEL_GRACE_SEC` | `45` | Disconnect soft-detach grace window after `>=30s` prefill |
| `AGY_WAF_AUTOBISECT` | `true` | Automatic Flash WAF bisection & cleanroom rule learning |
| `AGY_CAPACITY_FALLBACK_TTL_SEC` / `AGY_CAPACITY_FALLBACK_MAX_TTL_SEC` | `300` / `1800` | Cooldown TTL & exponential backoff cap before probing recovery from automatic lower-tier fallback (`0` = sticky) |

*(See [`.env.example`](.env.example) for the complete, annotated list of all environment variables.)*

### Key Prometheus Metrics (`GET :18891/metrics`)

- **Gateway & KV Cache**: `agy_gateway_store_ready`, `agy_gateway_live_workers`, `agy_gateway_hibernated_sessions`, `agy_gateway_parked_calls`, `agy_gateway_prompts_inflight`, `agy_gateway_requests_total`, `agy_gateway_cache_burn_warnings_total`, `agy_gateway_capacity_fallback_active_sessions`, `agy_gateway_capacity_fallback_events_total`, `agy_gateway_capacity_fallback_probes_total`, `agy_gateway_soft_detach_total{outcome}`, `agy_gateway_upgrade_state{state}`, `agy_gateway_turns_total{model,mode,stop_reason,usage_source}`, `agy_gateway_tokens_total{model,kind}`.
- **Multi-Account Pool (`v2.3`)**: `agy_account_inflight{account,state}`, `agy_account_sticky_sessions{account}`, `agy_account_cooldown_seconds{account}`, `agy_account_requests_total{account}`, `agy_account_failures_total{account}`, `agy_account_rate_limits_total{account}`, `agy_account_failovers_total{account}`.

---

<a id="safe-upgrader"></a>

## 4-Gate Safe Upgrader (`G1-G4`) & Zero-Quota Test Suite

Official `agy` updates frequently. Both local CLI upgrades (`scripts/agy-upgrade.sh`) and the optional host-side production agent (`deploy/prod-upgrader/agy_prod_upgrader.py`) enforce a strict **4-Gate (`G1-G4`) pipeline** before touching `bin/agy-acp-900k`:

1. **G1 (Multi-Arch Supply-Chain Hash & Version)**: Verifies dual-source `SHA-512` / `SHA-256` digests across `linux_amd64`, `linux_arm64`, and `darwin_arm64`, binary size, and `--version`.
2. **G2 (Binary Patch & Native `SessionId` Audit)**: Applies `900K` context unlock (`agy-patch.py`) + trajectory `SessionId` verification (`tools/agy_extra_patch.py --check`, `SPEC_VERSION=3`).
3. **G3 (Zero-Quota Offline ACP Contract)**: Runs `scripts/agy_acp_smoke.py` in a throwaway home directory to verify JSON-RPC stdio framing and `--version` / `models` contracts.
4. **G4 (Isolated Canary Verification & Symlink Hot-Swap)**: Validates text + 2-step MCP tool round-trips inside an isolated test instance (`:18892`), then atomically flips `data/agy/current` once active turns drain (with automatic rollback on failure).

```bash
# Check latest upstream agy release (read-only, zero quota)
python3 scripts/upstream_watch.py --official-only
./scripts/agy-upgrade.sh --status

# Run full zero-quota Go race tests + Python upgrader tests + fake-ACP E2E suite
cd go-gateway && go test -race -count=1 ./... && cd ..
python3 -m unittest discover -s scripts -p "test_*.py"
python3 -m unittest discover -s deploy/prod-upgrader -p "test_*.py"
```

---

<a id="troubleshooting"></a>

## Troubleshooting & FAQ

<details>
<summary><strong>1. <code>403 permission_error</code> ("model is not allowed for this account / group")</strong></summary>

Your API key is bound to an account or `AccountGroup` whose `allowedModels` restricts that model. Open `:18891` -> **Account Control Center** -> **Edit** on the account or group, click **All Models** (or check the target canonical model), and save.
</details>

<details>
<summary><strong>2. <code>403 Forbidden</code> when opening the dashboard or API via LAN IP / reverse proxy</strong></summary>

When `AGY_API_KEYS` is empty, `agy-acp-gateway` strictly rejects any request with a non-loopback `Host` header to prevent accidental public exposure. Set `AGY_API_KEYS=sk-...` in `.env` and restart.
</details>

<details>
<summary><strong>3. <code>403 #3501</code> / <code>GEMINI_ENTERPRISE_LICENSE_NOT_VALID</code> or <code>auth_error</code> badge</strong></summary>

The Google account's OAuth session expired or lacks an active Antigravity entitlement. Unlike ToS errors, `#3501` does **not** halt the whole gateway — it quarantines only that account for `30m` (`auth_error`). Re-authorize that account in `:18891` -> **OAuth** (or click **Clear Cooldown** after refreshing credentials).
</details>

<details>
<summary><strong>4. <code>503 Service Unavailable</code> ("Account bound to this active conversation is unavailable")</strong></summary>

**Decision U8 Anti-Ban Protection** is active: the account serving your current multi-turn conversation was disabled or hit an auth/ToS hard block, and the gateway refuses to replay mid-conversation history onto a different personal OAuth account. Start a **new conversation** in your IDE/client so Turn 1 routes cleanly to a healthy account.
</details>

<details>
<summary><strong>5. <code>403 Forbidden</code> on <code>POST / DELETE /api/accounts*</code> from custom scripts</strong></summary>

Admin mutations enforce Origin + CSRF protection. When automating via CLI `curl`, omit browser `Origin` and `Sec-Fetch-Site` headers; when calling with a browser/script `Origin`, pass BOTH a same-origin `Origin` matching `Host` AND `-H "X-Agy-Requested-With: dashboard"`.
</details>

<details>
<summary><strong>6. Named proxy preset fails with "missing AGY_RESIN_PROXY_TOKEN"</strong></summary>

Named proxy presets (`AGY_PROXY_PRESETS_FILE`) fail closed so an account configured for proxy isolation never leaks your host's direct IP. Either set `AGY_RESIN_HOST` + `AGY_RESIN_PROXY_TOKEN` in `.env`, or switch the account's proxy mode to **`custom`** (`http://user:pass@host:port` / `socks5://...`) or **`direct`**.
</details>

<details>
<summary><strong>7. Stopping an isolated non-default test instance with <code>scripts/gw.sh</code></strong></summary>

Non-default ports automatically use isolated runtime directories (`runtime/instance-<port>`). Pass the exact same port variables when stopping: `AGY_GW_PORT=18892 AGY_DASH_PORT=18893 ./scripts/gw.sh stop`. Never run `pkill -x agy-acp-900k` on a host serving live traffic.
</details>

---

## Security & Privacy Boundary

- **Official Subprocess Only (Constraint C1)**: Zero spoofed Google OAuth or internal Protobuf HTTP requests; all upstream model traffic executes via official `agy acp`.
- **Zero-Secret Persistence & Redaction**: Logs automatically scrub Bearer tokens, `?key=` query strings, and proxy credentials (including username-only tokens); `/api/dashboard` masks account/group `boundKeys` previews, while master-key-only `/api/accounts` masks `customProxy` credentials (`maskProxyURL`).
- **No Telemetry Phone-Home & Strict Admin Isolation**: Zero external analytics; the warm-paper console and Lucide icon set are `100%` embedded inside the Go binary (`//go:embed`), and `AGY_CORS_ORIGINS` is strictly scoped to `:18890` data-plane routes (`/v1/*`, `/v1beta/*`), never `:18891` admin endpoints.

---

## Release History

<a id="changelog"></a>

- **2026-11-23 (`v2.3.1 — PR #1 Integration, Isolated Quota HOME, macOS Headless Keychain Guard & Non-Blocking Cold Quota`)**:
  - **Isolated `HOME` & macOS File-Token Enforcement (`dashboard.go`, `acp_engine.go`, `account_oauth.go`, `account_manager.go`)**: Integrated and extended community PR #1 (`@HamsteRider-m`) with `resolveIsolatedAgyHome` (per-profile `quota-home-<profile>` + atomic `.gemini` symlink, or direct parent resolution for `.gemini` child sandboxes) and automatic `SSH_CONNECTION` headless file-token enforcement on `darwin` across `/usage`, `agy acp`, and multi-account OAuth/child workers — eliminating 22s `signal: killed` hangs when host `$HOME` is unauthenticated or macOS Keychain blocks.
  - **Strictly Non-Blocking Cold/Error Quota (`dashboard.go`)**: `FetchOfficialQuota(false)` now returns immediately (`<1ms`, `status: "loading"`) on cold start while snapshotting subprocess env synchronously and launching background refresh, and persists `lastQuotaErr` across both command and JSON unmarshal errors with exponential backoff (`30s -> 2m -> 5m -> 15m`) so `GET /api/dashboard` and federated polls never stall.
  - **Client Attribution & Proxy Header Disambiguation (`internal/protocol/client_detect.go`)**: Added `GenericAgent` (`ga-platform` / `ws_call_tool`) and `T3 Code` (`<t3_code_instructions>`) detection and prioritized explicit prompt/tool signatures over proxy-injected `Originator` / `User-Agent` headers.
- **2026-11-22 (`v2.3.0 — Multi-Account Pool v3, Channel Groups, Turn-1 Pre-Stream Failover, agy 1.5.14 Native SessionId & Security Hardening`)**:
  - **Multi-Account Pool & Channel Groups (`account_scheduler.go`, `account_manager.go`)**: Added 5 pool/group routing strategies (`round-robin`, `fill-first` with `2%` quota buckets, clamped Nginx `weighted-rr`, `least-load`, and `manual-active` with optional strict lock), `AccountGroup` channel pools with dedicated keys & model allowlists, `@acct:<id>` / `@group:<id>` / `X-Agy-Group` routing, and 1-click `rotate_next` & `drain_and_rotate` actions.
  - **Pre-Stream 1st-Turn Failover & U8 Continuity**: Added bounded Turn-1 pre-stream failover across child workers on `429`/`529`/`502`/`503`/`401(#3501)` (`X-Agy-Failover-Count`), accurate `[system, user]` Turn-1 vs. Turn-2+ fingerprinting (`inspectConversationTurns`), model-family-isolated cooldowns (`gemini` / `claude` / `argon`), structured SSE tail error detection, and non-upstream (`count_tokens`) health isolation.
  - **Warm-Paper Multi-Account Control Center & Observability**: Upgraded embedded `:18891` console with live Pool Routing Bar, Account & Group CRUD tabs, per-card scheduling/cooldown badges, and 7 `agy_account_*` Prometheus metrics.
  - **Upstream `agy 1.5.14` & Security Hardening**: Verified `agy 1.5.14` native CCPA `SessionId` propagation (`SPEC_VERSION=3`), hardened Gemini `:action` path parsing against colon-stuffing bypasses, enforced fail-closed proxy checks across OAuth/probes, masked `/api/dashboard` group keys, and tightened URL credential log redaction.
- **2026-11-20 (`v2.2.0 — Single-Container Multi-Account Sandboxes, Pro/Ultra Quota Caps, Federated Console & agy 1.5.13 Silent-Retry Parity`)**:
  - Added single-container isolated multi-account sandboxes, headless PTY PKCE WebUI OAuth, strict per-key model allowlists, automatic Google AI Pro vs. Ultra `20x` quota cap detection, federated multi-instance dashboard (`AGY_DASHBOARD_PEERS`), and zero-patch SQLite `step_type=17` silent-retry sensing.
- **2026-11-19 (`Gemini 4 Neon, Hot-Path Zero-Copy Optimizations & Tier Sync`)**:
  - Added `gemini-4-neon` (`xhigh` tier), pooled hot-path `gzip`/JSON buffers, `sync.OnceFunc` lazy cleanroom regex compilation, and SQLite `TurnUsage.ActualTier` decoding.
- **2026-11-18 (`agy 1.5.11+ Acyclic internal/* Refactor & Cache Preservation v2`)**:
  - Refactored `go-gateway/internal/` into a 7-package acyclic DAG guarded by AST tests, added SQLite WAL read/write separation, frozen Turn-1 system/tool prefixes, context-aware `PrefillSoftDetach`, and Desktop `AGY_APP` read-only telemetry.

<details>
<summary><strong>Earlier Milestones (2026-11-08 to 2026-11-17)</strong></summary>

- **2026-11-16 ~ 2026-11-17**: Added 3-stage Multimodal v2 pipeline with SSRF dialer guards, `#3501` `403` isolation, `ReadSlice('\n')` `128MB` anti-deadlock pipe reader, Gate 5 per-trajectory `sessionID` binary unlock, and Prefill Soft-Detach + Singleflight.
- **2026-11-12 ~ 2026-11-15**: Launched session-scoped `HMAC-SHA256` reversible cleanroom codec, Flash WAF auto-bisection, native Google Gemini REST (`/v1beta`) adapter, `gemini-4-hydrogen` (`xhigh`), and `TOOLS_UPDATED_IN_PLACE` dynamic MCP tool updates.
- **2026-11-08 ~ 2026-11-11**: Replaced Node prototype with pure-Go (`CGO_ENABLED=0`) single binary, zero-lock `.db`/`.1.19` protobuf telemetry, `PlannerResponse` sibling tool batching, and `G1-G4` safe upgrader.

</details>

---

## License

Released under the [MIT License](LICENSE).
