<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/banner-dark.svg" />
    <img src="assets/banner-light.svg" alt="agy-acp-gateway" width="760" />
  </picture>
</p>

<h3 align="center">Zero-CGO, quad-protocol AI gateway + OpenAI Image API for Google's official Antigravity CLI (<code>agy acp</code>): 11 chat models + image generation, Multi-Account Pool v3, Safety Monitor Governor v3, Carbon KV cache defense, and warm-paper live telemetry.</h3>

<p align="center">
  <a href="./README.md"><strong>English</strong></a> &nbsp;·&nbsp;
  <a href="./README_ZH.md"><strong>简体中文</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>Quick Start</strong></a> &nbsp;·&nbsp;
  <a href="#client-setup"><strong>Client Setup</strong></a> &nbsp;·&nbsp;
  <a href="#multi-account"><strong>Pool &amp; Governor</strong></a> &nbsp;·&nbsp;
  <a href="#models"><strong>Models &amp; Tiers</strong></a> &nbsp;·&nbsp;
  <a href="#engineering-pillars"><strong>Architecture</strong></a> &nbsp;·&nbsp;
  <a href="#configuration-reference"><strong>Config &amp; Metrics</strong></a> &nbsp;·&nbsp;
  <a href="#safe-upgrader"><strong>Upgrader</strong></a> &nbsp;·&nbsp;
  <a href="#troubleshooting"><strong>Troubleshooting</strong></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26%2B_(CGO__ENABLED%3D0)-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+ Zero CGO" />
  <img src="https://img.shields.io/badge/Protocols-Anthropic_%7C_OpenAI_%7C_Responses_%7C_Gemini_v1beta_%7C_Images-0856F5?style=flat-square" alt="Quad Protocol + Images" />
  <img src="https://img.shields.io/badge/Models-11_Chat_%2B_gemini--3.1--flash--image-7C3AED?style=flat-square" alt="11 Chat Models + Image Generation" />
  <img src="https://img.shields.io/badge/Multi--Account-Pool_v3_%2B_Governor_v3-167c52?style=flat-square" alt="Multi-Account Pool v3 and Safety Monitor Governor v3" />
  <img src="https://img.shields.io/badge/Context-900K_Unlocked-0D9488?style=flat-square" alt="900K Context" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-22c55e?style=flat-square" alt="MIT License" /></a>
</p>

---

**`agy-acp-gateway`** turns Google's official Antigravity CLI (`agy acp`, JSON-RPC 2.0 over stdio) into a production-hardened, single-binary multi-protocol gateway (`CGO_ENABLED=0`) and real-time control console.

Unlike reverse-engineered proxies that forge private OAuth/Protobuf HTTP calls and expose accounts to TLS/header fingerprint bans, **100% of upstream traffic runs through the official `agy` binary**. Google's own binary handles TLS BoringSSL handshakes, OAuth token refreshes, trajectory checkpoints, and upstream framing, while `agy-acp-gateway` provides:

- **Quad-Protocol Chat Endpoints + OpenAI Image API (`:18890`)**: Anthropic `/v1/messages`, OpenAI `/v1/chat/completions`, OpenAI `/v1/responses`, native Google Gemini REST `/v1beta`, plus OpenAI-compatible `POST /v1/images/generations` and `POST /v1/images/edits` (`gemini-3.1-flash-image` via an isolated official `--agent ws-imagegen` engine with `0` MCP servers and `0` safety monitor calls).
- **11 Routable Chat Models, Pure-Go PDF Fast-Path & Opt-in Safe Media Relay**: Full Gemini 4 family (`carbon`, `neon`, `argon`, `hydrogen`), `gemini-3.8-flash`, `claude-opus-5-5`, `claude-sonnet-5-5`, `gpt-oss-120b-medium`, and 3 `gemini-pro-orchestrator` variants; magic-sniffed `PNG`/`JPEG` (incl. grayscale Adobe APP14 strip)/`WEBP`/`GIF`/`HEIC`/`HEIF` images + 24 UTF-8 text/document formats (`<=2 MiB`) + zero-dependency pure-Go PDF text extraction (all models incl. Claude/GPT with full tools & KV cache) + opt-in **`AGY_MEDIA_RELAY=1`** isolated `ws-media` engine (`"toolPermission": "strict"` synchronous pre-open path whitelist, `0` `sir_flash_lite` calls) for Gemini native visual PDF, audio, and video.
- **Multi-Account Pool v3, Safety Monitor Governor v2 & Account Health Hardening**: Single-container physical sandboxes (`HOME` + SQLite + egress proxy + isolated `antigravity-cli-usage` / `antigravity-cli-imagegen` / `media-home` app-data-dirs), 5 scheduling strategies, `role: primary|standby`, live governance toggles (`priorityTiering`, `monitorSpillOnYellow`, `monitorBudgetSpread`), first-class `VALIDATION_REQUIRED` ("Verify your account") state with one-click Verify/Recheck, 1h OAuth refresh transient `401` grace, Decision U8 ban-safe sticky routing, Turn-1 pre-stream failover, 3-zone soft-budget monitor protection, and quota-burn anomaly detection.
- **Resident Memory Governor, 5-Layer KV Cache Defense, `<2s` Shutdown & Warm-Paper Console (`:18891`)**: Global Live Worker Cap (`AGY_GLOBAL_MAX_LIVE_WORKERS`) + cross-account LRU hibernation (`180s` idle TTL + real-time cgroup `70%/80%` pressure guard keeping `30-50` logical resident sessions in `~1.5-1.8 GiB` RAM via lossless `session/resume`), native `agy 1.5.14+` `SessionId` verification + `--acp-only` `900K` context unlock, Turn-1 prefix freeze, Prefill Soft-Detach, protocol/stream hardening (split `360s`/`180s` timeouts, `9600B` MCP `tool_result` 70/30 pre-bounding, `\r` runaway guard), bounded SQLite `8d`+AllTime rollup/compaction, `<2s` graceful shutdown, Pro/Ultra `20x` quota auto-detection, Prometheus `/metrics`, and `G1-G5` upgrader (`G5a` strict-permission binary check + `AGY_BLOCKED_VERSIONS`).

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
| **Multi-account rate limits & standby overflow** | Single global `~/.gemini` home; manual restarts on `429`/`529` | **Pool v3 & Channel Groups**: 5 strategies (`round-robin`, `fill-first`, `weighted-rr`, `least-load`, `manual-active`), `role: primary\|standby`, `priorityTiering`, Turn-1 failover, family cooldowns, `VALIDATION_REQUIRED` verify/recheck, 1-click **Drain & Rotate** |
| **Pre-tool safety monitor `80x` escalation** | Exhausts `sir_flash_lite` silently and burns the 5h Gemini pool within minutes | **Safety Monitor Governor v2**: 3-zone soft budget (`Green`/`Yellow`/`Red`), Green balancing, Yellow soft-drain, zero-tool reverse routing, hop cancel, `monitor-trips.json` v2 window persistence, and burn anomaly alerts |
| **Gemini 4 Carbon concurrent 0% KV cache** | Shares empty trajectory IDs or mutates `system`/tool prefixes mid-conversation | **Trajectory `SessionId` Isolation + Prefix Freeze**: per-session `SessionId`, frozen Turn-1 system/tool catalog, and `TOOLS_UPDATED_IN_PLACE` |
| **120s+ long-context prefill disconnect storms** | Sends `session/cancel` on client timeout, destroying 150K+ in-flight prefill | **Prefill Soft-Detach (`30s`/`45s`) + Singleflight**: keeps upstream compute alive so retrying clients re-attach seamlessly |
| **Client harness tags trigger 400 WAF blocks** | Fails outright or strips user instructions via lossy regexes | **3-Stage Cleanroom + Session HMAC Codec + Flash Auto-Bisect**: losslessly encodes harness tags/paths and auto-isolates novel WAF triggers |
| **Opaque usage, silent retries & DB/RAM bloat** | Estimates tokens blindly; OOMs across multi-account workers or stalls on shutdown | **Resident Memory Governor + Zero-Lock `.1.19` SQLite + `<2s` Shutdown**: global live worker cap + cross-account LRU hibernation + cgroup `70%/80%` guard (`30-50` sessions in `~1.5-1.8 GiB` RAM), exact `cached`/`thinking`/`output` tokens, `8d` rollup, and daily compaction |

---

<a id="quick-start"></a>

## 5-Minute Quick Start

### Prerequisites

- **OS**: Linux (`x86_64` / `arm64`), macOS (`arm64` Apple Silicon), or Windows via **WSL2 Ubuntu**.
- **Official Antigravity CLI (`agy`)**: Installed at `~/.local/bin/agy` (**`1.5.16` recommended baseline**; `1.5.17`/`1.5.18` suffer from upstream `agy#1890` `4x` `--agent` Gemini quota burn and `agy#1872`; the global binary is never modified in place).
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
# Edit deploy/.env -> set a strong AGY_API_KEYS value
# (deploy/docker-compose.yml pins AGY_LISTEN_HOST=0.0.0.0 inside the container and defaults
#  AGY_GLOBAL_MAX_LIVE_WORKERS=8, AGY_IDLE_WORKER_TTL_SEC=180, AGY_CONV_KEEP_DAYS=4, and
#  AGY_ACP_LOG_KEEP_DAYS=7; leave AGY_UPSTREAM_PROXY commented out unless using a reachable proxy)
docker compose -f deploy/docker-compose.yml up -d --build

# 4. Verify liveness
curl -fsS http://127.0.0.1:18890/healthz
# -> {"ok":true}
```

> **Multi-Account Resident Memory Sizing**: [`deploy/docker-compose.yml`](deploy/docker-compose.yml) defaults to `mem_limit: 3200m`, `stop_grace_period: 25s`, `AGY_GLOBAL_MAX_LIVE_WORKERS=8`, `AGY_IDLE_WORKER_TTL_SEC=180`, and `AGY_CONV_KEEP_DAYS=4`. With the **Resident Memory Governor** (global live worker cap across all accounts, cross-account LRU hibernation, `180s` idle TTL, and real-time cgroup `70%/80%` pressure guard), `30-50` logical resident sessions stay within `~1.5-1.8 GiB` RAM while rehydrating transparently via lossless `session/resume`. Each active secondary account adds only a lightweight child gateway (`AGY_CHILD_GATEWAY_GOMEMLIMIT=128MiB`) sharing the container-wide worker budget; the `:18891` console displays a live **cgroup RAM budget meter** (`totalMaxLiveWorkers` / `estPeakRssMB` / `hostMemLimitMB`).

---

### Option B: Native Linux / macOS / WSL2 (`./scripts/gw.sh`)

```bash
# 1. Run race-enabled test suite & build zero-CGO binary
cd go-gateway
gofmt -l . && go vet ./... && go test -race -count=1 ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway .
cd ..

# 2. Generate & verify --acp-only patched ./bin/agy-acp-900k (leaves ~/.local/bin/agy untouched)
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. Configure .env & start local service
cp .env.example .env
# Edit .env -> set AGY_API_KEYS=sk-your-master-key and AGY_UPSTREAM_PROXY (or AGY_UPSTREAM_PROXY= for direct)
./scripts/gw.sh start
./scripts/gw.sh status
```

---

### Step 3: Onboarding & OAuth Account Authorization (3 Methods)

Open the **Warm-Paper Console** at `http://127.0.0.1:18891/` (enter any username and your `AGY_API_KEYS` master key as the password), then click **Account Control Center (`账号控制中心`)** in the top bar:

1. **Method 1: Zero-SSH WebUI PKCE OAuth (Linux / WSL2 / Docker; Recommended for Headless Servers & Secondary Accounts)**
   - *(Native `macOS arm64` hosts do not expose `/dev/ptmx` Linux PTYs; use Method 2 offline token JSON import or Method 3 CLI login on macOS.)*
   - Click **Add Account (`新增子账号`)** (or **OAuth (`网页 OAuth 授权`)** on an existing card), choose an egress proxy (`direct`, named preset, or custom `http://` / `https://` / `socks5://` / `socks5h://` proxy), and save.
   - Click **Step 1: Generate Google OAuth URL**. The gateway spawns `agy login` inside an isolated `/dev/ptmx` pseudo-terminal scoped to that account's sandbox directory and proxy (auto-reaped after `70s`, with `.oauth-bak` rollback protection).
   - Open the generated `https://accounts.google.com/...` URL in **any** local browser, sign in, and when the browser redirects to `http://localhost:.../?code=4/0A...` (even if localhost shows "connection refused"), copy that full URL (or just the `code` parameter) into **Step 2** and click **Activate**.
   - The gateway completes token exchange, automatically probes available models and plan tier via official `agy models`, and boots the isolated worker.
2. **Method 2: Offline Token JSON Import**
   - In the same OAuth panel, paste an existing `antigravity-oauth-token` JSON payload (`{"access_token":...,"refresh_token":...}`) and click **Import Token JSON**.
3. **Method 3: Host/Container Interactive CLI *(Primary Account Only)***
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
| **`Claude Code`** | Anthropic `/v1/messages` | `http://127.0.0.1:18890` | `x-api-key` / `ANTHROPIC_API_KEY` | `metadata.user_id` + `ws-bridge` MCP + client `WebFetch` |
| **`Codex` CLI** | OpenAI `/v1/responses` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | `prompt_cache_key` / Turn-1 fingerprint + stateless history + MCP |
| **Cursor / Continue / `Cline` / `Roo Code` / `OpenCode`** | OpenAI `/v1/chat/completions` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | Turn-1 `(system + first_user)` fingerprint + Sibling Tool Batching |
| **Gemini CLI / `google-genai` SDK** | Gemini `/v1beta` | `http://127.0.0.1:18890` | `x-goog-api-key` / `?key=` | Native `functionCall` / `thoughtSignature` + MCP |
| **Pi Coding Agent** | `/v1/messages` or `/v1/responses` | `http://127.0.0.1:18890` | `x-api-key` / `Bearer` | `x-pi-session-id` + `TOOLS_UPDATED_IN_PLACE` |
| **OpenAI Image Clients / SDKs** | OpenAI `/v1/images/{generations,edits}` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` / `x-api-key` | Isolated `--agent ws-imagegen` engine (`gemini-3.1-flash-image`) |

### 1. `Claude Code` (`/v1/messages`)

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
# Tip: append @acct:<id> or @group:<id> to pin this IDE window to a specific account or channel group
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

### 5. OpenAI-Compatible Image Generation & Editing (`/v1/images/generations`, `/v1/images/edits`)

Powered by a dedicated, lazily started official `agy acp --agent ws-imagegen` engine (`tools: [generate_image]`, `0` MCP servers, `0` safety monitor calls; outer turn pinned to `gemini-3.8-flash` `low`, generating via **`gemini-3.1-flash-image`** with `60s` idle auto-stop, `O_NOFOLLOW` + bounded-fd artifact reads, and automatic session cleanup):

- **Parameters & Output**: Returns `b64_json` (`response_format: "b64_json"`), supports `n` (`1..4`), explicit `aspect_ratio` (`1:1`, `2:3`, `3:2`, `3:4`, `4:3`, `9:16`, `16:9`) or OpenAI `size` (`1024x1024`, `1536x1024`, `1024x1536`, `1792x1024`, `1024x1792`, `auto`, or nearest `WxH` mapping), and sets `X-Agy-Served-Model: gemini-3.1-flash-image`.
- **Image Editing (`/v1/images/edits`)**: Accepts `multipart/form-data` (`image` / `image[]`) or `application/json` (`image` / `images` as base64 or `data:` URI) with up to `3` magic-sniffed reference images (`PNG`, `JPEG`, `WEBP`, `GIF`, `HEIC`, `HEIF`).

```bash
# Text-to-image generation (application/json)
curl -sS http://127.0.0.1:18890/v1/images/generations \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image",
    "prompt": "Minimalist isometric blueprint of a multi-protocol API gateway on warm paper",
    "size": "1536x1024",
    "n": 1,
    "response_format": "b64_json"
  }'

# Image editing with up to 3 reference images (multipart/form-data)
curl -sS http://127.0.0.1:18890/v1/images/edits \
  -H "Authorization: Bearer sk-your-master-key" \
  -F "prompt=Convert the sketch into a clean technical line illustration" \
  -F "aspect_ratio=16:9" \
  -F "image[]=@./ref1.png"
```

---

<a id="multi-account"></a>

## Multi-Account Pool v3, Safety Monitor Governor v3 & Ban-Safe Routing

`agy-acp-gateway` embeds a multi-account scheduler, sandbox supervisor, and safety-monitor governor designed specifically for official `agy acp` subprocesses, combining channel-pool ergonomics with strict personal-account anti-ban invariants.

### 1. Physical Sandbox, Per-Account Proxy Isolation & Resident Memory Governor

Every secondary account (`acct-b`, `acct-c`, ...) added in `:18891` receives a hermetically isolated runtime directory (`<data_dir>/accounts/<id>/`) and loopback child gateway (`127.0.0.1:28902+`, booted in bounded-4 parallel batches):

- **Zero Shared State & Isolated Auxiliary `app-data-dir`s**: Isolated `HOME`, `.gemini/antigravity-cli/antigravity-oauth-token` (`0600`), `workspace`, `gateway.db`, `tos-tripped`, and `logs/gateway.log`. Auxiliary invocations use dedicated `app-data-dir` sandboxes (`antigravity-cli-usage` for `/usage` / `/credits` / `models`, `antigravity-cli-imagegen` for `ws-imagegen`, and `<PureApiHome>/media-home` with `antigravity-cli-media` for `ws-media`) so quota checks and media/image engines never scan thousands of chat `.db` files or share conversations/plugins.
- **Per-Account Egress Proxy**: Each account independently selects `direct`, a named preset (`AGY_PROXY_PRESETS_FILE` + `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN`, failing closed if the token is missing), or a custom `http://`, `https://`, `socks5://`, or `socks5h://` proxy.
- **Dual Egress Probing**:
  - `POST /api/accounts/probe-ip` verifies masked exit IP/region and tests Google reachability through official `agy models` (when authenticated) or `generate_204`, never sending raw OAuth tokens outside `agy`.
  - `/usage` login prompts are classified by probing `oauth2.googleapis.com` through that account's configured egress proxy so proxy/network outages surface as amber egress errors instead of misclassifying healthy accounts as `tos_tripped` or `auth_error`.
- **Global Live Worker Cap, Cross-Account LRU Hibernation & Cgroup Pressure Guard**: `AGY_GLOBAL_MAX_LIVE_WORKERS` (`8` in Docker Compose, `0` = auto from cgroup memory limit) caps total live `agy acp` workers across primary and all child accounts, paired with cross-account LRU hibernation, `AGY_IDLE_WORKER_TTL_SEC=180` (`3m`), and a real-time cgroup v2/v1 working-set pressure guard (`AGY_MEM_PRESSURE_WARN_PCT=70` / `AGY_MEM_PRESSURE_HIGH_PCT=80`). Logical resident sessions scale to `30-50` inside `~1.5-1.8 GiB` RAM with transparent lossless `session/resume`, while `:18891` renders a live **cgroup RAM budget meter** (`totalMaxLiveWorkers` / `estPeakRssMB` / `hostMemLimitMB`).

### 2. Five Routing Strategies, Standby Role & Governance Guardrails

Configure globally on the dashboard overview bar (`#pool-ctrl-bar`) or override per **Channel Group (`AccountGroup`)**:

| Strategy ID | Aliases | Scheduling Behavior | Best For |
| :--- | :--- | :--- | :--- |
| **`round-robin`** *(default)* | `rr` | Cycles evenly across active, healthy, non-cooling accounts (with Green monitor-level balancing) | Equal-tier accounts with uniform traffic |
| **`fill-first`** | `priority` | Sorts by lowest `priority` (`1..1000`), breaking ties by discrete `2%` remaining-quota buckets, `inFlight`, and config order | Burning expiring/promo accounts first while keeping backups warm |
| **`weighted-rr`** | `weighted`, `wrr` | Clamped Nginx Smooth Weighted Round-Robin (`weight` `1..100`, e.g., Ultra `20x` = `10`, Pro = `1`) without burst starvation | Mixing Google AI Ultra `20x` and AI Pro accounts in one pool |
| **`least-load`** | `smart`, `quota-aware` | Minimizes `inFlight / maxConcurrency - 0.35 * remainingQuota` across healthy candidates | High-concurrency team coding workloads with uneven turn durations |
| **`manual-active`** | `manual`, `active` | Pins new sessions to `activeAccountId`; falls back by priority when `manualAutoFailover: true` (default) or fails closed (`429`/`503`) when `false` | Solo developers who want deterministic active account control + 1-click rotation |

- **Standby Role & Live Governance Guardrails (`POST /api/accounts/routing`, hot-reloaded without restart)**:
  - **`role: "primary" | "standby"`**: Any account can be marked `standby` (rendered with a dashed standby pill in `:18891`); standby accounts stay idle for both tool and zero-tool traffic while primary accounts are healthy, serving overflow and failover only.
  - **`priorityTiering`**: Enforces strict lowest-`priority` tier-first scheduling across all 4 pool strategies (lower-priority tiers stay idle until higher tiers saturate or enter spillover).
  - **`monitorSpillOnYellow`**: Spills new tool-bearing sessions to standby or lower tiers as soon as primaries enter the Yellow monitor zone (when disabled, spills only on Red/unavailable).
  - **`monitorBudgetSpread`**: Live runtime override for proactive Green-zone tool spreading and zero-tool reverse routing.

### 3. Safety Monitor Governor v2 (3-Zone Soft Budget & Burn Anomaly Detector)

Upstream `agy` runs a lightweight pre-tool safety monitor (`sir_flash_lite`) on every external MCP tool call. If an account exhausts its 5h `sir_flash_lite` limit (`429`), `agy` escalates 1:1 to `severe_internal_risks_agy` (~`80x` cost), rapidly draining the 5h Gemini quota pool. Governor v2 observes worker telemetry passively without altering upstream behavior:

- **3-Zone Soft Budget**: Tracks per-account 5h monitor calls across **Green** (`< SoftCap`), **Yellow** (`[SoftCap, DrainCap)`), and **Red** (`>= DrainCap` or tripped). Defaults: Ultra **`2800 / 3800 / 4800`**, Pro **`1800 / 2600 / 3400`** (`AGY_MONITOR_*_CAP_*`), auto-calibrated proportionally on real `429` lockouts.
- **Green Balancing & Yellow Soft-Drain**: Balances new tool sessions across healthy Green peers (`narrowCandidatesByMonitorUtil`, in-flight-aware) in `round-robin` and `weighted-rr`. Entering **Yellow** soft-drains the account cleanly: existing U8 sticky sessions remain pinned (preserving KV cache), while new tool sessions steer to Green peers.
- **Zero-Tool Exemption & Reverse Routing**: Plain text / zero-tool requests are always exempt from monitor trips and are reverse-routed (`preferWarmNoTools`) onto Yellow/Red healthy-quota accounts so Green accounts retain maximum monitor headroom.
- **Hop Cancel & Adaptive MCP Relay Hold**: When `AGY_MONITOR_TRIP_HOP=true` moves a sticky tool session off a tripped/Red account onto a healthy peer, the gateway cancels/hibernates the old parked session locally and across child workers via authenticated loopback `POST /api/internal/cancel-parked-session`. Parked keep-alive relays adapt automatically (`55s` Green `<50%`, `75s` Green `>=50%`, `80s`/`135s` Green long-running exec tools at `>=32K`/`<32K` context, `135s` base / `150s` cap after `30m` parked in Yellow/Red, safely below `agy`'s `180s` MCP ceiling; explicit `AGY_TOOL_RELAY_SEC` / `AGY_TOOL_RELAY_MS` overrides).
- **Window Persistence v2 & Burn Anomaly Detector**: Persists `monitor-trips.json` (`Version: 2`) with `windowKey`, `windowStartAt`, `windowResetAt`, independent 5h `liteResetAt` (with `10s` wrap-safe rings fixing `~20m` drift vs. Gemini `/usage`), and `logCalls5h`, eliminating transient post-restart false-red lockouts. `detectQuotaBurnAnomaly` compares 5h/weekly official spend against expected local + monitor cost to flag `multiplier_anomaly` (`warn >=2.2x` / `critical >=3.0x`) and `unattributed_burn` (`10m`-debounced `QUOTA_BURN_ANOMALY` logs + `:18891` alert banners/pills).

### 4. Key Suffixes, Channel Groups & Routing Precedence

Target a specific account, target a **Channel Group**, or let the global pool schedule automatically without restarting clients:

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

### 5. Pre-Stream 1st-Turn Failover vs. Decision U8 Ban-Safe Sticky Continuity

```text
  [Brand-New Turn 1 Request] (!isContinuation, e.g. [system, user_1])
         |
         +--> Pick Account A --> Pre-header 429 / 529 / 502 / 503 / 401(#3501)?
         |                           | YES (up to MaxFailoverHops = 2)
         |                           v
         |                   Record cooldown on A + transparent hop to Account B
         |                           |
         +--> Account B accepts --> Commit Sticky(callerScope | sessionKey -> B)
                                    + emit X-Agy-Account: B, X-Agy-Failover-Count: 1

  [Active Multi-Turn Continuation] (isContinuation, e.g. Turn 2+ / tool_result / function_call_output)
         |
         +--> Strict U8 Guard (`strictU8NoMidTurnHop = true`):
              ALWAYS stays pinned to Account B (even if B is `draining`, Yellow, or in transient cooldown;
              except explicit `AGY_MONITOR_TRIP_HOP` when B is Red/monitor-tripped with a healthy peer).
              NEVER replays mid-turn conversation history across accounts on routine errors (returns 503 if B is disabled/revoked).
```

- **Why U8 Matters**: Replaying a 50-turn coding transcript with mid-session tool calls from Account A onto Account B creates a cross-account trajectory anomaly upstream and resets KV cache to `0%`. Restricting load-balancing and pre-stream failover to **Turn 1** while keeping **Turn 2+** sticky-pinned via caller-scoped LRU (`session_id` / `prompt_cache_key` / `(system + first_user)` SHA-256 fingerprint) balances high availability with personal-account safety.
- **Turn-1 System Prompt Disambiguation**: Stateless OpenAI `/v1/chat/completions` requests carrying `[system, user]` are recognized as Turn 1 (`isContinuation = false`) and fingerprinted over `(system + first_user)`, so brand-new threads from the same IDE distribute cleanly across the pool while Turn 2+ (`[system, user, assistant, user_2]`) hashes to the exact same Turn-1 fingerprint and stays pinned.

### 6. Family-Scoped Cooldowns, `VALIDATION_REQUIRED`, 1h OAuth Grace & One-Click Rotation

- **Model-Family Cooldown Isolation**: Rate-limit (`429`) and overload (`529`) cooldowns are tracked independently across 3 cooldown families: `gemini`, `claude` (covering both `claude-*` and `gpt-*` 3P models), and `argon` (`gemini-4-argon`). Hitting a `429` on `Claude Opus 5.5` cools down only that account's 3P (`claude`) family while keeping `Gemini 4 Neon` / `Carbon` `100%` routable.
  - `429`: Honors upstream `Retry-After` (capped at `6h`) or exponential strikes (`30s -> 60s -> 2m -> 5m -> 10m`).
  - `529`: Scoped `15s -> 30s -> 60s` backoff (never shortens an existing longer `429` cooldown).
  - **1h OAuth Refresh Transient `401` Grace**: Mid-session `401`/`UNAUTHENTICATED` glitches during `agy`'s hourly OAuth refresh race get `1` short in-turn retry, and the first transient `401` on a healthy account enters a `30s` grace cooldown + async `/usage` re-probe (escalating to `30m` `auth_error` quarantine only on a second consecutive failure or permanent `403 (#3501)` auth error, without tripping the global ToS circuit breaker).
  - **`VALIDATION_REQUIRED` ("Verify your account") First-Class State**: Upstream Google account verification challenges (`403 VALIDATION_REQUIRED`) are classified as a dedicated `verification_required` account state (never confused with `tos_tripped`), safely extracting the Google verification link and exposing one-click **Verify** + **Recheck** (`health_check`) actions on `:18891` banners and account cards.
  - Non-upstream probes (`count_tokens`, `:countTokens`, `GET /v1/models`) are health-isolated (`skipHealth`) so local `200 OK` responses never clear active upstream cooldowns.
- **One-Click WebUI / API Actions (`POST /api/accounts/action`)**:
  - **Rotate Next (`rotate_next`)**: Advances `activeAccountId` to the next healthy authenticated account (primary-first) in the pool or group.
  - **Drain & Rotate (`drain_and_rotate`)**: Marks the current account as **`draining`** (stops accepting new Turn-1 sessions while letting active U8 sticky conversations finish cleanly) and promotes the next healthy account atomically.
  - **Verify & Recheck (`health_check`)**: Re-probes an account immediately after completing Google verification (`VALIDATION_REQUIRED`) or credential refresh to restore it to healthy routing.

---

<a id="models"></a>

## Supported Models, Quota Pools & Thinking Tiers

### Canonical Chat Model Matrix (11 Routable Chat Models, `agy 1.5.14+`)

| Canonical Model ID | Family | Quota Pool | Context Window | Max Images | Thinking Tiers | Built-in Aliases |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`gemini-4-carbon`** *(default)* | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `carbon` |
| **`gemini-4-neon`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high`, **`xhigh`** *(native)* | `neon` |
| **`gemini-4-argon`** | `gemini` | Gemini / Argon Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `argon` |
| **`gemini-4-hydrogen`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high`, **`xhigh`** *(native)* | `hydrogen` |
| **`gemini-3.8-flash`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `gemini-flash`, `gemini-2.5-flash*`, `gemini-3-flash*`, `gemini-3.0-flash*` |
| **`claude-opus-5-5`** | `claude` | 3P (`Claude`/GPT) Pool | `1,048,576` (`900K` safe) | `100` | `low`, `medium`, `high` (`xhigh` -> `high`) | `agy-opus` *(+ `*opus*` with `AGY_ROUTE_CLAUDE_NAMES=1`)* |
| **`claude-sonnet-5-5`** | `claude` | 3P (`Claude`/GPT) Pool | `1,048,576` (`900K` safe) | `100` | `low`, `medium`, `high` (`xhigh` -> `high`) | `agy-sonnet` *(+ `*sonnet*` with `AGY_ROUTE_CLAUDE_NAMES=1`)* |
| **`gpt-oss-120b-medium`** | `gpt` | 3P (`Claude`/GPT) Pool | `131,072` (`128K`) | `100` | **Fixed `medium`** | `gpt-oss`, `gpt-oss-120b` |
| **`gemini-pro-orchestrator`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | **Fixed `high`** | `gemini-pro-orchestrator*` |
| **`gemini-pro-orchestrator-neon`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | **Fixed `high`** | `gemini-pro-orchestrator-neon*` |
| **`gemini-pro-orchestrator-argon`** | `gemini` | Gemini Pool | `1,048,576` (`900K` safe) | `200` | **Fixed `high`** | `gemini-pro-orchestrator-g1-pro` |

### Image Generation Model (OpenAI Images API)

| Model ID | Endpoints | Engine & Isolation | Supported Features |
| :--- | :--- | :--- | :--- |
| **`gemini-3.1-flash-image`** | `POST /v1/images/generations`, `POST /v1/images/edits` | Isolated `agy acp --agent ws-imagegen` (outer turn `gemini-3.8-flash` `low`, `0` MCP servers, `0` monitor calls, `60s` idle stop) | `b64_json`, `n=1..4`, `aspect_ratio` (`1:1`, `2:3`, `3:2`, `3:4`, `4:3`, `9:16`, `16:9`) or OpenAI `size`, up to `3` ref images (`PNG`/`JPEG`/`WEBP`/`GIF`/`HEIC`/`HEIF`), `X-Agy-Served-Model: gemini-3.1-flash-image` |

- **Automatic Pro vs. Ultra `20x` Quota Cap Detection**: Inspects `<agy> -p /usage` bucket topology and `/credits` `upgrade_uri` to render exact pool caps: **Google AI Pro** (`$10` 5h / `$35` Weekly Gemini + `$2.50` / `$5` 3P + dedicated Weekly Argon pool) vs. **Google AI Ultra `20x`** (`$200` 5h / `$800` Weekly Gemini + `$25` / `$50` 3P).
- **Fixed-Tier Protection & Smart Alias Normalization**: Fixed-tier models (`gpt-oss-120b-medium`, `gemini-pro-orchestrator*`) automatically skip `thought_level` RPC overrides to prevent `-32602` rejections (orchestrator variant entitlement depends on account tier). All lookups strip `models/`, `[1m]`, and tier suffixes (`-minimal`, `-low`, `-medium`, `-high`, `-xhigh`, `-max`, `-ultra`). Set `AGY_ROUTE_CLAUDE_NAMES=1` to route generic `*opus*` / `*sonnet*` / `*haiku*` names automatically, and `AGY_EXPOSE_TIER_MODELS=1` to list explicit tier-suffixed model IDs in `/v1/models` and `/v1beta/models`.

### Cross-Protocol Thinking Tier Mapping

Explicit body fields take precedence over model-name suffixes (`<model>-high`, `<model>-xhigh`); default tier is `medium`:

| Gateway Tier | Model Suffix | OpenAI (`reasoning_effort` / `reasoning.effort`) | Anthropic (`thinking.budget_tokens` / `output_config.effort`) | Gemini `/v1beta` (`thinkingLevel` / `thinkingBudget`) |
| :--- | :--- | :--- | :--- | :--- |
| **`low`** | `-minimal`, `-low` | `none`, `minimal`, `low` | `type: "disabled"`, `<= 4096`, or `low` | `NONE`, `MINIMAL`, `LOW` or `0..4096` |
| **`medium`** *(default)* | `-medium` | `medium` *(or omitted)* | `4097 .. 16384` or `medium` | `MEDIUM` or `4097..16384` |
| **`high`** | `-high` | `high` | `16385 .. 32768` or `high` | `HIGH` or `16385..32768` |
| **`xhigh`** *(Neon / Hydrogen)* | `-xhigh`, `-max`, `-ultra` | `xhigh`, `max`, `ultra` | `> 32768` or `xhigh`/`max` | `XHIGH`, `MAX`, `ULTRA` or `> 32768` |

---

<a id="engineering-pillars"></a>

## Core Engineering Pillars, Performance & Shutdown

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

1. **5-Layer Carbon KV Cache Defense & Disconnect Rescue**
   - Verifies native `agy 1.5.14`–`1.5.18+` CCPA `SessionId` propagation (`SPEC_VERSION=5`) alongside Gate 5 binary trajectory `sessionID` stamping so concurrent sessions never collide on empty upstream cache keys.
   - Freezes Turn-1 `system`/`developer` prompts and initial tool declarations across the session lifetime; refreshes deferred MCP tools (`ToolSearch`) in place via `TOOLS_UPDATED_IN_PLACE`.
   - **Prefill Soft-Detach + Singleflight**: Holds long-context (`>=30s`) in-flight prefills alive for `45s` on client disconnect so retries re-attach without `0%` cache resets; pairs with multi-client compaction hot-slot reclamation and cooldown-backed `capacityFallbackTier` recovery.
2. **Dual-Channel MCP Tool Bridge (`ws-bridge`), `tool_result` Image Visibility & Client `WebFetch`**
   - Serves an ephemeral loopback-only Streamable HTTP MCP bridge (`/mcp/{tenantKey}`) with `ws-bridge` customization isolation (`inheritCustomizations: false`) and fail-closed `allowedTools` enforcement, suspending upstream tool execution cleanly and resuming the parked worker on the next HTTP turn.
   - Delivers images returned inside parked MCP `tool_result` payloads natively via ACP `ImageContentBlock`, pre-bounds oversized `tool_result` text with UTF-8-safe 70/30 head+tail slicing (`AGY_MCP_TOOL_RESULT_MAX_BYTES=9600`, avoiding `agy`'s silent `10,240B` mid-truncation), coalesces parallel tool calls from `.db` `PlannerResponse` steps, truncates long MCP tool names within upstream `64`-char limits, and caps empty `0/0` orphan tool relays (`AGY_MAX_TOOL_RELAYS=2`).
   - **Web Tool Handling**: `search_web` executes natively inside `agy` (client `WebSearch` declarations are skipped so the model uses built-in `search_web`). Client-defined `WebFetch` (carrying `input_schema` or `parameters`) passes through natively via the MCP bridge because upstream deprecated `read_url_content` to a no-op, while schema-less server-side `web_fetch_*` and `NotebookEdit` remain skipped.
3. **3-Stage Semantic Cleanroom & Self-Healing WAF Bisection**
   - Deterministically neutralizes client harness metadata (`<system-reminder>`, workspace paths, reserved tool names) via session-scoped `HMAC-SHA256` reversible tokens (`reversible_codec.go`) restored transparently on outbound SSE streams.
   - On unexpected upstream `400` WAF rejections, bisects prompt segments via `gemini-3.8-flash` probes to isolate and persist minimal rewrite rules (`AGY_WAF_AUTOBISECT=true`).
4. **Full-Multimodal Support: Images, Pure-Go PDF Fast-Path, Opt-in Safe Media Relay & Image Generation**
   - **Images & Integrity Checks**: Accepts magic-sniffed **`PNG`, `JPEG` (with lossless Adobe APP14 strip on grayscale JPEGs fixing upstream `400 INVALID_ARGUMENT`), `WEBP`, `GIF`, `HEIC`, and `HEIF`** images (`<=10 MiB`/image, `<=32 MiB`/prompt) with structural container integrity validation, bounded per-process remote image cache (`AGY_IMAGE_CACHE_MAX_MB=16`, `15m` TTL), and kernel-level `net.Dialer.Control` private/link-local IP blocking against DNS-rebinding SSRF.
   - **24 UTF-8 Text Formats + Pure-Go PDF Text Fast-Path (`AGY_PDF_TEXT_FASTPATH=1`)**: Supports 24 UTF-8 text/document formats (`<=2 MiB`) plus zero-dependency pure-Go PDF text extraction across **all models** (including Claude/GPT), tool-bearing turns, or when `AGY_MEDIA_RELAY=0`, preserving full MCP tools and Carbon KV cache.
   - **Opt-in Safe Full-Multimodal Media Relay (`AGY_MEDIA_RELAY=1`)**: Runs an isolated, lazily started official `agy acp --agent ws-media` engine inside `<PureApiHome>/media-home` (`antigravity-cli-media`, `tools: [read_file]`, `inheritCustomizations: false`, `0` MCP servers, `0` `sir_flash_lite` calls) guarded by `"toolPermission": "strict"` synchronous pre-open path whitelisting (`allow_once` only, strictly scoped via `EvalSymlinks` to per-request staged files), end-of-turn `.db` usage ingest + full auto-cleanup, global cross-account concurrency semaphore (`AGY_MEDIA_RELAY_MAX_CONCURRENCY=1`), and the upgrader **G5a** binary drift gate. Supports **Gemini native visual/scanned PDF**, **audio** (`wav`, `mp3`, `m4a`, `ogg`, `flac`, `opus`, `aac`, `webm`, `amr`), and **video** (`mp4`, `webm`, `mov`, `3gp`, `m4v`; default `<=20 MiB`, hard ceiling `25 MiB`, rejected pre-decode with HTTP `413`; up to `3` files, max `5`). Scanned PDFs when disabled or audio/video on non-Gemini models return explicit HTTP `400`.
   - **Structured Output & Image Generation**: Unifies JSON Schema output (`output_config.format`, `response_format`, `text.format`, `responseJsonSchema`), enforces strict `/v1/responses` continuation validation (`previous_response_not_found`, `thread_not_found`), and serves `POST /v1/images/{generations,edits}` via isolated `--agent ws-imagegen` (`antigravity-cli-imagegen` app-data-dir).
5. **Resident Memory Governor, Zero-Lock SQLite Rollup, Protocol/Stream Hardening & `<2s` Shutdown**
   - **Resident Memory Governor**: Combines `AGY_GLOBAL_MAX_LIVE_WORKERS` container/host-wide worker capping across all accounts, cross-account LRU hibernation, `AGY_IDLE_WORKER_TTL_SEC=180` (`3m`), and real-time cgroup v2/v1 working-set pressure monitoring (`AGY_MEM_PRESSURE_WARN_PCT=70` / `AGY_MEM_PRESSURE_HIGH_PCT=80`) to hold `30-50` logical resident sessions inside `~1.5-1.8 GiB` RAM with transparent lossless `session/resume`.
   - **Protocol & Stream Hardening**: Enforces split first-chunk (`AGY_PROMPT_FIRST_CHUNK_TIMEOUT_SEC=360`) vs. inter-chunk stream idle (`AGY_PROMPT_STREAM_IDLE_TIMEOUT_SEC=180`) timeouts with cancel resync and blind-EOF abort, immediate rollover on corrupted trajectory steps, ACP wire malformed-RPC & bounded peer string guards, and runaway `\r` loop abort (`AGY_RUNAWAY_CR_MAX_CONSECUTIVE=4096`).
   - **Zero-Lock SQLite & Storage Hygiene**: Parses `agy`'s SQLite B-Tree + `.db-wal` frames in pure Go (`internal/store/sqlite_protobuf.go`) for authoritative `cached`/`thinking`/`output` tokens and `step_type=17` silent-retry sensing; uses bounded `8d`+AllTime rollup (`QueryColdWindowRollup`), 5 `conversations`/`turns` indexes, `2h` startup restore (`7d` lazy), and daily `CompactExpiredData` + WAL `TRUNCATE` (recommended `AGY_CONV_KEEP_DAYS=4` in Docker Compose).
   - **`<2s` Shutdown & Hot-Path Speedups**: Calls `BeginShutdown()` before HTTP draining to unblock idle `HotParked` and pending-sibling MCP calls without disturbing active SSE turns, tears down child gateways and ACP engines concurrently, boots child accounts in bounded-4 parallel batches, keeps idle shared engines warm for `AGY_ENGINE_IDLE_GRACE_SEC=180`, skips redundant `set_config_option` RPCs, caches `IsTosTripped()` atomically (`1s`), and streams via typed `acpUpdatePayload` fast-paths.
6. **Strict Dual-Port Isolation & Warm-Paper Console (`:18891`) Highlights**
   - `:18890` serves **only** AI generation endpoints + `/healthz` (`404` on `/`, `/api/*`, `/metrics`); `:18891` serves **only** master-key-authenticated console/admin/metrics endpoints (`404` on `/v1/*`, `/v1beta/*`, `/mcp/*`).
   - The `//go:embed` warm-paper console (with embedded Lucide icons and zero external CDN calls) provides live Pool Routing Bar controls, Account & Channel Group CRUD with 11-model allowlists and GPT/Claude/Gemini family filters, `VALIDATION_REQUIRED` alert banners with one-click Verify & Recheck (`health_check`), Pro/Ultra `20x` quota gauges, live Safety Monitor 3-zone pills with `/hr` burn rates and Yellow/Red ETA countdowns, quota-burn anomaly top banners & inline pills, per-account `agy` binary version badges, a cgroup-aware RAM budget bar, multi-node federation (`AGY_DASHBOARD_PEERS`), and a 1-click `G1-G5` upgrade approval modal (`verify_deferred` aware).

---

<a id="configuration-reference"></a>

## HTTP Endpoints, Configuration & Prometheus Metrics

### Surface & Port Matrix

| Surface | Method & Path | Authentication | Description |
| :--- | :--- | :--- | :--- |
| **Gateway `:18890`** | `GET /healthz` | None | Container/load-balancer liveness probe (`{"ok":true}`) |
| **Gateway `:18890`** | `GET /v1/models`, `GET /v1beta/models` | API Key (master / bound / `?key=`) | 11-model discovery with window metadata (`max_input_tokens` / `max_tokens`), filtered by caller's account/group allowlist |
| **Gateway `:18890`** | `POST /v1/messages`, `POST /v1/messages/count_tokens` | `x-api-key` or `Bearer` | Anthropic Messages API (SSE + JSON + tools + vision/PDF/media + thinking) |
| **Gateway `:18890`** | `POST /v1/chat/completions` | `Authorization: Bearer` | OpenAI Chat Completions API |
| **Gateway `:18890`** | `POST /v1/responses` | `Authorization: Bearer` | OpenAI Responses API (`Codex` CLI) |
| **Gateway `:18890`** | `POST /v1/images/generations`, `POST /v1/images/edits` | `Authorization: Bearer` or `x-api-key` | OpenAI-compatible image generation & editing (`gemini-3.1-flash-image`, `b64_json`, `n=1..4`, up to `3` ref images) |
| **Gateway `:18890`** | `POST /v1beta/models/{m}:generateContent` / `:streamGenerateContent` / `:countTokens` | `x-goog-api-key`, `Bearer`, or `?key=` | Native Google Gemini REST v1beta API |
| **Console `:18891`** | `GET /`, `GET /api/dashboard`, `GET /api/quota` | **Master Key Only** (Basic/Bearer/`x-api-key`) | Warm-Paper live console, federated instances, and quota snapshots |
| **Console `:18891`** | `GET /metrics` | **Master Key Only** | Prometheus `0.0.4` gateway, scheduler & per-account worker metrics |
| **Console `:18891`** | `GET/POST/DELETE /api/accounts`, `/routing`, `/groups`, `/action`, `/probe-ip`, `/oauth/*` | **Master Key + CSRF Guard** (`X-Agy-Requested-With: dashboard`) | Multi-account sandbox, group, governance routing, rotation, `health_check` & PKCE OAuth APIs |
| **Console `:18891`** | `GET /api/upgrade`, `POST /api/upgrade/approve` | **Master Key + CSRF Guard** | `G1-G5` upstream `agy` upgrade status & 1-click hot-swap approval |

### Environment Variables Reference (`.env`)

Every default below matches the Go gateway (`go-gateway/internal/config/snapshot.go`, `go-gateway/session_runner.go`, `go-gateway/mcp_bridge.go`) and host upgrader (`deploy/prod-upgrader/agy_prod_upgrader.py`) 1:1:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `AGY_API_KEYS` | *(empty on loopback)* | Comma-separated master keys for `:18890` & `:18891` (supports `@acct:<id>` / `@group:<id>`) |
| `AGY_LISTEN_HOST` | `127.0.0.1` | Bind host (`0.0.0.0` in `deploy/docker-compose.yml`) |
| `AGY_GW_PORT` / `AGY_DASH_PORT` | `18890` / `18891` | Strict data-plane API port vs. admin/console/metrics port |
| `AGY_ACCOUNTS_FILE` | `<data>/accounts.json` | Multi-account sandboxes, `AccountGroup` pools, standby roles & routing governance config |
| `AGY_PROXY_PRESETS_FILE` / `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN` | *(opt)* / `resin:2260` / *(empty)* | Named egress proxy presets JSON and fail-closed preset proxy host/token |
| `AGY_DASHBOARD_PEERS` | *(optional)* | Federated remote console nodes (`id\|label\|url\|key;...`) |
| `AGY_UPSTREAM_PROXY` | *(empty; `gw.sh`: `http://127.0.0.1:7890`)* | Default HTTP/HTTPS/SOCKS5 upstream proxy for primary `agy` engine |
| `AGY_MAX_LIVE_WORKERS` / `AGY_GLOBAL_MAX_LIVE_WORKERS` | `8` / `0` *(auto; `8` in Compose)* | Primary worker cap & container/host-wide live worker cap across all accounts (`0` = auto from cgroup RAM) |
| `AGY_MAX_CONCURRENT_PROMPTS` | `clamp(workers, 6, 16)` | Adaptive live-resizing concurrent prompt limiter |
| `AGY_IDLE_WORKER_TTL_SEC` / `AGY_ENGINE_IDLE_GRACE_SEC` | `180` / `180` | Idle ACP worker hibernation TTL (`3m`, rehydrates via `session/resume`) & shared engine idle grace (`3m`) |
| `AGY_MEM_PRESSURE_WARN_PCT` / `AGY_MEM_PRESSURE_HIGH_PCT` | `70` / `80` | Real-time cgroup v2/v1 working-set pressure thresholds (%) for proactive cross-account LRU hibernation |
| `AGY_IMAGE_CACHE_MAX_MB` | `16` | Per-process remote image fetch cache ceiling in MiB (`0` disables) |
| `AGY_PROMPT_IDLE_TIMEOUT_SEC` / `AGY_PROMPT_MAX_DURATION_SEC` | `1800` / `43200` | Activity-aware `session/prompt` idle timeout (`30m`) & absolute ceiling (`12h`) |
| `AGY_PROMPT_FIRST_CHUNK_TIMEOUT_SEC` / `AGY_PROMPT_STREAM_IDLE_TIMEOUT_SEC` | `360` / `180` | Split streaming timeouts: first-chunk prefill wait (`6m`) & post-first-chunk inter-chunk idle (`3m`) |
| `AGY_MCP_TOOL_RESULT_MAX_BYTES` / `AGY_RUNAWAY_CR_MAX_CONSECUTIVE` | `9600` / `4096` | UTF-8 70/30 MCP `tool_result` pre-bounding ceiling (`<10,240B` upstream truncation) & `\r` loop guard (`0` disables) |
| `AGY_PARKED_TTL_SEC` / `AGY_MAX_TOOL_RELAYS` / `AGY_TOOL_RELAY_SEC` | `300` / `2` / `55` *(adaptive)* | Parked MCP tool resume TTL (`5m`), orphan `0/0` relay hop cap (`2`), & adaptive keep-alive interval (`55s`-`150s` unless pinned) |
| `AGY_PDF_TEXT_FASTPATH` / `AGY_MEDIA_RELAY` | `1` / `0` | Pure-Go PDF text extraction (`1`, all models) & opt-in isolated `ws-media` Gemini PDF/audio/video relay (`1` enables) |
| `AGY_MEDIA_RELAY_MAX_BYTES` / `MAX_FILES` / `MAX_CONCURRENCY` | `20971520` / `3` / `1` | Media Relay per-file cap (`20 MiB`, ceiling `25 MiB`, pre-decode `413`), file cap (max `5`), & global concurrency semaphore |
| `AGY_MONITOR_TRIP_ROUTING` / `AGY_MONITOR_TRIP_HOP` / `AGY_MONITOR_BUDGET_SPREAD` | `true` / `true` / `true` | Safety Monitor Governor v3: trip rerouting, sticky hop + parked cancel, Green spread, Smart Pacing, & hot UI presets |
| `AGY_MONITOR_SOFT_CAP_ULTRA` / `DRAIN` / `HARD` | `3300` / `4250` / `4800` | Ultra `20x` 5h monitor budget `balanced` (88%) defaults (`Green` / `Yellow` / `Red`, hot-tunable in `:18891` & auto-calibrated on 429) |
| `AGY_MONITOR_SOFT_CAP_PRO` / `DRAIN` / `HARD` | `2100` / `2900` / `3400` | AI Pro & Ultra 5x 5h monitor budget `balanced` (88%) defaults (`Green` / `Yellow` / `Red`, hot-tunable in `:18891` & auto-calibrated on 429) |
| `AGY_ACCOUNT_PLAN_TIER_OVERRIDE` | *(empty = auto)* | Optional manual quota tier override (`pro` \| `ultra_5x` \| `ultra`; also configurable persistently per account in `:18891`) |
| `AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC` / `AGY_PREFILL_CANCEL_GRACE_SEC` | `30` / `45` | Prefill Soft-Detach threshold (`30s`) & disconnect grace window (`45s`) |
| `AGY_CAPACITY_FALLBACK_TTL_SEC` / `AGY_CAPACITY_FALLBACK_MAX_TTL_SEC` | `300` / `1800` | Cooldown TTL & exponential backoff cap before probing recovery from automatic lower-tier fallback (`0` = sticky) |
| `AGY_WAF_AUTOBISECT` | `true` | Automatic Flash WAF bisection & cleanroom rule learning |
| `AGY_CONV_KEEP_DAYS` / `AGY_ACP_LOG_KEEP_DAYS` / `AGY_ACP_LOG_MAX_MB` | `0` (`4` Compose) / `0` (`7` Compose) / `64` | Conversation & `acp-worker` log retention days (`4` / `7` recommended in Docker Compose) & per-account log cap (`64 MiB`, `>=24h` floor) |
| `AGY_VERIFY_DEFER_BACKOFF_SECS` | `900,1800,3600` | Host `prod-upgrader` exponential backoff schedule (`15m`/`30m`/`60m`) for `verify_deferred` quota retries |
| `AGY_BLOCKED_VERSIONS` | `1.5.17,1.5.18` | Host `prod-upgrader` known-regression blocklist (skips `agy#1890` & `agy#1872` pre-download and on apply; `""` or `none` overrides) |
| `AGY_SKIP_G5_MEDIA_CHECK` | `0` | Upgrader offline 0-quota **G5a** binary check verifying `strict` permission enforcement (`1` = emergency override only) |

*(See [`.env.example`](.env.example) for the complete, annotated template covering all multimodal, media relay, model exposure, memory, and storage overrides.)*

### Key Prometheus Metrics (`GET :18891/metrics`)

- **Gateway & KV Cache**: `agy_gateway_store_ready`, `agy_gateway_live_workers`, `agy_gateway_max_live_workers`, `agy_gateway_hibernated_sessions`, `agy_gateway_parked_calls`, `agy_gateway_prompts_inflight`, `agy_gateway_max_concurrent_prompts`, `agy_gateway_requests_total`, `agy_gateway_cache_burn_warnings_total`, `agy_gateway_capacity_fallback_active_sessions`, `agy_gateway_capacity_fallback_events_total`, `agy_gateway_capacity_fallback_probes_total`, `agy_gateway_soft_detach_total{outcome}`, `agy_gateway_upgrade_state{state}`, `agy_gateway_turns_total{model,mode,stop_reason,usage_source}`, `agy_gateway_tokens_total{model,kind}`.
- **Multi-Account Pool & Federated Workers**: `agy_account_inflight{account,state}`, `agy_account_sticky_sessions{account}`, `agy_account_cooldown_seconds{account}`, `agy_account_requests_total{account}`, `agy_account_failures_total{account}`, `agy_account_rate_limits_total{account}`, `agy_account_failovers_total{account}`, `agy_account_worker_up{account}`, `agy_account_live_workers{account}`, `agy_account_max_live_workers{account}`, `agy_account_hibernated_sessions{account}`, `agy_account_prompts_inflight{account}`, `agy_account_parked_calls{account}`.

---

<a id="safe-upgrader"></a>

## 5-Gate Safe Upgrader (`G1-G5`) & Zero-Quota Test Suite

Official `agy` updates frequently. Recommended baseline is **`agy 1.5.16`** (upstream `1.5.17`/`1.5.18` suffer from `agy#1890` `4x` `--agent` Gemini quota burn and `agy#1872`). Both local upgrades ([`scripts/agy-upgrade.sh`](scripts/agy-upgrade.sh)) and the optional host-side production agent ([`deploy/prod-upgrader/agy_prod_upgrader.py`](deploy/prod-upgrader/agy_prod_upgrader.py)) enforce a strict **5-Gate (`G1-G5`) pipeline** plus known-regression protection:

- **Known-Regression Blocklist (`AGY_BLOCKED_VERSIONS=1.5.17,1.5.18`)**: Skips blocked versions before download and `G1-G5` (zero quota spent) and refuses them fail-closed in `cmd_apply()`, while leaving `1.5.19+` automatically eligible (set `AGY_BLOCKED_VERSIONS=""` or `none` to override, or pass `<ver>:<reason>` entries).
- **G1 (Multi-Arch Supply-Chain Hash & Version)**: Verifies dual-source `SHA-512` / `SHA-256` digests across `linux_amd64`, `linux_arm64`, and `darwin_arm64`, archive size, and `--version`.
- **G2 (`--acp-only` Binary Patch & Native `SessionId` Audit)**: Applies `900K` ACP context unlock via [`third_party/agy-context-unlock/agy-patch.py`](third_party/agy-context-unlock/agy-patch.py) `--acp-only` (skipping dead TUI Gate 1/2 while retaining all 5 ACP patches, with fail-closed `state=native` self-skip on Gates 3A/3B/4B) plus `disable-internal-checkpoint` and core-invariant trajectory `SessionId` verification via [`tools/agy_extra_patch.py`](tools/agy_extra_patch.py) `--check` (`SPEC_VERSION=5`, supporting `agy 1.5.14`-`1.5.18+`).
- **G5a (Offline 0-Quota Media Strict-Permission Binary Gate)**: Runs [`tools/agy_media_strict_check.py`](tools/agy_media_strict_check.py) (supporting `x86-64`/`arm64` ELF, Mach-O, PE) to verify 5 fail-closed invariants proving the candidate `agy` binary still enforces `"toolPermission": "strict"` synchronously BEFORE hardcoded/default `read_file` grants (`permission_grants_for`, `apply_settings_strict`, `check_single_target_order`, `read_file_required_permissions`, `acp_permission_prompt_path`). Prevents silent security drift in `ws-media` across future `agy` upgrades (`AGY_SKIP_G5_MEDIA_CHECK=1` for emergency override only).
- **G3 (Zero-Quota Offline ACP Contract)**: Runs [`scripts/agy_acp_smoke.py`](scripts/agy_acp_smoke.py) in an isolated home directory to verify JSON-RPC stdio framing and `--version` / `models` contracts.
- **G4 (Isolated Canary, `verify_deferred` Backoff, Hot-Swap & Disk Pruning)**: Runs text + 2-step MCP tool round-trips inside an isolated test instance (`:18892`), selecting enabled Flash-capable accounts by Gemini 5h headroom while skipping monitor-tripped/Red accounts. Atomically flips `data/agy/current` once active turns drain; if the post-switch probe hits pool-wide quota `429`, enters **`verify_deferred`** with exponential backoff (`AGY_VERIFY_DEFER_BACKOFF_SECS=900,1800,3600`, up to `3` deferrals) instead of falsely rolling back a healthy binary, and runs **`prune_installed_agy`** (`keep=2`, protecting `current`, all live multi-account `engine.json` workers, and rollback targets).

```bash
# Check latest upstream agy release (read-only, zero quota)
python3 scripts/upstream_watch.py --official-only
./scripts/agy-upgrade.sh --status

# Run full zero-quota Go race tests + Python upgrader & script tests
cd go-gateway && go test -race -count=1 ./... && cd ..
python3 -m unittest discover -s scripts -p "test_*.py"
python3 -m unittest discover -s deploy/prod-upgrader -p "test_*.py"
```

---

<a id="troubleshooting"></a>

## Troubleshooting & FAQ

<details>
<summary><strong>1. <code>403 permission_error</code> ("model is not allowed for this account / group")</strong></summary>

Your API key is bound to an account or `AccountGroup` whose `allowedModels` restricts that model. Open `:18891` -> **Account Control Center** -> **Edit** on the account or group, click **All Models** (or check any of the 11 canonical models), and save.
</details>

<details>
<summary><strong>2. <code>403 Forbidden</code> when opening the dashboard or API via LAN IP / reverse proxy</strong></summary>

When `AGY_API_KEYS` is empty, `agy-acp-gateway` strictly rejects any request with a non-loopback `Host` header to prevent accidental public exposure. Set `AGY_API_KEYS=sk-...` in `.env` and restart.
</details>

<details>
<summary><strong>3. <code>403 #3501</code> / <code>VALIDATION_REQUIRED</code> ("Verify your account") or <code>auth_error</code> badge</strong></summary>

- **`VALIDATION_REQUIRED` ("Verify your account")**: Surfaced as a dedicated first-class `validation_required` state (never tripping the global ToS circuit breaker). Open `:18891`, click **Verify** on the alert banner or account card to complete Google verification in your browser, then click **Recheck** (`health_check`) to restore routing immediately.
- **Transient 1h OAuth Refresh `401`**: Single mid-session `401`/`UNAUTHENTICATED` glitches retry once in-turn and apply only a `30s` grace cooldown + async `/usage` re-probe.
- **Permanent `403 #3501` / `GEMINI_ENTERPRISE_LICENSE_NOT_VALID`**: Quarantines only that account for `30m` (`auth_error`) without halting other accounts. Re-authorize in `:18891` -> **OAuth** (or click **Clear Cooldown** after refreshing credentials).
</details>

<details>
<summary><strong>4. <code>503 Service Unavailable</code> ("Account bound to this active conversation is unavailable")</strong></summary>

**Decision U8 Anti-Ban Protection** is active: the account serving your current multi-turn conversation was disabled or hit an auth/ToS hard block, and the gateway refuses to replay mid-conversation history onto a different personal OAuth account. Start a **new conversation** in your IDE/client so Turn 1 routes cleanly to a healthy account.
</details>

<details>
<summary><strong>5. <code>400 invalid_request_error</code> or <code>413 Payload Too Large</code> on PDF, audio, or video attachments</strong></summary>

- **Text-based PDFs**: Automatically extracted via zero-dependency pure-Go `AGY_PDF_TEXT_FASTPATH=1` on all models (including Claude/GPT and tool-bearing turns) with full tools and KV cache.
- **Visual/scanned PDFs, audio (`wav/mp3/m4a/ogg/flac/opus/aac/webm/amr`), and video (`mp4/webm/mov/3gp/m4v`)**: Require a **Gemini model (`gemini-*`)**, **no client tools**, and opt-in **`AGY_MEDIA_RELAY=1`** (using isolated `ws-media` with `"toolPermission": "strict"`). Payloads exceeding `AGY_MEDIA_RELAY_MAX_BYTES` (default `20 MiB`, hard ceiling `25 MiB`) fail pre-decode with HTTP `413`; non-Gemini models, tool-bearing visual PDFs, or requests sent while `AGY_MEDIA_RELAY=0` return explicit HTTP `400`.
</details>

<details>
<summary><strong>6. <code>403 Forbidden</code> on <code>POST / DELETE /api/accounts*</code> from custom scripts</strong></summary>

Admin mutations enforce Origin + CSRF protection. When automating via CLI `curl`, omit browser `Origin` and `Sec-Fetch-Site` headers; when calling with a browser/script `Origin`, pass BOTH a same-origin `Origin` matching `Host` AND `-H "X-Agy-Requested-With: dashboard"`.
</details>

<details>
<summary><strong>7. Named proxy preset fails with "missing AGY_RESIN_PROXY_TOKEN"</strong></summary>

Named proxy presets (`AGY_PROXY_PRESETS_FILE`) fail closed so an account configured for proxy isolation never leaks your host's direct IP. Either set `AGY_RESIN_HOST` + `AGY_RESIN_PROXY_TOKEN` in `.env`, or switch the account's proxy mode to **`custom`** (`http://user:pass@host:port` / `socks5://...`) or **`direct`**.
</details>

<details>
<summary><strong>8. Stopping an isolated non-default test instance with <code>scripts/gw.sh</code></strong></summary>

Non-default ports automatically use isolated runtime directories (`runtime/instance-<port>`). Pass the exact same port variables when stopping: `AGY_GW_PORT=18892 AGY_DASH_PORT=18893 ./scripts/gw.sh stop`. Never run `pkill -x agy-acp-900k` on a host serving live traffic.
</details>

---

## Security & Privacy Boundary

- **Official Subprocess Only (Constraint C1)**: Zero spoofed Google OAuth or internal Protobuf HTTP requests; all upstream model and image traffic executes via official `agy acp`.
- **Zero-Secret Persistence & Redaction**: Logs automatically scrub Bearer tokens, `?key=` query strings, and proxy credentials (including username-only tokens); `/api/dashboard` masks account/group `boundKeys` previews, while master-key-only `/api/accounts` masks `customProxy` credentials (`maskProxyURL`).
- **No Telemetry Phone-Home & Strict Admin Isolation**: Zero external analytics; the warm-paper console and Lucide icon set are `100%` embedded inside the Go binary (`//go:embed`), and `AGY_CORS_ORIGINS` is strictly scoped to `:18890` data-plane routes (`/v1/*`, `/v1beta/*`), never `:18891` admin endpoints.

---

<a id="changelog"></a>

## Release History

- **`v2.3.24` (2026-11-27): Monitor Governor v3 (Hot Presets, Smart Pacing, Cache-Safe Soft-Red, Shared-Account Toggle) & Ultra 5x Tier (PR #10)**:
  - Upgraded Monitor Governor defaults to **`balanced` (88%)** (`3300/4250/4800` Ultra, `2100/2900/3400` Pro / Ultra 5x) with live `:18891` presets (`conservative` 80% / `balanced` 88% / `edge` 93% / `custom`) and zero-restart propagation to child workers (`POST /api/internal/monitor-policy`).
  - Added **Smart Pacing** (`monitorSmartPacing=true` default): dynamically relaxes effective soft/drain thresholds near the 5h reset (with a `+20m` reverse-drift safety pad when `liteResetAt` is unknown) based on burn rate and remaining window time, exposed via `effectiveSoftCap`/`effectiveDrainCap`/`smartPacingActive` and inline monitor bar markers.
  - Made Soft-Red non-destructive by default (`monitorSoftRedHop=false`): pauses new tool sessions while keeping active U8 sticky conversations pinned (protecting 99% Prompt Cache; forcible hop triggers only on real 429 `Tripped` or emergency ceiling with a same-tier winning healthy peer), keeps Soft-Red Primary accounts below `emergencyCap` ahead of Standby accounts when `monitorSpillOnYellow=false`, and sorts Soft-Red fallback by highest remaining headroom.
  - Added per-account **`ignoreBurnAnomaly`** (`Shared Account` toggle + neutral badge in `:18891`) to suppress expected multi-device quota-multiplier banners, and fixed expired stale `/usage` rollover polls poisoning `logCalls5h` in `monitor_budget.go`.
  - Integrated community PR #10 (`@HamsteRider-m`): added **Google AI Ultra 5x (`ultra_5x`)** tier support (preserving real quota fractions without fabricating 20x dollar caps and applying conservative Pro monitor defaults) plus persistent per-account **`planTierOverride`** (`AGY_ACCOUNT_PLAN_TIER_OVERRIDE` and `:18891` Quota Tier selector) that is never overwritten by `/usage`, `/credits`, `agy models`, or federated probes.
- **`v2.3.23` (2026-11-27): Opt-in Safe Full-Multimodal Media Relay (`ws-media`), Pure-Go PDF Text Fast-Path & Upgrader G5a Gate**:
  - Added zero-dependency pure-Go PDF text extraction (`AGY_PDF_TEXT_FASTPATH=1`, default enabled) serving text-based PDFs across all models including Claude/GPT and tool-bearing turns with full MCP tools and Carbon KV cache.
  - Added opt-in **`AGY_MEDIA_RELAY=1`** isolated lazy `--agent ws-media` engine inside `<PureApiHome>/media-home` (`antigravity-cli-media`, `tools: [read_file]`, `0` MCP servers, `0` `sir_flash_lite` calls) with `"toolPermission": "strict"` synchronous pre-open path whitelisting (`allow_once` only), end-of-turn `.db` usage ingest + full staging/media-home auto-cleanup, global cross-account concurrency semaphore (`AGY_MEDIA_RELAY_MAX_CONCURRENCY=1`), and pre-decode HTTP `413` size enforcement (`AGY_MEDIA_RELAY_MAX_BYTES=20971520`, hard ceiling `25 MiB`; `AGY_MEDIA_RELAY_MAX_FILES=3`, max `5`) for Gemini native visual PDF, audio (`wav/mp3/m4a/ogg/flac/opus/aac/webm/amr`), and video (`mp4/webm/mov/3gp/m4v`).
  - Added offline 0-quota **G5a** binary contract check ([`tools/agy_media_strict_check.py`](tools/agy_media_strict_check.py)) to both `scripts/agy_upgrade.py` and `deploy/prod-upgrader/agy_prod_upgrader.py` verifying 5 fail-closed `toolPermission=strict` invariants before promoting any future `agy` update (`AGY_SKIP_G5_MEDIA_CHECK=0`).
- **`v2.3.22` (2026-11-27): Resident Memory Governor & Protocol/Stream Hardening**:
  - Shipped **Resident Memory Governor** across primary and child accounts: `AGY_GLOBAL_MAX_LIVE_WORKERS` container/host-wide live worker cap (`8` in Docker Compose, `0` = auto from cgroup RAM), cross-account LRU hibernation, `AGY_IDLE_WORKER_TTL_SEC=180` (`3m`, down from `600`/`1200`), real-time cgroup v2/v1 working-set pressure guard (`AGY_MEM_PRESSURE_WARN_PCT=70` / `AGY_MEM_PRESSURE_HIGH_PCT=80`), `AGY_IMAGE_CACHE_MAX_MB=16` (down from `64`), and `AGY_CONV_KEEP_DAYS=4` in `deploy/docker-compose.yml`, keeping `30-50` logical resident sessions inside `~1.5-1.8 GiB` RAM via lossless `session/resume`.
  - Shipped **Protocol & Stream Hardening**: split `AGY_PROMPT_FIRST_CHUNK_TIMEOUT_SEC=360` and `AGY_PROMPT_STREAM_IDLE_TIMEOUT_SEC=180` with cancel resync & blind-EOF abort, UTF-8 70/30 MCP `tool_result` pre-bounding (`AGY_MCP_TOOL_RESULT_MAX_BYTES=9600` under `agy`'s `10,240B` truncation), runaway `\r` output loop guard (`AGY_RUNAWAY_CR_MAX_CONSECUTIVE=4096`), grayscale JPEG Adobe APP14 strip + container integrity validation, corrupted trajectory step immediate rollover, fail-closed `allowedTools`, and ACP wire malformed-RPC/bounded-string hardening.
- **`v2.3.21` (2026-11-27): `VALIDATION_REQUIRED` First-Class State, 1h OAuth Refresh 401 Grace, MCP `tool_result` Image Visibility & Isolated Auxiliary `app-data-dir`s**:
  - Promoted Google account verification challenges (`403 VALIDATION_REQUIRED` / "Verify your account") to a first-class `validation_required` account state with safe verification URL extraction and one-click Verify + Recheck (`health_check`) controls in `:18891` banners and cards.
  - Added 1h OAuth refresh transient `401`/`UNAUTHENTICATED` resilience (`1` short in-turn retry + `30s` scheduler grace cooldown + async `/usage` re-probe before escalating to `30m` `auth_error`).
  - Fixed parked MCP `tool_result` image visibility by delivering nested images natively via ACP `ImageContentBlock`, isolated auxiliary `app-data-dir`s (`antigravity-cli-usage` for `/usage`/`/credits`/`models`, `antigravity-cli-imagegen` for `ws-imagegen`), and isolated `ws-bridge` customizations (`inheritCustomizations: false`).
- **`v2.3.20` (2026-11-26) — OpenAI Image API, 11 Routable Chat Models, HEIC/HEIF & Client `WebFetch`**:
  - Added OpenAI-compatible `POST /v1/images/generations` & `POST /v1/images/edits` backed by a lazily started isolated `agy acp --agent ws-imagegen` engine (`gemini-3.1-flash-image`, `0` MCP servers, `0` monitor calls, `b64_json`, `n=1..4`, `aspect_ratio`/`size` mapping, multipart & JSON editing up to `3` ref images, `O_NOFOLLOW` containment).
  - Expanded chat model catalog from `7 -> 11` routable models: promoted `gpt-oss-120b-medium` (`128K`, fixed `medium`, 3P pool) and added `gemini-pro-orchestrator`, `gemini-pro-orchestrator-neon`, `gemini-pro-orchestrator-argon` (fixed `high`, aliases `gpt-oss`, `gpt-oss-120b`, `gemini-pro-orchestrator-g1-pro`) with automatic fixed-tier `thought_level` skip, `ModelFromEnumOrServed` telemetry attribution, and `:18891` 11-model UI parity.
  - Added ISOBMFF `ftyp` magic sniffing for `HEIC`/`HEIF`, clarified HTTP `400` responses on PDF/audio/video citing `PromptCapabilities{Image:true, Audio:false}`, and restored client `WebFetch` pass-through (with `input_schema`) after upstream deprecated `read_url_content`.
- **`v2.3.19` (2026-11-26) — SQLite `8d` Rollup, `<2s` Shutdown, Green Balancing, Burn Anomaly Detector & Patch Hygiene**:
  - Replaced unbounded `coldSteps` RAM retention with bounded `8d`+AllTime SQLite rollup (`QueryColdWindowRollup`), added 5 `conversations`/`turns` indexes, scoped startup restore to `2h` (`7d` lazy), and scheduled daily `CompactExpiredData` + WAL `TRUNCATE`.
  - Delivered `<2s` graceful shutdown via `BeginShutdown()` + parallel teardown, bounded-4 parallel child boot, `AGY_ENGINE_IDLE_GRACE_SEC=180`, redundant `set_config_option` skip, and typed streaming fast-paths.
  - Enabled Green-zone monitor balancing (`narrowCandidatesByMonitorUtil`), zero-tool reverse routing (`preferWarmNoTools`), Green long-running exec relay tuning (`80s`/`135s`), 5h/weekly quota-burn anomaly detection (`QUOTA_BURN_ANOMALY`), and `:18891` cgroup RAM budget + `agy` version + `/hr` burn-rate/ETA UI upgrades.
  - Added `--acp-only` mode & fail-closed `state=native` self-skip to `agy-patch.py`, `prune_installed_agy` (`keep=2`) in `prod-upgrader`, and `AGY_ACP_LOG_MAX_MB=64` (`>=24h` floor).
- **`v2.3.18` (2026-11-26) — Monitor Windows v2, Standby/Tiering Governance, `135s`/`150s` Relay & `agy#1890` Guard**:
  - Extended `monitor-trips.json` to `Version: 2` with `liteResetAt` drift anchoring, eliminating post-restart false-red lockouts and `~20m` window drift.
  - Added `role: primary|standby` plus hot-reloaded `priorityTiering`, `monitorSpillOnYellow`, and runtime `monitorBudgetSpread` toggles in `:18891` and `/api/accounts/routing`.
  - Widened Yellow/Red parked MCP relays to `135s` base / `150s` cap (`<180s` upstream ceiling) and added `AGY_BLOCKED_VERSIONS=1.5.17,1.5.18` in `prod-upgrader` to block `agy#1890`/`agy#1872`.
- **`v2.3.17` (2026-11-26) — Safety Monitor Governor v2, Hop Cancel, Egress Probe & `verify_deferred`**:
  - Shipped 3-zone soft-budget monitor governance (`v2.3.10`/`v2.3.15`/`v2.3.17`), zero-tool trip exemption, cross-worker `MONITOR_TRIP_HOP` parked-session cancellation (`/api/internal/cancel-parked-session`), `AGY_MAX_TOOL_RELAYS=2`, and `:18891` monitor progress bars/countdowns.
  - Added `/usage` egress probing against `oauth2.googleapis.com` (`v2.3.16`/`v2.3.17`) and multi-account `G3/G4` probing + `verify_deferred` exponential backoff (`AGY_VERIFY_DEFER_BACKOFF_SECS=900,1800,3600`).
- **`v2.3.13` – `v2.3.14` (2026-11-26) — Live-Resizable Prompt Limiter, Non-Blocking Scans & `agy 1.5.17+` Patch Profile**:
  - Added FIFO `dynamicPromptLimiter` scaling live with `maxLiveWorkers`, aborted-sibling MCP drain on compaction, streaming `64KB` background `acp-worker` log scanning, and `SPEC_VERSION=5` native `SessionId` verification.
- **`v2.3.5` – `v2.3.12` (2026-11-24 – 2026-11-25) — Production Audit Hardening, Cleanroom PR #4 & Pro Tier Detection**:
  - Added activity-aware `AGY_PROMPT_IDLE_TIMEOUT_SEC`/`AGY_PROMPT_MAX_DURATION_SEC`, `64`-char MCP tool truncation, subagent sticky isolation (`:ag:<id>`), federated per-account `/metrics`, opt-in log/conv retention (`AGY_ACP_LOG_KEEP_DAYS`/`AGY_CONV_KEEP_DAYS`), Cleanroom PR #4 whitespace preservation, `/credits` Pro vs. Ultra detection, and quota calibration for the then-active 7 models.
- **`v2.3.1` – `v2.3.4` (2026-11-23) — PR #1 Isolated Quota `HOME`, Compaction Slot Reclaim & Capacity Fallback Recovery**:
  - Integrated PR #1 (`@HamsteRider-m`) isolated quota `HOME` + macOS headless file-token enforcement, strictly non-blocking cold quota refresh, multi-client compaction hot-slot reclamation, `Pi`/`dsh` resilience, and cooldown-backed `capacityFallbackTier` recovery.

<details>
<summary><strong>Earlier Releases (v2.3.0 and 2026-11-08 to 2026-11-22)</strong></summary>

- **`v2.3.0` (2026-11-22)**: Launched Multi-Account Pool v3 (5 routing strategies, `AccountGroup` channel pools, `@acct:`/`@group:` routing, Turn-1 pre-stream failover, Decision U8 sticky guard, family-scoped cooldowns, `rotate_next`/`drain_and_rotate`) and `agy 1.5.14` native `SessionId` verification.
- **`v2.2.0` (2026-11-20)**: Added single-container isolated multi-account sandboxes, headless PTY PKCE WebUI OAuth, Pro vs. Ultra `20x` quota caps, federated console (`AGY_DASHBOARD_PEERS`), and SQLite `step_type=17` silent-retry sensing.
- **2026-11-18 – 2026-11-19**: Added `gemini-4-neon` (`xhigh`), hot-path zero-copy pools, 7-package acyclic `go-gateway/internal/` DAG refactor, SQLite WAL read/write separation, frozen Turn-1 prefixes, and Desktop `AGY_APP` read-only telemetry.
- **2026-11-08 – 2026-11-17**: Completed pure-Go (`CGO_ENABLED=0`) rewrite, zero-lock `.db`/`.1.19` protobuf telemetry, `PlannerResponse` sibling tool batching, `G1-G4` safe upgrader, `HMAC-SHA256` reversible cleanroom + Flash WAF auto-bisect, Gemini `/v1beta` adapter, Multimodal v2 with SSRF guards, Gate 5 `900K` unlock, and Prefill Soft-Detach + Singleflight.

</details>

---

## License

Released under the [MIT License](LICENSE).
