<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/banner-zh-dark.svg" />
    <img src="assets/banner-zh-light.svg" alt="agy-acp-gateway" width="760" />
  </picture>
</p>

<h3 align="center">基于 Google 官方 Antigravity CLI (<code>agy acp</code>) 的零 CGO 四协议 AI 网关与 OpenAI 文生图引擎：11 个对话模型 + 图像生成编辑、多账号池 v3、安全监测治理器 v2、Carbon KV 缓存防御与暖纸实时控制台。</h3>

<p align="center">
  <a href="./README.md"><strong>English</strong></a> &nbsp;·&nbsp;
  <a href="./README_ZH.md"><strong>简体中文</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>5 分钟快速上手</strong></a> &nbsp;·&nbsp;
  <a href="#client-setup"><strong>客户端配置</strong></a> &nbsp;·&nbsp;
  <a href="#multi-account"><strong>多账号池与治理器</strong></a> &nbsp;·&nbsp;
  <a href="#models"><strong>模型与思考档位</strong></a> &nbsp;·&nbsp;
  <a href="#engineering-pillars"><strong>核心架构</strong></a> &nbsp;·&nbsp;
  <a href="#configuration-reference"><strong>配置与指标</strong></a> &nbsp;·&nbsp;
  <a href="#safe-upgrader"><strong>安全升级</strong></a> &nbsp;·&nbsp;
  <a href="#troubleshooting"><strong>常见问题排障</strong></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26%2B_(CGO__ENABLED%3D0)-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+ Zero CGO" />
  <img src="https://img.shields.io/badge/Protocols-Anthropic_%7C_OpenAI_%7C_Responses_%7C_Gemini_v1beta_%7C_Images-0856F5?style=flat-square" alt="四协议与图像接口" />
  <img src="https://img.shields.io/badge/Models-11_Chat_%2B_gemini--3.1--flash--image-7C3AED?style=flat-square" alt="11 个对话模型 + 图像生成" />
  <img src="https://img.shields.io/badge/Multi--Account-Pool_v3_%2B_Governor_v2-167c52?style=flat-square" alt="多账号池 v3 与安全监测治理器 v2" />
  <img src="https://img.shields.io/badge/Context-900K_Unlocked-0D9488?style=flat-square" alt="900K 上下文解锁" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-22c55e?style=flat-square" alt="MIT License" /></a>
</p>

---

**`agy-acp-gateway`** 将 Google 官方 Antigravity CLI（`agy acp`，基于 stdio 的 JSON-RPC 2.0 协议）封装为生产级纯 Go（`CGO_ENABLED=0`）单二进制多协议 AI 网关与实时观测控制中心。

不同于逆向抓包伪造私有 OAuth / Protobuf HTTP 请求的第三方中转方案，**本项目 `100%` 的上游通信均运行在官方 `agy` 二进制内部**。TLS BoringSSL 握手特征、OAuth 令牌自动续期、轨迹检查点与上游封包全部由 Google 官方客户端原生托管；`agy-acp-gateway` 在此安全基座之上提供：

- **四协议原生对话入口 + OpenAI 图像 API (`:18890`)**：同时支持 Anthropic `/v1/messages`、OpenAI `/v1/chat/completions`、OpenAI `/v1/responses`、原生 Google Gemini REST `/v1beta`，以及 OpenAI 兼容 `POST /v1/images/generations` 与 `POST /v1/images/edits`（通过延迟拉起的独立官方 `--agent ws-imagegen` 引擎调用 `gemini-3.1-flash-image`，`0` MCP 服务器、`0` 次安全监测调用）。
- **11 个可路由对话模型与多模态 v2**：完整覆盖 Gemini 4 系列（`carbon`、`neon`、`argon`、`hydrogen`）、`gemini-3.8-flash`、`claude-opus-5-5`、`claude-sonnet-5-5`、`gpt-oss-120b-medium` 及 3 款 `gemini-pro-orchestrator` 变体；支持魔数嗅探的 `PNG`/`JPEG`/`WEBP`/`GIF`/`HEIC`/`HEIF` 图片与 24 种 UTF-8 文本/文档格式（`<=2 MiB`），并对 PDF/音频/视频附件返回明确 HTTP `400`（严守上游 `PromptCapabilities{Image:true, Audio:false}`）。
- **多账号池 v3 与安全监测治理器 v2**：单容器物理沙盒隔离（`HOME` + SQLite + 出口代理）、5 种路由策略、`role: primary|standby`、免重启治理热开关（`priorityTiering`、`monitorSpillOnYellow`、`monitorBudgetSpread`）、Decision U8 防封粘性护栏、首轮流前自动容灾、三区软预算外审治理与额度倍率异常检测。
- **五层 KV 缓存防御、`<2s` 优雅停机与暖纸控制台 (`:18891`)**：`agy 1.5.14+` 原生 `SessionId` 核验 + `--acp-only` `900K` 解锁、首轮前缀冻结、Prefill Soft-Detach、有界 SQLite `8d`+全量汇总与每日压缩、`<2s` 优雅停机、Pro/Ultra `20x` 额度自动识别、cgroup 内存预算指示、Prometheus `/metrics` 及带 `AGY_BLOCKED_VERSIONS` 防护的 `G1-G4` 升级代理。

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/arch-overview-zh-dark.png" />
    <img src="assets/arch-overview-zh-light.png" alt="agy-acp-gateway 四协议网关架构图" width="960" />
  </picture>
</p>

---

## 为什么选择 `agy-acp-gateway`？

| 生产痛点 | 简易 ACP 包装器 / 逆向裸发包代理 | `agy-acp-gateway` 工程化方案 |
| :--- | :--- | :--- |
| **协议伪造风控与串号封禁风险** | 伪造内部 OAuth/Protobuf HTTP 请求头，或在多轮对话中途跨账号重放历史 | **`100%` 官方 `agy acp` 引擎 + U8 硬护栏**：零私有接口裸发包；多轮续接严格粘性锁定原账号；账号级独立目录与出口代理隔离 |
| **单账号限流与备用号调度僵硬** | 全局共用单一 `~/.gemini` 目录，遇 `429`/`529` 需手工停机换号 | **多账号池 v3 与渠道分组**：5 种路由策略（`round-robin` / `fill-first` / `weighted-rr` / `least-load` / `manual-active`）、`role: primary\|standby`、`priorityTiering`、首轮容灾、家族冷却、一键**排空并切换** |
| **前置安全监测 `80x` 升级雪崩** | `sir_flash_lite` 静默打满后触发高价全轨迹外审，数分钟烧光 5h 额度池 | **安全监测治理器 v2**：三区软预算（绿/黄/红）、绿区均衡、黄区软排空、无工具反向分流、Hop 挂起联动取消、`monitor-trips.json` v2 窗口持久化与倍率异常告警 |
| **Gemini 4 Carbon 并发 0% 缓存命中** | 多会话共用空轨迹 ID，或在中途修改 `system` / 工具列表导致前缀失效 | **轨迹 `SessionId` 隔离 + 首轮前缀冻结**：独立会话 ID、冻结首轮系统提示词与工具目录，配合 `TOOLS_UPDATED_IN_PLACE` 原地增量更新 |
| **120s+ 超长上下文预填充断连雪崩** | 客户端超时断开即发送 `session/cancel`，白白销毁 150K+ 预填充算力 | **Prefill Soft-Detach (`30s`/`45s`) + Singleflight**：断连后保留上游热算力，客户端重试无缝接回同一请求 |
| **客户端元标签触发上游 400 WAF 误杀** | 直接报错中断，或用粗糙正则误删用户真实指令与代码片段 | **三层语义净室 + 会话 HMAC 可逆编解码 + Flash 自动二分**：无损编码敏感标签/路径，自动二分定位新型 WAF 触发片段 |
| **Token 盲估、静默重试与内存/库膨胀** | 仅靠字符估算用量；长运行后内存暴涨或停机卡死 | **零锁 `.1.19` SQLite 直读 + `8d` 汇总 + `<2s` 停机**：精确提取缓存/思考/输出 Token、`step_type=17` 重试遥测、每日压缩瘦身与 cgroup 内存预算观测 |

---

<a id="quick-start"></a>

## 5 分钟快速上手指南

### 环境前置要求

- **操作系统**：Linux（`x86_64` / `arm64`）、macOS（`arm64` Apple Silicon），或 Windows 下的 **WSL2 Ubuntu**。
- **官方 Antigravity CLI (`agy`)**：已安装于 `~/.local/bin/agy`（**推荐基线 `1.5.16`**；`1.5.17`/`1.5.18` 存在上游 `agy#1890` `--agent` 4 倍 Gemini 额度消耗回归及 `agy#1872`；网关从不直接修改全局官方文件）。
- **编译环境** *(从源码构建时)*：Go `1.26+` 与 Python `3.10+`。

---

### 方式 A：Docker Compose 部署（推荐服务器与多账号池使用）

```bash
# 1. 克隆仓库
git clone https://github.com/yuhxuiao/agy-acp-gateway.git
cd agy-acp-gateway

# 2. 编译纯静态零 CGO 网关二进制，并生成 --acp-only 补丁版 ./bin/agy-acp-900k
cd go-gateway && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway . && cd ..
# 先在隔离沙盒生成并验证候选目录，再传入上一步输出的 <candidate_dir> 原子安装
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. 配置环境变量并启动容器
cp .env.example deploy/.env
# 编辑 deploy/.env -> 设置强随机密钥 AGY_API_KEYS
# （deploy/docker-compose.yml 已内置固定容器监听 AGY_LISTEN_HOST=0.0.0.0，
#  并默认设置 AGY_CONV_KEEP_DAYS=14 与 AGY_ACP_LOG_KEEP_DAYS=7；
#  无可用代理时保持 AGY_UPSTREAM_PROXY 注释状态即可）
docker compose -f deploy/docker-compose.yml up -d --build

# 4. 验证健康检查
curl -fsS http://127.0.0.1:18890/healthz
# -> {"ok":true}
```

> **多账号容器内存规划提示**：[`deploy/docker-compose.yml`](deploy/docker-compose.yml) 默认配置 `mem_limit: 3200m` 与 `stop_grace_period: 25s`。每个活跃子账号会启动一个轻量级子网关进程（`AGY_CHILD_GATEWAY_GOMEMLIMIT=128MiB`）及 `1..N` 个官方 `agy acp` 进程（热态约 `250-350MB` RSS）。建议按每新增 1 个并发活跃账号预留 `+512MB ~ +1GB` 容器内存上限；`:18891` 控制台亦内置实时 **cgroup 内存预算指示条**（`totalMaxLiveWorkers` / `estPeakRssMB` / `hostMemLimitMB`）。

---

### 方式 B：Linux / macOS / WSL2 原生脚本启动 (`./scripts/gw.sh`)

```bash
# 1. 运行竞态检测单测并编译零 CGO 二进制
cd go-gateway
gofmt -l . && go vet ./... && go test -race -count=1 ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway .
cd ..

# 2. 基于官方 agy 生成并通过离线契约校验 ./bin/agy-acp-900k（不改动 ~/.local/bin/agy）
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. 初始化配置并启动本机服务
cp .env.example .env
# 编辑 .env -> 配置 AGY_API_KEYS=sk-your-master-key 与 AGY_UPSTREAM_PROXY（直连则设 AGY_UPSTREAM_PROXY=）
./scripts/gw.sh start
./scripts/gw.sh status
```

---

### 第 3 步：完成 Google 账号 OAuth 授权引导（3 种方式任选）

在浏览器打开 **暖纸实时控制台** `http://127.0.0.1:18891/`（HTTP Basic Auth 用户名任意填写，密码输入 `.env` 中的 `AGY_API_KEYS` 主密钥），点击顶栏 **账号控制中心**：

1. **方式一：网页免 SSH PKCE OAuth 授权（适用于 Linux / WSL2 / Docker，推荐无头服务器与全部子账号使用）**
   - *（注：原生 `macOS arm64` 无 Linux `/dev/ptmx` 伪终端，macOS 原生环境请使用方式二离线导入 Token JSON 或方式三命令行授权。）*
   - 点击 **新增子账号**（或在已有账号卡片点 **授权**），选择出口代理模式（`direct` 宿主机直连、命名代理预设、或自定义 `http://` / `https://` / `socks5://` / `socks5h://` 代理）并保存。
   - 点击 **第 1 步：生成 Google OAuth 授权链接**。网关会在该账号专属沙盒目录与代理环境下通过 `/dev/ptmx` 伪终端启动 `agy login`（`70s` 超时自动回收，并带 `.oauth-bak` 失败回滚保护）。
   - 将生成的 `https://accounts.google.com/...` 链接复制到**任意本地浏览器**完成登录；当浏览器跳转到 `http://localhost:.../?code=4/0A...`（即使页面提示无法访问此网站也完全正常），复制完整地址栏 URL 或 `code` 参数粘贴回 **第 2 步**，点击 **提交授权码激活**。
   - 网关会自动落盘凭据、调用官方 `agy models` 探测账号订阅档位（AI Pro / Ultra `20x`）与可用模型，并拉起独立子进程。
2. **方式二：离线导入 Token JSON**
   - 在同一授权面板中，直接粘贴已有的 `antigravity-oauth-token` JSON 原文并点击 **直接写入 Token JSON**。
3. **方式三：宿主机/容器命令行交互登录 *(仅主账号 `primary`)***
   - 本机：`./bin/agy-acp-900k login`
   - 容器：`docker exec -it agy-acp-gateway /app/bin/agy-acp-900k login`

---

### 第 4 步：发送首条测试请求验证连通

```bash
curl -sS http://127.0.0.1:18890/v1/chat/completions \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.8-flash",
    "reasoning_effort": "low",
    "messages": [{"role": "user", "content": "请只回复 READY 一个词。"}]
  }'
```

---

<a id="client-setup"></a>

## 主流客户端接入配置（复制即用）

| 客户端 / SDK | 协议入口 | Base URL | 鉴权头 / 环境变量 | 会话粘性与工具桥接机制 |
| :--- | :--- | :--- | :--- | :--- |
| **`Claude Code`** | Anthropic `/v1/messages` | `http://127.0.0.1:18890` | `x-api-key` / `ANTHROPIC_API_KEY` | `metadata.user_id` + `ws-bridge` MCP + 客户端 `WebFetch` |
| **`Codex` CLI** | OpenAI `/v1/responses` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | `prompt_cache_key` / 首轮指纹 + 无状态全量历史 + MCP |
| **Cursor / Continue / `Cline` / `Roo Code` / `OpenCode`** | OpenAI `/v1/chat/completions` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | 首轮 `(system + first_user)` 指纹 + 并行工具合并 |
| **Gemini CLI / `google-genai` SDK** | Gemini `/v1beta` | `http://127.0.0.1:18890` | `x-goog-api-key` / `?key=` | 原生 `functionCall` / `thoughtSignature` + MCP |
| **Pi Coding Agent** | `/v1/messages` 或 `/v1/responses` | `http://127.0.0.1:18890` | `x-api-key` / `Bearer` | `x-pi-session-id` + `TOOLS_UPDATED_IN_PLACE` |
| **OpenAI 图像客户端 / SDK** | OpenAI `/v1/images/{generations,edits}` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` / `x-api-key` | 独立 `--agent ws-imagegen` 引擎（`gemini-3.1-flash-image`） |

### 1. `Claude Code` (`/v1/messages`)

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
# 进阶技巧：可在主 Key 后追加 @acct:<账号ID> 或 @group:<分组ID> 将当前窗口绑定至指定账号或渠道分组池
export ANTHROPIC_API_KEY="sk-your-master-key"
export ANTHROPIC_MODEL="gemini-4-carbon-medium"
export ANTHROPIC_SMALL_FAST_MODEL="gemini-3.8-flash-low"

claude
```

### 2. `Codex` CLI / OpenAI Responses API (`/v1/responses`)

写入 `~/.codex/config.toml`：

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

或使用 `curl` 直调：

```bash
curl -N http://127.0.0.1:18890/v1/responses \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-4-neon",
    "reasoning": {"effort": "high"},
    "input": "请简要说明 Go sync.Pool 如何降低 GC 压力。",
    "stream": true
  }'
```

### 3. OpenAI SDK / Cursor / `Cline` (`/v1/chat/completions`)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:18890/v1",
    api_key="sk-your-master-key@group:team-pro",  # 支持主 Key、分组专属 Key 或 @group:/@acct: 后缀
)

resp = client.chat.completions.create(
    model="gemini-4-neon",
    reasoning_effort="high",
    messages=[
        {"role": "system", "content": "你是一名严谨的分布式系统架构师。"},
        {"role": "user", "content": "请用三句话说明 Go sync.Pool 的适用边界。"},
    ],
    stream=True,
)
for chunk in resp:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

### 4. Google Gemini CLI 与 `google-genai` SDK (`/v1beta`)

```bash
export GOOGLE_GEMINI_BASE_URL="http://127.0.0.1:18890"
export GEMINI_API_KEY="sk-your-master-key"

curl -sS "http://127.0.0.1:18890/v1beta/models/gemini-4-hydrogen:generateContent" \
  -H "x-goog-api-key: ${GEMINI_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [{"role": "user", "parts": [{"text": "用三条要点总结 Raft 选主流程。"}]}],
    "generationConfig": {"thinkingConfig": {"thinkingLevel": "XHIGH"}}
  }'
```

### 5. OpenAI 兼容文生图与图生图编辑 (`/v1/images/generations`, `/v1/images/edits`)

通过独立的官方延迟拉起引擎 `agy acp --agent ws-imagegen` 执行（仅启用原生 `generate_image` 工具，`0` MCP 服务器、`0` 次安全监测调用；外层对话锁低成本 `gemini-3.8-flash` `low`，底层调用 **`gemini-3.1-flash-image`**，空闲 `60s` 自动休眠，严格 `O_NOFOLLOW` + fd 限长安全读盘与退场自动清理）：

- **参数与输出**：返回 `b64_json`（`response_format: "b64_json"`），支持 `n`（`1..4`）、显式 `aspect_ratio`（`1:1`、`2:3`、`3:2`、`3:4`、`4:3`、`9:16`、`16:9`）或 OpenAI `size`（`1024x1024`、`1536x1024`、`1024x1536`、`1792x1024`、`1024x1792`、`auto` 或自动映射至最接近宽高比），并回传响应头 `X-Agy-Served-Model: gemini-3.1-flash-image`。
- **图生图编辑 (`/v1/images/edits`)**：同时支持 `multipart/form-data`（`image` / `image[]`）与 `application/json`（`image` / `images` 传 base64 或 `data:` URI），最多传入 `3` 张经魔数校验的参考图（`PNG`、`JPEG`、`WEBP`、`GIF`、`HEIC`、`HEIF`）。

```bash
# 文生图 (application/json)
curl -sS http://127.0.0.1:18890/v1/images/generations \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image",
    "prompt": "暖纸风格的四协议 AI 网关等距工程架构插画",
    "size": "1536x1024",
    "n": 1,
    "response_format": "b64_json"
  }'

# 图生图编辑（multipart/form-data，最多 3 张参考图）
curl -sS http://127.0.0.1:18890/v1/images/edits \
  -H "Authorization: Bearer sk-your-master-key" \
  -F "prompt=将草图转为清晰的工程线稿" \
  -F "aspect_ratio=16:9" \
  -F "image[]=@./ref1.png"
```

---

<a id="multi-account"></a>

## 多账号池 v3、安全监测治理器 v2 与防封路由调度

`agy-acp-gateway` 内置面向官方 `agy acp` 有状态进程模型的多账号调度器、沙盒监管器与安全监测治理器，兼顾渠道池灵活分组与个人 OAuth 长期防封安全。

### 1. 单容器物理沙盒、账号级代理与双重出口探测

在 `:18891` 面板添加的每一个子账号（`acct-b`、`acct-c` 等），均拥有独立的数据沙盒（`<data_dir>/accounts/<id>/`）与本地回环子网关进程（`127.0.0.1:28902+`，按 4 并发并行启动）：

- **零状态串染**：独立隔离 `HOME`、`.gemini/antigravity-cli/antigravity-oauth-token`（`0600` 权限）、`workspace`、`gateway.db`、`tos-tripped` 与 `logs/gateway.log`。
- **账号级出口代理**：每个账号可独立配置 `direct`（直连）、命名代理预设（`AGY_PROXY_PRESETS_FILE` + `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN`，缺令牌时自动阻断防漏宿主机真实 IP），或自定义 `http://`、`https://`、`socks5://`、`socks5h://` 代理。
- **双重出口网络探测**：
  - `POST /api/accounts/probe-ip` 检测脱敏出口 IP/地区，并通过官方 `agy models`（已授权时）或 `generate_204` 验证 Google 连通性，绝不在 `agy` 之外裸发 OAuth 令牌。
  - `/usage` 遇交互式登录提示时先经该账号出口代理探测 `oauth2.googleapis.com`，将代理或网络抖动明确标示为琥珀色 `出口网络异常`，杜绝误判为 `tos_tripped` 或 `auth_error`。
- **cgroup 感知内存预算**：自动检测容器/宿主机内存上限，并在 `:18891` 池顶栏与 Worker 槽位编辑器实时展示 `totalMaxLiveWorkers` / `estPeakRssMB` / `hostMemLimitMB`，防止多账号并发槽位超配引发 OOM。

### 2. 五种路由策略、Standby 备用角色与治理热开关

可在控制台首页顶部的 **池路由状态栏 (`#pool-ctrl-bar`)** 一键实时切换，也可为每个 **渠道分组 (`AccountGroup`)** 单独指定策略：

| 策略标识 | 别名 | 调度行为说明 | 推荐场景 |
| :--- | :--- | :--- | :--- |
| **`round-robin`** *(默认)* | `rr` | 在活跃、健康且未冷却的账号间均匀轮询（叠加密集工具绿区水位均衡） | 同档位多账号日常均衡负载 |
| **`fill-first`** | `priority` | 按 `priority`（`1..1000` 越小越优先）排序，同优先级按 `2%` 离散剩余额度桶、并发数与配置顺序裁决 | 优先消耗即将到期或活动号额度，主力号作热备 |
| **`weighted-rr`** | `weighted`, `wrr` | 变动集限幅版 Nginx 平滑加权轮询（`weight` `1..100`，如 Ultra `20x` 设 `10`、Pro 设 `1`），消除突发饥饿 | Google AI Ultra `20x` 与 AI Pro 混合组池 |
| **`least-load`** | `smart`, `quota-aware` | 最小化综合负载分 `inFlight / maxConcurrency - 0.35 * remainingQuota` | 多人团队高并发编码、长短请求混跑 |
| **`manual-active`** | `manual`, `active` | 固定路由至当前激活主用号 `activeAccountId`；`manualAutoFailover: true`（默认）时故障自动降级，设为 `false` 时严格锁死 | 个人开发者确定性控号 + 一键手动切号 |

- **Standby 角色与池级治理热开关 (`POST /api/accounts/routing`，免重启生效)**：
  - **`role: "primary" | "standby"`**：任意账号可标记为 `standby`（`:18891` 显示虚线 `备用` 标签）；主力账号健康时备用账号不承接含工具或无工具流量，仅在溢出或主力不可用时接管。
  - **`priorityTiering`**：在全部 4 种池策略中启用严格按最低 `priority` 分层调度，低优先级层仅在主力层打满或溢出时启用。
  - **`monitorSpillOnYellow`**：主力层进入外审黄区即向备用/低优先级层提前溢出新工具会话（关闭时仅红区或不可用时溢出）。
  - **`monitorBudgetSpread`**：运行期热切换绿区工具均衡分摊与无工具反向分流策略。

### 3. 安全监测治理器 v2（三区软预算与额度倍率异常检测）

上游 `agy` 在每次外部 MCP 工具调用前执行轻量前置安全监测（`sir_flash_lite`）。一旦单账号 5h 监测额度打满触发 `429`，`agy` 会 1:1 升级至高价 `severe_internal_risks_agy`（约 `80x` 成本），数分钟内烧空整个 5h Gemini 额度池。治理器 v2 完全通过被动观测实现零侵入保护：

- **三区预测软预算**：按账号独立追踪 5h 监测调用量并划分为 **绿区**（`< SoftCap`）、**黄区**（`[SoftCap, DrainCap)`）与 **红区**（`>= DrainCap` 或已熔断）。默认阈值：Ultra **`2800 / 3800 / 4800`**、Pro **`1800 / 2600 / 3400`**（`AGY_MONITOR_*_CAP_*`），遇真实 `429` 锁定后按比例自校准。
- **绿区均衡与黄区软排空**：在 `round-robin` / `weighted-rr` 中对绿区候选执行含在途预估的水位均衡（`narrowCandidatesByMonitorUtil`）；账号进入 **黄区** 后自动软排空，既有 U8 粘性会话继续留守原号（零 KV 缓存损失），新工具会话自动转向绿区账号。
- **无工具请求豁免与反向分流**：纯文本无工具请求始终豁免监测熔断，并经 `preferWarmNoTools` 优先反向分流至黄/红区但 Gemini 额度健康的账号，为绿区账号留出最大外审余量。
- **Hop 挂起联动取消与自适应 MCP 保活**：当 `AGY_MONITOR_TRIP_HOP=true` 将粘性工具会话从红区/熔断号切往健康号时，同步取消/休眠原账号挂起会话（本机及跨子账号经管理端口鉴权 `POST /api/internal/cancel-parked-session` 联动）。MCP 挂起保活周期随外审压力自适应拉宽（绿区 `<50%` 保持 `55s`，绿区 `>=50%` 拉宽至 `75s`，绿区长耗时执行工具按 `>=32K`/`<32K` 上下文设为 `80s`/`135s`，黄/红区拉宽至 `135s` 基准、挂起 `>=30m` 封顶 `150s`，安全低于 `agy` 原生 `180s` 硬上限；显式 `AGY_TOOL_RELAY_SEC` / `AGY_TOOL_RELAY_MS` 优先）。
- **监测窗口 v2 持久化与倍率异常检测**：`monitor-trips.json`（`Version: 2`）持久化 `windowKey`、`windowStartAt`、`windowResetAt`、独立 5h `liteResetAt`（配合 `10s` 防回绕环消除与 Gemini `/usage` 窗口错位 `~20m` 的双向漂移）与 `logCalls5h`，杜绝重启后瞬时误红。`detectQuotaBurnAnomaly` 自动对比 5h/周度实扣美元与预期本地+外审消耗，检出 `multiplier_anomaly`（`warn >=2.2x` / `critical >=3.0x`）与 `unattributed_burn`（`10m` 去抖 `QUOTA_BURN_ANOMALY` 日志 + `:18891` 顶部告警与卡片徽章）。

### 4. Key 后缀绑定、渠道分组与请求头路由矩阵

无需重启客户端，即可通过专属 Key、主 Key 后缀或请求头将流量精准导向指定账号或分组池：

| 路由模式 | 客户端传参写法 | `:18891` 管理面权限 | 模型白名单校验 |
| :--- | :--- | :--- | :--- |
| **账号专属 Key (`boundKeys`)** | `Authorization: Bearer sk-acct-only-key` | **拒绝 (`401`)** | 账号 `allowedModels` |
| **分组池专属 Key (`boundKeys`)** | `Authorization: Bearer sk-team-group-key` | **拒绝 (`401`)** | 分组 `allowedModels` ∩ 账号 `allowedModels` |
| **主 Key + 账号后缀** | `<master_key>@acct:<id>` 或 `<master_key>@<id>` *(亦支持 `#`)* | 仅限 API 数据面 | 账号 `allowedModels` |
| **主 Key + 分组后缀** | `<master_key>@group:<group_id>` *(亦支持 `#group:<group_id>`)* | 仅限 API 数据面 | 分组 `allowedModels` ∩ 账号 `allowedModels` |
| **显式路由请求头** | `X-Agy-Account: <id>` 或 `X-Agy-Group: <group_id>` *(配合主 Key)* | 主 Key 权限 | 若 `X-Agy-Account` 不属于 `X-Agy-Group` 直接返回 `403` |
| **全局默认池调度** | `<master_key>` | 主 Key 权限 | 命中账号的 `allowedModels` |

每次响应均回传可观测路由响应头（已配置 CORS 暴露）：
`X-Agy-Account`、`X-Agy-Group`、`X-Agy-Route-Mode`、`X-Agy-Failover-Count`、`X-Agy-Served-Model`、`X-Agy-Cache-Warning`。

### 5. 首轮流前故障转移 vs. Decision U8 防封粘性连续性

```text
  [全新对话第 1 轮请求] (!isContinuation，例如 [system, user_1])
         |
         +--> 选中账号 A --> 响应头写出前遇 429 / 529 / 502 / 503 / 401(#3501)？
         |                        | 是（最多自动跳接 MaxFailoverHops = 2 次）
         |                        v
         |                记录账号 A 家族冷却 + 透明切换至健康账号 B 重放首轮请求
         |                        |
         +--> 账号 B 正常接单 --> 提交粘性绑定 Sticky(callerScope | sessionKey -> B)
                                  + 输出响应头 X-Agy-Account: B, X-Agy-Failover-Count: 1

  [进行中的多轮续接请求] (isContinuation，例如第 2+ 轮 / tool_result / function_call_output)
         |
         +--> Strict U8 防封护栏 (`strictU8NoMidTurnHop = true`)：
              始终锁定在原账号 B（即使 B 处于平滑排空 `draining`、黄区或短暂限流冷却中；
              仅在启用 `AGY_MONITOR_TRIP_HOP` 且 B 进入红区/外审熔断且存在健康同池号时跳接）。
              绝不在常规错误下将半截工具调用重放至其他个人 OAuth 账号（若 B 停用/凭证失效则返回 503）。
```

- **为什么必须坚守 U8 边界**：在多轮 Agent 编码中途将包含数十轮工具调用的历史轨迹切往另一个个人 Google 账号，会瞬间打碎 `100%` 的 TPU KV 缓存并制造跨账号轨迹异常。仅在**全新第 1 轮**执行负载均衡与流前故障转移、而在**第 2 轮及后续回合**严格粘性锁定，兼顾了高可用容灾与账号长期安全。
- **静态 System Prompt 首轮精准识别**：针对无状态 OpenAI `/v1/chat/completions` 客户端常见的 `[system, user]` 首轮请求，`inspectConversationTurns` 准确判定为首轮（`isContinuation = false`），并联合 `(system + 首条 user)` 计算会话指纹，既保证同一 IDE 新开多窗口均匀打散，又保证第 2+ 轮命中完全一致指纹实现无缝粘性续接。

### 6. 按模型家族独立冷却、平滑排空与一键轮转

- **3 家族独立冷却隔离**：限流（`429`）与过载（`529`）按 `gemini`、`claude`（涵盖 `claude-*` 与 `gpt-*` 第三方模型）与 `argon`（`gemini-4-argon`）三类家族独立计时。某账号在 `Claude Opus 5.5` 上触发 `429` 冷却时，其 `Gemini 4 Neon` / `Carbon` 依然 `100%` 正常接单。
  - `429`：优先遵循上游 `Retry-After`（上限 `6h`），否则按连续命中阶梯退避（`30s -> 60s -> 2m -> 5m -> 10m`）。
  - `529`：`15s -> 30s -> 60s` 家族退避（绝不截断已有的更长 `429` 冷却窗口）。
  - `401` / `403 (#3501)`：仅对该账号施加 `30m` `auth_error` 隔离，不误触全局 ToS 熔断器。
  - 本地快速探测接口（`count_tokens`、`:countTokens`、`GET /v1/models`）均标记 `skipHealth`，绝不会因本地返回 `200 OK` 而误清上游冷却状态。
- **控制台与 API 一键运维动作 (`POST /api/accounts/action`)**：
  - **轮转下一号 (`rotate_next`)**：将池或分组的 `activeAccountId` 切换至下一个健康已授权账号（优先主力角色）。
  - **排空并切换 (`drain_and_rotate`)**：原子将旧账号置为 **`draining`（平滑排空）**（不再接收新会话，存量 U8 粘性对话无感跑完），同时激活下一健康账号接管新流量。

---

<a id="models"></a>

## 支持模型、额度池自动识别与思考档位映射

### 标准对话模型矩阵（11 个可路由对话模型，`agy 1.5.14+`）

| 标准模型 ID | 所属家族 | 归属额度池 | 上下文窗口 | 最大图片数 | 支持思考档位 | 内置别名 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`gemini-4-carbon`** *(默认)* | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `carbon` |
| **`gemini-4-neon`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high`, **`xhigh`** *(原生)* | `neon` |
| **`gemini-4-argon`** | `gemini` | Gemini / Argon 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `argon` |
| **`gemini-4-hydrogen`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high`, **`xhigh`** *(原生)* | `hydrogen` |
| **`gemini-3.8-flash`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` (`xhigh` -> `high`) | `gemini-flash`, `gemini-2.5-flash*`, `gemini-3-flash*`, `gemini-3.0-flash*` |
| **`claude-opus-5-5`** | `claude` | 第三方 (`Claude`/GPT) 池 | `1,048,576` (`900K` 安全区) | `100` | `low`, `medium`, `high` (`xhigh` -> `high`) | `agy-opus` *(设 `AGY_ROUTE_CLAUDE_NAMES=1` 含 `*opus*`)* |
| **`claude-sonnet-5-5`** | `claude` | 第三方 (`Claude`/GPT) 池 | `1,048,576` (`900K` 安全区) | `100` | `low`, `medium`, `high` (`xhigh` -> `high`) | `agy-sonnet` *(设 `AGY_ROUTE_CLAUDE_NAMES=1` 含 `*sonnet*`)* |
| **`gpt-oss-120b-medium`** | `gpt` | 第三方 (`Claude`/GPT) 池 | `131,072` (`128K`) | `100` | **固定 `medium`** | `gpt-oss`, `gpt-oss-120b` |
| **`gemini-pro-orchestrator`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | **固定 `high`** | `gemini-pro-orchestrator*` |
| **`gemini-pro-orchestrator-neon`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | **固定 `high`** | `gemini-pro-orchestrator-neon*` |
| **`gemini-pro-orchestrator-argon`** | `gemini` | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | **固定 `high`** | `gemini-pro-orchestrator-g1-pro` |

### 图像生成模型（OpenAI Images API）

| 模型 ID | 端点路径 | 引擎与隔离方式 | 支持能力 |
| :--- | :--- | :--- | :--- |
| **`gemini-3.1-flash-image`** | `POST /v1/images/generations`, `POST /v1/images/edits` | 独立 `agy acp --agent ws-imagegen`（外层锁 `gemini-3.8-flash` `low`，`0` MCP、`0` 安全监测调用，`60s` 空闲停机） | `b64_json`、`n=1..4`、`aspect_ratio`（`1:1`/`2:3`/`3:2`/`3:4`/`4:3`/`9:16`/`16:9`）或 OpenAI `size`、最多 `3` 张参考图（`PNG`/`JPEG`/`WEBP`/`GIF`/`HEIC`/`HEIF`）、回传 `X-Agy-Served-Model: gemini-3.1-flash-image` |

- **Google AI Pro 与 Ultra `20x` 额度池自动识别**：结合 `<agy> -p /usage` 桶拓扑与 `/credits` `upgrade_uri` 自动匹配真实美元上限：**AI Pro**（Gemini `$10` 5h / `$35` 周度 + 第三方 `$2.50` / `$5` + 独立 Argon 周池）与 **AI Ultra `20x`**（Gemini `$200` 5h / `$800` 周度 + 第三方 `$25` / `$50`）。
- **固定档位保护与智能别名解析**：固定档位模型（`gpt-oss-120b-medium`、`gemini-pro-orchestrator*`）自动跳过 `thought_level` RPC 覆写防上游 `-32602` 拒绝（各 Orchestrator 变体可用性取决于账号订阅档位）。别名自动剥离 `models/`、`[1m]` 及思考档位后缀（`-minimal`/`-low`/`-medium`/`-high`/`-xhigh`/`-max`/`-ultra`）；设置 `AGY_ROUTE_CLAUDE_NAMES=1` 可自动映射通用 `*opus*` / `*sonnet*` / `*haiku*` 名称，设置 `AGY_EXPOSE_TIER_MODELS=1` 可在 `/v1/models` 与 `/v1beta/models` 展开带档位后缀的模型条目。

### 四协议思考档位统一映射表

请求体显式参数优先级高于模型名后缀（如 `gemini-4-neon-xhigh`），未指定时默认使用 `medium`：

| 网关标准档位 | 模型名后缀 | OpenAI (`reasoning_effort` / `reasoning.effort`) | Anthropic (`thinking.budget_tokens` / `output_config.effort`) | Gemini `/v1beta` (`thinkingLevel` / `thinkingBudget`) |
| :--- | :--- | :--- | :--- | :--- |
| **`low`** | `-minimal`, `-low` | `none`, `minimal`, `low` | `type: "disabled"`、`<= 4096` 或 `low` | `NONE`, `MINIMAL`, `LOW` 或 `0..4096` |
| **`medium`** *(默认)* | `-medium` | `medium` *(或缺省)* | `4097 .. 16384` 或 `medium` | `MEDIUM` 或 `4097..16384` |
| **`high`** | `-high` | `high` | `16385 .. 32768` 或 `high` | `HIGH` 或 `16385..32768` |
| **`xhigh`** *(Neon / Hydrogen)* | `-xhigh`, `-max`, `-ultra` | `xhigh`, `max`, `ultra` | `> 32768` 或 `xhigh`/`max` | `XHIGH`, `MAX`, `ULTRA` 或 `> 32768` |

---

<a id="engineering-pillars"></a>

## 核心工程架构、性能优化与优雅停机

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/lifecycle-cache-zh-dark.png" />
    <img src="assets/lifecycle-cache-zh-light.png" alt="Worker 生命周期与 Carbon KV 缓存保持机制" width="48%" />
  </picture>
  &nbsp;
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/seq-prefill-zh-dark.png" />
    <img src="assets/seq-prefill-zh-light.png" alt="慢预填充软分离与 Singleflight 重连时序图" width="48%" />
  </picture>
</p>

1. **五层 Carbon KV 缓存防御与断连救援**
   - 核验 `agy 1.5.14`–`1.5.18+` 原生 CCPA `SessionId` 透传（`SPEC_VERSION=5`）并配合 Gate 5 轨迹 `sessionID` 注入，杜绝多并发会话因空缓存键互相冲刷。
   - 冻结首轮 `system`/`developer` 提示词与初始工具定义；当客户端动态加载延迟 MCP 工具（如 `ToolSearch`）时，通过 `TOOLS_UPDATED_IN_PLACE` 原地刷新桥接目录而不重启进程。
   - **Prefill Soft-Detach + Singleflight**：预填充已耗时 `>=30s` 的重型请求遇客户端断连时保留 `45s` 热算力窗口，重试请求直接复用同一上游流；同时支持多客户端压缩热槽回收与带冷却恢复的 `capacityFallbackTier`。
2. **双通道 MCP 工具挂起桥 (`ws-bridge`)、内置 `search_web` 与客户端 `WebFetch` 透传**
   - 内置仅监听本地回环的临时 Streamable HTTP MCP 桥（`/mcp/{tenantKey}`），拦截上游工具调用并转换为四大协议原生工具结构返回客户端，下一轮请求自动唤醒挂起的工作进程。
   - 从 `.db` `PlannerResponse` 步骤中提取同轮并行工具调用一次性打包下发，自动将超长 MCP 工具名截断至上游 `64` 字符硬上限内，并限制空回合（`0/0`）孤儿接力上限（`AGY_MAX_TOOL_RELAYS=2`）。
   - **网页工具边界**：`search_web` 由 `agy` 原生内置执行（跳过客户端 `WebSearch` 声明以直接走内置搜索）；上游已将 `read_url_content` 废弃为空实现，因此网关原生透传客户端自带 `input_schema`（或 `parameters`）的 `WebFetch` 工具至 MCP 桥，继续过滤无 schema 的服务端 `web_fetch_*` 与 `NotebookEdit`。
3. **三层语义净室与自愈式 Flash WAF 二分**
   - 使用会话级 `HMAC-SHA256` 可逆令牌（`reversible_codec.go`）无损替换客户端注入的敏感元标签、宿主机路径与保留工具名，并在出站 SSE 流中实时透明还原。
   - 遇未知上游 `400` WAF 拦截时，自动通过低成本 `gemini-3.8-flash` 探针二分定位触发片段并持久化最小重写规则（`AGY_WAF_AUTOBISECT=true`）。
4. **多模态 v2、结构化输出对等与独立文生图引擎**
   - 四协议全面支持经魔数嗅探的 **`PNG`、`JPEG`、`WEBP`、`GIF`、`HEIC` 与 `HEIF`** 图片（单图 `<=10 MiB`、单轮 `<=32 MiB`）及 **24 种 UTF-8 文本/文档格式**（`.txt`、`.md`、`.markdown`、`.json`、`.jsonl`、`.ndjson`、`.csv`、`.tsv`、`.yaml`、`.yml`、`.xml`、`.log`、`.ini`、`.toml`、`.py`、`.go`、`.js`、`.ts`、`.tsx`、`.jsx`、`.html`、`.css`、`.sql`、`.sh` 及 `text/*` / JSON / XML / YAML / CSV / NDJSON MIME，单文件 `<=2 MiB`），远程拉图内置 `64MB/15m` 增量缓存及内核级 `net.Dialer.Control` 私网/链路本地 IP 阻断防 DNS 重绑定 SSRF。
   - 明确执行上游 `agy acp` **`PromptCapabilities{Image:true, Audio:false}`** 边界：对 PDF、音频、视频附件直接返回清晰 HTTP `400` 错误（绝不静默丢弃）。
   - 四协议统一支持 JSON Schema 结构化输出（`output_config.format`、`response_format`、`text.format`、`responseJsonSchema`），暴露精确 `max_input_tokens` 元数据，对 `/v1/responses` 严格校验续接状态（`previous_response_not_found`、`thread_not_found`），并通过独立 `--agent ws-imagegen` 引擎提供 `POST /v1/images/{generations,edits}`。
5. **零锁 SQLite 汇总压缩与 `<2s` 优雅停机**
   - 纯 Go 解析 SQLite B-Tree 与 `.db-wal` 帧（`internal/store/sqlite_protobuf.go`），零写锁竞争提取真实输入/缓存/思考/输出 Token、`step_type=17` 静默重试计数与耗时（`agy 1.5.13+`）、Argon `.1.19` 影子路由告警及桌面端 `AGY_APP` 只读遥测。
   - **有界内存与存储瘦身**：采用有界 `8d` 活跃步数查询 + 冷窗汇总（`QueryColdWindowRollup` + `QueryActiveUsageStepsSince`，彻底移除无界 `coldSteps` 内存常驻），为 `conversations`/`turns` 建立 5 个索引，启动恢复收窄至 `2h`（保留 `7d` 懒恢复），并每日自动执行 `CompactExpiredData`（保护 `conversation_hot` 活跃会话）与 WAL `TRUNCATE` 回收。
   - **`<2s` 停机与热路径提速**：HTTP 排空前调用 `BeginShutdown()` 立即解除空闲 `HotParked` 与待决兄弟 MCP 挂起且不干扰活跃 SSE 回合，并行关闭子网关与 ACP 引擎；子账号按 4 并发并行启动；共享引擎空闲保活 `AGY_ENGINE_IDLE_GRACE_SEC=180`；跳过冗余 `set_config_option` RPC；`IsTosTripped()` 启用 `1s` 原子负缓存；SSE 采用强类型 `acpUpdatePayload` 快路径。
6. **双端口严格隔离与暖纸控制台 (`:18891`) 亮点**
   - `:18890` 仅暴露 AI 生成协议与 `/healthz`（访问 `/`、`/api/*`、`/metrics` 均返回 `404`）；`:18891` 仅服务主密钥鉴权的控制台、账号管理与监控指标（访问 `/v1/*`、`/v1beta/*`、`/mcp/*` 均返回 `404`）。
   - `//go:embed` 内嵌暖纸控制台（内置全套 Lucide 图标，零外部 CDN）集成池路由快捷栏、支持 11 模型白名单与 GPT/Claude/Gemini 家族筛选的账号/分组控制中心、Pro/Ultra `20x` 额度进度条、外审三区徽章 + `/hr` 燃烧速率与黄/红区 ETA 倒计时、额度倍率异常顶部横幅与卡片徽章、账号级 `agy` 版本徽章（高亮混跑或待切换）、cgroup 内存预算条、多节点联邦（`AGY_DASHBOARD_PEERS`）以及兼容 `verify_deferred` 态的一键 `G1-G4` 升级弹窗。

---

<a id="configuration-reference"></a>

## 接口矩阵、环境变量与 Prometheus 指标参考

### 端口与接口总览

| 服务面 | 方法与路径 | 鉴权要求 | 说明 |
| :--- | :--- | :--- | :--- |
| **数据面 `:18890`** | `GET /healthz` | 免鉴权 | 容器/负载均衡存活探针（`{"ok":true}`） |
| **数据面 `:18890`** | `GET /v1/models`, `GET /v1beta/models` | API Key（主 Key / 专属 Key / `?key=`） | 按调用方所属账号或分组白名单过滤后的 11 模型列表 |
| **数据面 `:18890`** | `POST /v1/messages`, `POST /v1/messages/count_tokens` | `x-api-key` 或 `Bearer` | Anthropic Messages API（流式/非流式/工具/视觉/思考） |
| **数据面 `:18890`** | `POST /v1/chat/completions` | `Authorization: Bearer` | OpenAI Chat Completions 接口 |
| **数据面 `:18890`** | `POST /v1/responses` | `Authorization: Bearer` | OpenAI Responses 接口（`Codex` CLI 原生协议） |
| **数据面 `:18890`** | `POST /v1/images/generations`, `POST /v1/images/edits` | `Authorization: Bearer` 或 `x-api-key` | OpenAI 兼容文生图与图生图编辑（`gemini-3.1-flash-image`、`b64_json`、`n=1..4`、最多 `3` 张参考图） |
| **数据面 `:18890`** | `POST /v1beta/models/{m}:generateContent` / `:streamGenerateContent` / `:countTokens` | `x-goog-api-key`、`Bearer` 或 `?key=` | Google Gemini 原生 REST v1beta 接口 |
| **管理面 `:18891`** | `GET /`, `GET /api/dashboard`, `GET /api/quota` | **仅限主 Key**（Basic/Bearer/`x-api-key`） | 暖纸实时控制台、多节点联邦数据与额度快照 |
| **管理面 `:18891`** | `GET /metrics` | **仅限主 Key** | Prometheus `0.0.4` 网关、调度与每账号 Worker 指标 |
| **管理面 `:18891`** | `GET/POST/DELETE /api/accounts`, `/routing`, `/groups`, `/action`, `/probe-ip`, `/oauth/*` | **仅限主 Key + CSRF 校验**（`X-Agy-Requested-With: dashboard`） | 多账号沙盒、渠道分组、路由治理、一键轮转与网页 PKCE OAuth |
| **管理面 `:18891`** | `GET /api/upgrade`, `POST /api/upgrade/approve` | **仅限主 Key + CSRF 校验** | `G1-G4` 官方 `agy` 升级状态查询与一键热切换审批 |

### 核心环境变量速查表 (`.env`)

以下全部默认值均与 Go 网关代码（`go-gateway/internal/config/snapshot.go`）及宿主机升级代理（`deploy/prod-upgrader/agy_prod_upgrader.py`）保持 1:1 一致：

| 变量名 | 默认值 | 作用说明 |
| :--- | :--- | :--- |
| `AGY_API_KEYS` | *(本地回环可空)* | 逗号分隔主管理密钥，保护 `:18890` 与 `:18891`（支持 `@acct:<id>` / `@group:<id>` 后缀） |
| `AGY_LISTEN_HOST` | `127.0.0.1` | 监听地址（`deploy/docker-compose.yml` 容器内设为 `0.0.0.0`） |
| `AGY_GW_PORT` / `AGY_DASH_PORT` | `18890` / `18891` | 数据面 API 端口与管理面控制台/监控端口 |
| `AGY_ACCOUNTS_FILE` | `<data>/accounts.json` | 多账号沙盒、`AccountGroup` 渠道分组、备用角色与池路由治理持久化路径 |
| `AGY_PROXY_PRESETS_FILE` / `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN` | *(可选)* / `resin:2260` / *(空)* | 命名出口代理预设 JSON 与失败关闭预设代理主机/令牌 |
| `AGY_DASHBOARD_PEERS` | *(可选)* | 跨机多节点联邦面板配置（`id\|label\|url\|key;...`） |
| `AGY_UPSTREAM_PROXY` | *(空；`gw.sh` 默认 `http://127.0.0.1:7890`)* | 主账号 `agy` 子进程默认 HTTP/HTTPS/SOCKS5 出口代理 |
| `AGY_MAX_LIVE_WORKERS` / `AGY_MAX_CONCURRENT_PROMPTS` | `8` / `clamp(workers, 6, 16)` | 主账号热存活 ACP 会话上限与随 `maxLiveWorkers` 实时弹性缩放的并发限流器 |
| `AGY_PROMPT_IDLE_TIMEOUT_SEC` / `AGY_PROMPT_MAX_DURATION_SEC` | `1800` / `43200` | 活跃感知 `session/prompt` 空闲超时（`30m`）与单回合绝对上限（`12h`） |
| `AGY_IDLE_WORKER_TTL_SEC` / `AGY_ENGINE_IDLE_GRACE_SEC` | `1200` / `180` | 空闲 ACP 会话休眠超时（`20m`）与共享引擎空闲保活宽限期（`3m`） |
| `AGY_PARKED_TTL_SEC` / `AGY_MAX_TOOL_RELAYS` / `AGY_TOOL_RELAY_SEC` | `300` / `2` / `55` *(自适应)* | MCP 挂起恢复超时（`5m`）、`0/0` 空回合孤儿接力上限（`2`）与自适应保活间隔（`55s`–`150s`，显式设置则锁定） |
| `AGY_MONITOR_TRIP_ROUTING` / `AGY_MONITOR_TRIP_HOP` / `AGY_MONITOR_BUDGET_SPREAD` | `true` / `true` / `true` | 安全监测治理器 v2：熔断改道、粘性 Hop + 挂起取消、绿区均衡与无工具反向分流 |
| `AGY_MONITOR_SOFT_CAP_ULTRA` / `DRAIN` / `HARD` | `2800` / `3800` / `4800` | Ultra `20x` 5h 外审三区阈值（绿/黄/红，遇 429 自动校准） |
| `AGY_MONITOR_SOFT_CAP_PRO` / `DRAIN` / `HARD` | `1800` / `2600` / `3400` | AI Pro 5h 外审三区阈值（绿/黄/红，遇 429 自动校准） |
| `AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC` / `AGY_PREFILL_CANCEL_GRACE_SEC` | `30` / `45` | 慢预填充软分离触发阈值（`30s`）与断连热算力保留窗口（`45s`） |
| `AGY_CAPACITY_FALLBACK_TTL_SEC` / `AGY_CAPACITY_FALLBACK_MAX_TTL_SEC` | `300` / `1800` | 自动降档保护的冷却恢复窗口与指数退避上限秒数（`0` 为会话期内永久锁定） |
| `AGY_WAF_AUTOBISECT` | `true` | 上游 `400` 误拦截自动 Flash 二分定位与规则自愈开关 |
| `AGY_CONV_KEEP_DAYS` / `AGY_ACP_LOG_KEEP_DAYS` / `AGY_ACP_LOG_MAX_MB` | `0` / `0` / `64` | 会话与 `acp-worker` 日志留存天数（Docker Compose 默认 `14` / `7` 天）与单账号日志容量上限（`64 MiB`，`>=24h` 保护） |
| `AGY_VERIFY_DEFER_BACKOFF_SECS` | `900,1800,3600` | 宿主机 `prod-upgrader` `verify_deferred` 切换后额度重试指数退避计划（`15m`/`30m`/`60m`） |
| `AGY_BLOCKED_VERSIONS` | `1.5.17,1.5.18` | 宿主机 `prod-upgrader` 已知回归版本黑名单（下载/`G1-G4` 及 apply 前拦截 `agy#1890`/`agy#1872`；设为空或 `none` 可覆写） |

*（多模态、模型暴露、内存上限与路径覆盖的完整注释清单请查阅 [`.env.example`](.env.example)。）*

### 核心 Prometheus 监控指标 (`GET :18891/metrics`)

- **网关全局与缓存指标**：`agy_gateway_store_ready`、`agy_gateway_live_workers`、`agy_gateway_max_live_workers`、`agy_gateway_hibernated_sessions`、`agy_gateway_parked_calls`、`agy_gateway_prompts_inflight`、`agy_gateway_max_concurrent_prompts`、`agy_gateway_requests_total`、`agy_gateway_cache_burn_warnings_total`、`agy_gateway_capacity_fallback_active_sessions`、`agy_gateway_capacity_fallback_events_total`、`agy_gateway_capacity_fallback_probes_total`、`agy_gateway_soft_detach_total{outcome}`、`agy_gateway_upgrade_state{state}`、`agy_gateway_turns_total{model,mode,stop_reason,usage_source}`、`agy_gateway_tokens_total{model,kind}`。
- **多账号池与联邦 Worker 指标**：`agy_account_inflight{account,state}`、`agy_account_sticky_sessions{account}`、`agy_account_cooldown_seconds{account}`、`agy_account_requests_total{account}`、`agy_account_failures_total{account}`、`agy_account_rate_limits_total{account}`、`agy_account_failovers_total{account}`、`agy_account_worker_up{account}`、`agy_account_live_workers{account}`、`agy_account_max_live_workers{account}`、`agy_account_hibernated_sessions{account}`、`agy_account_prompts_inflight{account}`、`agy_account_parked_calls{account}`。

---

<a id="safe-upgrader"></a>

## 四门禁安全升级 (`G1-G4`) 与零额度测试套件

官方 `agy` 发版频繁，当前推荐基线为 **`agy 1.5.16`**（上游 `1.5.17`/`1.5.18` 存在 `agy#1890` `--agent` 4 倍 Gemini 额度消耗回归与 `agy#1872`）。本机升级脚本（[`scripts/agy-upgrade.sh`](scripts/agy-upgrade.sh)）与宿主机生产升级代理（[`deploy/prod-upgrader/agy_prod_upgrader.py`](deploy/prod-upgrader/agy_prod_upgrader.py)）在替换 `bin/agy-acp-900k` 前均强制执行 **四道门禁 (`G1-G4`)** 与回归拦阻：

- **已知回归版本黑名单 (`AGY_BLOCKED_VERSIONS=1.5.17,1.5.18`)**：在下载与 `G1-G4` 前零额度自动跳过黑名单版本，并在 `cmd_apply()` 阶段失败关闭拒绝，同时对 `1.5.19+` 保持自动放行（设为空 `""` 或 `none` 可覆写，支持 `<ver>:<reason>` 自定义）。
- **G1（多架构供应链哈希与版本校验）**：交叉核验 `linux_amd64`、`linux_arm64` 与 `darwin_arm64` 双源 `SHA-512` / `SHA-256` 摘要、压缩包字节大小与 `--version`。
- **G2（`--acp-only` 二进制补丁与原生 `SessionId` 审计）**：经 [`third_party/agy-context-unlock/agy-patch.py`](third_party/agy-context-unlock/agy-patch.py) `--acp-only` 执行 `900K` ACP 上下文解锁（跳过废弃 TUI Gate 1/2 并保留全部 5 个必选 ACP 补丁，为 Gate 3A/3B/4B 内置失败关闭的 `state=native` 原生自跳过），配合 [`tools/agy_extra_patch.py`](tools/agy_extra_patch.py) `--check` 完成 `disable-internal-checkpoint` 与核心不变式轨迹 `SessionId` 核验（`SPEC_VERSION=5`，支持 `agy 1.5.14`–`1.5.18+`）。
- **G3（零额度离线 ACP 契约测试）**：在一次性隔离目录运行 [`scripts/agy_acp_smoke.py`](scripts/agy_acp_smoke.py)，验证 JSON-RPC stdio 握手与模型列表契约。
- **G4（隔离沙盒验证、`verify_deferred` 退避、软链热切与磁盘清理）**：在独立测试实例（`:18892`）按 Gemini 5h 额度余量择优选号（自动避开外审熔断/红区账号）跑通文本 + 2 步 MCP 工具闭环，待面板审批且在途请求排空后原子切换 `data/agy/current` 软链；若切换后探针仅因号池全局 `429` 失败，则进入 **`verify_deferred`** 暂缓验收态并按 `AGY_VERIFY_DEFER_BACKOFF_SECS=900,1800,3600` 指数退避重试（最多 `3` 次），绝不误回滚健康版本；最后运行 **`prune_installed_agy`**（保留最近 `2` 个有效目录，保护 `current`、全部运行中多账号 `engine.json` worker 与回滚目标）。

```bash
# 只读探测上游最新正式版 agy（零额度消耗）
python3 scripts/upstream_watch.py --official-only
./scripts/agy-upgrade.sh --status

# 运行全量零额度 Go 竞态单测 + Python 升级门禁与脚本单测
cd go-gateway && go test -race -count=1 ./... && cd ..
python3 -m unittest discover -s scripts -p "test_*.py"
python3 -m unittest discover -s deploy/prod-upgrader -p "test_*.py"
```

---

<a id="troubleshooting"></a>

## 常见问题排障 (Troubleshooting & FAQ)

<details>
<summary><strong>1. 报错 <code>403 permission_error</code>（提示该账号或分组不允许访问某模型）</strong></summary>

当前使用的专属 Key 或分组绑定了受限模型白名单 `allowedModels`。打开 `:18891` -> **账号控制中心** -> 编辑对应账号或渠道分组，点击 **全选全部模型**（或勾选 11 个标准模型中的目标模型）并保存即可立即生效。
</details>

<details>
<summary><strong>2. 通过局域网 IP 或反向代理访问面板/API 返回 <code>403 Forbidden</code></strong></summary>

当 `AGY_API_KEYS` 为空时，网关为防止无鉴权暴露公网，会强制拦截所有非本地回环 `Host` 请求。请在 `.env` 中配置 `AGY_API_KEYS=sk-...` 后重启。
</details>

<details>
<summary><strong>3. 报错 <code>403 #3501</code> / <code>GEMINI_ENTERPRISE_LICENSE_NOT_VALID</code> 或账号显示 <code>auth_error</code></strong></summary>

说明该 Google 账号 OAuth 凭证已过期或未开通有效 Antigravity 权限。该错误**不会**触发全局 ToS 熔断，仅将该账号隔离 `30` 分钟（`auth_error`）。前往 `:18891` -> **账号控制中心** 点击该账号的 **授权** 重新完成网页 OAuth 激活即可自动恢复。
</details>

<details>
<summary><strong>4. 报错 <code>503 Service Unavailable</code>（提示当前活跃会话绑定的账号不可用）</strong></summary>

这是 **Decision U8 防封粘性保护** 正在生效：当前多轮对话粘性锁定的原账号已被停用或处于 `auth_error`/ToS 阻断态，网关拒绝将半截对话历史跨账号重放到其他个人 Google 账号。只需在客户端**新建一个对话窗口**，第 1 轮请求即会自动调度至其他健康账号。
</details>

<details>
<summary><strong>5. 传入 PDF、音频或视频附件时返回 <code>400 invalid_request_error</code></strong></summary>

上游 `agy acp` 声明的原生能力为 `PromptCapabilities{Image:true, Audio:false}`，仅接受光栅图片（`PNG`/`JPEG`/`WEBP`/`GIF`/`HEIC`/`HEIF`）或 UTF-8 文本/文档（24 种扩展名，`<=2 MiB`）。为避免静默丢弃二进制附件导致模型幻觉，网关直接返回明确 HTTP `400`。请先将 PDF 转为纯文本或逐页图片后再提交。
</details>

<details>
<summary><strong>6. 自定义脚本调用 <code>POST / DELETE /api/accounts*</code> 返回 <code>403 Forbidden</code></strong></summary>

管理面写接口启用了 Origin 与 CSRF 双重校验。CLI `curl` 脚本调用时请勿携带浏览器 `Origin` 与 `Sec-Fetch-Site` 头；若从带 `Origin` 的上下文调用，须同时保证 `Origin` 与 `Host` 同源且附带 `-H "X-Agy-Requested-With: dashboard"`。
</details>

<details>
<summary><strong>7. 命名代理预设报错“缺少 AGY_RESIN_PROXY_TOKEN”</strong></summary>

为防止配置了代理隔离的账号意外回退宿主机直连泄露真实 IP，命名预设在缺少 `AGY_RESIN_PROXY_TOKEN` 时强制失败关闭（Fail-Closed）。请在 `.env` 补齐 `AGY_RESIN_HOST` 与 `AGY_RESIN_PROXY_TOKEN`，或将该账号代理模式改为 **`custom`（自定义代理 URL）** 或 **`direct`（宿主机直连）**。
</details>

<details>
<summary><strong>8. 使用 <code>scripts/gw.sh</code> 停止非默认端口的独立测试实例</strong></summary>

非默认端口会自动隔离到 `runtime/instance-<port>` 目录。停止测试实例时必须传入与启动时完全一致的端口变量：`AGY_GW_PORT=18892 AGY_DASH_PORT=18893 ./scripts/gw.sh stop`。严禁在有在途流量的机器上直接执行 `pkill -x agy-acp-900k`。
</details>

---

## 安全与隐私边界

- **严守官方子进程边界 (红线 C1)**：绝不伪造 Google OAuth 或私有 Protobuf HTTP 请求，所有对话与图像模型流量 `100%` 经由官方 `agy acp` 执行。
- **全链路敏感信息脱敏**：运行日志自动擦除 Bearer 令牌、`?key=` 参数与代理凭据（含单用户名 Token）；`/api/dashboard` 对账号/分组 `boundKeys` 做掩码预览脱敏，仅主 Key 可访问的 `/api/accounts` 自动掩码脱敏 `customProxy` 代理密码。
- **零外部遥测与严格管理面隔离**：不向任何第三方收集或上报数据；暖纸控制台与全套 Lucide 图标均通过 `//go:embed` 完整内嵌于单二进制中，`AGY_CORS_ORIGINS` 严格限定于 `:18890` 数据面接口（`/v1/*`、`/v1beta/*`），绝不向 `:18891` 管理端点放开跨域读取。

---

<a id="changelog"></a>

## 版本更新记录

- **`v2.3.20` (2026-11-26) — OpenAI 图像 API、11 个可路由对话模型、HEIC/HEIF 与客户端 `WebFetch` 透传**：
  - 新增 OpenAI 兼容 `POST /v1/images/generations` 与 `POST /v1/images/edits`，基于独立延迟拉起 `agy acp --agent ws-imagegen` 引擎调用 `gemini-3.1-flash-image`（`0` MCP、`0` 安全监测调用，`b64_json`、`n=1..4`、`aspect_ratio`/`size` 映射、multipart 与 JSON 最多 `3` 张参考图编辑、`O_NOFOLLOW` 安全读盘）。
  - 对话模型由 `7 -> 11` 个扩展：正式上线 `gpt-oss-120b-medium`（`128K`、固定 `medium`、第三方池）并新增 `gemini-pro-orchestrator`、`gemini-pro-orchestrator-neon`、`gemini-pro-orchestrator-argon`（固定 `high`，别名 `gpt-oss`、`gpt-oss-120b`、`gemini-pro-orchestrator-g1-pro`），固定档位自动跳过 `thought_level` RPC、`ModelFromEnumOrServed` 遥测归因，`:18891` 控制台同步升级至 11 模型目录。
  - 新增 ISOBMFF `ftyp` 魔数嗅探支持 `HEIC`/`HEIF`，对 PDF/音频/视频返回明确 HTTP `400` 说明 `PromptCapabilities{Image:true, Audio:false}`，并在上游废弃 `read_url_content` 后恢复客户端自带 `input_schema` 的 `WebFetch` 透传。
- **`v2.3.19` (2026-11-26) — SQLite `8d` 汇总、`<2s` 停机、绿区均衡、倍率异常检测与补丁清理**：
  - 面板改走有界 `8d`+全量 SQLite 汇总（`QueryColdWindowRollup`），移除无界 `coldSteps` 内存常驻，补齐 5 个 `conversations`/`turns` 索引，启动恢复收窄至 `2h`（`7d` 懒恢复），并每日执行 `CompactExpiredData` 与 WAL `TRUNCATE`。
  - 落地 `BeginShutdown()` + 并行收尾实现 `<2s` 优雅停机、4 并发并行子账号启动、`AGY_ENGINE_IDLE_GRACE_SEC=180`、冗余 `set_config_option` 跳过与强类型流式快路径。
  - 启用绿区外审水位均衡（`narrowCandidatesByMonitorUtil`）、无工具反向分流（`preferWarmNoTools`）、绿区长耗时工具保活分级（`80s`/`135s`）、5h/周度额度倍率异常检测（`QUOTA_BURN_ANOMALY`），以及 `:18891` cgroup 内存预算、`agy` 版本徽章与 `/hr` 速率/ETA 展示。
  - `agy-patch.py` 新增 `--acp-only` 与失败关闭的 `state=native` 自跳过，`prod-upgrader` 新增 `prune_installed_agy`（`keep=2`），新增单账号 `AGY_ACP_LOG_MAX_MB=64`（`>=24h` 保护）。
- **`v2.3.18` (2026-11-26) — 监测窗口 v2、Standby/分层治理、`135s`/`150s` 保活与 `agy#1890` 拦截**：
  - 将 `monitor-trips.json` 升级至 `Version: 2` 并锚定独立 `liteResetAt`，消除重启瞬时误红与 `~20m` 窗口漂移。
  - 新增 `role: primary|standby` 及 `:18891` / `/api/accounts/routing` 免重启热开关 `priorityTiering`、`monitorSpillOnYellow` 与运行期 `monitorBudgetSpread`。
  - 黄/红区 MCP 挂起保活自适应拉宽至 `135s` 基准 / `150s` 封顶（安全低于 `180s` 上限），`prod-upgrader` 默认内置 `AGY_BLOCKED_VERSIONS=1.5.17,1.5.18` 拦阻 `agy#1890`/`agy#1872`。
- **`v2.3.17` (2026-11-26) — 安全监测治理器 v2、Hop 联动取消、出口探针与 `verify_deferred`**：
  - 落地三区软预算治理器 v2（`v2.3.10`/`v2.3.15`/`v2.3.17`）、无工具请求豁免、跨进程 `MONITOR_TRIP_HOP` 挂起取消（`/api/internal/cancel-parked-session`）、`AGY_MAX_TOOL_RELAYS=2` 与 `:18891` 外审进度条/倒计时。
  - 新增 `/usage` 经代理探测 `oauth2.googleapis.com`（`v2.3.16`/`v2.3.17`）与多账号 `G3/G4` 择优选号 + `verify_deferred` 指数退避（`AGY_VERIFY_DEFER_BACKOFF_SECS=900,1800,3600`）。
- **`v2.3.13` – `v2.3.14` (2026-11-26) — 动态 Prompt 限流器、非阻塞扫描与 `agy 1.5.17+` 适配**：
  - 落地随 `maxLiveWorkers` 实时弹性缩放的 FIFO `dynamicPromptLimiter`、压缩中断兄弟排空、复用 `64KB` 缓冲的后台流式 `acp-worker` 日志扫描及 `SPEC_VERSION=5` 原生 `SessionId` 核验。
- **`v2.3.5` – `v2.3.12` (2026-11-24 – 2026-11-25) — 生产审计加固、净室 PR #4 与 Pro 套餐识别**：
  - 新增活跃感知 `AGY_PROMPT_IDLE_TIMEOUT_SEC`/`AGY_PROMPT_MAX_DURATION_SEC`、`64` 字符 MCP 工具截断、子代理粘性隔离（`:ag:<id>`）、联邦每账号 `/metrics`、可选日志/会话清理（`AGY_ACP_LOG_KEEP_DAYS`/`AGY_CONV_KEEP_DAYS`）、合入净室 PR #4、`/credits` Pro/Ultra 自动区分与当时 7 个模型的真机额度标定。
- **`v2.3.1` – `v2.3.4` (2026-11-23) — 合入 PR #1 隔离配额 `HOME`、压缩热槽回收与降档冷却恢复**：
  - 合入社区 PR #1（`@HamsteRider-m`）隔离配额 `HOME` 与 macOS 无头文件 Token 直读，实现配额冷启动严格非阻塞、多客户端压缩热槽回收、`Pi`/`dsh` 韧性增强与带退避恢复的 `capacityFallbackTier`。

<details>
<summary><strong>更早历史版本 (v2.3.0 及 2026-11-08 至 2026-11-22)</strong></summary>

- **`v2.3.0` (2026-11-22)**：推出多账号池 v3（5 种路由策略、`AccountGroup` 渠道池、`@acct:`/`@group:` 路由、首轮流前故障转移、Decision U8 粘性护栏、按家族独立冷却、`rotate_next`/`drain_and_rotate`）及 `agy 1.5.14` 原生 `SessionId` 核验。
- **`v2.2.0` (2026-11-20)**：上线单容器多账号物理沙盒、网页无头 PTY PKCE OAuth、Pro/Ultra `20x` 额度识别、多节点联邦大盘（`AGY_DASHBOARD_PEERS`）与 SQLite `step_type=17` 静默重试感知。
- **2026-11-18 – 2026-11-19**：新增 `gemini-4-neon`（`xhigh`）、热路径零拷贝缓冲池、7 子包无环 `go-gateway/internal/` 重构、SQLite WAL 读写分离、首轮前缀冻结与桌面端 `AGY_APP` 只读遥测。
- **2026-11-08 – 2026-11-17**：完成纯 Go（`CGO_ENABLED=0`）单二进制重写、零锁 `.db`/`.1.19` Protobuf 遥测、`PlannerResponse` 并行工具合并、`G1-G4` 安全升级、`HMAC-SHA256` 可逆净室 + Flash WAF 二分、Gemini `/v1beta` 适配、防 SSRF 多模态 v2、Gate 5 `900K` 解锁及 Prefill Soft-Detach + Singleflight。

</details>

---

## 开源协议

本项目基于 [MIT License](LICENSE) 开源发布。
