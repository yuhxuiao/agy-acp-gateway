<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/banner-zh-dark.svg" />
    <img src="assets/banner-zh-light.svg" alt="agy-acp-gateway" width="760" />
  </picture>
</p>

<h3 align="center">基于 Google 官方 Antigravity CLI (<code>agy acp</code>) 的零 CGO 单二进制四协议网关 —— 内置多账号隔离池、防封粘性路由、Carbon KV 缓存防御与暖纸实时控制台。</h3>

<p align="center">
  <a href="./README.md"><strong>English</strong></a> &nbsp;·&nbsp;
  <a href="./README_ZH.md"><strong>简体中文</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>5 分钟快速上手</strong></a> &nbsp;·&nbsp;
  <a href="#client-setup"><strong>客户端配置</strong></a> &nbsp;·&nbsp;
  <a href="#multi-account"><strong>多账号与渠道分组</strong></a> &nbsp;·&nbsp;
  <a href="#models"><strong>模型与思考档位</strong></a> &nbsp;·&nbsp;
  <a href="#engineering-pillars"><strong>核心架构</strong></a> &nbsp;·&nbsp;
  <a href="#troubleshooting"><strong>常见问题排障</strong></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26%2B_(CGO__ENABLED%3D0)-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+ Zero CGO" />
  <img src="https://img.shields.io/badge/Protocols-Anthropic_%7C_OpenAI_%7C_Responses_%7C_Gemini_v1beta-0856F5?style=flat-square" alt="Quad Protocol" />
  <img src="https://img.shields.io/badge/Multi--Account-5_Strategies_%2B_U8_Sticky_Guard-167c52?style=flat-square" alt="Multi-Account Pool" />
  <img src="https://img.shields.io/badge/Context-900K_Unlocked-0D9488?style=flat-square" alt="900K Context" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-22c55e?style=flat-square" alt="MIT License" /></a>
</p>

---

**`agy-acp-gateway`** 将 Google 官方 Antigravity CLI（`agy acp`，基于 stdio 的 JSON-RPC 2.0 协议）封装为生产级纯 Go（`CGO_ENABLED=0`）单二进制多协议 AI 网关与实时观测控制中心。

不同于逆向抓包伪造私有 OAuth / Protobuf HTTP 请求的第三方中转方案，**本项目 `100%` 的上游通信均运行在官方 `agy` 二进制内部**。TLS BoringSSL 握手特征、OAuth 令牌自动续期、轨迹检查点与上游封包全部由 Google 官方客户端原生托管；`agy-acp-gateway` 在此安全基座之上提供：

- **四协议原生接入 (`:18890`)**：同时支持 `Anthropic` `/v1/messages`、OpenAI `/v1/chat/completions`、OpenAI `/v1/responses` 与原生 Google Gemini REST `/v1beta`。
- **多账号沙盒池与渠道分组 (`v2.3`)**：单容器内多账号物理目录/SQLite/出口代理隔离、网页免 SSH PKCE OAuth 扫码登录、5 种调度策略、首轮流前自动故障转移、按模型家族独立冷却、平滑排空与 **Decision U8 防封粘性会话护栏**。
- **五层 KV 缓存防御体系**：校验 `agy 1.5.14+` 原生轨迹 `SessionId` 透传 + Gate 5 二进制 `900K` 上下文解锁、首轮 `system`/工具目录前缀冻结、MCP 动态工具原地热更新（`TOOLS_UPDATED_IN_PLACE`）以及慢预填充软分离（`45s` 宽限期 + Singleflight 重连复用）。
- **零锁 SQLite 权威遥测与联邦控制台 (`:18891`)**：纯 Go 直读 `.1.19` Protobuf 步骤/缓存/思考 Token 与 TTFT/TPS、自动识别 AI Pro 与 Ultra `20x` 额度池上限、`step_type=17` 静默重试感知、Prometheus `/metrics` 指标以及多节点联邦大盘。

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
| **单账号限流与人工切号繁琐** | 全局共用单一 `~/.gemini` 目录，遇 `429`/`529` 需手工停机换号 | **v3 多账号池与渠道分组**：5 种路由策略（轮询/填满/平滑权重/最低负载/手动主用）、首轮流前自动容灾、按家族独立冷却、一键**排空并切换** |
| **Gemini 4 Carbon 并发 0% 缓存命中** | 多会话共用空轨迹 ID，或在中途修改 `system` / 工具列表导致前缀失效 | **轨迹 `SessionId` 隔离 + 首轮前缀冻结**：独立会话 ID、冻结首轮系统提示词与工具目录，配合 `TOOLS_UPDATED_IN_PLACE` 原地增量更新 |
| **120s+ 超长上下文预填充断连雪崩** | 客户端超时断开即发送 `session/cancel`，白白销毁 150K+ 预填充算力 | **Prefill Soft-Detach (`30s`/`45s`) + Singleflight**：断连后保留上游热算力，客户端重试无缝接回同一请求 |
| **客户端元标签触发上游 400 WAF 误杀** | 直接报错中断，或用粗糙正则误删用户真实指令与代码片段 | **三层语义净室 + 会话 HMAC 可逆编解码 + Flash 自动二分**：无损编码敏感标签/路径，自动二分定位新型 WAF 触发片段 |
| **Token 盲估、静默重试与额度黑盒** | 仅靠字符数估算用量，无法感知上游静默退避与 Argon 影子池切换 | **零锁 `.1.19` SQLite B-Tree/WAL 直读**：精确提取缓存/思考/输出 Token、`step_type=17` 重试次数及 Pro/Ultra `5h` + 周度额度池 |

---

<a id="quick-start"></a>

## 5 分钟快速上手指南

### 环境前置要求

- **操作系统**：Linux（`x86_64` / `arm64`）、macOS（`arm64` Apple Silicon），或 Windows 下的 **WSL2 Ubuntu**。
- **官方 Antigravity CLI (`agy`)**：已安装于 `~/.local/bin/agy`（推荐 `1.5.14+`；网关从不直接修改全局官方文件）。
- **编译环境** *(从源码构建时)*：Go `1.26+` 与 Python `3.10+`。

---

### 方式 A：Docker Compose 部署（推荐服务器与多账号池使用）

```bash
# 1. 克隆仓库
git clone https://github.com/yuhxuiao/agy-acp-gateway.git
cd agy-acp-gateway

# 2. 编译纯静态零 CGO 网关二进制，并生成 Gate 1-5 补丁版 ./bin/agy-acp-900k
cd go-gateway && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway . && cd ..
# 先在隔离沙盒生成并验证候选目录，再传入上一步输出的 <candidate_dir> 原子安装
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. 配置环境变量并启动容器
cp .env.example deploy/.env
# 编辑 deploy/.env -> 设置 AGY_DASHBOARD_PASSWORD（独立管理面密码）与 AGY_API_KEYS
# （也可在 :18891 面板内按需签发 SHA-256 哈希存储的 agy-ck-* 客户端 Key；无可用代理时保持 AGY_UPSTREAM_PROXY 注释状态即可）
docker compose -f deploy/docker-compose.yml up -d --build

# 4. 验证健康检查
curl -fsS http://127.0.0.1:18890/healthz
# -> {"ok":true}
```

> **多账号容器内存规划提示**：`deploy/docker-compose.yml` 默认配置 `mem_limit: 3200m` 与 `stop_grace_period: 25s`。每个活跃子账号会启动一个轻量级子网关进程（`128MiB` 上限）及 `1..N` 个官方 `agy acp` 进程（热态约 `250-350MB` RSS）。并发挂载多个账号时，建议按每新增 1 个并发活跃账号预留 `+512MB ~ +1GB` 容器内存上限。

---

### 方式 B：Linux / macOS / WSL2 原生脚本启动 (`./scripts/gw.sh`)

```bash
# 1. 运行竞态检测单测并编译零 CGO 二进制
cd go-gateway
gofmt -l . && go vet ./... && go test -race -count=1 ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway .
cd ..

# 2. 基于官方 agy 生成并通过离线契约校验 `./bin/agy-acp-900k`（不改动 ~/.local/bin/agy）
# 先在隔离沙盒生成并验证候选目录，再传入上一步输出的 <candidate_dir> 原子安装
./scripts/agy-upgrade.sh --official ~/.local/bin/agy --contract
./scripts/agy-upgrade.sh --install --candidate <candidate_dir>

# 3. 初始化配置并启动本机服务
cp .env.example .env
# 编辑 .env -> 配置 AGY_DASHBOARD_PASSWORD（推荐独立密码）/ AGY_API_KEYS 与 AGY_UPSTREAM_PROXY
./scripts/gw.sh start
./scripts/gw.sh status
```

---

### 第 3 步：完成 Google 账号 OAuth 授权引导（3 种方式任选）

在浏览器打开 **暖纸实时控制台** `http://127.0.0.1:18891/`（HTTP Basic Auth 用户名任意填写，密码输入 `.env` 中的 `AGY_DASHBOARD_PASSWORD` 管理密码；未配置时兼容回退输入 `AGY_API_KEYS` 主密钥），点击顶栏 **账号控制中心**（亦可在其中的 **客户端 API Key** 标签页签发仅限 `:18890` 数据面使用的 `agy-ck-*` 密钥）：

1. **方式一：网页免 SSH PKCE OAuth 授权（适用于 Linux / WSL2 / Docker，推荐无头服务器与全部子账号使用）**
   - *（注：原生 `macOS arm64` 无 Linux `/dev/ptmx` 伪终端，macOS 原生环境请使用方式二直接导入 Token JSON 或方式三命令行授权。）*
   - 点击 **新增子账号**（或在已有账号卡片点 **授权**），选择出口代理模式（宿主机直连、命名代理预设、或自定义 `http://` / `socks5://` 代理）并保存。
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
| **`Claude Code`** | `Anthropic` `/v1/messages` | `http://127.0.0.1:18890` | `x-api-key` / `ANTHROPIC_API_KEY` | `metadata.user_id` + `ws-bridge` MCP 挂起桥 |
| **`Codex` CLI** | OpenAI `/v1/responses` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | `prompt_cache_key` / 首轮指纹 + 无状态全量历史 + MCP |
| **Cursor / Continue / `Cline` / `Roo Code` / `OpenCode`** | OpenAI `/v1/chat/completions` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` | 首轮 `(system + first_user)` 指纹 + 并行工具合并 |
| **Gemini CLI / `google-genai` SDK** | Gemini `/v1beta` | `http://127.0.0.1:18890` | `x-goog-api-key` / `?key=` | 原生 `functionCall` / `thoughtSignature` + MCP |
| **Pi Coding Agent** | `/v1/messages` 或 `/v1/responses` | `http://127.0.0.1:18890` | `x-api-key` / `Bearer` | `x-pi-session-id` + `TOOLS_UPDATED_IN_PLACE` |

### 1. `Claude Code` (`/v1/messages`)

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
# 进阶技巧：可在主 Key 后追加 @acct:<账号ID> 或 @group:<分组ID> 将当前窗口绑定至指定账号或渠道分组池！
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

---

<a id="multi-account"></a>

## 多账号池、渠道分组与防封路由调度指南 (`v2.3`)

`agy-acp-gateway` 深度融合了成熟渠道池网关（`sub2api` / `CLIProxyAPI`）的分组与权重调度体验，并针对官方 `agy acp` 有状态进程模型设计了严格的个人 OAuth 防封护栏。

### 1. 单容器物理沙盒与一账号一代理隔离

在 `:18891` 面板添加的每一个子账号（`acct-b`、`acct-c` 等），均拥有独立的数据沙盒（`<data_dir>/accounts/<id>/`）与本地回环子网关进程（`127.0.0.1:28902+`）：

- **零状态串染**：独立隔离 `HOME`、`.gemini/antigravity-cli/antigravity-oauth-token`（`0600` 权限）、`workspace`、`gateway.db`、`tos-tripped` 与 `logs/gateway.log`。
- **账号级出口代理**：每个账号可独立配置 `direct`（直连）、命名代理预设（`AGY_PROXY_PRESETS_FILE` + `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN`，缺令牌时自动阻断防漏宿主机真实 IP），或自定义 `http://`、`https://`、`socks5://`、`socks5h://` 代理。
- **符合 C1 红线的出口探测 (`POST /api/accounts/probe-ip`)**：检测脱敏出口 IP/地区，并通过官方 `agy models`（已授权时）或 `generate_204` 验证 Google 连通性，绝不在 `agy` 之外裸发 OAuth 令牌。

### 2. 五种池与渠道分组路由策略

可在控制台首页顶部的 **池路由状态栏 (`#pool-ctrl-bar`)** 一键实时切换，也可为每个 **渠道分组 (`AccountGroup`)** 单独指定策略：

| 策略标识 | 别名 | 调度行为说明 | 推荐场景 |
| :--- | :--- | :--- | :--- |
| **`round-robin`** *(默认)* | `rr` | 在池/组内所有活跃、健康且未冷却的账号间均匀轮询分发新会话 | 同档位多账号日常均衡负载 |
| **`fill-first`** | `priority` | 按 `priority`（`1..1000` 越小越优先）排序，同优先级按 `2%` 离散剩余额度桶、并发数与配置顺序裁决 | 优先消耗即将到期或活动号额度，主力号作热备 |
| **`weighted-rr`** | `weighted`, `wrr` | 变动集限幅版 Nginx 平滑加权轮询（`weight` `1..100`，如 Ultra `20x` 设 `10`、Pro 设 `1`），消除突发饥饿 | Google AI Ultra `20x` 与 AI Pro 混合组池 |
| **`least-load`** | `smart`, `quota-aware` | 最小化综合负载分 `inFlight / maxConcurrency - 0.35 * remainingQuota` | 多人团队高并发编码、长短请求混跑 |
| **`manual-active`** | `manual`, `active` | 固定路由至当前激活主用号 `activeAccountId`；`manualAutoFailover: true`（默认）时故障自动降级，设为 `false` 时严格锁死不切号 | 个人开发者确定性控号 + 一键手动切号 |

### 3. Key 后缀绑定、渠道分组与请求头路由矩阵

无需重启客户端，即可通过专属 Key、主 Key 后缀或请求头将流量精准导向指定账号或分组池：

| 路由模式 | 客户端传参写法 | `:18891` 管理面权限 | 模型白名单校验 |
| :--- | :--- | :--- | :--- |
| **控制台客户端 Key (`agy-ck-*`)** | `Authorization: Bearer agy-ck-...` *(`:18891` 签发，服务端仅存 SHA-256)* | **拒绝 (`401`)** | Key `allowedModels` ∩ 作用域（`global` / `group` / `account`）白名单 |
| **账号专属 Key (`boundKeys`)** | `Authorization: Bearer sk-acct-only-key` | **拒绝 (`401`)** | 账号 `allowedModels` |
| **分组池专属 Key (`boundKeys`)** | `Authorization: Bearer sk-team-group-key` | **拒绝 (`401`)** | 分组 `allowedModels` ∩ 账号 `allowedModels` |
| **主 Key + 账号后缀** | `<master_key>@acct:<id>` 或 `<master_key>@<id>` *(亦支持 `#`)* | 仅限 API 数据面 | 账号 `allowedModels` |
| **主 Key + 分组后缀** | `<master_key>@group:<group_id>` *(亦支持 `#group:<group_id>`)* | 仅限 API 数据面 | 分组 `allowedModels` ∩ 账号 `allowedModels` |
| **显式路由请求头** | `X-Agy-Account: <id>` 或 `X-Agy-Group: <group_id>` *(配合主/全局 Key)* | 主 Key *(仅未设 `AGY_DASHBOARD_PASSWORD` 时)* | 若 `X-Agy-Account` 不属于 `X-Agy-Group` 直接返回 `403` |
| **全局默认池调度** | `<master_key>` | 主 Key *(仅未设 `AGY_DASHBOARD_PASSWORD` 时)* | 命中账号的 `allowedModels` |

每次响应均回传可观测路由响应头（已配置 CORS 暴露）：
`X-Agy-Account`、`X-Agy-Group`、`X-Agy-Route-Mode`、`X-Agy-Failover-Count`、`X-Agy-Served-Model`、`X-Agy-Cache-Warning`。

### 4. 首轮流前故障转移 vs. Decision U8 防封粘性连续性

```text
  [全新对话第 1 轮请求] (!isContinuation，例如 [system, user_1])
         │
         ├──► 选中账号 A ──► 响应头写出前遇 429 / 529 / 502 / 503 / 401(#3501)？
         │                        │ 是（最多自动跳接 MaxFailoverHops = 2 次）
         │                        ▼
         │                记录账号 A 家族冷却 + 透明切换至健康账号 B 重放首轮请求
         │                        │
         └──► 账号 B 正常接单 ──► 提交粘性绑定 Sticky(callerScope | sessionKey -> B)
                                  + 输出响应头 X-Agy-Account: B, X-Agy-Failover-Count: 1

  [进行中的多轮续接请求] (isContinuation，例如第 2+ 轮 / tool_result / function_call_output)
         │
         └──► Strict U8 防封护栏 (`strictU8NoMidTurnHop = true`)：
              始终锁定在原账号 B（即使 B 处于平滑排空 `draining` 或短暂限流冷却中）。
              绝不将半截工具调用或多轮上下文重放到其他个人 OAuth 账号（若 B 已停用/凭证失效则返回 503）。
```

- **为什么必须坚守 U8 边界**：在多轮 Agent 编码中途将包含数十轮工具调用的历史轨迹突然切往另一个个人 Google 账号，不仅会瞬间打碎 `100%` 的 TPU KV 缓存，还会造成明显的跨账号轨迹指纹异常。仅在**全新第 1 轮**执行负载均衡与流前故障转移、而在**第 2 轮及后续回合**严格粘性锁定，兼顾了高可用容灾与账号长期安全。
- **静态 System Prompt 首轮精准识别**：针对无状态 OpenAI `/v1/chat/completions` 客户端常见的 `[system, user]` 首轮请求，`inspectConversationTurns` 会准确将其判定为首轮（`isContinuation = false`），并联合 `(system + 首条 user)` 计算会话指纹 —— 既保证同一 IDE 新开的多个对话窗口均匀打散到不同账号，又保证第 2+ 轮（`[system, user, assistant, user_2]`）命中完全一致的首轮指纹实现无缝粘性续接！

### 5. 按模型家族独立冷却、平滑排空与一键轮转

- **模型家族级精细冷却隔离**：限流（`429`）与过载（`529`）按模型家族（`gemini` / `claude` / `argon`）独立计时。某账号在 `Claude Opus 5.5` 上触发 `429` 冷却时，该账号的 `Gemini 4 Neon` / `Carbon` 依然 `100%` 正常接单！
  - `429`：优先遵循上游 `Retry-After`（上限 `6h`），否则按连续命中阶梯退避（`30s -> 60s -> 2m -> 5m -> 10m`）。
  - `529`：`15s -> 30s -> 60s` 家族退避（绝不截断已有的更长 `429` 冷却窗口）。
  - `401` / `403 (#3501)`：仅对该账号施加 `30m` `auth_error` 隔离，不误触全局 ToS 熔断器。
  - 本地快速探测接口（`count_tokens`、`:countTokens`、`GET /v1/models`）均标记 `skipHealth`，绝不会因本地返回 `200 OK` 而误清上游冷却状态。
- **控制台与 API 一键运维动作 (`POST /api/accounts/action`)**：
  - **轮转下一号 (`rotate_next`)**：一键将池或分组的 `activeAccountId` 切换至下一个健康已授权账号。
  - **排空并切换 (`drain_and_rotate`)**：原子将旧账号置为 **`draining`（平滑排空）**（不再接收新会话，但存量进行中的多轮对话无感跑完），同时激活下一健康账号接管新流量。

---

<a id="models"></a>

## 支持模型、额度池自动识别与思考档位映射

### 标准模型矩阵 (`agy 1.5.14+`)

| 标准模型 ID | 上游显示名称 | 归属额度池 | 上下文窗口 | 最大图片数 | 支持思考档位 | `xhigh` 极高档 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`gemini-4-carbon`** *(默认)* | Gemini 4 Carbon | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` | 自动收敛至 `high` |
| **`gemini-4-neon`** | Gemini 4 Neon | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high`, **`xhigh`** | **原生支持** |
| **`gemini-4-hydrogen`** | Gemini 4 Hydrogen | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high`, **`xhigh`** | **原生支持** |
| **`gemini-4-argon`** | Gemini 4 Argon | Argon / Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` | 自动收敛至 `high` |
| **`gemini-3.8-flash`** | Gemini 3.8 Flash | Gemini 池 | `1,048,576` (`900K` 安全区) | `200` | `low`, `medium`, `high` | 自动收敛至 `high` |
| **`claude-opus-5-5`** | `Claude` Opus 5.5 | 第三方 (`Claude`/GPT) 池 | `1,048,576` (`900K` 安全区) | `100` | `low`, `medium`, `high` | 自动收敛至 `high` |
| **`claude-sonnet-5-5`** | `Claude` Sonnet 5.5 | 第三方 (`Claude`/GPT) 池 | `1,048,576` (`900K` 安全区) | `100` | `low`, `medium`, `high` | 自动收敛至 `high` |

- **Google AI Pro 与 Ultra `20x` 额度池自动识别**：面板根据 `<agy> -p /usage` 返回的桶拓扑自动匹配真实美元上限 —— **AI Pro**（Gemini `$10` 5h / `$35` 周度 + 第三方 `$2.50` / `$5` + 独立 Argon 周池）与 **AI Ultra `20x`**（Gemini `$200` 5h / `$800` 周度 + 第三方 `$25` / `$50`）。
- **智能别名解析**：自动剥离 `models/`、`[1m]` 及尾部思考档位后缀（`-low`/`-medium`/`-high`/`-xhigh`），开箱支持 `carbon`、`neon`、`hydrogen`、`argon`、`gemini-flash`、`agy-opus`、`agy-sonnet` 等简写；设置 `AGY_ROUTE_CLAUDE_NAMES=1` 可将通用 `*opus*` / `*sonnet*` / `*haiku*` 模型名自动映射至对应上游模型。

### 四协议思考档位统一映射表

请求体显式参数优先级高于模型名后缀（如 `gemini-4-neon-xhigh`），未指定时默认使用 `medium`：

| 网关标准档位 | 模型名后缀 | OpenAI (`reasoning_effort` / `reasoning.effort`) | `Anthropic` (`thinking.budget_tokens`) | Gemini `/v1beta` (`thinkingLevel` / `thinkingBudget`) |
| :--- | :--- | :--- | :--- | :--- |
| **`low`** | `-minimal`, `-low` | `none`, `minimal`, `low` | `type: "disabled"` 或 `<= 4096` | `NONE`, `MINIMAL`, `LOW` 或 `0..4096` |
| **`medium`** *(默认)* | `-medium` | `medium` *(或缺省)* | `4097 .. 16384` | `MEDIUM` 或 `4097..16384` |
| **`high`** | `-high` | `high` | `16385 .. 32768` | `HIGH` 或 `16385..32768` |
| **`xhigh`** *(Neon / Hydrogen)* | `-xhigh`, `-max`, `-ultra` | `xhigh`, `max` | `> 32768` | `XHIGH` 或 `> 32768` |

---

<a id="engineering-pillars"></a>

## 核心工程能力详解
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

1. **五层 Carbon KV 缓存防御与断连救援**：
   - 核验 `agy 1.5.14+` 原生 CCPA `SessionId` 透传（`SPEC_VERSION=4`）并配合 Gate 5 轨迹 `sessionID` 注入，杜绝多并发会话因空缓存键互相冲刷。
   - 冻结首轮 `system`/`developer` 提示词与初始工具定义；当客户端动态加载延迟 MCP 工具（如 `ToolSearch`）时，通过 `TOOLS_UPDATED_IN_PLACE` 原地刷新桥接目录而不重启进程。
   - **Prefill Soft-Detach + Singleflight**：预填充已耗时 `>=30s` 的重型请求遇客户端断连时保留 `45s` 热算力窗口，重试请求直接复用同一上游流。
2. **双通道 MCP 工具挂起桥 (`ws-bridge`) 与并行工具合并**：
   - 内置仅监听本地回环的临时 Streamable HTTP MCP 桥（`/mcp/{tenantKey}`），拦截上游工具调用并转换为四大协议原生工具结构返回客户端，下一轮请求自动唤醒挂起的工作进程。
   - 从 `.db` `PlannerResponse` 步骤中提取同轮并行工具调用一次性打包下发，并限制空回合孤儿接力上限（`AGY_MAX_TOOL_RELAYS=4`）。
3. **三层语义净室与自愈式 Flash WAF 二分**：
   - 使用会话级 `HMAC-SHA256` 可逆令牌（`reversible_codec.go`）无损替换客户端注入的敏感元标签、宿主机路径与保留工具名，并在出站 SSE 流中实时透明还原。
   - 遇未知上游 `400` WAF 拦截时，自动通过低成本 `gemini-3.8-flash` 探针二分定位触发片段并持久化最小重写规则（`AGY_WAF_AUTOBISECT=true`）。
4. **零锁 `.1.19` SQLite 遥测直读与静默重试感知**：
   - 纯 Go 解析 SQLite B-Tree 与 `.db-wal` 帧（`internal/store/sqlite_protobuf.go`），零写锁竞争提取真实输入/缓存/思考/输出 Token、`step_type=17` 上游静默退避重试计数与耗时（`agy 1.5.13+`）、Argon `.1.19` 影子路由告警及桌面端 `AGY_APP` 只读遥测。
5. **双端口严格物理隔离、统一结构化输出与防 SSRF 多模态 v2 管线**：
   - `:18890` 仅暴露 AI 生成协议与 `/healthz`（访问 `/`、`/api/*`、`/metrics` 均返回 `404`）；`:18891` 仅服务管理员鉴权的控制台、账号/客户端 Key 管理与监控指标（访问 `/v1/*`、`/v1beta/*` 均返回 `404`）。配置 `AGY_DASHBOARD_PASSWORD`（支持明文或 `sha256:<hex>` 摘要）即可实现管理面密码与数据面 API Key 彻底解耦。
   - 四协议统一支持 JSON Schema 结构化输出（`output_config.format`、`response_format`、`text.format`、`responseJsonSchema`），暴露精确 `max_input_tokens` 模型元数据，并对 `/v1/responses` 严格校验会话续接状态（`previous_response_not_found`、`thread_not_found`）。
   - 远程图片拉取具备 `16` 字节魔数 MIME 嗅探、`64MB/15m` 增量缓存及内核级 `net.Dialer.Control` 私网/链路本地地址阻断，彻底防御 DNS 重绑定 SSRF。

---

<a id="configuration-reference"></a>

## 接口矩阵与 Prometheus 指标参考

### 端口与接口总览

| 服务面 | 方法与路径 | 鉴权要求 | 说明 |
| :--- | :--- | :--- | :--- |
| **数据面 `:18890`** | `GET /healthz` | 免鉴权 | 容器/负载均衡存活探针（`{"ok":true}`） |
| **数据面 `:18890`** | `GET /v1/models`, `GET /v1beta/models` | API Key（客户端 `agy-ck-*` / 主 Key / 专属 Key / `?key=`） | 按调用方客户端 Key、所属账号或分组白名单过滤后的模型列表 |
| **数据面 `:18890`** | `POST /v1/messages`, `POST /v1/messages/count_tokens` | `x-api-key` 或 `Bearer` | `Anthropic` Messages API（流式/非流式/工具/视觉/思考） |
| **数据面 `:18890`** | `POST /v1/chat/completions` | `Authorization: Bearer` | OpenAI Chat Completions 接口 |
| **数据面 `:18890`** | `POST /v1/responses` | `Authorization: Bearer` | OpenAI Responses 接口（`Codex` CLI 原生协议） |
| **数据面 `:18890`** | `POST /v1beta/models/{m}:generateContent` / `:streamGenerateContent` / `:countTokens` | `x-goog-api-key`、`Bearer` 或 `?key=` | Google Gemini 原生 REST v1beta 接口 |
| **管理面 `:18891`** | `GET /`, `GET /api/dashboard`, `GET /api/quota` | **管理员密码**（`AGY_DASHBOARD_PASSWORD`，未设回退主 Key；Basic/Bearer/`x-api-key`） | 暖纸实时控制台、多节点联邦数据与额度快照 |
| **管理面 `:18891`** | `GET /metrics` | **管理员密码**（未设回退主 Key） | Prometheus `0.0.4` 网关与多账号指标 |
| **管理面 `:18891`** | `GET/POST/DELETE /api/accounts`, `/routing`, `/groups`, `/action`, `/probe-ip`, `/oauth/*` | **管理员密码 + CSRF 校验**（`X-Agy-Requested-With: dashboard`） | 多账号沙盒、渠道分组、路由策略、一键轮转与网页 PKCE OAuth |
| **管理面 `:18891`** | `GET/POST/DELETE /api/client-keys`, `/update`, `/toggle` | **管理员密码 + CSRF 校验**（`X-Agy-Requested-With: dashboard`） | 签发、作用域（`global`/`group`/`account`）、模型白名单、启停与吊销仅限 `:18890` 的 SHA-256 客户端 Key |
| **管理面 `:18891`** | `GET /api/upgrade`, `POST /api/upgrade/approve` | **管理员密码 + CSRF 校验** | `G1-G4` 官方 `agy` 升级状态查询与一键热切换审批 |

### 核心环境变量速查表 (`.env`)

| 变量名 | 默认值 | 作用说明 |
| :--- | :--- | :--- |
| `AGY_DASHBOARD_PASSWORD` / `AGY_DASHBOARD_PASSWORD_FILE` | *(可选，未设回退 `AGY_API_KEYS`)* | 独立保护 `:18891` 控制台/管理 API/指标的管理员密码（支持明文或 `sha256:<64位hex>`），与 `:18890` 彻底解耦 |
| `AGY_API_KEYS` / `AGY_API_KEYS_FILE` | *(本地回环可空)* | 逗号分隔 `:18890` 主密钥（支持 `@acct:<id>` / `@group:<id>` 后缀；仅当未设 `AGY_DASHBOARD_PASSWORD` 时兼容登录 `:18891`） |
| `AGY_CLIENT_KEYS_FILE` | `<data>/client_keys.json` | 控制台签发客户端 Key（`agy-ck-*`，仅限 `:18890` 数据面）的 SHA-256 哈希持久化路径 |
| `AGY_GW_PORT` / `AGY_DASH_PORT` | `18890` / `18891` | 数据面 API 端口与管理面控制台/监控端口 |
| `AGY_ACCOUNTS_FILE` | `<data>/accounts.json` | 多账号沙盒、`AccountGroup` 渠道分组与池路由配置持久化路径 |
| `AGY_PROXY_PRESETS_FILE` | *(可选)* | 外部命名出口代理预设 JSON 文件路径 |
| `AGY_DASHBOARD_PEERS` | *(可选)* | 跨机多节点联邦面板配置（`id\|label\|url\|key;...`） |
| `AGY_UPSTREAM_PROXY` | *(空 / `127.0.0.1:7890`)* | 主账号 `agy` 子进程默认 HTTP/SOCKS5 出口代理 |
| `AGY_MAX_LIVE_WORKERS` | `8` | 主账号池最大热存活 `agy acp` 会话数 |
| `AGY_IDLE_WORKER_TTL_SEC` | `1200` | 空闲 ACP 会话休眠回收秒数（默认 `20m`） |
| `AGY_PARKED_TTL_SEC` | `300` | MCP 工具调用挂起等待恢复超时秒数（默认 `5m`） |
| `AGY_PREFILL_CANCEL_GRACE_SEC` | `45` | 慢预填充（`>=30s`）客户端断连后的热算力保留秒数 |
| `AGY_WAF_AUTOBISECT` | `true` | 上游 `400` 误拦截自动 Flash 二分定位与规则自愈开关 |
| `AGY_CAPACITY_FALLBACK_TTL_SEC` / `AGY_CAPACITY_FALLBACK_MAX_TTL_SEC` | `300` / `1800` | 自动降档保护的冷却恢复窗口与指数退避上限秒数（`0` 为会话期内永久锁定） |

*（完整环境变量清单请查阅注释详尽的 [`.env.example`](.env.example)。）*

### 核心 Prometheus 监控指标 (`GET :18891/metrics`)

- **网关全局与缓存指标**：`agy_gateway_store_ready`、`agy_gateway_live_workers`、`agy_gateway_hibernated_sessions`、`agy_gateway_parked_calls`、`agy_gateway_prompts_inflight`、`agy_gateway_requests_total`、`agy_gateway_cache_burn_warnings_total`、`agy_gateway_capacity_fallback_active_sessions`、`agy_gateway_capacity_fallback_events_total`、`agy_gateway_capacity_fallback_probes_total`、`agy_gateway_soft_detach_total{outcome}`、`agy_gateway_upgrade_state{state}`、`agy_gateway_turns_total{model,mode,stop_reason,usage_source}`、`agy_gateway_tokens_total{model,kind}`。
- **多账号池指标 (`v2.3`)**：`agy_account_inflight{account,state}`、`agy_account_sticky_sessions{account}`、`agy_account_cooldown_seconds{account}`、`agy_account_requests_total{account}`、`agy_account_failures_total{account}`、`agy_account_rate_limits_total{account}`、`agy_account_failovers_total{account}`。

---

<a id="safe-upgrader"></a>

## 四门禁安全升级 (`G1-G4`) 与零额度测试套件

针对官方 `agy` 频繁发版，本机升级脚本（`scripts/agy-upgrade.sh`）与宿主机生产自动升级代理（`deploy/prod-upgrader/agy_prod_upgrader.py`）在替换 `bin/agy-acp-900k` 前均强制执行 **四道门禁 (`G1-G4`)**：

1. **G1（多架构供应链哈希与版本校验）**：交叉核验 `linux_amd64`、`linux_arm64` 与 `darwin_arm64` 双源 `SHA-512` / `SHA-256` 摘要、文件字节大小与 `--version`。
2. **G2（二进制补丁与原生 `SessionId` 审计）**：执行 `900K` 上下文解锁（`agy-patch.py`）与轨迹 `SessionId` 静态核验（`tools/agy_extra_patch.py --check`，`SPEC_VERSION=4`）。
3. **G3（零额度离线 ACP 契约测试）**：在一次性隔离目录中运行 `scripts/agy_acp_smoke.py`，验证 JSON-RPC stdio 握手与模型列表契约。
4. **G4（隔离沙盒真路径验证与软链热切换）**：在独立测试实例（`:18892`）跑通文本生成 + 2 步 MCP 工具闭环，待面板点击确认且在途请求排空后原子切换 `data/agy/current` 软链（异常自动回滚）。

```bash
# 只读探测上游最新正式版 agy（零额度消耗）
python3 scripts/upstream_watch.py --official-only
./scripts/agy-upgrade.sh --status

# 运行全量零额度 Go 竞态单测 + Python 升级门禁单测
cd go-gateway && go test -race -count=1 ./... && cd ..
python3 -m unittest discover -s scripts -p "test_*.py"
python3 -m unittest discover -s deploy/prod-upgrader -p "test_*.py"
```

---

<a id="troubleshooting"></a>

## 常见问题排障 (Troubleshooting & FAQ)

<details>
<summary><strong>1. 报错 <code>403 permission_error</code>（提示该账号或分组不允许访问某模型）</strong></summary>

当前使用的专属 Key 或分组绑定了受限模型白名单 `allowedModels`。打开 `:18891` -> **账号控制中心** -> 编辑对应账号或渠道分组，点击 **全选全部模型**（或勾选所需标准模型）并保存即可立即生效。
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
<summary><strong>5. 自定义脚本调用 <code>POST / DELETE /api/accounts*</code> 返回 <code>403 Forbidden</code></strong></summary>

管理面写接口启用了 Origin 与 CSRF 双重校验。CLI `curl` 脚本调用时请勿携带浏览器 `Origin` 与 `Sec-Fetch-Site` 头；若从带 `Origin` 的上下文调用，须同时保证 `Origin` 与 `Host` 同源且附带 `-H "X-Agy-Requested-With: dashboard"`。
</details>

<details>
<summary><strong>6. 命名代理预设报错“缺少 AGY_RESIN_PROXY_TOKEN”</strong></summary>

为防止配置了代理隔离的账号意外回退宿主机直连泄露真实 IP，命名预设在缺少 `AGY_RESIN_PROXY_TOKEN` 时强制失败关闭（Fail-Closed）。请在 `.env` 补齐 `AGY_RESIN_HOST` 与 `AGY_RESIN_PROXY_TOKEN`，或将该账号代理模式改为 **`custom`（自定义代理 URL）** 或 **`direct`（宿主机直连）**。
</details>

<details>
<summary><strong>7. 使用 <code>scripts/gw.sh</code> 停止非默认端口的独立测试实例</strong></summary>

非默认端口会自动隔离到 `runtime/instance-<port>` 目录。停止测试实例时必须传入与启动时完全一致的端口变量：`AGY_GW_PORT=18892 AGY_DASH_PORT=18893 ./scripts/gw.sh stop`。严禁在有在途流量的机器上直接执行 `pkill -x agy-acp-900k`。
</details>

---

## 安全与隐私边界

- **严守官方子进程边界 (红线 C1)**：绝不伪造 Google OAuth 或私有 Protobuf HTTP 请求，所有模型流量 `100%` 经由官方 `agy acp` 执行。
- **全链路敏感信息脱敏**：运行日志自动擦除 Bearer 令牌、`?key=` 参数与代理凭据（含单用户名 Token）；`/api/dashboard` 对账号/分组 `boundKeys` 做掩码预览脱敏，仅主 Key 可访问的 `/api/accounts` 自动掩码脱敏 `customProxy` 代理密码。
- **零外部遥测与严格管理面隔离**：不向任何第三方收集或上报数据；暖纸控制台与全套 Lucide 图标均通过 `//go:embed` 完整内嵌于单二进制中，`AGY_CORS_ORIGINS` 严格限定于 `:18890` 数据面接口（`/v1/*`、`/v1beta/*`），绝不向 `:18891` 管理端点放开跨域读取。

---

## 版本更新记录

<a id="changelog"></a>

- **2026-11-23 (`v2.3.1 — 合入并深化社区 PR #1：配额 HOME 隔离、macOS 无头凭据直读与冷启动非阻塞`)**：
  - **按账号目录隔离 `HOME` 与 macOS 文件凭据强制直读 (`dashboard.go`, `acp_engine.go`, `account_oauth.go`, `account_manager.go`)**：合入并扩展社区 PR #1（`@HamsteRider-m`），引入 `resolveIsolatedAgyHome`（按 profile 隔离 `quota-home-<profile>` 原子 `.gemini` 软链，子实例 `.gemini` 直接解析父目录）并在 `darwin` 全子进程链路（`/usage`、`agy acp`、多账号 OAuth 与子工作进程）自动注入 `SSH_CONNECTION` 走文件 Token，彻底根治宿主机未登录或 macOS Keychain 弹窗导致的 22s `signal: killed` 阻塞。
  - **配额冷启动与错误态全非阻塞 (`dashboard.go`)**：`FetchOfficialQuota(false)` 冷启动在 `<1ms` 内立即返回 `loading` 态并在主协程同步快照环境后异步刷新，同时对命令失败与非 JSON 输出统一持久化 `lastQuotaErr` 并叠加指数退避（`30s -> 2m -> 5m -> 15m`），确保 `/api/dashboard` 与多节点联邦拉取永不卡顿。
  - **客户端识别扩展与代理请求头消歧 (`internal/protocol/client_detect.go`)**：新增 `GenericAgent`（`ga-platform` / `ws_call_tool`）与 `T3 Code`（`<t3_code_instructions>`）识别，并在聚合代理注入 `Originator` / `User-Agent` 时优先采信明确的提示词与工具集指纹。
- **2026-11-22 (`v2.3.0 — 多账号池 v3、渠道分组、首轮流前故障转移、agy 1.5.14 原生 SessionId 与安全加固`)**：
  - **多账号池与渠道分组 (`account_scheduler.go`, `account_manager.go`)**：新增 5 种池/分组调度策略（`round-robin`、`2%` 离散额度桶 `fill-first`、变动集限幅版 Nginx `weighted-rr`、`least-load` 及支持严格锁定的 `manual-active`）、支持专属 Key 与模型白名单的 `AccountGroup` 渠道池、`@acct:<id>` / `@group:<id>` / `X-Agy-Group` 路由，以及一键 `rotate_next` 与 `drain_and_rotate` 排空轮转动作。
  - **首轮流前自动故障转移与 U8 防封连续性**：新增子实例首轮 `429`/`529`/`502`/`503`/`401(#3501)` 流前透明跳接（`X-Agy-Failover-Count`）、`[system, user]` 首轮与续接精准指纹识别（`inspectConversationTurns`）、按模型家族（`gemini` / `claude` / `argon`）独立冷却、结构化 SSE 尾部错误帧解析及 `count_tokens` 非上游健康隔离。
  - **暖纸多账号控制中心与可观测性**：升级 `:18891` 内嵌控制台，新增首页池路由快捷控制栏、账号池与渠道分组双标签管理、卡片级实时并发/冷却徽章，以及 7 项 `agy_account_*` Prometheus 指标。
  - **上游 `agy 1.5.14` 适配与安全加固**：核验 `agy 1.5.14` 原生 CCPA `SessionId` 透传（`SPEC_VERSION=3`），封堵 Gemini `:action` 冒号注入绕过，补齐 OAuth/探活全链路代理失败关闭校验、`/api/dashboard` 分组 Key 掩码及单用户名代理 URL 日志脱敏。
- **2026-11-20 (`v2.2.0 — 单容器多账号隔离沙盒、Pro/Ultra 额度识别、联邦大盘与 agy 1.5.13 静默重试感知`)**：
  - 新增单容器多账号物理沙盒、网页无头 PTY PKCE OAuth 授权、Key 级模型白名单、Google AI Pro 与 Ultra `20x` 额度池上限自动检测、多实例联邦控制台（`AGY_DASHBOARD_PEERS`）以及零补丁 SQLite `step_type=17` 静默重试感知。
- **2026-11-19 (`Gemini 4 Neon、热路径零拷贝优化与思考档位同步`)**：
  - 新增 `gemini-4-neon`（原生 `xhigh` 档位）、热路径 `gzip`/JSON 缓冲池复用、净室正则惰性初始化与 SQLite `TurnUsage.ActualTier` 真实档位解码。
- **2026-11-18 (`agy 1.5.11+ 无环 internal/* 架构演进与缓存保护 v2`)**：
  - 拆分 7 子包无环 `go-gateway/internal/` 架构（由 AST 单测守护）、SQLite WAL 读写池分离、首轮系统提示词与工具前缀跨重启冻结、自适应 `PrefillSoftDetach` 与桌面端 `AGY_APP` 只读遥测。

<details>
<summary><strong>更早历史里程碑 (2026-11-08 至 2026-11-17)</strong></summary>

- **2026-11-16 ~ 2026-11-17**：上线防 SSRF 三阶段多模态 v2 管线、`#3501` `403` 非熔断隔离、`128MB` `ReadSlice('\n')` 管道防死锁读取、Gate 5 独立轨迹 `sessionID` 二进制注入与 Prefill Soft-Detach + Singleflight。
- **2026-11-12 ~ 2026-11-15**：推出会话级 `HMAC-SHA256` 可逆净室编码器、Flash WAF 自动二分自愈、Google Gemini 原生 REST（`/v1beta`）适配、`gemini-4-hydrogen`（`xhigh`）与 `TOOLS_UPDATED_IN_PLACE` 原地工具更新。
- **2026-11-08 ~ 2026-11-11**：完成纯 Go（`CGO_ENABLED=0`）单二进制重写、零锁 `.db`/`.1.19` Protobuf 遥测、`PlannerResponse` 并行工具合并与 `G1-G4` 四门禁安全升级流水线。

</details>

---

## 开源协议

本项目基于 [MIT License](LICENSE) 开源发布。
