<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/banner-zh-dark.svg" />
    <img src="assets/banner-zh-light.svg" alt="agy-acp-gateway" width="760" />
  </picture>
</p>

<p align="center">
  将 Google Antigravity CLI (<code>agy</code>) 转为标准 Anthropic、OpenAI、Gemini API，自带多账号管理后台。
</p>

<p align="center">
  <a href="./README.md"><strong>English</strong></a> &nbsp;·&nbsp;
  <a href="./README_ZH.md"><strong>简体中文</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>快速开始</strong></a> &nbsp;·&nbsp;
  <a href="#client-setup"><strong>客户端配置</strong></a> &nbsp;·&nbsp;
  <a href="#models"><strong>模型与附件</strong></a> &nbsp;·&nbsp;
  <a href="#image-generation"><strong>生图与改图</strong></a> &nbsp;·&nbsp;
  <a href="#multi-account"><strong>多账号管理</strong></a> &nbsp;·&nbsp;
  <a href="#configuration-reference"><strong>接口与配置</strong></a> &nbsp;·&nbsp;
  <a href="#troubleshooting"><strong>常见问题</strong></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+" />
  <img src="https://img.shields.io/badge/API-Anthropic_%7C_OpenAI_%7C_Gemini-0856F5?style=flat-square" alt="APIs" />
  <img src="https://img.shields.io/badge/Models-11_Chat_%2B_Image-7C3AED?style=flat-square" alt="12 Models" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-22c55e?style=flat-square" alt="MIT License" /></a>
</p>

---

网关在 `:18890` 提供 AI 接口，在 `:18891` 提供网页后台，底层直接调用官方 `agy` 客户端收发请求：

- **多协议接口 (`:18890`)**：兼容 Anthropic `/v1/messages`、OpenAI `/v1/chat/completions` 与 `/v1/responses`、OpenAI `/v1/images/*`、Google Gemini `/v1beta`。
- **12 个模型**：包含 11 个对话模型（Gemini 4 系列、`gemini-3.8-flash`、Claude Opus/Sonnet 5.5、`gpt-oss-120b-medium`、`gemini-pro-orchestrator` 系列）与 `gemini-3.1-flash-image` 生图模型。
- **多账号池 (`:18891`)**：集中管理多个 Google 账号，可按账号单独设置 HTTP/SOCKS5 代理、可用模型、专属 Key 与备用号顶替。
- **单文件低内存**：单个可执行文件已内置网页后台，无外部依赖；空闲进程自动休眠，6 GB 内存可同时跑约 20 个 `Claude Code` / `Codex` 会话。

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/arch-overview-zh-dark.png" />
    <img src="assets/arch-overview-zh-light.png" alt="agy-acp-gateway 架构图" width="920" />
  </picture>
</p>

---

<a id="quick-start"></a>

## 快速开始

### 环境要求

- 系统：Linux（`linux_amd64` / `linux_arm64`）、macOS（`darwin_amd64` / `darwin_arm64`）或 Windows WSL2。
- 已安装官方 `agy`（默认路径 `~/.local/bin/agy`），建议 `1.5.16` 版本（`1.5.17`、`1.5.18` 有多扣额度 bug）。
- 打 `900K` 上下文补丁需 Python `3.10+`；网关程序可直接从 [Releases](https://github.com/yuhxuiao/agy-acp-gateway/releases) 下载预编译包，或安装 Go `1.26+` 从源码编译。

### 1. 准备二进制文件

```bash
git clone https://github.com/yuhxuiao/agy-acp-gateway.git
cd agy-acp-gateway
mkdir -p bin

# 获取网关程序（也可直接从 GitHub Releases 下载对应架构压缩包解压到 bin/agy-acp-gateway）
cd go-gateway && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/agy-acp-gateway . && cd ..

# 基于本机官方 agy 生成 900K 上下文副本（不会修改 ~/.local/bin/agy 原文件）
cp ~/.local/bin/agy bin/agy-acp-900k
python3 third_party/agy-context-unlock/agy-patch.py --binary bin/agy-acp-900k --acp-only --in-place --no-backup
python3 tools/agy_extra_patch.py --binary bin/agy-acp-900k --in-place
```

### 2. 启动服务

**方式一：Docker Compose（推荐服务器部署）**

```bash
cp .env.example deploy/.env
# 编辑 deploy/.env，修改 AGY_API_KEYS（如需代理再填 AGY_UPSTREAM_PROXY）
docker compose -f deploy/docker-compose.yml up -d --build
```

**方式二：本机直接运行**

```bash
cp .env.example .env
# 编辑 .env，修改 AGY_API_KEYS（如需代理再填 AGY_UPSTREAM_PROXY）
./scripts/gw.sh start
```

检查是否启动成功：

```bash
curl -fsS http://127.0.0.1:18890/healthz
# 返回 {"ok":true}
```

### 3. 登录 Google 账号

浏览器打开 `http://127.0.0.1:18891/`（登录框用户名随便填，密码填 `.env` 里的 `AGY_API_KEYS`），点击右上角 **账号**：

- **网页链接授权**（Linux / WSL2 / Docker 推荐；原生 `darwin_arm64` 请用下面两种方式）：点击 **新增账号槽位** 选好代理并保存，再点 **生成 Google 授权链接** 在浏览器登录。登录完成后跳转到 `http://localhost:.../?code=...` 显示无法访问是正常的，把地址栏里的完整链接复制回弹窗输入框提交即可。
- **导入 Token JSON**：在授权弹窗里直接粘贴已有的 `antigravity-oauth-token` JSON 内容。
- **命令行登录**（仅主账号）：运行 `./bin/agy-acp-900k login`（Docker 容器内执行 `docker exec -it agy-acp-gateway /app/bin/agy-acp-900k login`）。

### 4. 测试调用

```bash
curl -sS http://127.0.0.1:18890/v1/chat/completions \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.8-flash",
    "reasoning_effort": "low",
    "messages": [{"role": "user", "content": "回复 READY"}]
  }'
```

---

<a id="client-setup"></a>

## 客户端配置

| 客户端 / SDK | 接口路径 | Base URL | 鉴权方式 |
| :--- | :--- | :--- | :--- |
| **`Claude Code`** | `/v1/messages` | `http://127.0.0.1:18890` | `x-api-key` / `ANTHROPIC_API_KEY` |
| **`Codex` CLI** | `/v1/responses` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` |
| **Cursor / `Cline` / `Roo Code` / `OpenCode`** | `/v1/chat/completions` | `http://127.0.0.1:18890/v1` | `Authorization: Bearer` |
| **Gemini CLI / `google-genai` SDK** | `/v1beta` | `http://127.0.0.1:18890` | `x-goog-api-key` 或 `?key=` |
| **Pi Coding Agent** | `/v1/messages` 或 `/v1/responses` | `http://127.0.0.1:18890` | `x-api-key` / `Bearer` |

### `Claude Code`

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:18890"
export ANTHROPIC_API_KEY="sk-your-master-key"
export ANTHROPIC_MODEL="gemini-4-carbon-medium"
export ANTHROPIC_SMALL_FAST_MODEL="gemini-3.8-flash-low"
claude
```

### `Codex` CLI (`~/.codex/config.toml`)

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

### OpenAI SDK / 通用客户端

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:18890/v1",
    api_key="sk-your-master-key",  # 可加 @acct:账号ID 或 @group:分组ID 指定账号或分组
)

resp = client.chat.completions.create(
    model="gemini-4-neon",
    reasoning_effort="high",
    messages=[{"role": "user", "content": "用三句话讲清楚 Go context 的取消传递。"}],
)
print(resp.choices[0].message.content)
```

### Google Gemini `/v1beta`

```bash
curl -sS "http://127.0.0.1:18890/v1beta/models/gemini-4-hydrogen:generateContent" \
  -H "x-goog-api-key: sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [{"role": "user", "parts": [{"text": "列出 Raft 选主的三个关键条件。"}]}],
    "generationConfig": {"thinkingConfig": {"thinkingLevel": "HIGH"}}
  }'
```

---

<a id="models"></a>

## 模型与附件输入

### 支持的模型列表

| 模型 ID | 所属额度池 | 上下文窗口 | 思考档位 | 常用别名 |
| :--- | :--- | :--- | :--- | :--- |
| **`gemini-4-carbon`** *(默认)* | Gemini 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high` | `carbon` |
| **`gemini-4-neon`** | Gemini 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high`, `xhigh` | `neon` |
| **`gemini-4-argon`** | Gemini / Argon 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high` | `argon` |
| **`gemini-4-hydrogen`** | Gemini 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high`, `xhigh` | `hydrogen` |
| **`gemini-3.8-flash`** | Gemini 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high` | `gemini-flash`, `gemini-2.5-flash*` |
| **`claude-opus-5-5`** | Claude / GPT 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high` | `agy-opus` |
| **`claude-sonnet-5-5`** | Claude / GPT 池 | `1M`（建议 `<=900K`） | `low`, `medium`, `high` | `agy-sonnet` |
| **`gpt-oss-120b-medium`** | Claude / GPT 池 | `128K` | 固定 `medium` | `gpt-oss`, `gpt-oss-120b` |
| **`gemini-pro-orchestrator`** | Gemini 池 | `1M`（建议 `<=900K`） | 固定 `high` | `gemini-pro-orchestrator*` |
| **`gemini-pro-orchestrator-neon`** | Gemini 池 | `1M`（建议 `<=900K`） | 固定 `high` | `gemini-pro-orchestrator-neon*` |
| **`gemini-pro-orchestrator-argon`** | Gemini 池 | `1M`（建议 `<=900K`） | 固定 `high` | `gemini-pro-orchestrator-g1-pro` |
| **`gemini-3.1-flash-image`** | Gemini 池 | 生图 / 参考图编辑 | 无 | `gemini-3.1-flash-image-preview` |

- **思考档位**：默认 `medium`。可在请求体传 `reasoning_effort`（OpenAI）、`thinking.budget_tokens` / `output_config.effort`（Anthropic）、`thinkingLevel`（Gemini），也可直接在模型名后加 `-low`、`-medium`、`-high`、`-xhigh`。
- **模型查询与结构化输出**：`GET /v1/models` 和 `GET /v1beta/models` 按当前 Key 的白名单返回模型及 `max_input_tokens`。各对话接口均支持 JSON Schema 结构化输出（`response_format`、`output_config.format`、`responseJsonSchema`）。
- **兼容官方 Claude 模型名**：若客户端写死了 `claude-sonnet-*` 或 `claude-opus-*`，在 `.env` 设 `AGY_ROUTE_CLAUDE_NAMES=1` 即可自动映射。

### 图片、PDF 与音视频输入

- **图片与文本附件**：支持 `PNG`、`JPEG`、`WEBP`、`GIF`、`HEIC`、`HEIF`（单张 `<=10 MiB`，单次总计 `<=32 MiB`）及常见代码/纯文本文件（`<=2 MiB`）。
- **文字版 PDF**：默认支持，所有模型和带工具调用的对话都可直接读取（`AGY_PDF_TEXT_FASTPATH=1`）。
- **扫描版 PDF、音频与视频**：默认关闭。在 `.env` 设 `AGY_MEDIA_RELAY=1` 后，不传工具（`tools`）的普通聊天请求可向 Gemini 传入扫描 PDF、常见音频（`mp3`、`wav`、`m4a`、`ogg`、`flac`、`opus`、`aac`）和视频（`mp4`、`webm`、`mov`），单文件默认上限 `20 MiB`。

---

<a id="image-generation"></a>

## 生图与改图 (`gemini-3.1-flash-image`)

先在 `:18891` 的账号编辑里勾选 `gemini-3.1-flash-image`。支持参数：

- **画幅比例**：`1:1`、`2:3`、`3:2`、`3:4`、`4:3`、`9:16`、`16:9`（或 OpenAI `size` 参数）。
- **张数与参考图**：单次生成 `n=1..4` 张，支持传入 `1..3` 张参考图做局部修改或风格参考。
- **输出格式**：`output_format` 支持 `"jpeg"`（默认）和 `"png"`。

### 1. OpenAI `/v1/images/generations` 与 `/v1/images/edits`

```bash
# 文生图
curl -sS http://127.0.0.1:18890/v1/images/generations \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image",
    "prompt": "浅木桌面上的银色机械键盘与咖啡杯，自然光摄影",
    "aspect_ratio": "16:9",
    "n": 1,
    "output_format": "jpeg",
    "response_format": "b64_json"
  }'

# 带参考图改图（支持 1..3 张 image[]）
curl -sS http://127.0.0.1:18890/v1/images/edits \
  -H "Authorization: Bearer sk-your-master-key" \
  -F "model=gemini-3.1-flash-image" \
  -F "prompt=把这张草图改成干净的黑白工程线稿" \
  -F "aspect_ratio=3:2" \
  -F "output_format=png" \
  -F "image[]=@./sketch.png"
```

### 2. OpenAI `/v1/chat/completions`

方便接入只支持聊天接口的聚合中转（如 `gpt-load`、`new-api`）和对话客户端，图片以 Markdown `![image](data:image/jpeg;base64,...)` 返回：

```bash
curl -sS http://127.0.0.1:18890/v1/chat/completions \
  -H "Authorization: Bearer sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image",
    "aspect_ratio": "4:3",
    "messages": [{"role": "user", "content": "画一只戴着护目镜的橘猫插画"}]
  }'
```

### 3. Google Gemini `/v1beta`

```bash
curl -sS "http://127.0.0.1:18890/v1beta/models/gemini-3.1-flash-image:generateContent" \
  -H "x-goog-api-key: sk-your-master-key" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [{"role": "user", "parts": [{"text": "极简风格的山脉日出海报"}]}],
    "generationConfig": {
      "candidateCount": 1,
      "imageConfig": {"aspectRatio": "16:9"}
    }
  }'
```

---

<a id="multi-account"></a>

## 多账号管理与路由

在 `:18891` 面板点 **账号** 可添加多个 Google 账号（自动识别 Pro、Ultra 5x、Ultra 20x 额度），每个账号独立保存登录态，可单独选直连或 HTTP/HTTPS/SOCKS5 代理，也可将多个账号编成账号分组。

### 账号分配与指定方式

面板顶栏可实时切换分配策略：`轮询调度`（默认）、`优先填满`、`平滑加权`、`最轻负载`、`手动指定`。备用账号可将角色设为 `备用溢出 (Standby)`，平时待命，主力号满载或限流时自动顶上。

| 调用需求 | 客户端写法 | 说明 |
| :--- | :--- | :--- |
| **走全局号池** | `Bearer <主Key>` | 按面板当前策略自动选健康账号 |
| **指定某个账号** | `Bearer <主Key>@acct:<账号ID>` | 也可传请求头 `X-Agy-Account: <账号ID>` |
| **指定某个账号分组** | `Bearer <主Key>@group:<分组ID>` | 也可传请求头 `X-Agy-Group: <分组ID>` |
| **发给下游的受限 Key** | `Bearer <专属Key>` | 在面板的账号或分组里生成，无法登录 `:18891` 后台 |

### 调度说明

- **多轮对话不中途换号**：新对话首轮遇 `429`/`503` 会自动换健康账号重试；从第 2 轮起固定使用原账号，保留上下文缓存并避免账号风控。
- **防工具调用超限扣费**：`Claude Code`、`Codex` 每次调本地工具都会消耗官方 `agy` 的 5 小时免费安全检查次数，超限会额外扣额度。网关会在接近上限前自动把新对话切给其他账号（面板顶栏可调 `80%` / `88%` / `93%`）。

---

<a id="configuration-reference"></a>

## 接口与常用配置

### 端口与接口

`:18890` 提供 AI 接口，`:18891` 提供管理后台（公网反代时只暴露 `:18890`）：

| 端口 | 路径 | 说明 |
| :--- | :--- | :--- |
| `:18890` | `GET /healthz` | 存活检查（免鉴权） |
| `:18890` | `GET /v1/models`, `GET /v1beta/models` | 当前 Key 可用的模型列表与窗口大小 |
| `:18890` | `POST /v1/messages`, `/v1/messages/count_tokens` | Anthropic Messages 接口 |
| `:18890` | `POST /v1/chat/completions`, `/v1/responses` | OpenAI 对话与 Responses 接口 |
| `:18890` | `POST /v1/images/generations`, `/v1/images/edits` | OpenAI 文生图与参考图改图接口 |
| `:18890` | `POST /v1beta/models/{model}:generateContent` / `:streamGenerateContent` | Google Gemini 原生接口 |
| `:18891` | `GET /`, `/api/dashboard`, `/api/accounts*`, `/api/upgrade*` | Web 管理面板与账号接口（仅主 Key 可访问） |
| `:18891` | `GET /metrics` | Prometheus 监控指标（仅主 Key 可访问） |

### 常用环境变量 (`.env`)

日常部署通常只需改前 4 项，完整配置见 [`.env.example`](.env.example)：

| 变量名 | 默认值 | 用途 |
| :--- | :--- | :--- |
| `AGY_API_KEYS` | *(仅本机访问可留空)* | 主访问密钥（多个用逗号分隔），用于 `:18890` 调用与 `:18891` 登录 |
| `AGY_LISTEN_HOST` | `127.0.0.1` | 监听地址（Docker 容器内默认设为 `0.0.0.0`） |
| `AGY_GW_PORT` / `AGY_DASH_PORT` | `18890` / `18891` | AI 接口端口 / Web 管理面板端口 |
| `AGY_UPSTREAM_PROXY` | *(空)* | 主账号默认代理（如 `http://127.0.0.1:7890`；子账号在网页里单独配） |
| `AGY_MAX_LIVE_WORKERS` | `8` | 主账号最大并发进程数（子账号在网页里单独配） |
| `AGY_GLOBAL_IDLE_TARGET` | `8` | 全池平时保留的热进程数 |
| `AGY_GLOBAL_BURST_MAX_LIVE_WORKERS` | `0` *(按内存自动计算)* | 突发高并发时的全池进程上限（6 GB 容器自动设为 `18`） |
| `AGY_IDLE_WORKER_TTL_SEC` | `180` | 进程空闲多少秒后自动休眠释放内存 |
| `AGY_ROUTE_CLAUDE_NAMES` | `0` | 设为 `1` 时将 `claude-sonnet-*` / `claude-opus-*` 自动映射到网关模型 |
| `AGY_MEDIA_RELAY` | `0` | 设为 `1` 时开启 Gemini 扫描版 PDF、音频与视频输入 |

<details>
<summary><strong>更多进阶配置与升级维护命令</strong></summary>

#### 进阶环境变量

| 变量名 | 默认值 | 用途 |
| :--- | :--- | :--- |
| `AGY_DASHBOARD_PEERS` | *(空)* | 在同一个 `:18891` 面板汇总查看多台机器的网关（`id\|名称\|url\|key;...`） |
| `AGY_PROXY_PRESETS_FILE` / `AGY_RESIN_HOST` / `AGY_RESIN_PROXY_TOKEN` | *(可选)* | 为面板配置命名的静态代理池预设 |
| `AGY_MEM_PRESSURE_WARN_PCT` / `HIGH_PCT` | `70` / `80` | 容器内存告警与高压回收百分比阈值 |
| `AGY_PROMPT_FIRST_CHUNK_TIMEOUT_SEC` / `STREAM_IDLE_TIMEOUT_SEC` | `360` / `180` | 首字返回超时（秒）与流式输出间隔超时（秒） |
| `AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC` / `GRACE_SEC` | `30` / `45` | 慢请求运行满 30 秒后若客户端断开，后台继续保留 45 秒供重试复用 |
| `AGY_MEDIA_RELAY_MAX_BYTES` / `MAX_FILES` | `20971520` / `3` | 音视频/扫描 PDF 单文件字节上限（默认 20 MiB）与单次文件数上限 |
| `AGY_CONV_KEEP_DAYS` / `AGY_ACP_LOG_MAX_MB` | `4` *(Compose)* / `64` | 会话文件保留天数与单账号日志体积上限（MiB） |
| `AGY_BLOCKED_VERSIONS` | `1.5.17,1.5.18` | 升级脚本自动跳过的官方问题版本列表 |

#### 升级官方 `agy` 与运行测试

更新本机 `~/.local/bin/agy` 后，可用脚本校验并替换 `./bin/agy-acp-900k`：

```bash
# 查看官方最新版本与当前补丁状态
python3 scripts/upstream_watch.py --official-only
./scripts/agy-upgrade.sh --status

# 运行本地单元测试（不消耗模型额度）
cd go-gateway && go test -race -count=1 ./... && cd ..
python3 -m unittest discover -s scripts -p "test_*.py"
```

Web 后台样式规范见 [`DESIGN.md`](DESIGN.md)。
</details>

---

<a id="troubleshooting"></a>

## 常见问题

<details>
<summary><strong>1. 调用模型或生图报错 <code>403 permission_error</code></strong></summary>

当前账号或账号分组没勾选该模型。打开 `:18891` -> **账号** -> **编辑**，勾选对应模型（生图需勾选 `gemini-3.1-flash-image`）后保存。
</details>

<details>
<summary><strong>2. 用局域网 IP 或域名访问返回 <code>403 Forbidden</code></strong></summary>

`.env` 里的 `AGY_API_KEYS` 留空时只允许本机 `127.0.0.1` 访问。填上 `AGY_API_KEYS=sk-...` 重启即可。
</details>

<details>
<summary><strong>3. 账号显示 <code>需要验证</code>（<code>VALIDATION_REQUIRED</code>）或报错 <code>403 #3501</code></strong></summary>

- **显示 `需要验证`**：Google 要求该账号做一次网页验证。点面板上的 **前往验证** 在浏览器完成后，回来点 **已验证, 重检** 即可恢复。
- **报错 `403 #3501` 或 `GEMINI_ENTERPRISE_LICENSE_NOT_VALID`**：该账号登录失效或无可用资格，网关会单独暂停该账号 30 分钟，不影响其他账号。在 `:18891` 重新做一次网页授权即可。
</details>

<details>
<summary><strong>4. 对话中途报错 <code>503 Service Unavailable</code>（绑定的账号不可用）</strong></summary>

当前多轮对话绑定的那个账号刚刚下线或受限，网关不会把半截对话搬到别的账号。在客户端**新开一个对话**，会自动分配到其他正常账号。
</details>

<details>
<summary><strong>5. <code>/v1/responses</code> 报错 <code>previous_response_not_found</code> 或 <code>thread_not_found</code></strong></summary>

客户端传入的上一轮会话 ID（如 `previous_response_id`）已过期或不存在。在客户端开启新会话即可。
</details>

<details>
<summary><strong>6. 上传扫描版 PDF、音频或视频报错 <code>400</code> 或 <code>413</code></strong></summary>

文字版 PDF 默认全模型直接可用；扫描版 PDF 和音视频需在 `.env` 开启 `AGY_MEDIA_RELAY=1`，且仅支持不传 `tools` 的 Gemini 聊天请求。单文件超过 `AGY_MEDIA_RELAY_MAX_BYTES`（默认 20 MiB）会返回 `413`。
</details>

---

<a id="changelog"></a>

## 近期更新

- **`v2.3.26` (2026-11-27)**：打通 `/v1/images/*`、`/v1/chat/completions` 与 `/v1beta` 三种 `gemini-3.1-flash-image` 生图/改图入口（支持 JPEG/PNG，生图内存减半）；新增空闲与突发双层进程上限，等待工具返回时自动休眠省内存；精简后台文案与说明文档。
- **`v2.3.24` (2026-11-27)**：面板新增 5 小时工具检查档位切换（`80%` / `88%` / `93%`）、临近重置自动放宽、多设备「共享账号」开关，并支持识别 Google AI Ultra `5x` 套餐（PR #10）。
- **`v2.3.23` (2026-11-27)**：内置文字版 PDF 解析（全模型默认可用），新增可选的 Gemini 扫描 PDF 与音视频支持（`AGY_MEDIA_RELAY=1`）。
- **`v2.3.21` - `v2.3.22` (2026-11-27)**：新增跨账号空闲进程休眠、`VALIDATION_REQUIRED` 一键验证重检、Token 刷新时的短暂 `401` 自动重试，支持 MCP 工具返回图片。
- **`v2.3.20` (2026-11-26)**：上线 `/v1/images/*` 接口，对话模型扩充到 11 款，支持 `HEIC`/`HEIF` 图片与客户端 `WebFetch`。

---

## 开源协议

基于 [MIT License](LICENSE) 开源。
