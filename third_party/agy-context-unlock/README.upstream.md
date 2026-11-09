# agy-context-unlock

把本机官方 Antigravity 客户端的上下文窗口从默认 256K 放到约 1M（983,040），并同步解除单条用户消息 64K（192KB）截断上限。支持 macOS（arm64 / x86-64）、Linux（x86-64 / arm64）与 Windows（x86-64 / arm64），要求 `agy >= 1.5.0`。

这个目录不进 DSH 插件安装包。只运行这里的脚本才会改写本机官方 `agy` 二进制。`agy` 或 Antigravity 桌面端升级后，新二进制会回到 256K，要再运行一次。

插件 [`dsh-llm-antigravity-acp`](../dsh-llm-antigravity-acp) 的 `autoPatch` 默认会寻找：

- macOS / Linux：`~/.gemini/antigravity/tools/agy-patch.py`
- Windows：`%USERPROFILE%\.gemini\antigravity\tools\agy-patch.py`

把本目录的 `agy-patch.py` 放到这个路径后，插件下次启动会自动检查并在未解锁时完成补丁写入。找不到脚本时，窗口保持官方的 256K，模型仍可使用。

## 解锁终端里的 agy

先确认 `agy --version` 为 `1.5.x`（旧版本先执行 `agy update` 升级），退出正在运行的 `agy` 进程后执行：

**macOS / Linux：**

```sh
python3 agy-patch.py --binary ~/.local/bin/agy --dry-run
python3 agy-patch.py --binary ~/.local/bin/agy --in-place
```

**Windows（PowerShell）：**

```powershell
python agy-patch.py --binary "$env:LOCALAPPDATA\agy\bin\agy.exe" --dry-run
python agy-patch.py --binary "$env:LOCALAPPDATA\agy\bin\agy.exe" --in-place
```

省略 `--binary` 时，脚本在 macOS / Linux 默认读取 `~/.local/bin/agy`，在 Windows 默认读取 `%LOCALAPPDATA%\agy\bin\agy.exe`。`--dry-run` 只报告门禁状态，不写文件；`--in-place` 会先生成 `.bak` 备份再改写（macOS 自动重签名，Windows 自动清理失效的旧签名块）。

## 解锁桌面端

退出 Antigravity 桌面客户端并自行备份原文件后执行：

**macOS：**

```sh
python3 agy-patch.py \
  --binary "/Applications/Antigravity.app/Contents/Resources/bin/language_server" \
  --in-place --no-backup
```

**Windows（PowerShell）：**

```powershell
python agy-patch.py `
  --binary "$env:LOCALAPPDATA\Programs\Antigravity\resources\bin\language_server.exe" `
  --in-place
```

桌面端下次启动后使用新二进制。

## 升级后自动再解锁（macOS）

Windows / Linux 下使用 DSH 插件时，插件自带的 `autoPatch` 会在每次启动 `agy acp` 前自动检查并补打补丁。

macOS 下若同时使用官方终端与桌面客户端，可用 `agy-autopatch.py` 配合 `launchd` 自动重打补丁：它先做 `--dry-run`，仅在二进制回到官方原版时写入，并向当前桌面端 `language_server` 发送一次 `SIGTERM` 触发热重载。

脚本默认寻找这三个路径：

```text
~/.local/bin/agy
/Applications/Antigravity.app/Contents/Resources/bin/language_server
~/.gemini/antigravity/tools/agy-patch.py
```

手动跑一次：

```sh
python3 agy-autopatch.py
```

要在升级后自动跑，用 `launchd` 监听 `~/.local/bin` 和 `/Applications/Antigravity.app`，程序指向本机的 `agy-autopatch.py`（按本机实际家目录填写绝对路径）。
