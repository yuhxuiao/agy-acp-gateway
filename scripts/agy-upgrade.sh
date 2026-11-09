#!/usr/bin/env bash
# Default: build a fresh candidate with offline/fake checks only.
# --contract --live-flash: isolated ACP checks and one authorized Flash request.
# --install --candidate DIR: verify evidence, back up, restart, probe, rollback on failure.
# --rollback [BACKUP]: restore the complete previous bundle including its manifest.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/agy_upgrade.py" "$@"
