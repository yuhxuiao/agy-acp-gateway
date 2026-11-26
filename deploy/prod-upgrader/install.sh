#!/bin/sh
# Install or update the host update agent. Run as root from the uploaded bundle directory.
# Enables the daily check timer and the approval watcher.
set -eu
cd "$(dirname "$0")"
sha256sum -c --quiet SHA256SUMS

ROOT=${AGY_PROD_ROOT:-/root/agy-acp-gateway}
ROOT=${ROOT%/}
if [ -z "$ROOT" ]; then
  echo "invalid AGY_PROD_ROOT" >&2
  exit 2
fi
SYSTEMD_DIR=${AGY_SYSTEMD_DIR:-/etc/systemd/system}
dest="$ROOT/upgrader"
test -d "$ROOT/data"

mkdir -p "$dest/state"
chmod 700 "$dest" "$dest/state"

# Serialize installation against active check/apply runs using the same lock file.
exec 9>>"$dest/state/upgrade.lock"
flock -w 60 9

backup="$ROOT/backups/upgrader-$(date -u +%Y%m%dT%H%M%SZ)-$$"
umask 077
mkdir -p "$backup" "$backup/systemd"
chmod 700 "$backup" "$backup/systemd"

for f in agy_prod_upgrader.py gateway_release.sh scripts tools third_party; do
  if [ -e "$dest/$f" ]; then
    cp -a "$dest/$f" "$backup/"
  fi
done
for unit_path in "$SYSTEMD_DIR"/agy-upgrader-*.service "$SYSTEMD_DIR"/agy-upgrader-*.timer "$SYSTEMD_DIR"/agy-upgrader-*.path; do
  if [ -e "$unit_path" ]; then
    cp -a "$unit_path" "$backup/systemd/"
  fi
done

# Stage into a sibling directory before swapping entries under the exclusive lock.
stage="$dest.new.$$"
cleanup_stage() {
  rm -rf "$stage"
}
trap cleanup_stage EXIT
mkdir -p "$stage"
chmod 700 "$stage"
cp -a upgrader/. "$stage/"
if [ "$(id -u)" -eq 0 ]; then
  chown -R root:root "$stage"
fi

for item_path in "$stage"/*; do
  [ -e "$item_path" ] || continue
  item=$(basename "$item_path")
  if [ "$item" = "state" ]; then
    continue
  fi
  if [ -d "$item_path" ] && [ -e "$dest/$item" ]; then
    old_item="$dest/.old-${item}.$$"
    rm -rf "$old_item"
    mv "$dest/$item" "$old_item"
    mv "$item_path" "$dest/$item"
    rm -rf "$old_item"
  else
    mv -f "$item_path" "$dest/$item"
  fi
done

if [ "$(id -u)" -eq 0 ]; then
  chown root:root "$dest" "$dest/state"
fi

# The weekly cache probe was dropped (2026-11-11); retire units left by older bundles.
for unit in agy-upgrader-probe.timer agy-upgrader-probe.service; do
  if [ -f "$SYSTEMD_DIR/$unit" ]; then
    if [ "${AGY_SKIP_SYSTEMCTL:-0}" != "1" ]; then
      systemctl disable --now "$unit" 2>/dev/null || true
    fi
    mv "$SYSTEMD_DIR/$unit" "$backup/systemd/"
  fi
done

mkdir -p "$SYSTEMD_DIR"
for src_unit in systemd/*; do
  [ -f "$src_unit" ] || continue
  unit_name=$(basename "$src_unit")
  tmp_unit="$stage/$unit_name"
  python3 -I - "$src_unit" "$tmp_unit" "$ROOT" <<'PY'
import pathlib, sys
src, dst, root = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
text = src.read_text(encoding="utf-8")
text = text.replace("/root/agy-acp-gateway", root)
dst.write_text(text, encoding="utf-8")
PY
  install -m 0644 "$tmp_unit" "$SYSTEMD_DIR/$unit_name"
done

if [ "${AGY_SKIP_SYSTEMCTL:-0}" != "1" ]; then
  systemctl daemon-reload
  systemctl enable --now agy-upgrader-check.timer agy-upgrader-apply.path
fi
echo "installed to $dest (backup: $backup)"
