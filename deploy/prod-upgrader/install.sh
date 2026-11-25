#!/bin/sh
# Install or update the host update agent. Run as root from the uploaded bundle directory.
# Enables the daily check timer and the approval watcher.
set -eu
cd "$(dirname "$0")"
sha256sum -c --quiet SHA256SUMS
dest=/root/agy-acp-gateway/upgrader
test -d /root/agy-acp-gateway/data
if [ -d "$dest" ]; then
  backup=/root/agy-acp-gateway/backups/upgrader-$(date -u +%Y%m%dT%H%M%SZ)
  mkdir -p "$backup"
  for f in agy_prod_upgrader.py gateway_release.sh scripts tools third_party; do
    if [ -e "$dest/$f" ]; then cp -a "$dest/$f" "$backup/"; fi
  done
fi
mkdir -p "$dest"
chmod 700 "$dest"
cp -a upgrader/. "$dest/"
chown -R root:root "$dest"
# The weekly cache probe was dropped (2026-11-11); retire units left by older bundles.
for unit in agy-upgrader-probe.timer agy-upgrader-probe.service; do
  if [ -f "/etc/systemd/system/$unit" ]; then
    systemctl disable --now "$unit" 2>/dev/null || true
    mkdir -p "${backup:-/root/agy-acp-gateway/backups/upgrader-retired}"
    mv "/etc/systemd/system/$unit" "${backup:-/root/agy-acp-gateway/backups/upgrader-retired}/"
  fi
done
install -m 0644 systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now agy-upgrader-check.timer agy-upgrader-apply.path
echo "installed to $dest"
