#!/bin/sh
# Collect the host agent and the repo scripts it reuses into one directory for upload.
# Usage: deploy/prod-upgrader/bundle.sh <out-dir>
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out=$1
test ! -e "$out" || { echo "refusing to overwrite $out" >&2; exit 1; }
mkdir -p "$out/upgrader/scripts" "$out/upgrader/tools" "$out/upgrader/third_party/agy-context-unlock" "$out/systemd"
cp "$here/agy_prod_upgrader.py" "$here/gateway_release.sh" "$out/upgrader/"
cp "$root/scripts/agy_acp_smoke.py" "$root/scripts/agy_notice_templates.py" "$root/scripts/agy_notice_templates.json" "$root/scripts/agy_upgrade.py" "$out/upgrader/scripts/"
cp "$root/tools/agy_extra_patch.py" "$out/upgrader/tools/"
cp "$root/third_party/agy-context-unlock/agy-patch.py" "$root/third_party/agy-context-unlock/LICENSE" "$out/upgrader/third_party/agy-context-unlock/"
cp "$here"/systemd/* "$out/systemd/"
cp "$here/install.sh" "$out/"
(cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)
echo "bundle ready: $out"
