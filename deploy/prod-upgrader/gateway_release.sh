#!/bin/sh
# Gateway update release (U3), run by the host agent after a dashboard approval.
#
# Staging (operator, after the local candidate passed its gates):
#   mkdir -p releases/<name>/bin
#   upload bin/agy-acp-gateway and the candidate manifest.json into releases/<name>/
#   cp upgrader/gateway_release.sh releases/<name>/release.sh && touch releases/<name>/READY
# The next check run offers it on the dashboard; nothing changes until it is approved.
#
# Keeps agy as is (data/agy/current). Stops the container for ~20 s, swaps the image and the
# gateway binary, verifies, and restores the previous image and binary on any failure.
set -eu
orig_release_dir=$(dirname "$0")
release_dir=$(cd "$orig_release_dir" && pwd -P)
name=$(basename "$release_dir")
if [ -n "${AGY_PROD_ROOT:-}" ]; then
  ROOT=$(cd "$AGY_PROD_ROOT" && pwd -P)
else
  ROOT=$(cd "$release_dir/../.." && pwd -P)
fi
cd "$ROOT"
case "$release_dir" in
  "$ROOT"/releases/*) ;;
  *) echo "unexpected release dir" >&2; exit 2 ;;
esac
GW_PORT=${AGY_GW_PORT:-${GW_PORT:-18890}}
DASH_PORT=${AGY_DASH_PORT:-${DASH_PORT:-18891}}
GW_SHA=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["artifacts"]["gateway"])' "$release_dir/manifest.json")
image="agy-acp-gateway:gw-$name"
rollback_image="agy-acp-gateway:rollback-$name"
changed=0
rollback() {
  result=$?
  if [ "$result" -ne 0 ] && [ "$changed" -eq 1 ]; then
    echo "Release check failed; restoring previous image and gateway binary"
    docker compose stop -t 20 agy-acp-gateway || true
    docker image tag "$rollback_image" agy-acp-gateway:local
    cp -p "$release_dir/previous-gateway" bin/agy-acp-gateway.rollback
    mv bin/agy-acp-gateway.rollback bin/agy-acp-gateway
    docker compose up -d --no-build --force-recreate agy-acp-gateway
    echo ROLLED_BACK > "$release_dir/result"
  elif [ "$result" -ne 0 ]; then
    echo NOT_STARTED > "$release_dir/result"
  fi
  exit "$result"
}
trap rollback EXIT

# Gate tos-tripped only for single-primary mode or when primary is enabled in multi-account mode.
if [ -e data/tos-tripped ]; then
  python3 -I - data/accounts.json <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
if not p.is_file() or p.is_symlink():
    sys.stderr.write("data/tos-tripped exists in single-primary mode; refusing release\n")
    raise SystemExit(1)
try:
    raw = json.loads(p.read_text(encoding="utf-8"))
    accs = raw.get("accounts") if isinstance(raw, dict) else None
except Exception:
    accs = None
if not isinstance(accs, list) or not accs:
    sys.stderr.write("data/tos-tripped exists and accounts.json has no accounts; refusing release\n")
    raise SystemExit(1)
primary_enabled = False
enabled_children = 0
for a in accs:
    if not isinstance(a, dict):
        continue
    en = bool(a.get("enabled", True)) and str(a.get("schedState") or "active").strip().lower() != "disabled"
    if a.get("primary"):
        primary_enabled = en
    elif en:
        enabled_children += 1
if primary_enabled or enabled_children == 0:
    sys.stderr.write("data/tos-tripped exists with enabled primary or no enabled child accounts; refusing release\n")
    raise SystemExit(1)
sys.stderr.write(f"notice: ignoring primary data/tos-tripped (primary disabled, {enabled_children} child account(s) enabled)\n")
PY
fi

test -L data/agy/current
test "$(sha256sum "$release_dir/bin/agy-acp-gateway" | cut -d ' ' -f1)" = "$GW_SHA"

# Build the image next to the release (agy in the image is only the fallback; the container
# runs data/agy/current).
cp -p Dockerfile "$release_dir/Dockerfile"
if [ -f data/agy/current/agy-acp-900k ]; then
  cp -p data/agy/current/agy-acp-900k "$release_dir/bin/agy-acp-900k"
elif [ -f bin/agy-acp-900k ]; then
  cp -p bin/agy-acp-900k "$release_dir/bin/agy-acp-900k"
else
  echo "missing agy-acp-900k in data/agy/current and bin/" >&2
  exit 1
fi
docker build -q -t "$image" "$release_dir" >/dev/null
test "$(docker run --rm --network none --entrypoint sha256sum "$image" /app/bin/agy-acp-gateway | cut -d ' ' -f1)" = "$GW_SHA"
cp -p bin/agy-acp-gateway "$release_dir/previous-gateway"
docker image tag agy-acp-gateway:local "$rollback_image"

changed=1
docker compose stop -t 20 agy-acp-gateway
mkdir -p "$release_dir/state"
chmod 700 "$release_dir/state"
for f in data/gateway.db data/gateway.db-wal data/gateway.db-shm; do
  if [ -f "$f" ]; then cp -p "$f" "$release_dir/state/"; fi
done
cp -p "$release_dir/bin/agy-acp-gateway" bin/agy-acp-gateway.install
mv bin/agy-acp-gateway.install bin/agy-acp-gateway
docker image tag "$image" agy-acp-gateway:local
docker compose up -d --no-build --force-recreate agy-acp-gateway
ready=0
for n in $(seq 1 40); do
  if curl -fsS --max-time 2 "http://127.0.0.1:${GW_PORT}/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
test "$ready" -eq 1

auth_enabled=$(docker exec agy-acp-gateway sh -c '
  k=""
  if [ -n "${AGY_API_KEYS:-}" ]; then
    k=$(printf "%s\n" "$AGY_API_KEYS" | tr "," "\n" | sed "s/^[[:space:]]*//;s/[[:space:]]*$//" | grep -v "^$" | grep -v "^#" | head -n 1 || true)
  fi
  if [ -z "$k" ] && [ -n "${AGY_API_KEYS_FILE:-}" ] && [ -r "$AGY_API_KEYS_FILE" ]; then
    k=$(sed "s/^[[:space:]]*//;s/[[:space:]]*$//" "$AGY_API_KEYS_FILE" | grep -v "^$" | grep -v "^#" | head -n 1 || true)
  fi
  if [ -n "$k" ]; then printf "1"; else printf "0"; fi
')
if [ "$auth_enabled" = "1" ]; then
  test "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${GW_PORT}/v1/models")" = 401
  test "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${GW_PORT}/v1beta/models")" = 401
fi
test "$(docker exec agy-acp-gateway sha256sum /app/bin/agy-acp-gateway | cut -d ' ' -f1)" = "$GW_SHA"

docker exec -e HOST_GW_PORT="$GW_PORT" -e HOST_DASH_PORT="$DASH_PORT" agy-acp-gateway sh -ec '
  key=""
  if [ -n "${AGY_API_KEYS:-}" ]; then
    key=$(printf "%s\n" "$AGY_API_KEYS" | tr "," "\n" | sed "s/^[[:space:]]*//;s/[[:space:]]*$//" | grep -v "^$" | grep -v "^#" | head -n 1 || true)
  fi
  if [ -z "$key" ] && [ -n "${AGY_API_KEYS_FILE:-}" ] && [ -r "$AGY_API_KEYS_FILE" ]; then
    key=$(sed "s/^[[:space:]]*//;s/[[:space:]]*$//" "$AGY_API_KEYS_FILE" | grep -v "^$" | grep -v "^#" | head -n 1 || true)
  fi
  gw_port=${AGY_GW_PORT:-${GW_PORT:-$HOST_GW_PORT}}
  dash_port=${AGY_DASH_PORT:-${DASH_PORT:-$HOST_DASH_PORT}}
  api_get() {
    hdr=$1; url=$2; tmo=$3
    if [ -n "$key" ]; then
      curl -fsS --max-time "$tmo" "$url" -H "$hdr: $key"
    else
      curl -fsS --max-time "$tmo" "$url"
    fi
  }
  api_get "x-api-key" "http://127.0.0.1:${gw_port}/v1/models" 10 | grep -q "\"gemini-"
  api_get "x-goog-api-key" "http://127.0.0.1:${gw_port}/v1beta/models" 10 | grep -q "\"models/gemini-"
  api_get "x-api-key" "http://127.0.0.1:${dash_port}/api/dashboard" 30 | grep -q "\"geminiPool\""
  if [ -n "$key" ]; then
    api_get "x-api-key" "http://127.0.0.1:${dash_port}/api/upgrade" 10 | grep -q "\"enabled\":true"
  else
    api_get "x-api-key" "http://127.0.0.1:${dash_port}/api/upgrade" 10 | grep -q "\"enabled\":"
  fi
'

smoke_rc=0
python3 -I - "$release_dir" "$GW_PORT" <<'PY' || smoke_rc=$?
import contextlib, json, pathlib, re, subprocess, sys, time, urllib.error, urllib.request

QUOTA_CODES = {
    "rate_limit_error",
    "pool_cooldown",
    "pool-cooldown",
    "pool-monitor-tripped",
    "pool_monitor_tripped",
    "resource_exhausted",
}

def resolve_gateway_key():
    cmd = (
        'if [ -n "${AGY_API_KEYS:-}" ]; then printf "%s\\n" "$AGY_API_KEYS" | tr "," "\\n"; fi; '
        'if [ -n "${AGY_API_KEYS_FILE:-}" ] && [ -r "$AGY_API_KEYS_FILE" ]; then cat "$AGY_API_KEYS_FILE"; fi'
    )
    raw = subprocess.check_output(["docker", "exec", "agy-acp-gateway", "sh", "-c", cmd], text=True)
    for line in raw.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            return s
    return ""

def parse_err_code(body_text):
    try:
        obj = json.loads(body_text)
    except Exception:
        obj = None
    if isinstance(obj, dict):
        err = obj.get("error")
        if isinstance(err, dict):
            for k in ("code", "type", "status"):
                v = err.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip().lower()
        elif isinstance(err, str) and err.strip():
            return err.strip().lower()
        for k in ("code", "type", "status"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip() and v.strip().lower() != "error":
                return v.strip().lower()
    return ""

key = resolve_gateway_key()
gw_port = int(sys.argv[2])
body = {
    "model": "gemini-3.8-flash-low",
    "max_tokens": 12,
    "system": "Reply briefly. Do not use any tools.",
    "messages": [{"role": "user", "content": "Reply with exactly: OK"}],
}
headers = {"Content-Type": "application/json", "x-session-id": f"prod-probe-release-smoke-{time.time_ns()}"}
if key:
    headers["x-api-key"] = key
req = urllib.request.Request(f"http://127.0.0.1:{gw_port}/v1/messages", data=json.dumps(body).encode(), headers=headers)
t0 = time.monotonic()
p = pathlib.Path(sys.argv[1]) / "production-smoke.json"
try:
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.load(r)
        status = r.status
except urllib.error.HTTPError as exc:
    err_body = ""
    with contextlib.closing(exc), contextlib.suppress(Exception):
        err_body = exc.read(512).decode("utf-8", "replace")
    err_code = parse_err_code(err_body)
    is_quota = exc.code == 429 or err_code in QUOTA_CODES
    out = {
        "ok": is_quota,
        "quota_deferred": is_quota,
        "model": "gemini-3.8-flash-low",
        "generation_requests": 1,
        "http_status": exc.code,
        "error_code": err_code,
        "detail": err_body[:200],
        "elapsed_seconds": round(time.monotonic() - t0, 2),
    }
    p.write_text(json.dumps(out, indent=2) + "\n")
    p.chmod(0o600)
    print(json.dumps(out))
    raise SystemExit(42 if is_quota else 1)

text = "".join(x.get("text", "") for x in data.get("content", []) if isinstance(x, dict) and x.get("type") == "text")
ok = status == 200 and data.get("stop_reason") == "end_turn" and bool(re.fullmatch(r"\s*OK[.!]?\s*", text, re.I))
out = {
    "ok": ok,
    "quota_deferred": False,
    "model": "gemini-3.8-flash-low",
    "generation_requests": 1,
    "http_status": status,
    "stop_reason": data.get("stop_reason"),
    "elapsed_seconds": round(time.monotonic() - t0, 2),
}
p.write_text(json.dumps(out, indent=2) + "\n")
p.chmod(0o600)
print(json.dumps(out))
if not ok:
    raise SystemExit(1)
PY

if [ "$smoke_rc" -eq 0 ]; then
  printf '%s\n' DEPLOYED_AND_VERIFIED > "$release_dir/result"
elif [ "$smoke_rc" -eq 42 ]; then
  printf '%s\n' DEPLOYED_QUOTA_DEFERRED > "$release_dir/result"
else
  exit "$smoke_rc"
fi
changed=0
docker inspect -f 'startedAt={{.State.StartedAt}} image={{.Image}} health={{.State.Health.Status}} restarts={{.RestartCount}}' agy-acp-gateway
