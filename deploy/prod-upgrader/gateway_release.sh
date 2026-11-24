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
test ! -e data/tos-tripped
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
test "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${GW_PORT}/v1/models")" = 401
test "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${GW_PORT}/v1beta/models")" = 401
test "$(docker exec agy-acp-gateway sha256sum /app/bin/agy-acp-gateway | cut -d ' ' -f1)" = "$GW_SHA"
docker exec -e HOST_GW_PORT="$GW_PORT" -e HOST_DASH_PORT="$DASH_PORT" agy-acp-gateway sh -ec '
  key=${AGY_API_KEYS%%,*}
  dash_key=${AGY_DASHBOARD_PASSWORD:-$key}
  gw_port=${AGY_GW_PORT:-${GW_PORT:-$HOST_GW_PORT}}
  dash_port=${AGY_DASH_PORT:-${DASH_PORT:-$HOST_DASH_PORT}}
  curl -fsS --max-time 10 "http://127.0.0.1:${gw_port}/v1/models" -H "x-api-key: $key" | grep -q "\"gemini-4-carbon\""
  curl -fsS --max-time 10 "http://127.0.0.1:${gw_port}/v1/models" -H "x-api-key: $key" | grep -q "\"gemini-4-neon\""
  curl -fsS --max-time 10 "http://127.0.0.1:${gw_port}/v1/models" -H "x-api-key: $key" | grep -q "\"gemini-4-hydrogen\""
  curl -fsS --max-time 10 "http://127.0.0.1:${gw_port}/v1beta/models" -H "x-goog-api-key: $key" | grep -q "\"models/gemini-4-carbon\""
  curl -fsS --max-time 10 "http://127.0.0.1:${gw_port}/v1beta/models" -H "x-goog-api-key: $key" | grep -q "\"models/gemini-4-neon\""
  curl -fsS --max-time 30 "http://127.0.0.1:${dash_port}/api/dashboard" -H "x-api-key: $dash_key" | grep -q "\"geminiPool\""
  curl -fsS --max-time 10 "http://127.0.0.1:${dash_port}/api/upgrade" -H "x-api-key: $dash_key" | grep -q "\"enabled\":true"
'
python3 -I - "$release_dir" "$GW_PORT" <<'PY'
import json, pathlib, re, subprocess, sys, time, urllib.request
key = subprocess.check_output(["docker", "exec", "agy-acp-gateway", "sh", "-c", "printf '%s' \"${AGY_API_KEYS%%,*}\""], text=True).strip()
gw_port = int(sys.argv[2])
body = {"model": "gemini-3.8-flash-low", "max_tokens": 12, "system": "Reply briefly. Do not use any tools.", "messages": [{"role": "user", "content": "Reply with exactly: OK"}]}
headers = {"Content-Type": "application/json", "x-api-key": key, "x-session-id": f"prod-probe-release-smoke-{time.time_ns()}"}
req = urllib.request.Request(f"http://127.0.0.1:{gw_port}/v1/messages", data=json.dumps(body).encode(), headers=headers)
t0 = time.monotonic()
with urllib.request.urlopen(req, timeout=120) as r:
    data = json.load(r)
    status = r.status
text = "".join(x.get("text", "") for x in data.get("content", []) if x.get("type") == "text")
ok = status == 200 and data.get("stop_reason") == "end_turn" and bool(re.fullmatch(r"\s*OK[.!]?\s*", text, re.I))
out = {"ok": ok, "model": "gemini-3.8-flash-low", "generation_requests": 1, "http_status": status,
       "stop_reason": data.get("stop_reason"), "elapsed_seconds": round(time.monotonic() - t0, 2)}
p = pathlib.Path(sys.argv[1]) / "production-smoke.json"
p.write_text(json.dumps(out, indent=2) + "\n")
p.chmod(0o600)
print(json.dumps(out))
if not ok:
    raise SystemExit(1)
PY
printf '%s\n' DEPLOYED_AND_VERIFIED > "$release_dir/result"
changed=0
docker inspect -f 'startedAt={{.State.StartedAt}} image={{.Image}} health={{.State.Health.Status}} restarts={{.RestartCount}}' agy-acp-gateway
