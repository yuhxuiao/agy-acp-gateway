#!/usr/bin/env bash
# agy-acp-gateway: start [go], stop, restart [go], status, logs
# Each port has its own runtime paths. Default ports retain the legacy production paths.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GW_PORT="${AGY_GW_PORT:-18890}"
DASH_PORT="${AGY_DASH_PORT:-$((GW_PORT + 1))}"
if [[ ! "$GW_PORT" =~ ^[0-9]+$ || ! "$DASH_PORT" =~ ^[0-9]+$ ]] || (( GW_PORT<1 || GW_PORT>65535 || DASH_PORT<1 || DASH_PORT>65535 || GW_PORT==DASH_PORT )); then
  echo 'Invalid gateway/dashboard ports' >&2; exit 1
fi
if [[ "$GW_PORT" == 18890 ]]; then
  RUN="${AGY_RUN_DIR:-$ROOT/runtime}"
  LOGS="${AGY_LOG_DIR:-$ROOT/logs}"
  APPDATA_DEFAULT="$HOME/.gemini/antigravity-cli"
else
  RUN="${AGY_RUN_DIR:-$ROOT/runtime/instance-$GW_PORT}"
  LOGS="${AGY_LOG_DIR:-$ROOT/logs/instance-$GW_PORT}"
  APPDATA_DEFAULT=""
fi
BIN="${AGY_GATEWAY_BIN:-$ROOT/bin/agy-acp-gateway}"
mkdir -p "$RUN" "$LOGS"
export AGY_GW_HOME="$ROOT"
export AGY_STATE_DB="${AGY_STATE_DB:-$RUN/gateway.db}"
export AGY_TOS_TRIPPED_FILE="${AGY_TOS_TRIPPED_FILE:-$RUN/tos-tripped}"
export AGY_PURE_API_HOME="${AGY_PURE_API_HOME:-$RUN/pure-api-home}"
export AGY_GW_LOG="${AGY_GW_LOG:-$LOGS/gateway.log}"
export AGY_APPDATA_LINK="${AGY_APPDATA_LINK-$APPDATA_DEFAULT}"
export AGY_GW_PORT="$GW_PORT" AGY_DASH_PORT="$DASH_PORT"
port_busy() { ss -ltnH | awk -v port=":$1" '$4 ~ port"$" {found=1} END {exit !found}'; }
start_ticks() { [[ -r "/proc/$1/stat" ]] && awk '{print $22}' "/proc/$1/stat"; }
owned_pid() {
  local name="$1" pid actual
  [[ -f "$RUN/$name.pid" ]] || return 1
  pid="$(cat "$RUN/$name.pid")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null || return 1
  if [[ -f "$RUN/$name.start" ]]; then
    [[ "$(start_ticks "$pid")" == "$(cat "$RUN/$name.start")" ]] || { echo 'PID was reused; refusing signal' >&2; return 2; }
  else
    # Legacy production PID files: verify the executable before a one-time upgrade stop.
    actual="$(readlink "/proc/$pid/exe" || true)"
    [[ "${actual% (deleted)}" == "$BIN" ]] || { echo 'Unverified legacy PID; refusing signal' >&2; return 2; }
  fi
  printf '%s\n' "$pid"
}
remember_pid() { printf '%s\n' "$2" >"$RUN/$1.pid"; start_ticks "$2" >"$RUN/$1.start"; }
stop_one() {
  local name="$1" pid code child ticks i
  if pid="$(owned_pid "$name")"; then :; else
    code=$?; [[ "$code" == 1 ]] && return 0; return "$code"
  fi
  # Snapshot only this instance's descendants, including ACP's independent process groups.
  local -a descendants=()
  while read -r child; do
    [[ -n "$child" ]] && descendants+=("$child:$(start_ticks "$child" || true)")
  done < <(ps -eo pid=,ppid= | awk -v root="$pid" '{parent[$1]=$2} END {for(p in parent){q=p;for(n=0;n<128 && q in parent;n++){if(parent[q]==root){print p;break}q=parent[q]}}}')
  kill -TERM "$pid" 2>/dev/null || true
  for ((i=0;i<120;i++)); do
    kill -0 "$pid" 2>/dev/null || break
    [[ "$(ps -o stat= -p "$pid" || true)" == Z* ]] && break
    sleep 0.1
  done
  for item in "${descendants[@]}"; do
    child="${item%%:*}"; ticks="${item#*:}"
    if [[ -n "$ticks" && "$(start_ticks "$child" || true)" == "$ticks" ]]; then kill -TERM "$child" 2>/dev/null || true; fi
  done
  if kill -0 "$pid" 2>/dev/null; then
    if owned_pid "$name" >/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
  fi
  # Preserve stop metadata for diagnosis; the next start replaces it.
  mv "$RUN/$name.pid" "$RUN/$name.last.pid"
  [[ ! -f "$RUN/$name.start" ]] || mv "$RUN/$name.start" "$RUN/$name.last.start"
}
stop() { stop_one gateway; echo "Stopped instance $GW_PORT"; }
start() {
  local mode="${1:-go}"
  if [[ "$mode" != "go" ]]; then echo "Unsupported mode: $mode (only 'go' is supported)" >&2; return 1; fi
  if port_busy "$GW_PORT" || port_busy "$DASH_PORT"; then echo 'Instance port already occupied' >&2; return 1; fi
  cd "$ROOT"
  AGY_UPSTREAM_PROXY="${AGY_UPSTREAM_PROXY-http://127.0.0.1:7890}" \
    setsid nohup "$BIN" >>"$LOGS/go-gateway.out" 2>&1 < /dev/null 9>&- &
  remember_pid gateway "$!"
  printf '%s\n' "$mode" >"$RUN/mode"
  for ((i=0;i<40;i++)); do
    if curl -fsS -m 1 "http://127.0.0.1:$GW_PORT/healthz" >/dev/null 2>&1; then status; return 0; fi
    sleep 0.25
  done
  echo 'Gateway readiness timed out' >&2; stop; return 1
}
status() {
  echo "Instance: $GW_PORT/$DASH_PORT; runtime: $RUN"
  for p in "$GW_PORT" "$DASH_PORT"; do
    if port_busy "$p"; then echo "Port $p: listening"; else echo "Port $p: stopped"; fi
  done
  local pid
  if pid="$(owned_pid gateway)"; then ps -o pid=,rss=,comm= -p "$pid"; fi
  curl -fsS -m 3 "http://127.0.0.1:$GW_PORT/healthz" || return 1
  echo
}
# Serialize mutations so two concurrent starts cannot overwrite ownership metadata.
case "${1:-status}" in
  start|stop|restart) exec 9>"$RUN/control.lock"; flock -n 9 || { echo 'Instance operation already running' >&2; exit 1; } ;;
esac
case "${1:-status}" in
  start) start "${2:-go}" ;;
  stop) stop ;;
  restart) stop; start "${2:-go}" ;;
  status) status ;;
  logs) tail -f "$AGY_GW_LOG" ;;
  *) echo 'Usage: gw.sh start [go] | stop | restart [go] | status | logs' ;;
esac
