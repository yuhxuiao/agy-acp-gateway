#!/usr/bin/env bash
# agy-acp-gateway: start [go], stop, restart [go], status, logs
# Each port has its own runtime paths. Default ports retain the legacy production paths.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNAME_S="$(uname -s)"
load_dotenv() {
  local file="$1" line key val
  [[ -n "$file" && -f "$file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
    if [[ "$line" == export[[:space:]]* ]]; then
      line="${line#export}"
      line="${line#"${line%%[![:space:]]*}"}"
    fi
    [[ "$line" == *=* ]] || continue
    key="${line%%=*}"
    val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    if [[ "${#val}" -ge 2 && "${val:0:1}" == '"' && "${val:${#val}-1:1}" == '"' ]]; then
      val="${val:1:${#val}-2}"
    elif [[ "${#val}" -ge 2 && "${val:0:1}" == "'" && "${val:${#val}-1:1}" == "'" ]]; then
      val="${val:1:${#val}-2}"
    fi
    if [[ -z "${!key+x}" ]]; then
      export "$key=$val"
    fi
  done < "$file"
}
if [[ -n "${AGY_ENV_FILE+x}" ]]; then
  load_dotenv "$AGY_ENV_FILE"
elif [[ "${AGY_GW_PORT:-18890}" == 18890 && "${AGY_RUN_DIR:-$ROOT/runtime}" == "$ROOT/runtime" ]]; then
  load_dotenv "$ROOT/.env"
fi
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
port_busy() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH | awk -v port=":$1" '$4 ~ port"$" {found=1} END {exit !found}'
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  elif [[ -x /usr/sbin/lsof ]]; then
    /usr/sbin/lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else
    python3 -c 'import socket,sys; s=socket.socket(); s.settimeout(0.2); r=s.connect_ex(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0 if r == 0 else 1)' "$1" >/dev/null 2>&1
  fi
}
start_ticks() {
  if [[ -r "/proc/$1/stat" ]]; then
    sed 's/^.*)[[:space:]]*//' "/proc/$1/stat" 2>/dev/null | awk '{print $20}' || true
  elif [[ "$UNAME_S" == "Darwin" ]]; then
    LC_ALL=C ps -o lstart= -p "$1" 2>/dev/null | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true
  fi
}
owned_pid() {
  local name="$1" pid actual expected actual_ticks
  [[ -f "$RUN/$name.pid" ]] || return 1
  pid="$(cat "$RUN/$name.pid")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && [[ "$(ps -o stat= -p "$pid" 2>/dev/null || true)" != Z* ]] || return 1
  if [[ -f "$RUN/$name.start" ]]; then
    expected="$(cat "$RUN/$name.start")"
    actual_ticks="$(start_ticks "$pid")"
    [[ -n "$expected" && "$actual_ticks" == "$expected" ]] || { echo 'PID was reused; refusing signal' >&2; return 2; }
  else
    # Legacy production PID files: verify the executable before a one-time upgrade stop.
    if [[ -d /proc ]]; then
      actual="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
      actual="${actual% (deleted)}"
      expected="$(readlink -f "$BIN" 2>/dev/null || printf '%s' "$BIN")"
    elif [[ "$UNAME_S" == "Darwin" ]]; then
      actual="$(python3 -c 'import ctypes,os,sys; lp=ctypes.CDLL("/usr/lib/libproc.dylib"); b=ctypes.create_string_buffer(4096); r=lp.proc_pidpath(ctypes.c_int(int(sys.argv[1])),b,ctypes.c_uint32(len(b))); print(os.path.realpath(os.fsdecode(b.value)) if r>0 else "")' "$pid" 2>/dev/null || true)"
      expected="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$BIN" 2>/dev/null || printf '%s' "$BIN")"
    else
      actual=""
      expected="$BIN"
    fi
    [[ -n "$actual" && ( "$actual" == "$BIN" || "$actual" == "$expected" ) ]] || { echo 'Unverified legacy PID; refusing signal' >&2; return 2; }
  fi
  printf '%s\n' "$pid"
}
remember_pid() {
  local ticks
  printf '%s\n' "$2" >"$RUN/$1.pid"
  ticks="$(start_ticks "$2")"
  if [[ -n "$ticks" ]]; then
    printf '%s\n' "$ticks" >"$RUN/$1.start"
  else
    rm -f "$RUN/$1.start"
  fi
}
stop_one() {
  local name="$1" pid code child ticks i
  if pid="$(owned_pid "$name")"; then :; else
    code=$?; [[ "$code" == 1 ]] && return 0; return "$code"
  fi
  # Snapshot only this instance's descendants, including ACP's independent process groups.
  local -a descendants=()
  while read -r child; do
    if [[ -n "$child" ]]; then
      ticks="$(start_ticks "$child" || true)"
      [[ -n "$ticks" ]] && descendants+=("$child:$ticks")
    fi
  done < <(ps -eo pid=,ppid= | awk -v root="$pid" '{parent[$1]=$2} END {for(p in parent){q=p;for(n=0;n<128 && q in parent;n++){if(parent[q]==root){print p;break}q=parent[q]}}}')
  kill -TERM "$pid" 2>/dev/null || true
  for ((i=0;i<120;i++)); do
    kill -0 "$pid" 2>/dev/null || break
    [[ "$(ps -o stat= -p "$pid" 2>/dev/null || true)" == Z* ]] && break
    sleep 0.1
  done
  if (( ${#descendants[@]} > 0 )); then
    for item in "${descendants[@]}"; do
      child="${item%%:*}"; ticks="${item#*:}"
      if [[ -n "$ticks" && "$(start_ticks "$child" || true)" == "$ticks" ]]; then kill -TERM "$child" 2>/dev/null || true; fi
    done
  fi
  if kill -0 "$pid" 2>/dev/null && [[ "$(ps -o stat= -p "$pid" 2>/dev/null || true)" != Z* ]]; then
    if owned_pid "$name" >/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    for ((i=0;i<20;i++)); do
      kill -0 "$pid" 2>/dev/null || break
      [[ "$(ps -o stat= -p "$pid" 2>/dev/null || true)" == Z* ]] && break
      sleep 0.1
    done
  fi
  # Preserve stop metadata for diagnosis; the next start replaces it.
  mv "$RUN/$name.pid" "$RUN/$name.last.pid"
  [[ ! -f "$RUN/$name.start" ]] || mv "$RUN/$name.start" "$RUN/$name.last.start"
}
stop() { stop_one gateway; echo "Stopped instance $GW_PORT"; }
start() {
  local mode="${1:-go}" gw_pid
  if [[ "$mode" != "go" ]]; then echo "Unsupported mode: $mode (only 'go' is supported)" >&2; return 1; fi
  if [[ ! -x "$BIN" ]]; then echo "Gateway binary not executable: $BIN" >&2; return 1; fi
  if port_busy "$GW_PORT" || port_busy "$DASH_PORT"; then echo 'Instance port already occupied' >&2; return 1; fi
  cd "$ROOT"
  if command -v setsid >/dev/null 2>&1; then
    AGY_UPSTREAM_PROXY="${AGY_UPSTREAM_PROXY-http://127.0.0.1:7890}" \
      setsid nohup "$BIN" >>"$LOGS/go-gateway.out" 2>&1 < /dev/null 9>&- &
  else
    AGY_UPSTREAM_PROXY="${AGY_UPSTREAM_PROXY-http://127.0.0.1:7890}" \
      nohup python3 -c 'import os,sys; os.setsid(); os.execv(sys.argv[1], sys.argv[1:])' "$BIN" >>"$LOGS/go-gateway.out" 2>&1 < /dev/null 9>&- &
  fi
  gw_pid="$!"
  remember_pid gateway "$gw_pid"
  printf '%s\n' "$mode" >"$RUN/mode"
  for ((i=0;i<40;i++)); do
    if curl --noproxy '*' -fsS -m 1 "http://127.0.0.1:$GW_PORT/healthz" >/dev/null 2>&1 && port_busy "$DASH_PORT"; then status; return 0; fi
    if ! kill -0 "$gw_pid" 2>/dev/null; then
      echo 'Gateway process exited before readiness' >&2; stop; return 1
    fi
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
  curl --noproxy '*' -fsS -m 3 "http://127.0.0.1:$GW_PORT/healthz" || return 1
  echo
}
# Serialize mutations so two concurrent starts cannot overwrite ownership metadata.
case "${1:-status}" in
  start|stop|restart)
    if [[ "${_AGY_GW_FLOCKED:-}" == "$RUN/control.lock" ]]; then
      unset _AGY_GW_FLOCKED
    elif command -v flock >/dev/null 2>&1; then
      unset _AGY_GW_FLOCKED
      exec 9>"$RUN/control.lock"
      flock -n 9 || { echo 'Instance operation already running' >&2; exit 1; }
    else
      exec python3 -c '
import fcntl, os, sys
lock_path = sys.argv[1]
fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
if fd != 9:
    os.dup2(fd, 9)
    os.close(fd)
os.set_inheritable(9, True)
try:
    fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError:
    sys.stderr.write("Instance operation already running\n")
    sys.exit(1)
os.environ["_AGY_GW_FLOCKED"] = lock_path
os.execv(sys.argv[2], sys.argv[2:])
' "$RUN/control.lock" "$BASH" "$0" "$@"
    fi
    ;;
  *)
    unset _AGY_GW_FLOCKED
    ;;
esac
case "${1:-status}" in
  start) start "${2:-go}" ;;
  stop) stop ;;
  restart) stop; start "${2:-go}" ;;
  status) status ;;
  logs) tail -f "$AGY_GW_LOG" ;;
  *) echo 'Usage: gw.sh start [go] | stop | restart [go] | status | logs' ;;
esac
