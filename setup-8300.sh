#!/usr/bin/env bash
# llmlite-proxy (Zig) edge router on :8300 — A/B test double for the litellm pump.
# Routes (model name in the request -> backend):
# Model aliases (aliases.ini, same names as the litellm pump for clean A/B):
#   glm53f            -> openai/glm-5.3-flash    (172.30.30.29:8000, vLLM TP4, knapcio world)
#   glm53.tf3         -> deepseek/GLM-5.3-EXL3   (172.30.30.26:8888, TensorFold EXL3 TP3)
#   glm53f.llite      -> same backend as glm53f, separate name so pi-side A/B is readable in logs
#   glm53.tf3.llite   -> same backend as glm53.tf3
# Prefixed names (openai/... deepseek/...) also work directly.
# Auth: Authorization: Bearer sk-test-key (test key added at proxy startup).
# State is in-memory (KV backend: memory) — nothing to persist between runs.
#
# Request-size policy (2026-10-10): the proxy used to read a request into a 16 KiB stack buffer and
# forward the truncated remnant. It now sizes a heap buffer from Content-Length and refuses with
# 413 / 411 / 400. Ceiling below; raise it if a session legitimately needs more.
#   LLMLITE_MAX_REQUEST_BYTES  default 64 MiB (a 500k-token prompt plus images fits with room)
#   LLMLITE_CIRCUIT_BREAKER    0 = off. Off here on purpose: each provider slot is exactly one
#                              backend, so a latched breaker reads to clients as that backend dying.
set -euo pipefail
cd "$(dirname "$0")"
BIN=zig-out/bin/llmlite-proxy
LOG=proxy-8300.log
PID=proxy-8300.pid
PORT=8300
MAX_REQ=67108864        # 64 MiB
BREAKER=0               # off: single backend per provider slot

port_owner_pid() {
  # Neutralise pipefail/`set -e`: no listener is a normal answer, not an error.
  local line
  line=$(ss -ltnp 2>/dev/null | { grep ":$PORT " || true; } | head -1)
  { printf '%s' "$line" | { grep -o 'pid=[0-9]*' || true; }; } | head -1 | cut -d= -f2
}

start() {
  local owner
  owner=$(port_owner_pid)
  if [ -n "$owner" ]; then
    echo "already up on :$PORT (socket owner pid $owner, pid file $(cat "$PID" 2>/dev/null || echo '-'))"
    return 0
  fi
  nohup env LLMLITE_OPENAI_BASE_URL=http://172.30.30.29:8000/v1 \
            LLMLITE_DEEPSEEK_BASE_URL=http://172.30.30.26:8888/v1 \
            LLMLITE_MAX_REQUEST_BYTES=$MAX_REQ \
            LLMLITE_CIRCUIT_BREAKER=$BREAKER \
            "$BIN" --port=$PORT --aliases="$PWD/aliases.ini" >> "$LOG" 2>&1 &
  echo $! > "$PID"
  sleep 2
  owner=$(port_owner_pid)
  if [ -n "$owner" ]; then
    echo "up on :$PORT (pid $owner, ceiling ${MAX_REQ} B, breaker $BREAKER)"
  else
    echo "FAILED to start; log tail:"; tail -5 "$LOG"; exit 1
  fi
}

stop() {
  local killed=0 p owner
  if [ -f "$PID" ]; then
    p=$(cat "$PID" 2>/dev/null || echo '')
    if [ -n "$p" ] && kill "$p" 2>/dev/null; then killed=1; fi
  fi
  # Sweep every proxy process, not just the one the pid file names: a stale pid file used to leave an
  # older process holding :8300 (with an older alias table) while start() reported a fresh pid.
  for p in $(pgrep -x llmlite-proxy 2>/dev/null || true); do
    if kill "$p" 2>/dev/null; then killed=1; fi
  done
  sleep 1
  rm -f "$PID"
  owner=$(port_owner_pid)
  if [ -n "$owner" ]; then
    echo "STILL LISTENING on :$PORT as pid $owner — not reporting a clean stop:"
    ss -ltnp 2>/dev/null | grep ":$PORT "
    exit 1
  fi
  if [ "$killed" = 1 ]; then echo "stopped"; else echo "not running"; fi
}

status() {
  local owner filepid
  owner=$(port_owner_pid)
  filepid=$(cat "$PID" 2>/dev/null || echo '-')
  if [ -n "$owner" ]; then
    echo "up on :$PORT (socket owner pid $owner, pid file $filepid)"
    if [ "$owner" != "$filepid" ]; then
      echo "  WARNING: pid file disagrees with the socket owner — stop() sweeps both, but check for a stray"
    fi
  else
    echo "down"
  fi
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 {start|stop|status}"; exit 2 ;;
esac
