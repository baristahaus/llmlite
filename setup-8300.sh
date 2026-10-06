#!/usr/bin/env bash
# llmlite-proxy (Zig) edge router on :8300 — A/B test double for the litellm pump.
# Routes (model name in the request -> backend):
# Model aliases (aliases.ini, same names as the litellm pump for clean A/B):
#   glm53f    -> openai/glm-5.3-flash   (172.30.30.29:8000, vLLM TP4, knapcio world)
#   glm53.tf3 -> deepseek/GLM-5.3-EXL3  (172.30.30.26:8888, TensorFold EXL3 TP3)
# Prefixed names (openai/... deepseek/...) also work directly.
# Auth: Authorization: Bearer sk-test-key (test key added at proxy startup).
# State is in-memory (KV backend: memory) — nothing to persist between runs.
set -euo pipefail
cd "$(dirname "$0")"
BIN=zig-out/bin/llmlite-proxy
LOG=proxy-8300.log
PID=proxy-8300.pid
PORT=8300

start() {
  if ss -tln | grep -q ":$PORT "; then echo "already up on :$PORT (pid $(cat "$PID" 2>/dev/null || echo '?'))"; return; fi
  nohup env LLMLITE_OPENAI_BASE_URL=http://172.30.30.29:8000/v1 \
            LLMLITE_DEEPSEEK_BASE_URL=http://172.30.30.26:8888/v1 \
            "$BIN" --port=$PORT --aliases="$PWD/aliases.ini" >> "$LOG" 2>&1 &
  echo $! > "$PID"
  sleep 2
  ss -tln | grep -q ":$PORT " && echo "up on :$PORT (pid $(cat "$PID"))" || { echo "FAILED to start; log tail:"; tail -5 "$LOG"; exit 1; }
}
stop() {
  [ -f "$PID" ] && kill "$(cat "$PID")" 2>/dev/null && rm -f "$PID" && echo "stopped" || echo "not running (no pid file)"
}
status() {
  if ss -tln | grep -q ":$PORT "; then echo "up on :$PORT (pid $(cat "$PID" 2>/dev/null || echo '?'))"; else echo "down"; fi
}
case "${1:-status}" in start) start ;; stop) stop ;; status) status ;; *) echo "usage: $0 {start|stop|status}"; exit 2 ;; esac
