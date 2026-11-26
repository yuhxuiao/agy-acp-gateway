#!/usr/bin/env bash
# Baseline & regression benchmark script for agy-acp-gateway
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$HOME/.local/go/bin:$PATH"
DASH_PORT="${AGY_DASH_PORT:-18891}"

echo "=== 1. Gateway Process RSS (MB) ==="
ps -eo pid,rss,comm,args --sort=-rss | awk '$3 ~ /^agy-acp/ {printf "  PID %-8s %6.2f MB  %s\n", $1, $2/1024, $3}'

echo "=== 2. Dashboard /api/dashboard First-Byte Latency ==="
for i in 1 2 3; do
  curl -s -o /dev/null -w "  poll #$i: http=%{http_code} size=%{size_download}B time=%{time_total}s\n" "http://127.0.0.1:${DASH_PORT}/api/dashboard" || true
done

echo "=== 3. Largest Conversation DB Lookup Benchmark ==="
CONV_DIR="$HOME/.gemini/antigravity-cli/conversations"
if [ -d "$CONV_DIR" ]; then
  BIG_DB="$(ls -S "$CONV_DIR"/*.db 2>/dev/null | head -1 || true)"
  if [ -n "$BIG_DB" ]; then
    echo "  Target DB: $(basename "$BIG_DB") ($(du -h "$BIG_DB" | cut -f1))"
    cd "$ROOT/go-gateway"
    AGY_BENCH_DB="$BIG_DB" go test -run=^$ -bench=BenchmarkLatestStepLookup -benchmem . 2>/dev/null || echo "  (benchmark will run once BenchmarkLatestStepLookup is registered)"
  fi
fi
