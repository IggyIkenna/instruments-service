#!/usr/bin/env bash
# Local-only sports backfill with 30-day chunks to avoid OOM from accumulating
# leagues/teams/standings caches in a single 6-year proc.
#
# Each chunk: spawn a fresh instruments-service proc for a 30-day window. Proc
# dies after the window (or earlier on completion), reclaiming all cached DFs.
# Next chunk starts fresh. RAM bounded per chunk to ~500 MB.
#
# Usage: bash /tmp/sports-chunked-backfill.sh PROVIDER [START_DATE] [END_DATE]
# Defaults: START=2020-06-01, END=today
#
# Sequential within a provider (singleton-lock on shared API key respected).
# Multiple providers in parallel is safe (different keys, different APIs).
set -euo pipefail

PROVIDER="${1:?provider required: API_FOOTBALL|TRANSFERMARKT|FOOTYSTATS|UNDERSTAT|OPEN_METEO}"
START="${2:-2020-06-01}"
END="${3:-$(date -u +%Y-%m-%d)}"
CHUNK_DAYS="${CHUNK_DAYS:-30}"
LOG_DIR="/tmp/sports-chunked-${PROVIDER,,}"
mkdir -p "$LOG_DIR"

cd /home/hk/unified-trading-system-repos/instruments-service
export GCP_PROJECT_ID=central-element-323112
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false

echo "=== chunked sports backfill ==="
echo "provider:    $PROVIDER"
echo "range:       $START → $END ($CHUNK_DAYS-day chunks)"
echo "logs:        $LOG_DIR"
echo

# Chunk-iterate
current="$START"
chunk_n=0
while [[ "$current" < "$END" || "$current" == "$END" ]]; do
  chunk_n=$((chunk_n + 1))
  chunk_end=$(date -u -d "$current + $((CHUNK_DAYS - 1)) days" +%Y-%m-%d 2>/dev/null || \
              python3 -c "from datetime import date,timedelta; print((date.fromisoformat('$current')+timedelta(days=$((CHUNK_DAYS - 1)))).isoformat())")
  if [[ "$chunk_end" > "$END" ]]; then chunk_end="$END"; fi

  log="$LOG_DIR/chunk-${chunk_n}-${current}_${chunk_end}.log"
  echo "[$(date +%H:%M:%S)] chunk $chunk_n: $current → $chunk_end"

  # 60 min hard cap per chunk; if it stalls (rare), kill and continue.
  timeout 3600 .venv/bin/instruments-service \
    --operation instruments --mode batch \
    --asset-group SPORTS --sports-provider "$PROVIDER" \
    --start-date "$current" --end-date "$chunk_end" \
    > "$log" 2>&1
  rc=$?

  # Quick health summary of chunk
  done_count=$(grep -cE "DONE for date=|wrote [0-9]+ records|short-circuit" "$log" 2>/dev/null || echo 0)
  err_count=$(grep -cE "^[0-9-]+ [0-9:,]+ ERROR" "$log" 2>/dev/null || echo 0)
  echo "  rc=$rc done_lines=$done_count errors=$err_count"

  # Step current forward
  current=$(date -u -d "$chunk_end + 1 day" +%Y-%m-%d 2>/dev/null || \
            python3 -c "from datetime import date,timedelta; print((date.fromisoformat('$chunk_end')+timedelta(days=1)).isoformat())")
done

echo
echo "=== $PROVIDER chunked backfill done ($chunk_n chunks) ==="
