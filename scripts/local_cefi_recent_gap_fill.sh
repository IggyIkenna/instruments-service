#!/usr/bin/env bash
# Local backfill of CeFi recent-window gaps (last 30 days).
#
# Strategy: spawn 1 process per venue with --start-date 2026-04-04 --end-date
# 2026-05-04. Pre-flight skip in the orchestrator skips already-CAPTURED
# (date, venue) shards, so each process only attempts the missing days.
#
# Each process gets a unique VM_NAME so per-VM shards don't collide:
#   _index/per_vm/local_<venue>_<ts>_<rand>.parquet
#
# Bounded parallelism (default 4) to avoid Tardis rate limits.
#
# Required UAC + instruments-service code state (already in local repo):
#   - UAC: instrument_validation._CEFI_VENUES has OKX-SPOT/FUTURES/SWAP + COINBASE-SPOT
#   - tardis.py: TardisReferenceDataAdapter.get_instruments_cached overrides
#     base to drop date from cache_key (fixes DERIBIT memory leak)
#
# Usage:
#   bash scripts/local_cefi_recent_gap_fill.sh                    # all venues, parallel=4
#   bash scripts/local_cefi_recent_gap_fill.sh --parallel 2        # slower, gentler on Tardis
#   bash scripts/local_cefi_recent_gap_fill.sh --venues "DERIBIT"  # single venue
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

START_DATE="${START_DATE:-2026-04-04}"
END_DATE="${END_DATE:-2026-05-04}"
PARALLEL="${PARALLEL:-4}"
VENUES="${VENUES:-ASTER BINANCE-SPOT BITGET-SPOT BYBIT HYPERLIQUID UPBIT COINBASE-SPOT OKX-FUTURES OKX-SPOT OKX-SWAP DERIBIT}"
LOG_ROOT="$REPO_ROOT/logs/local-recent-fill-$(date +%Y%m%d-%H%M%S)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --parallel) PARALLEL="$2"; shift 2 ;;
    --venues) VENUES="$2"; shift 2 ;;
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date) END_DATE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$LOG_ROOT"
cd "$REPO_ROOT"

export GCP_PROJECT_ID=central-element-323112
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export MANIFEST_PER_VM_SHARDS=true

SUMMARY="$LOG_ROOT/summary.log"
{
  echo "=== local CeFi recent-gap fill $(date -u +%FT%TZ) ==="
  echo "  range:    $START_DATE → $END_DATE"
  echo "  parallel: $PARALLEL"
  echo "  venues:   $VENUES"
  echo "  log dir:  $LOG_ROOT"
  echo
} | tee "$SUMMARY"

run_venue() {
  local venue="$1"
  local v_lower="${venue,,}"
  local v_safe="${v_lower//[^a-z0-9]/_}"
  local ts="$(date +%Y%m%d-%H%M%S)"
  local rand="$(od -An -N3 -tx1 /dev/urandom | tr -d ' ')"
  local vm_tag="local_${v_safe}_${ts}_${rand}"
  local log="$LOG_ROOT/${v_safe}.log"

  echo "[$(date '+%H:%M:%S')] START $venue (vm_name=$vm_tag)" | tee -a "$SUMMARY"
  if VM_NAME="$vm_tag" .venv/bin/instruments-service \
       --operation instruments --mode batch \
       --asset-group CEFI --venues "$venue" \
       --start-date "$START_DATE" --end-date "$END_DATE" \
       > "$log" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  if [[ "$rc" -eq 0 ]]; then
    local n_wrote=$(grep -cE "wrote [0-9]+ records" "$log" 2>/dev/null || echo 0)
    local n_failed=$(grep -cE "All records/venues rejected|SHARD FAILED" "$log" 2>/dev/null || echo 0)
    echo "[$(date '+%H:%M:%S')] DONE  $venue rc=0 wrote=$n_wrote failed=$n_failed" | tee -a "$SUMMARY"
  else
    echo "[$(date '+%H:%M:%S')] FAIL  $venue rc=$rc — see $log" | tee -a "$SUMMARY"
  fi
}

active=0
for venue in $VENUES; do
  run_venue "$venue" &
  ((active+=1))
  if [[ "$active" -ge "$PARALLEL" ]]; then
    wait
    active=0
  fi
done
wait

echo                               | tee -a "$SUMMARY"
echo "=== complete $(date -u +%FT%TZ) ===" | tee -a "$SUMMARY"
