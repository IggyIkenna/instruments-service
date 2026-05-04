#!/usr/bin/env bash
# Fill a list of (venue, date) instrument-availability shards locally.
#
# Reads pairs from a file (one "VENUE,DATE" per line) or stdin. Spawns one
# instruments-service process per pair with a unique VM_NAME so per-VM
# manifest shards don't collide. Concurrency capped to keep Tardis API
# rate-limit-friendly.
#
# Faster than the date-range approach because:
#   - No pre-flight scan of the full range — each process does ONE date
#   - Single-day means the Tardis-instrument-cache leak doesn't accumulate
#   - Many short-lived procs amortize bootstrap overhead vs full sweep
#
# Usage:
#   bash scripts/local_fill_pairs.sh /tmp/cefi-missing-pairs.txt
#   PARALLEL=20 bash scripts/local_fill_pairs.sh /tmp/cefi-missing-pairs.txt
#   echo 'OKX-SPOT,2026-04-15' | bash scripts/local_fill_pairs.sh
#
# Env:
#   PARALLEL          concurrent procs (default 16)
#   ASSET_GROUP       defaults to CEFI
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PAIRS_FILE="${1:-/dev/stdin}"
PARALLEL="${PARALLEL:-16}"
ASSET_GROUP="${ASSET_GROUP:-CEFI}"
LOG_ROOT="$REPO_ROOT/logs/local-fill-pairs-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$LOG_ROOT"
cd "$REPO_ROOT"

export GCP_PROJECT_ID=central-element-323112
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export MANIFEST_PER_VM_SHARDS=true

SUMMARY="$LOG_ROOT/summary.log"
{
  echo "=== local fill-pairs run $(date -u +%FT%TZ) ==="
  echo "  pairs file:  $PAIRS_FILE"
  echo "  parallel:    $PARALLEL"
  echo "  asset group: $ASSET_GROUP"
  echo "  log dir:     $LOG_ROOT"
  echo
} | tee "$SUMMARY"

run_pair() {
  local venue="$1"
  local d="$2"
  local v_safe="${venue,,}"
  v_safe="${v_safe//[^a-z0-9]/_}"
  local d_safe="${d//-/}"
  local rand="$(od -An -N3 -tx1 /dev/urandom | tr -d ' ')"
  local vm_tag="local_${v_safe}_${d_safe}_${rand}"
  local log="$LOG_ROOT/${v_safe}_${d_safe}.log"

  if VM_NAME="$vm_tag" .venv/bin/instruments-service \
       --operation instruments --mode batch \
       --asset-group "$ASSET_GROUP" --venues "$venue" \
       --start-date "$d" --end-date "$d" \
       > "$log" 2>&1; then
    rc=0
  else
    rc=$?
  fi

  if [[ "$rc" -eq 0 ]]; then
    local n_wrote=$(grep -cE "wrote [0-9]+ records" "$log" 2>/dev/null | head -1)
    local n_failed=$(grep -cE "All records/venues rejected|SHARD FAILED" "$log" 2>/dev/null | head -1)
    n_wrote=${n_wrote:-0}
    n_failed=${n_failed:-0}
    if [[ "$n_failed" -gt 0 ]]; then
      printf "[%s] FAIL  %-22s %s rc=0 wrote=%s failed=%s\n" "$(date '+%H:%M:%S')" "$venue" "$d" "$n_wrote" "$n_failed" | tee -a "$SUMMARY"
    else
      printf "[%s] OK    %-22s %s wrote=%s\n" "$(date '+%H:%M:%S')" "$venue" "$d" "$n_wrote" | tee -a "$SUMMARY"
    fi
  else
    printf "[%s] FAIL  %-22s %s rc=%s — %s\n" "$(date '+%H:%M:%S')" "$venue" "$d" "$rc" "$log" | tee -a "$SUMMARY"
  fi
}

active=0
total=0
while IFS=, read -r venue d; do
  [[ -z "${venue:-}" ]] && continue
  [[ "${venue:0:1}" == "#" ]] && continue
  run_pair "$venue" "$d" &
  ((active+=1))
  ((total+=1))
  if [[ "$active" -ge "$PARALLEL" ]]; then
    wait -n
    ((active-=1))
  fi
done < "$PAIRS_FILE"
wait

{
  echo
  echo "=== complete $(date -u +%FT%TZ) ==="
  echo "  total pairs processed: $total"
  ok_count=$(grep -c "^\[.*\] OK " "$SUMMARY" 2>/dev/null | head -1)
  fail_count=$(grep -c "^\[.*\] FAIL " "$SUMMARY" 2>/dev/null | head -1)
  ok_count=${ok_count:-0}
  fail_count=${fail_count:-0}
  echo "  OK:    $ok_count"
  echo "  FAIL:  $fail_count"
} | tee -a "$SUMMARY"
