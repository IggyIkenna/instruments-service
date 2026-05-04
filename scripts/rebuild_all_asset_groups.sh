#!/usr/bin/env bash
# Sequential write-mode manifest rebuild across all instruments-service asset groups.
#
# Each rebuild:
#   - Scans GCS instrument_availability/by_date/ partitions for that AG bucket
#   - Adds any (date, venue, data_type) shards that exist on disk but are missing from
#     the canonical _index/availability_index.parquet
#   - Writes via ManifestWriter with VM_NAME + MANIFEST_PER_VM_SHARDS=true so output
#     goes to _index/per_vm/<vm_name>.parquet — the consolidator VM merges into
#     the canonical within ~60s (avoids CAS contention on the canonical).
#
# Sequential because each AG hits a different bucket but all share the local GCS
# client + ADC; running them in parallel saturates the auth thread and risks
# rate-limit churn. Sequential per-AG runtime: ~5–10 min CEFI/TRADFI, ~1 hour DEFI,
# ~1 hour SPORTS (manifest is large).
#
# Usage:
#   bash scripts/rebuild_all_asset_groups.sh [--dry-run]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN="--dry-run"
fi

LOG_ROOT="$REPO_ROOT/logs/rebuild-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_ROOT"

cd "$REPO_ROOT"
export GCP_PROJECT_ID=central-element-323112
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export MANIFEST_PER_VM_SHARDS=true

SUMMARY="$LOG_ROOT/summary.log"
echo "=== rebuild started $(date -u +%FT%TZ) ===" | tee "$SUMMARY"
echo "logs: $LOG_ROOT"                            | tee -a "$SUMMARY"
echo "dry_run: ${DRY_RUN:-no}"                    | tee -a "$SUMMARY"

for AG in CEFI TRADFI DEFI SPORTS; do
  ag_lower=${AG,,}
  ts=$(date +%Y%m%d-%H%M%S)
  log="$LOG_ROOT/${ag_lower}.log"
  vm_tag="rebuild_${ag_lower}_${ts}_$(od -An -N3 -tx1 /dev/urandom | tr -d ' ')"

  echo                                           | tee -a "$SUMMARY"
  echo "=== [$AG] start $(date -u +%FT%TZ) ===" | tee -a "$SUMMARY"
  echo "  vm_tag=$vm_tag log=$log"               | tee -a "$SUMMARY"

  start_epoch=$(date +%s)
  if VM_NAME="$vm_tag" .venv/bin/python scripts/rebuild_cefi_manifest.py \
       --asset-group "$AG" $DRY_RUN > "$log" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  end_epoch=$(date +%s)
  duration=$((end_epoch - start_epoch))

  if [[ "$rc" -eq 0 ]]; then
    new_count=$(grep -oP '\(\K[+\-]?[0-9]+ new\)' "$log" | head -1 || echo "?")
    total=$(grep -oP 'Result: \K[0-9]+' "$log" | head -1 || echo "?")
    echo "  [$AG] DONE rc=0 duration=${duration}s total=${total} new=${new_count}" | tee -a "$SUMMARY"
  else
    echo "  [$AG] FAILED rc=$rc duration=${duration}s — see $log"                  | tee -a "$SUMMARY"
  fi
done

echo                                              | tee -a "$SUMMARY"
echo "=== rebuild finished $(date -u +%FT%TZ) ===" | tee -a "$SUMMARY"
