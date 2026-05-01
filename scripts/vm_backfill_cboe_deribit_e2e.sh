#!/usr/bin/env bash
# End-to-end VM backfill for CBOE(VIX path) + DERIBIT, with verification.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DRY_RUN=0
SKIP_VERIFY=0
PARALLEL_CBOE=6
PARALLEL_DERIBIT=6
CHUNK_CBOE=60
CHUNK_DERIBIT=14

# Defaults from current remediation scope.
CBOE_START="2020-06-01"
CBOE_END="$(date +%F)"
DERIBIT_2019_START="2019-03-30"
DERIBIT_2019_END="2019-12-31"
DERIBIT_SINGLE_DATE="2026-04-05"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/vm_backfill_cboe_deribit_e2e.sh [options]

Options:
  --dry-run                    Print planned actions only
  --skip-verify                Skip final manifest verification checks
  --parallel-cboe <N>          CBOE workers (default: 6)
  --parallel-deribit <N>       DERIBIT workers (default: 6)
  --chunk-cboe <DAYS>          CBOE chunk size (default: 60)
  --chunk-deribit <DAYS>       DERIBIT chunk size (default: 14)
  --cboe-end <YYYY-MM-DD>      CBOE range end (default: today)
  --deribit-2019-end <YYYY-MM-DD> DERIBIT 2019 range end (default: 2019-12-31)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --skip-verify) SKIP_VERIFY=1; shift ;;
    --parallel-cboe) PARALLEL_CBOE="$2"; shift 2 ;;
    --parallel-deribit) PARALLEL_DERIBIT="$2"; shift 2 ;;
    --chunk-cboe) CHUNK_CBOE="$2"; shift 2 ;;
    --chunk-deribit) CHUNK_DERIBIT="$2"; shift 2 ;;
    --cboe-end) CBOE_END="$2"; shift 2 ;;
    --deribit-2019-end) DERIBIT_2019_END="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

echo "=== Backfill CBOE (TRADFI) ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue CBOE \
    --asset-group TRADFI \
    --start-date "$CBOE_START" \
    --end-date "$CBOE_END" \
    --chunk-days "$CHUNK_CBOE" \
    --parallel "$PARALLEL_CBOE" \
    --dry-run
else
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue CBOE \
    --asset-group TRADFI \
    --start-date "$CBOE_START" \
    --end-date "$CBOE_END" \
    --chunk-days "$CHUNK_CBOE" \
    --parallel "$PARALLEL_CBOE"
fi

echo "=== Backfill DERIBIT (CEFI 2019 range) ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue DERIBIT \
    --asset-group CEFI \
    --start-date "$DERIBIT_2019_START" \
    --end-date "$DERIBIT_2019_END" \
    --chunk-days "$CHUNK_DERIBIT" \
    --parallel "$PARALLEL_DERIBIT" \
    --dry-run
else
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue DERIBIT \
    --asset-group CEFI \
    --start-date "$DERIBIT_2019_START" \
    --end-date "$DERIBIT_2019_END" \
    --chunk-days "$CHUNK_DERIBIT" \
    --parallel "$PARALLEL_DERIBIT"
fi

echo "=== Backfill DERIBIT (single date gap) ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue DERIBIT \
    --asset-group CEFI \
    --start-date "$DERIBIT_SINGLE_DATE" \
    --end-date "$DERIBIT_SINGLE_DATE" \
    --chunk-days 1 \
    --parallel 1 \
    --dry-run
else
  bash "$SCRIPT_DIR/run_vm_backfill_e2e.sh" \
    --venue DERIBIT \
    --asset-group CEFI \
    --start-date "$DERIBIT_SINGLE_DATE" \
    --end-date "$DERIBIT_SINGLE_DATE" \
    --chunk-days 1 \
    --parallel 1
fi

if [[ "$SKIP_VERIFY" -eq 1 ]]; then
  echo "Verification skipped (--skip-verify)."
  exit 0
fi

echo "=== Verify CBOE coverage ==="
.venv/bin/python "$SCRIPT_DIR/verify_instrument_manifest_coverage.py" \
  --asset-group TRADFI \
  --venue CBOE \
  --start-date "$CBOE_START" \
  --end-date "$CBOE_END" \
  --min-count 1

echo "=== Verify DERIBIT 2019 coverage ==="
.venv/bin/python "$SCRIPT_DIR/verify_instrument_manifest_coverage.py" \
  --asset-group CEFI \
  --venue DERIBIT \
  --start-date "$DERIBIT_2019_START" \
  --end-date "$DERIBIT_2019_END" \
  --min-count 1

echo "=== Verify DERIBIT single date coverage ==="
.venv/bin/python "$SCRIPT_DIR/verify_instrument_manifest_coverage.py" \
  --asset-group CEFI \
  --venue DERIBIT \
  --start-date "$DERIBIT_SINGLE_DATE" \
  --end-date "$DERIBIT_SINGLE_DATE" \
  --min-count 1

echo "All backfill and verification steps completed."
