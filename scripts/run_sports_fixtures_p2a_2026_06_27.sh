#!/usr/bin/env bash
# Epic: sports_master
# Lifecycle: temporary
# Delete-when: Sports P2a complete (FIXTURES pending_fetch == 0, verified by run_fixture_completeness_audit_2026_06_25.py)
#
# One-off coordinator: backfill API-Football FIXTURES 2018-01-01 through today.
#
# Coverage window per plan sports_p2_history_apifootball_2015_to_present_2026_06_27.md:
#   FIXTURES: 2018-01-01 (subscription floor confirmed G2 — 2015-2017 inaccessible on our plan)
#
# Season-aware smart-skip: instruments-service skips off-season dates
# (EXPECTED_PRE_SEASON/POST_SEASON) and no-match days (EXPECTED_NO_FIXTURE)
# automatically via the season calendar + manifest check.
#
# Singleton-locked: API-Football enforces a per-key rate limit and the singleton
# lock in the adapter prevents concurrent api-football runs.
#
# Run from instruments-service directory with ADC credentials available:
#   bash scripts/run_sports_fixtures_p2a_2026_06_27.sh [--dry-run] [--start-date DATE]
#
# --dry-run:        print schedule without running
# --start-date DATE: override start date (default: 2018-01-01, for resume)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IS_DIR="$(dirname "$SCRIPT_DIR")"
RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
TODAY="$(date -u +%Y-%m-%d)"
DRY_RUN=false
START_DATE="2018-01-01"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)         DRY_RUN=true; shift ;;
    --start-date)      START_DATE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

echo "=== Sports P2a FIXTURES backfill ==="
echo "run_ts:     $RUN_TS"
echo "today:      $TODAY"
echo "dry-run:    $DRY_RUN"
echo "start_date: $START_DATE"
echo "end_date:   $TODAY"
echo

echo "--- FIXTURES (API_FOOTBALL, $START_DATE → $TODAY) ---"
if $DRY_RUN; then
  echo "  [DRY RUN] Would run: INSTRUMENTS_SERVICE_DIR=$IS_DIR bash $SCRIPT_DIR/sports_chunked_backfill.sh API_FOOTBALL $START_DATE $TODAY FIXTURES"
  exit 0
fi

log_dir="/tmp/sports-p2a-fixtures-${RUN_TS}"
mkdir -p "$log_dir"
echo "  logs: $log_dir"
INSTRUMENTS_SERVICE_DIR="$IS_DIR" \
  bash "$SCRIPT_DIR/sports_chunked_backfill.sh" "API_FOOTBALL" "$START_DATE" "$TODAY" "FIXTURES" \
  2>&1 | tee "$log_dir/coordinator.log"
echo "--- FIXTURES DONE ---"
echo

echo "=== P2a FIXTURES backfill complete ==="
