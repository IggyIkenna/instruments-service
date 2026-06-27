#!/usr/bin/env bash
# Epic: sports_master
# Lifecycle: temporary
# Delete-when: Sports P2a complete (enrichment+core pending_fetch == 0, verified by run_fixture_completeness_audit_2026_06_25.py)
#
# One-off coordinator: backfill API-Football enrichment + core data types from
# their DATA_TYPE_COVERAGE_START through today.
#
# Coverage windows per plan sports_p2_history_apifootball_2015_to_present_2026_06_27.md:
#   Enrichment (FIXTURE_EVENTS, FIXTURE_LINEUPS, FIXTURE_STATS, PLAYER_STATS): 2020-06-06
#   INJURIES: 2021-01-01
#   STANDINGS: 2018-01-01
#   (FIXTURES backfill is a separate task — NOT in scope here)
#
# Season-aware smart-skip: instruments-service skips off-season dates
# (EXPECTED_PRE_SEASON/POST_SEASON) and no-match days (EXPECTED_NO_FIXTURE)
# automatically via the season calendar + manifest check.
#
# Entities run SEQUENTIALLY: API-Football enforces a per-key rate limit and
# the singleton lock in the adapter prevents concurrent api-football runs.
#
# Run from instruments-service directory with ADC credentials available:
#   bash scripts/run_sports_enrichment_core_p2a_2026_06_27.sh [--dry-run] [--entity ENTITY]
#
# --dry-run: print schedule without running
# --entity ENTITY: run only that entity (for resume/targeted re-run)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IS_DIR="$(dirname "$SCRIPT_DIR")"
RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
TODAY="$(date -u +%Y-%m-%d)"
DRY_RUN=false
ONLY_ENTITY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --entity)    ONLY_ENTITY="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# entity|provider|coverage_start
ENTITIES=(
  "FIXTURE_EVENTS|API_FOOTBALL|2020-06-06"
  "FIXTURE_LINEUPS|API_FOOTBALL|2020-06-06"
  "FIXTURE_STATS|API_FOOTBALL|2020-06-06"
  "PLAYER_STATS|API_FOOTBALL|2020-06-06"
  "INJURIES|API_FOOTBALL|2021-01-01"
  "STANDINGS|API_FOOTBALL|2018-01-01"
)

echo "=== Sports P2a enrichment+core backfill ==="
echo "run_ts:  $RUN_TS"
echo "today:   $TODAY"
echo "dry-run: $DRY_RUN"
echo "filter:  ${ONLY_ENTITY:-ALL}"
echo

for entry in "${ENTITIES[@]}"; do
  IFS='|' read -r entity provider start <<< "$entry"
  if [[ -n "$ONLY_ENTITY" && "$entity" != "$ONLY_ENTITY" ]]; then
    continue
  fi

  echo "--- $entity ($provider, $start → $TODAY) ---"
  if $DRY_RUN; then
    echo "  [DRY RUN] Would run: INSTRUMENTS_SERVICE_DIR=$IS_DIR bash $SCRIPT_DIR/sports_chunked_backfill.sh $provider $start $TODAY $entity"
    continue
  fi

  log_dir="/tmp/sports-p2a-${entity,,}-${RUN_TS}"
  mkdir -p "$log_dir"
  echo "  logs: $log_dir"
  INSTRUMENTS_SERVICE_DIR="$IS_DIR" \
    bash "$SCRIPT_DIR/sports_chunked_backfill.sh" "$provider" "$start" "$TODAY" "$entity" \
    2>&1 | tee "$log_dir/coordinator.log"
  echo "--- $entity DONE ---"
  echo
done

echo "=== P2a enrichment+core backfill complete ==="
