#!/usr/bin/env bash
# migrate_entity_paths.sh — Migrate GCS entity paths for sports reference data.
#
# Renames:
#   entity=transfermarkt_teams/{file} → entity=player_values/player_values.parquet
#
# Other entities (fixtures, teams, standings, etc.) already match FSS reader
# expectations. Only transfermarkt_teams needs renaming.
#
# After migration, run the manifest rescan to update the availability index.
#
# Usage:
#   # Dry run (print what would be done):
#   bash scripts/migrate_entity_paths.sh --dry-run
#
#   # Run on a single date (test locally):
#   bash scripts/migrate_entity_paths.sh --date 2025-12-25
#
#   # Run all dates (parallel, for VM):
#   bash scripts/migrate_entity_paths.sh --all --parallel 32
#
# Requirements: gsutil, GNU parallel (for --parallel)

set -euo pipefail

BUCKET="gs://instruments-store-sports-central-element-323112"
PREFIX="sports_reference/by_date"
DRY_RUN=false
SINGLE_DATE=""
ALL=false
PARALLEL_JOBS=16

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --date) SINGLE_DATE="$2"; shift 2 ;;
        --all) ALL=true; shift ;;
        --parallel) PARALLEL_JOBS="$2"; shift 2 ;;
        --bucket) BUCKET="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

migrate_date() {
    local date="$1"
    local src="${BUCKET}/${PREFIX}/day=${date}/entity=transfermarkt_teams/"
    local dst_dir="${BUCKET}/${PREFIX}/day=${date}/entity=player_values/"

    # Check if source exists
    if ! gsutil -q stat "${src}transfermarkt_teams.parquet" 2>/dev/null; then
        # Also check for league-partitioned files
        if ! gsutil ls "${src}" 2>/dev/null | grep -q "."; then
            return 0  # No source data, skip
        fi
    fi

    # Check if destination already exists
    if gsutil -q stat "${dst_dir}player_values.parquet" 2>/dev/null; then
        echo "SKIP ${date}: player_values already exists"
        return 0
    fi

    if $DRY_RUN; then
        echo "DRY-RUN ${date}: would copy ${src} → ${dst_dir}"
        return 0
    fi

    # Copy the main parquet file (rename filename too)
    if gsutil -q stat "${src}transfermarkt_teams.parquet" 2>/dev/null; then
        gsutil -q cp "${src}transfermarkt_teams.parquet" "${dst_dir}player_values.parquet"
        echo "MIGRATED ${date}: transfermarkt_teams.parquet → player_values.parquet"
    fi

    # Also copy any league-partitioned sub-files
    local league_dirs
    league_dirs=$(gsutil ls "${src}" 2>/dev/null | grep "league=" || true)
    if [[ -n "$league_dirs" ]]; then
        gsutil -q -m cp -r "${src}league=*" "${dst_dir}"
        echo "MIGRATED ${date}: league-partitioned files copied"
    fi
}

export -f migrate_date
export BUCKET PREFIX DRY_RUN

if [[ -n "$SINGLE_DATE" ]]; then
    migrate_date "$SINGLE_DATE"
elif $ALL; then
    echo "Listing all dates with transfermarkt_teams data..."
    # List all day= partitions and filter for those with transfermarkt_teams
    DATES=$(gsutil ls "${BUCKET}/${PREFIX}/" 2>/dev/null \
        | grep "day=" \
        | sed 's|.*day=||' | sed 's|/||' \
        | sort)

    TOTAL=$(echo "$DATES" | wc -l | tr -d ' ')
    echo "Checking ${TOTAL} dates for transfermarkt_teams data..."

    if command -v parallel &>/dev/null; then
        echo "Using GNU parallel with ${PARALLEL_JOBS} jobs"
        echo "$DATES" | parallel -j "$PARALLEL_JOBS" migrate_date {}
    else
        echo "GNU parallel not found, running sequentially"
        echo "$DATES" | while read -r d; do
            migrate_date "$d"
        done
    fi
else
    echo "Usage: bash scripts/migrate_entity_paths.sh [--dry-run] [--date YYYY-MM-DD | --all] [--parallel N]"
    exit 1
fi

echo "Done. Run manifest rescan next."
