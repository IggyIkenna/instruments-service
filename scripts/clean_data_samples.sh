#!/usr/bin/env bash
# Clean CSV samples dropped by the service in dev mode.
# Run this whenever instruments_service/data/ gets too large.
#
# Usage:
#   bash scripts/clean_data_samples.sh           # dry run (show what would be deleted)
#   bash scripts/clean_data_samples.sh --delete  # actually delete

set -euo pipefail

DATA_DIR="$(dirname "$0")/../instruments_service/data"

echo "Scanning: $DATA_DIR"
FILES=$(find "$DATA_DIR" -type f \( -name "*.csv" -o -name "*.parquet" -o -name "*.json" \) 2>/dev/null || true)

if [ -z "$FILES" ]; then
    echo "Nothing to clean."
    exit 0
fi

TOTAL=$(echo "$FILES" | wc -l | tr -d ' ')
SIZE=$(echo "$FILES" | xargs du -sh 2>/dev/null | tail -1 | cut -f1 || echo "unknown")

echo "Found $TOTAL file(s) ($SIZE total):"
echo "$FILES"

if [ "${1:-}" = "--delete" ]; then
    echo "$FILES" | xargs rm -f
    echo "Deleted $TOTAL file(s)."
else
    echo ""
    echo "Dry run. Pass --delete to actually remove."
fi
