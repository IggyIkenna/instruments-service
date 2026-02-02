#!/bin/bash
# Migration script: instruments-service to by-venue folder structure
# 
# This script helps migrate from the legacy single-file instruments.parquet
# to the new by-venue folder structure:
#   - Old: instrument_availability/by_date/day-{date}/instruments.parquet
#   - New: instrument_availability/by_date/day-{date}/venue-{venue}/instruments.parquet
#
# Prerequisites:
#   - instruments-service code updated with by-venue storage (already done)
#   - Access to unified-trading-deployment-v2
#
# Usage:
#   ./migrate_to_by_venue.sh [--verify-only] [--cleanup-old]

set -e

PROJECT_ID="central-element-323112"
BUCKETS=(
    "instruments-store-cefi-${PROJECT_ID}"
    "instruments-store-tradfi-${PROJECT_ID}"
    "instruments-store-defi-${PROJECT_ID}"
)

# Parse arguments
VERIFY_ONLY=false
CLEANUP_OLD=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --verify-only) VERIFY_ONLY=true ;;
        --cleanup-old) CLEANUP_OLD=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

echo "=========================================="
echo "Instruments Service: By-Venue Migration"
echo "=========================================="
echo ""

if [ "$VERIFY_ONLY" = true ]; then
    echo "Mode: VERIFY ONLY (no changes)"
    echo ""
    
    for BUCKET in "${BUCKETS[@]}"; do
        echo "--- Checking $BUCKET ---"
        
        # Check for new by-venue structure
        VENUE_COUNT=$(gsutil ls "gs://${BUCKET}/instrument_availability/by_date/" 2>/dev/null | grep "venue-" | head -20 | wc -l || echo "0")
        echo "  By-venue folders found: ~$VENUE_COUNT+"
        
        # Check for legacy single-file structure
        LEGACY_COUNT=$(gsutil ls "gs://${BUCKET}/instrument_availability/by_date/day-*/instruments.parquet" 2>/dev/null | wc -l || echo "0")
        echo "  Legacy single-files found: $LEGACY_COUNT"
        
        # Sample the structure
        echo "  Sample paths:"
        gsutil ls "gs://${BUCKET}/instrument_availability/by_date/day-2024-01-15/" 2>/dev/null | head -5 || echo "    (no data for 2024-01-15)"
        echo ""
    done
    
    exit 0
fi

if [ "$CLEANUP_OLD" = true ]; then
    echo "Mode: CLEANUP OLD single-file instruments"
    echo ""
    echo "⚠️  WARNING: This will delete all legacy instruments.parquet files!"
    echo "             Make sure by-venue folders exist before running this."
    echo ""
    read -p "Are you sure? (yes/no): " CONFIRM
    
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
    
    for BUCKET in "${BUCKETS[@]}"; do
        echo "--- Cleaning up $BUCKET ---"
        
        # First verify by-venue folders exist
        VENUE_COUNT=$(gsutil ls "gs://${BUCKET}/instrument_availability/by_date/" 2>/dev/null | grep "venue-" | wc -l || echo "0")
        
        if [ "$VENUE_COUNT" -lt 100 ]; then
            echo "  ⚠️  Only $VENUE_COUNT by-venue folders found. Skipping cleanup for safety."
            continue
        fi
        
        echo "  Found $VENUE_COUNT by-venue folders. Proceeding with cleanup..."
        
        # Delete legacy single-file instruments.parquet (not inside venue folders)
        gsutil -m rm "gs://${BUCKET}/instrument_availability/by_date/day-*/instruments.parquet" 2>/dev/null || echo "  No legacy files to delete"
        
        echo "  ✅ Cleanup complete"
        echo ""
    done
    
    exit 0
fi

# Default: Run full migration (deploy + verify)
echo "Mode: FULL MIGRATION"
echo ""
echo "Step 1: Deploy instruments-service with --force"
echo "================================================"
echo ""
echo "Run the following command from unified-trading-deployment-v2:"
echo ""
echo "  python -m deployment.cli deploy \\"
echo "    --service instruments-service \\"
echo "    --compute vm \\"
echo "    --category CEFI,TRADFI,DEFI \\"
echo "    --start-date 2019-01-01 \\"
echo "    --end-date 2026-01-31 \\"
echo "    --force"
echo ""
echo "Or using the deployment UI, select instruments-service and click Deploy with --force."
echo ""
echo "Step 2: Verify new data"
echo "======================="
echo ""
echo "After deployment completes, run:"
echo "  ./migrate_to_by_venue.sh --verify-only"
echo ""
echo "Step 3: Cleanup old files"
echo "========================="
echo ""
echo "After verification, run:"
echo "  ./migrate_to_by_venue.sh --cleanup-old"
echo ""
