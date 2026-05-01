#!/usr/bin/env bash
# Resumable VM backfill runner for instruments-service.
# Splits date ranges into chunks, supports parallel workers, and writes checkpoints.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VENUE=""
ASSET_GROUP=""
START_DATE=""
END_DATE=""
CHUNK_DAYS=30
PARALLEL=1
CHECKPOINT_DIR=""
LOG_DIR=""
DRY_RUN=0
RESET_CHECKPOINT=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run_vm_backfill_e2e.sh \
    --venue DERIBIT \
    --asset-group CEFI \
    --start-date 2019-03-30 \
    --end-date 2019-12-31 \
    --chunk-days 14 \
    --parallel 4

Options:
  --venue <VENUE>             Venue shard to backfill (required)
  --asset-group <ASSET_GROUP>       Category (required)
  --start-date <YYYY-MM-DD>   Start date inclusive (required)
  --end-date <YYYY-MM-DD>     End date inclusive (required)
  --chunk-days <N>            Chunk size in days (default: 30)
  --parallel <N>              Max parallel chunk workers (default: 1)
  --checkpoint-dir <PATH>     Checkpoint root dir (default: .backfill-checkpoints)
  --log-dir <PATH>            Log root dir (default: logs/vm-backfill-<ts>)
  --reset-checkpoint          Delete venue checkpoint files before run
  --dry-run                   Print planned commands only
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue) VENUE="$2"; shift 2 ;;
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date) END_DATE="$2"; shift 2 ;;
    --chunk-days) CHUNK_DAYS="$2"; shift 2 ;;
    --parallel) PARALLEL="$2"; shift 2 ;;
    --checkpoint-dir) CHECKPOINT_DIR="$2"; shift 2 ;;
    --log-dir) LOG_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --reset-checkpoint) RESET_CHECKPOINT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$VENUE" || -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Missing required arguments." >&2
  usage
  exit 1
fi
if [[ "$CHUNK_DAYS" -lt 1 ]]; then
  echo "--chunk-days must be >= 1" >&2
  exit 1
fi
if [[ "$PARALLEL" -lt 1 ]]; then
  echo "--parallel must be >= 1" >&2
  exit 1
fi

if [[ -z "$CHECKPOINT_DIR" ]]; then
  CHECKPOINT_DIR="$REPO_ROOT/.backfill-checkpoints"
fi
if [[ -z "$LOG_DIR" ]]; then
  LOG_DIR="$REPO_ROOT/logs/vm-backfill-$(date +%Y%m%d-%H%M%S)"
fi

VENUE_UPPER="$(echo "$VENUE" | tr '[:lower:]' '[:upper:]')"
ASSET_GROUP_UPPER="$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
JOB_ID="${ASSET_GROUP_UPPER}_${VENUE_UPPER}_${START_DATE}_${END_DATE}_chunk${CHUNK_DAYS}"
JOB_CHECKPOINT_DIR="$CHECKPOINT_DIR/$JOB_ID"
JOB_LOG_DIR="$LOG_DIR/$JOB_ID"
SUMMARY_LOG="$JOB_LOG_DIR/summary.log"

mkdir -p "$JOB_CHECKPOINT_DIR" "$JOB_LOG_DIR"

if [[ "$RESET_CHECKPOINT" -eq 1 ]]; then
  rm -f "$JOB_CHECKPOINT_DIR"/*.done
fi

cd "$REPO_ROOT"

echo "========================================" | tee "$SUMMARY_LOG"
echo "Backfill job: $JOB_ID" | tee -a "$SUMMARY_LOG"
echo "Repo: $REPO_ROOT" | tee -a "$SUMMARY_LOG"
echo "Checkpoint: $JOB_CHECKPOINT_DIR" | tee -a "$SUMMARY_LOG"
echo "Logs: $JOB_LOG_DIR" | tee -a "$SUMMARY_LOG"
echo "Parallel workers: $PARALLEL" | tee -a "$SUMMARY_LOG"
echo "Dry run: $DRY_RUN" | tee -a "$SUMMARY_LOG"
echo "========================================" | tee -a "$SUMMARY_LOG"

CHUNKS_FILE="$JOB_LOG_DIR/chunks.txt"
.venv/bin/python "$SCRIPT_DIR/split_date_ranges.py" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --chunk-days "$CHUNK_DAYS" > "$CHUNKS_FILE"

chunk_count=$(wc -l < "$CHUNKS_FILE" | tr -d ' ')
if [[ "$chunk_count" -eq 0 ]]; then
  echo "No chunks generated; exiting." | tee -a "$SUMMARY_LOG"
  exit 0
fi

run_chunk() {
  local chunk_start="$1"
  local chunk_end="$2"
  local chunk_id="${chunk_start}_${chunk_end}"
  local done_file="$JOB_CHECKPOINT_DIR/$chunk_id.done"
  local log_file="$JOB_LOG_DIR/$chunk_id.log"

  if [[ -f "$done_file" ]]; then
    echo "[$(date '+%H:%M:%S')] SKIP  $chunk_start -> $chunk_end (checkpoint exists)" | tee -a "$SUMMARY_LOG"
    return 0
  fi

  local cmd=".venv/bin/instruments-service --operation instruments --mode batch --asset-group \"$ASSET_GROUP_UPPER\" --venues \"$VENUE_UPPER\" --start-date \"$chunk_start\" --end-date \"$chunk_end\""
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[$(date '+%H:%M:%S')] PLAN  $cmd" | tee -a "$SUMMARY_LOG"
    return 0
  fi

  echo "[$(date '+%H:%M:%S')] START $chunk_start -> $chunk_end" | tee -a "$SUMMARY_LOG"
  bash -lc "$cmd" >"$log_file" 2>&1
  local rc=$?
  if [[ "$rc" -ne 0 ]]; then
    echo "[$(date '+%H:%M:%S')] FAIL  $chunk_start -> $chunk_end (rc=$rc, log=$log_file)" | tee -a "$SUMMARY_LOG"
    return "$rc"
  fi

  date -u +"%Y-%m-%dT%H:%M:%SZ" > "$done_file"
  echo "[$(date '+%H:%M:%S')] DONE  $chunk_start -> $chunk_end (log=$log_file)" | tee -a "$SUMMARY_LOG"
}

active_jobs=0
while IFS= read -r line; do
  IFS=',' read -r chunk_start chunk_end <<< "$line"
  run_chunk "$chunk_start" "$chunk_end" &
  ((active_jobs+=1))

  if [[ "$active_jobs" -ge "$PARALLEL" ]]; then
    wait
    active_jobs=0
  fi
done < "$CHUNKS_FILE"

wait

done_count=$( (ls "$JOB_CHECKPOINT_DIR"/*.done 2>/dev/null || true) | wc -l | tr -d ' ' )
echo "========================================" | tee -a "$SUMMARY_LOG"
echo "Complete. checkpoints=$done_count chunks=$chunk_count" | tee -a "$SUMMARY_LOG"
echo "Summary log: $SUMMARY_LOG" | tee -a "$SUMMARY_LOG"
echo "========================================" | tee -a "$SUMMARY_LOG"
