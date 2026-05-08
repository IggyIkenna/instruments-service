"""Post-recovery phantom dedup — drop empty_confirmed / attempted_failed / captured-zero
rows for any (date, data_type, league_id) cell where we now have a real
captured row with instrument_count > 0.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.md``.

Why this script exists
----------------------
The recovery work (Phase 2 FIXTURES, Phase 3 5-entity chain, parallel
multi-provider VMs) writes new ``captured`` rows with ``instrument_count
> 0`` for cells previously labeled ``empty_confirmed`` (phantom) or
``attempted_failed``.

For api_football per-fixture entities the new rows often carry
``venue=API_FOOTBALL`` while the phantom rows carry ``venue=""`` —
different ``venue`` axis means ``_merge_shard_frames`` dedup keeps BOTH
rows in canonical. The data-status dashboard's ``% empty`` aggregator
then double-counts: same (date, league_id, data_type) cell appears as
both "captured" AND "empty_confirmed".

Fix: walk every shard (canonical + per-VM), identify cells with a
captured-w/data row anywhere, drop OTHER rows in those cells (which
are by definition phantoms now superseded by truth).

Mirrors the ``delete_phantom_rows_from_shards.py`` shape: backup-then-
modify per shard, idempotent, supports ``--dry-run``.

Scope
-----
Targets all per-fixture sport entities:
  FIXTURES / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS /
  PLAYER_STATS / INJURIES / XG / MATCHES / ODDS / PREDICTIONS /
  SFI_PROGRESSIVE_STATS / WEATHER

Pre-flight
----------
Only run this AFTER all recovery VMs have STOPPED. Running mid-recovery
risks deleting rows we're about to overwrite, or missing cells that
gain captured rows after the script reads canonical.

Verify all VMs done:
  gcloud compute instances list --filter='(name~"^af-backfill-" OR \\
    name~"^us-backfill-" OR name~"^fs-backfill-" OR \\
    name~"^weather-backfill-" OR name~"^sfi-backfill-") AND \\
    status=RUNNING' --zones=asia-northeast1-c

Should return empty.

Usage
-----
  python scripts/dedup_phantom_after_recovery.py --dry-run
  python scripts/dedup_phantom_after_recovery.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"

# Per-fixture sport entities the recovery writes captured rows for. Phantom
# rows for these data_types are the ones we want to drop after the recovery
# captures a real row.
_TARGET_DATA_TYPES: tuple[str, ...] = (
    "FIXTURES",
    "FIXTURE_STATS",
    "FIXTURE_EVENTS",
    "FIXTURE_LINEUPS",
    "PLAYER_STATS",
    "INJURIES",
    "XG",
    "MATCHES",
    "ODDS",
    "PREDICTIONS",
    "SFI_PROGRESSIVE_STATS",
    "WEATHER",
)


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _backup_path(blob_name: str, run_ts: str) -> str:
    """Insert .{run_ts}.dedup_phantom.bak before .parquet extension."""
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.dedup_phantom.bak.parquet"
    return f"{blob_name}.{run_ts}.dedup_phantom.bak"


def _list_shard_paths(storage: object, bucket: str) -> list[str]:
    """Return canonical + every per-VM shard path."""
    paths: list[str] = [_INDEX_PATH]
    for blob in storage.list_blobs(bucket=bucket, prefix=_PER_VM_PREFIX):  # type: ignore[attr-defined]
        if blob.name.endswith(".parquet") and ".bak.parquet" not in blob.name:
            paths.append(blob.name)
    return paths


def _identify_real_data_cells(storage: object, bucket: str, shard_paths: list[str]) -> set[tuple[str, str, str]]:
    """Build set of (date, data_type, league_id) cells with at least one
    captured-w/data row across ALL shards.

    The captured-w/data row might live in canonical OR a per-VM shard
    (consolidator merge cycle pending). We need the union.
    """
    real_cells: set[tuple[str, str, str]] = set()
    for path in shard_paths:
        try:
            raw = storage.download_bytes(bucket, path)  # type: ignore[attr-defined]
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("dedup-phantom: could not read %s: %s", path, exc)
            continue
        if df.empty or "capture_status" not in df.columns:
            continue
        ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(-1).astype(int)
        captured_w_data_mask = (
            (df["capture_status"].astype(str) == "captured") & (ic > 0) & (df["data_type"].isin(_TARGET_DATA_TYPES))
        )
        sub = df.loc[captured_w_data_mask, ["date", "data_type", "league_id"]]
        for _idx, row in sub.iterrows():
            real_cells.add((str(row["date"]), str(row["data_type"]), str(row["league_id"])))
    return real_cells


def _filter_phantom_rows(df: pd.DataFrame, real_cells: set[tuple[str, str, str]]) -> tuple[pd.DataFrame, int]:
    """Return (filtered_df, dropped_count). Drops rows whose
    (date, data_type, league_id) is in ``real_cells`` AND which themselves
    are NOT captured-w/data (= they're phantoms superseded by truth).
    """
    if df.empty or "capture_status" not in df.columns:
        return df, 0
    ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(-1).astype(int)
    cells = list(
        zip(
            df["date"].astype(str),
            df["data_type"].astype(str),
            df["league_id"].astype(str),
            strict=False,
        )
    )
    in_real_cell = pd.Series([c in real_cells for c in cells], index=df.index)
    is_target_dt = df["data_type"].isin(_TARGET_DATA_TYPES)
    is_phantom = (df["capture_status"].astype(str) != "captured") | (ic <= 0)
    phantom_mask = in_real_cell & is_target_dt & is_phantom
    n_drop = int(phantom_mask.sum())
    return df.loc[~phantom_mask].copy(), n_drop


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default="central-element-323112")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bucket = _bucket_for_project(args.project_id)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    storage = get_storage_client(project_id=args.project_id)

    # Step 1: enumerate every shard.
    shard_paths = _list_shard_paths(storage, bucket)
    logger.info("Shards in scope: %d (canonical + per-VM)", len(shard_paths))

    # Step 2: union cells with real captured data across ALL shards.
    real_cells = _identify_real_data_cells(storage, bucket, shard_paths)
    logger.info(
        "Cells with captured-w/data (across all shards): %d",
        len(real_cells),
    )
    if not real_cells:
        logger.info("No cells have captured-w/data — nothing to dedup. Exiting.")
        return 0

    # Step 3: per-shard dry-run / apply.
    total_dropped = 0
    per_dt_counts: dict[str, int] = dict.fromkeys(_TARGET_DATA_TYPES, 0)
    shard_modifications: list[tuple[str, pd.DataFrame, bytes, int]] = []

    for path in shard_paths:
        try:
            raw = storage.download_bytes(bucket, path)  # type: ignore[attr-defined]
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("dedup-phantom: skip %s (read failed: %s)", path, exc)
            continue

        new_df, n_drop = _filter_phantom_rows(df, real_cells)
        if n_drop == 0:
            continue
        total_dropped += n_drop

        # Per-data_type breakdown for the shard
        ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(-1).astype(int)
        cells_local = list(
            zip(
                df["date"].astype(str),
                df["data_type"].astype(str),
                df["league_id"].astype(str),
                strict=False,
            )
        )
        in_real = pd.Series([c in real_cells for c in cells_local], index=df.index)
        is_phantom = (df["capture_status"].astype(str) != "captured") | (ic <= 0)
        for dt in _TARGET_DATA_TYPES:
            n = int(((df["data_type"] == dt) & in_real & is_phantom).sum())
            per_dt_counts[dt] += n

        shard_modifications.append((path, new_df, raw, n_drop))
        logger.info("%s: %d phantom rows to drop", path, n_drop)

    logger.info("Total phantom rows to drop across all shards: %d", total_dropped)
    logger.info("Per-data_type breakdown:")
    for dt, n in per_dt_counts.items():
        if n > 0:
            logger.info("  %-25s %d", dt, n)

    if args.dry_run:
        logger.info("[dry-run] No writes. Re-run with --apply to commit.")
        return 0

    if total_dropped == 0:
        logger.info("Nothing to drop. Exiting.")
        return 0

    # Step 4: backup + write each modified shard.
    for path, new_df, raw, n_drop in shard_modifications:
        backup_blob = _backup_path(path, run_ts)
        storage.upload_bytes(bucket, backup_blob, raw)  # type: ignore[attr-defined]
        out = io.BytesIO()
        new_df.to_parquet(out, index=False, engine="pyarrow")
        out.seek(0)
        storage.upload_bytes(bucket, path, out.read())  # type: ignore[attr-defined]
        logger.info("%s: wrote (drop=%d, backup=gs://%s/%s)", path, n_drop, bucket, backup_blob)

    logger.info(
        "Dedup complete: %d phantom rows dropped from %d shards. Backups retained "
        "(suffix .{run_ts}.dedup_phantom.bak.parquet) — delete after verification.",
        total_dropped,
        len(shard_modifications),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
