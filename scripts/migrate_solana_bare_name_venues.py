"""Migrate legacy bare-name Solana DeFi manifest rows to canonical {PROTOCOL}-SOLANA naming.

Category A venues (MARINADE, RAYDIUM, ORCA, KAMINO, SOLEND, MARGINFI):
  - Have real captured parquets under bare venue names (e.g. venue=MARINADE).
  - Each parquet is read, the ``venue`` column is rewritten to ``MARINADE-SOLANA``,
    and the file is uploaded to the canonical path (``venue=MARINADE-SOLANA``).
  - Manifest row ``venue`` is updated; old row is marked ``attempted_failed``.

Category B venues (DRIFT, JITO):
  - 0 captured rows under bare name — just phantom-mark as ``attempted_failed``.
  - ``{PROTOCOL}-SOLANA`` rows already exist; adapter will write to them next run.

Per-VM shard isolation required in apply mode:
  ``VM_NAME=<tag>`` + ``MANIFEST_PER_VM_SHARDS=true``

Usage:
    # Dry run (safe, default — print what would change):
    DEPLOYMENT_ENV=prod \\
    python3 scripts/migrate_solana_bare_name_venues.py

    # Apply (operator authorization required):
    VM_NAME=ikenna-slot8-solana-venue-migration \\
    MANIFEST_PER_VM_SHARDS=true \\
    DEPLOYMENT_ENV=prod \\
    python3 scripts/migrate_solana_bare_name_venues.py --apply --confirm

Plan: solana_venue_naming_reconciliation_2026_05_14.md Phase 1-3.

execution:
  owner: Tab 8 Ikenna / VM launch operator
  cadence: one-shot (migration)
  verifier: MARINADE/RAYDIUM/ORCA captured rows = 0; {PROTOCOL}-SOLANA captured ≥ 30/31/31
  last_executed: NEVER
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_PROJECT_ID = "central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"
_CHAIN = "SOLANA"

# Category A: have captured parquets under bare name → copy + rewrite + manifest update
_CATEGORY_A_BARE_VENUES: frozenset[str] = frozenset({"MARINADE", "RAYDIUM", "ORCA", "KAMINO", "SOLEND", "MARGINFI"})
# Category B: empty rows → just phantom-mark
_CATEGORY_B_BARE_VENUES: frozenset[str] = frozenset({"DRIFT", "JITO"})

_ALL_BARE_VENUES: frozenset[str] = _CATEGORY_A_BARE_VENUES | _CATEGORY_B_BARE_VENUES

_TARGET_PHANTOM_STATUS = "attempted_failed"
_PHANTOM_REASON = "legacy_bare_name_migrated_to_protocol_solana_2026_05_14"

# Hive-key variants to probe when finding old parquets on disk.
# Probing both canonical (asset_group=) and legacy (category=) to avoid missing
# parquets written before the 2026-04 vocab rename.
_HIVE_KEY_CANDIDATES = ("asset_group", "category")


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(run_ts: str) -> str:
    return f"_index/availability_index.{run_ts}.bak.parquet"


def _old_prefix(*, hive_key: str, date: str, venue: str, chain: str, instrument_type: str, data_type: str) -> str:
    return (
        f"raw_tick_data/by_date/day={date}/{hive_key}=defi/"
        f"venue={venue}/chain={chain}/"
        f"instrument_type={instrument_type}/data_type={data_type}/"
    )


def _new_prefix(*, hive_key: str, date: str, new_venue: str, chain: str, instrument_type: str, data_type: str) -> str:
    return (
        f"raw_tick_data/by_date/day={date}/{hive_key}=defi/"
        f"venue={new_venue}/chain={chain}/"
        f"instrument_type={instrument_type}/data_type={data_type}/"
    )


def _validate_apply_prereqs() -> bool:
    missing: list[str] = []
    if os.environ.get("MANIFEST_PER_VM_SHARDS") != "true":
        missing.append("MANIFEST_PER_VM_SHARDS=true (required for multi-worker safety)")
    if not os.environ.get("VM_NAME"):
        missing.append("VM_NAME=<unique-tag> (required for per-VM shard isolation)")
    if missing:
        for m in missing:
            logger.error("Missing prerequisite: %s", m)
        return False
    return True


def _migrate_parquets_for_row(
    *,
    storage_client: object,
    bucket: str,
    date: str,
    old_venue: str,
    new_venue: str,
    chain: str,
    instrument_type: str,
    data_type: str,
    dry_run: bool,
) -> int:
    """Copy parquets from old venue path to new venue path, rewriting venue column.

    Returns number of parquets migrated (0 in dry-run).
    """
    migrated = 0
    for hive_key in _HIVE_KEY_CANDIDATES:
        old_pfx = _old_prefix(
            hive_key=hive_key,
            date=date,
            venue=old_venue,
            chain=chain,
            instrument_type=instrument_type,
            data_type=data_type,
        )
        blobs = list(storage_client.list_blobs(bucket, prefix=old_pfx))
        parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
        if not parquet_blobs:
            continue

        new_pfx = _new_prefix(
            hive_key=hive_key,
            date=date,
            new_venue=new_venue,
            chain=chain,
            instrument_type=instrument_type,
            data_type=data_type,
        )
        for blob in parquet_blobs:
            file_name = blob.name.split("/")[-1]
            new_path = f"{new_pfx}{file_name}"
            if dry_run:
                logger.info(
                    "[dry-run] Would copy %s/%s → %s/%s (rewrite venue=%s)",
                    bucket,
                    blob.name,
                    bucket,
                    new_path,
                    new_venue,
                )
                migrated += 1
                continue

            raw = storage_client.download_bytes(bucket, blob.name)
            df_parquet = pd.read_parquet(io.BytesIO(raw))
            if "venue" in df_parquet.columns:
                df_parquet["venue"] = new_venue
            out_buf = io.BytesIO()
            df_parquet.to_parquet(out_buf, index=False, engine="pyarrow")
            out_buf.seek(0)
            storage_client.upload_bytes(bucket, new_path, out_buf.read())
            logger.info("Migrated parquet: %s → %s (venue rewritten to %s)", blob.name, new_path, new_venue)
            migrated += 1

    return migrated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Execute writes (default: dry-run).")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip interactive confirmation prompt (required for --apply in CI/VM).",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    dry_run = not args.apply

    if not dry_run and not _validate_apply_prereqs():
        logger.error("Prerequisites not met — aborting apply.")
        return 1

    if not dry_run and not args.confirm:
        print(
            "\nAbout to migrate Solana bare-name venue parquets + manifest rows.\n"
            "Category A: MARINADE, RAYDIUM, ORCA, KAMINO, SOLEND, MARGINFI\n"
            "Category B: DRIFT, JITO (phantom-mark only)\n"
            "\nAdd --confirm to skip this prompt.\nProceed? [y/N] ",
            end="",
        )
        if input().strip().lower() != "y":
            logger.info("Aborted by operator.")
            return 0

    bucket = _resolve_bucket()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = _backup_path(run_ts)

    logger.info("Resolved bucket: %s", bucket)
    logger.info("Mode: %s", "DRY-RUN" if dry_run else "APPLY")

    storage_client = get_storage_client()

    logger.info("Reading manifest gs://%s/%s", bucket, _INDEX_PATH)
    raw_bytes = storage_client.download_bytes(bucket, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    required_cols = ("capture_status", "venue", "date", "data_type")
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error("Manifest missing required columns: %s — aborting.", missing_cols)
        return 2

    venue_col = df["venue"].astype(str)
    status_col = df["capture_status"].astype(str)

    # Category A: captured rows under bare name that need parquet migration
    cat_a_mask = venue_col.isin(_CATEGORY_A_BARE_VENUES) & (status_col == "captured")
    cat_a_rows = df[cat_a_mask]
    logger.info("Category A rows (captured bare-name to migrate): %d", len(cat_a_rows))
    for v in sorted(_CATEGORY_A_BARE_VENUES):
        n = int((venue_col == v).sum())
        n_captured = int(((venue_col == v) & (status_col == "captured")).sum())
        logger.info("  %-12s total=%d  captured=%d", v, n, n_captured)

    # Category B: any rows under bare name → just phantom-mark
    cat_b_mask = venue_col.isin(_CATEGORY_B_BARE_VENUES)
    cat_b_rows = df[cat_b_mask]
    logger.info("Category B rows (bare-name to phantom-mark): %d", len(cat_b_rows))

    total_parquet_migrations = 0
    total_manifest_updates = 0
    total_phantom_marks = 0

    # --- Category A: parquet copy + manifest venue update + old row phantom ---
    if len(cat_a_rows) > 0:
        logger.info("Processing Category A rows...")
        now_iso = datetime.now(UTC).isoformat()

        # We'll collect index positions of old rows to mark phantom + new rows to add
        old_row_indices = list(cat_a_rows.index)
        new_rows: list[dict[str, object]] = []

        for _idx, row in cat_a_rows.iterrows():
            old_venue = str(row["venue"])
            new_venue = f"{old_venue}-{_CHAIN}"
            date_val = str(row["date"])
            chain_val = str(row.get("chain", _CHAIN)) or _CHAIN
            instrument_type_val = str(row.get("instrument_type", ""))
            data_type_val = str(row["data_type"])

            # Copy + rewrite parquets
            n_parquets = _migrate_parquets_for_row(
                storage_client=storage_client,
                bucket=bucket,
                date=date_val,
                old_venue=old_venue,
                new_venue=new_venue,
                chain=chain_val,
                instrument_type=instrument_type_val,
                data_type=data_type_val,
                dry_run=dry_run,
            )
            total_parquet_migrations += n_parquets

            if n_parquets == 0 and not dry_run:
                logger.warning(
                    "No parquets found for %s/%s %s %s — skipping manifest update for this row",
                    old_venue,
                    date_val,
                    data_type_val,
                    instrument_type_val,
                )
                continue

            # Build updated manifest row with new venue
            new_row: dict[str, object] = row.to_dict()
            new_row["venue"] = new_venue
            new_row["written_at"] = now_iso
            new_rows.append(new_row)
            total_manifest_updates += 1

        if dry_run:
            logger.info(
                "[dry-run] Would add %d new manifest rows (venue → {PROTOCOL}-SOLANA)",
                len(new_rows),
            )
            logger.info(
                "[dry-run] Would mark %d old Category A rows as %s",
                len(old_row_indices),
                _TARGET_PHANTOM_STATUS,
            )
        else:
            # Append new canonical rows
            if new_rows:
                new_df = pd.DataFrame(new_rows)
                df = pd.concat([df, new_df], ignore_index=True)
                logger.info("Added %d canonical {PROTOCOL}-SOLANA manifest rows", len(new_rows))

            # Mark old rows phantom
            df.loc[old_row_indices, "capture_status"] = _TARGET_PHANTOM_STATUS
            df.loc[old_row_indices, "error_reason"] = _PHANTOM_REASON
            if "attempted_at" in df.columns:
                df.loc[old_row_indices, "attempted_at"] = now_iso
            logger.info("Marked %d old Category A rows as %s", len(old_row_indices), _TARGET_PHANTOM_STATUS)
            total_phantom_marks += len(old_row_indices)

    # --- Category B: phantom-mark only ---
    if len(cat_b_rows) > 0:
        logger.info("Processing Category B rows (phantom-mark only)...")
        cat_b_indices = list(cat_b_rows.index)
        if dry_run:
            logger.info("[dry-run] Would mark %d Category B rows as %s", len(cat_b_indices), _TARGET_PHANTOM_STATUS)
        else:
            now_iso = datetime.now(UTC).isoformat()
            df.loc[cat_b_indices, "capture_status"] = _TARGET_PHANTOM_STATUS
            df.loc[cat_b_indices, "error_reason"] = _PHANTOM_REASON
            if "attempted_at" in df.columns:
                df.loc[cat_b_indices, "attempted_at"] = now_iso
            logger.info("Marked %d Category B rows as %s", len(cat_b_indices), _TARGET_PHANTOM_STATUS)
            total_phantom_marks += len(cat_b_indices)

    # --- Summary ---
    logger.info(
        "Summary: parquets_migrated=%d manifest_rows_updated=%d rows_phantom_marked=%d",
        total_parquet_migrations,
        total_manifest_updates,
        total_phantom_marks,
    )

    if dry_run:
        logger.info("[dry-run] No writes performed. Add --apply --confirm to execute.")
        return 0

    if total_parquet_migrations == 0 and total_phantom_marks == 0:
        logger.info("Nothing to do — manifest already canonical.")
        return 0

    # Write backup
    logger.info("Writing manifest backup to gs://%s/%s", bucket, backup_path)
    storage_client.upload_bytes(bucket, backup_path, raw_bytes)

    # Write updated manifest
    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage_client.upload_bytes(bucket, _INDEX_PATH, out_buf.read())
    logger.info(
        "Manifest updated: %d total rows; backup at gs://%s/%s",
        len(df),
        bucket,
        backup_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
