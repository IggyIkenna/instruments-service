#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   canonical_id_builder_retrofit_checklist_2026_07_08.md Progress Log)
"""Fix 1,330 stale-colon MORPHO catalog rows left by the 2026-07-13 migration.

The 2026-07-13 canonicalization script
(``canonicalize_defi_lending_atoken_debttoken_catalog_2026_07_13.py``) used
``instrument_id.split(":", 2)`` to extract the pair_key from pre-``af05ece3``
LENDING_MARKET-shaped rows, forwarding any colon-containing suffix verbatim
into the new A_TOKEN/DEBT_TOKEN rows.  That produced rows whose instrument_id
still carries an embedded colon before the market-key hex suffix, e.g.
``MORPHO-BASE:A_TOKEN:ATOKEN1-TOKEN2:MARKET01`` (4 segments, 3 colons) instead
of the canonical 3-segment, 2-colon shape
``MORPHO-BASE:A_TOKEN:ATOKEN1-TOKEN2-MARKET01``.

A fresh query of the real ``prod/catalog.parquet`` (2026-08-05) finds:

* 1,330 stale MORPHO rows (3 colons): 466 LENDING, 432 A_TOKEN, 432 DEBT_TOKEN.
* 1,293 of those already have a clean counterpart (same fixed ID with the last
  ``:`` turned into ``-``) — these are genuine duplicates and are DELETED.
* 37 unique stale rows (all LENDING type) have no clean counterpart — their
  instrument_id is FIXED in place (and canonical_instrument_id synced).
* FLUID is clean (all 18 rows already 2-colon); no action taken.

Usage:
  python scripts/fix_morpho_stale_colon_catalog_rows_2026_08_05.py --dry-run
  python scripts/fix_morpho_stale_colon_catalog_rows_2026_08_05.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"
_MORPHO_PREFIX = "MORPHO-"


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(run_ts: str) -> str:
    return f"prod/catalog.{run_ts}.morpho-colon-fix.bak.parquet"


def _fix_instrument_id(instrument_id: str) -> str:
    """Replace the last ':' with '-' — the canonical 3-segment shape."""
    parts = instrument_id.rsplit(":", 1)
    return f"{parts[0]}-{parts[1]}"


def _is_stale_morpho(instrument_id: str) -> bool:
    """A MORPHO row is stale when it has exactly 3 colons (4 segments)."""
    return instrument_id.startswith(_MORPHO_PREFIX) and instrument_id.count(":") == 3


def migrate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return the migrated dataframe + a summary of rows touched.

    For each stale MORPHO row:
    - Compute the corrected instrument_id.
    - If a row with that corrected ID already exists → DELETE the stale row (duplicate).
    - Otherwise → UPDATE the stale row's instrument_id + canonical_instrument_id.
    """
    stale_mask = df["instrument_id"].apply(_is_stale_morpho)
    stale = df[stale_mask]
    keep = df[~stale_mask]

    logger.info("Found %d stale MORPHO rows (3-colon instrument_id)", len(stale))

    # Build a set of existing clean instrument_ids for dedup
    existing_ids = set(keep["instrument_id"])

    delete_mask: list[bool] = []
    update_rows: list[pd.Series] = []
    n_deleted = 0
    n_updated = 0

    for _idx, row in stale.iterrows():
        fixed_id = _fix_instrument_id(row["instrument_id"])
        if fixed_id in existing_ids:
            # Duplicate — the clean version already exists
            delete_mask.append(True)
            n_deleted += 1
        else:
            # Unique — fix in place
            delete_mask.append(False)
            new_row = row.copy()
            new_row["instrument_id"] = fixed_id
            # Sync canonical_instrument_id if it matches the old instrument_id
            if row.get("canonical_instrument_id") == row["instrument_id"]:
                new_row["canonical_instrument_id"] = fixed_id
            update_rows.append(new_row)
            existing_ids.add(fixed_id)  # prevent intra-stale collisions
            n_updated += 1

    # Keep all non-stale rows + the updated stale rows
    migrated = pd.concat([keep, pd.DataFrame(update_rows)], ignore_index=True) if update_rows else keep.copy()

    # Verify invariant: no instrument_id has >2 colons
    morpho_after = migrated[migrated["instrument_id"].str.startswith(_MORPHO_PREFIX, na=False)]
    bad = morpho_after[morpho_after["instrument_id"].str.count(":") > 2]
    if len(bad) > 0:
        logger.error(
            "INVARIANT BROKEN: %d MORPHO rows still have >2 colons after migration",
            len(bad),
        )
        for _, r in bad.head(10).iterrows():
            logger.error("  BAD: %s", r["instrument_id"])
        raise SystemExit(1)

    summary = {
        "morpho_stale_found": int(stale_mask.sum()),
        "morpho_duplicates_deleted": n_deleted,
        "morpho_colon_fixed_in_place": n_updated,
        "rows_before": len(df),
        "rows_after": len(migrated),
        "rows_removed": len(df) - len(migrated),
    }
    return migrated, summary


def _check_fluid(df: pd.DataFrame) -> dict[str, int]:
    """Scope check: verify FLUID rows are already clean (all 2-colon)."""
    fluid = df[df["instrument_id"].str.startswith("FLUID-", na=False)]
    bad = fluid[fluid["instrument_id"].str.count(":") != 2]
    return {
        "fluid_total": len(fluid),
        "fluid_stale": len(bad),
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    bucket = _catalogue_bucket()
    client = get_storage_client(project_id=None)
    logger.info("Reading gs://%s/%s", bucket, _CATALOG_BLOB)
    raw = client.download_bytes(bucket, _CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d catalogue rows", len(df))

    # FLUID scope check (informational only — no action taken)
    fluid_check = _check_fluid(df)
    logger.info("FLUID scope check: %d total, %d stale", fluid_check["fluid_total"], fluid_check["fluid_stale"])
    if fluid_check["fluid_stale"] > 0:
        logger.warning("UNEXPECTED: %d FLUID rows are stale — manual investigation needed", fluid_check["fluid_stale"])

    migrated, summary = migrate(df)
    for k, v in summary.items():
        logger.info("  %s = %d", k, v)

    if args.dry_run:
        logger.info(
            "[dry-run] would write %d rows (was %d) — no cloud writes performed",
            len(migrated),
            len(df),
        )
        logger.info(
            "  → %d duplicates deleted, %d colons fixed in place",
            summary["morpho_duplicates_deleted"],
            summary["morpho_colon_fixed_in_place"],
        )
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_blob = _backup_path(run_ts)
    logger.info("Backing up current catalogue to gs://%s/%s", bucket, backup_blob)
    client.upload_bytes(bucket, backup_blob, raw)  # pyright: ignore[reportAttributeAccessIssue]

    buf = io.BytesIO()
    migrated.to_parquet(buf, index=False)
    buf.seek(0)
    logger.info(
        "Writing migrated catalogue (%d rows, was %d) to gs://%s/%s",
        len(migrated),
        len(df),
        bucket,
        _CATALOG_BLOB,
    )
    client.upload_bytes(bucket, _CATALOG_BLOB, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
