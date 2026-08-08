#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after --apply confirmed in live prod/catalog.parquet (zero ICE-venue rows)
"""Purge 1,063 ICE-qualifier-variant tradFi catalogue rows.

These rows (venue='ICE', QUARANTINE_UNPARSEABLE:ice_qualifier_variant status from the 2026-07-25
canonicalization pass — raw ICE qualifier symbols that could not be canonicalized before the
2026-07-28 UAC fix) are being deleted because ICE is non-MVP and this data will not be used.

Operator decision 2026-08-07 (consolidated NA-blocker-digest audit): delete, do not canonicalize.

Delete-safety: /codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a reversibility-
qualified carve-out — soft-delete retention ≥ 604800s confirmed fresh at run time.

Context: /plans/active/tradfi_manifest_content_recovery_completion_2026_07_24.md

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_tradfi_ice_catalogue_rows_2026_08_07.py          # dry-run
    .venv/bin/python scripts/purge_tradfi_ice_catalogue_rows_2026_08_07.py --apply  # backup + write
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import Counter
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import (
    gcs_bucket_soft_delete_retention_seconds,
    get_config,
    get_storage_client,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG_FILENAME = "catalog.parquet"
_MIN_SOFT_DELETE_RETENTION_SEC = 604800  # 7 days — /codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a
_ICE_VENUE = "ICE"
_ICE_QUARANTINE_INSTRUMENT_TYPE = "FUTURE"  # specifically the QUARANTINE_UNPARSEABLE:ice_qualifier_variant rows
_EXPECTED_ROWS_REMOVED = 1_063
_EXPECTED_TOLERANCE = max(20, int(_EXPECTED_ROWS_REMOVED * 0.05))  # 5% or 20, whichever is larger


def _deployment_env() -> str:
    return get_config("DEPLOYMENT_ENV", "prod")


def _ice_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask selecting ICE-qualifier-variant (FUTURE) catalogue rows for deletion.

    The 1,063-row quarantine bucket = ICE FUTURE rows that got QUARANTINE_UNPARSEABLE:
    ice_qualifier_variant from the 2026-07-25 canonicalization pass. COMBO and INDEX rows
    are a different quarantine category and are NOT in scope for this pass.
    """
    venue = df["venue"].fillna("").astype(str).str.upper()
    itype = df["instrument_type"].fillna("").astype(str).str.upper()
    return (venue == _ICE_VENUE) & (itype == _ICE_QUARANTINE_INSTRUMENT_TYPE)


def plan_removal(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return (kept_df, counts). Pure — no IO."""
    mask = _ice_mask(df)
    kept = df.loc[~mask].copy()
    counts: dict[str, int] = {
        "total_rows_before": len(df),
        "total_rows_after": len(kept),
        "rows_removed": int(mask.sum()),
    }
    return kept, counts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Backup then write. Default is dry-run.")
    parser.add_argument("--bucket", default=None, help="Override the tradFi instruments-store bucket.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    env = _deployment_env()
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")
    catalog_path = f"{env}/{CATALOG_FILENAME}"
    storage = get_storage_client()

    logger.info("Reading tradFi catalogue gs://%s/%s", bucket, catalog_path)
    raw = storage.download_bytes(bucket, catalog_path)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d catalogue rows", len(df))

    mask = _ice_mask(df)
    ice_rows = df.loc[mask]
    if len(ice_rows):
        logger.info(
            "ICE rows by instrument_type: %s",
            dict(Counter(ice_rows["instrument_type"].fillna("").astype(str).tolist())),
        )
        logger.info("ICE instrument_id sample (first 10): %s", ice_rows["instrument_id"].head(10).tolist())

    kept, counts = plan_removal(df)

    logger.info("ICE-qualifier-variant catalogue row removal plan:")
    for k, v in counts.items():
        logger.info("  %-25s %d", k, v)

    rows_ok = counts["total_rows_after"] == counts["total_rows_before"] - counts["rows_removed"]
    count_in_range = abs(counts["rows_removed"] - _EXPECTED_ROWS_REMOVED) <= _EXPECTED_TOLERANCE
    gate_ok = rows_ok and count_in_range
    logger.info(
        "GATE: row-arithmetic=%s count-in-expected-range=%s (expected≈%d±%d, actual=%d)",
        rows_ok,
        count_in_range,
        _EXPECTED_ROWS_REMOVED,
        _EXPECTED_TOLERANCE,
        counts["rows_removed"],
    )
    if not gate_ok:
        logger.error(
            "GATE FAILED — refusing to apply. row-arithmetic=%s count-ok=%s. "
            "Investigate before proceeding.",
            rows_ok,
            count_in_range,
        )
        return 2

    if counts["rows_removed"] == 0:
        logger.info("Nothing to remove. No-op.")
        return 0

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. %d row(s) would be REMOVED. Pass --apply to backup + write.",
            counts["rows_removed"],
        )
        return 0

    # --- delete-safety gate: fresh soft-delete retention check (§3a) ---
    retention = gcs_bucket_soft_delete_retention_seconds(bucket)
    logger.info("Fresh GCS Soft Delete retention for %s: %ss", bucket, retention)
    if retention < _MIN_SOFT_DELETE_RETENTION_SEC:
        logger.error(
            "REFUSING to write: bucket %s soft-delete retention is %ss, below the required %ss (7 days) "
            "per /codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a. No object touched.",
            bucket,
            retention,
            _MIN_SOFT_DELETE_RETENTION_SEC,
        )
        return 3

    # --- backup ---
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = f"{env}/backups/{CATALOG_FILENAME}.pre_tradfi_ice_purge_{ts}.bak.parquet"
    logger.info("Backing up catalogue to gs://%s/%s", bucket, backup_path)
    storage.upload_bytes(bucket, backup_path, raw)  # pyright: ignore[reportAttributeAccessIssue]

    # --- write filtered catalogue ---
    logger.info("Writing filtered catalogue back to gs://%s/%s (%d rows)", bucket, catalog_path, len(kept))
    obuf = io.BytesIO()
    kept.to_parquet(obuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage.upload_bytes(bucket, catalog_path, obuf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("APPLY complete — %d ICE-qualifier-variant catalogue rows removed.", counts["rows_removed"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
