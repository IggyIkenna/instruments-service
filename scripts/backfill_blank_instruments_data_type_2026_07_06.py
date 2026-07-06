#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after blank-data_type backfill verified complete across all non-sports asset groups
#   (0 blank captured venue-grain rows on 2026-06-27..2026-07-06 in cefi + defi + tradfi buckets)

"""One-off patch — promote ``data_type=''`` → ``'instruments'`` on IS non-sports captures 2026-06-27+.

ROOT CAUSE (issue ``is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06``):
  Every IS non-sports captured row (cefi / defi / tradfi) emitted 2026-06-29..2026-07-06
  landed with ``data_type=""`` (blank) in the availability index. The regression was
  first identified on cefi (~260 blank rows across 26 venues); the writer emits all
  three asset groups through the same non-sports code path in
  ``instruments_service/engine/orchestrator/writers.py`` (line 245 post-fix), so
  defi + tradfi were vulnerable to the same regression window. Item (a) of the
  recommended fix — stamp ``data_type="instruments"`` at ``record_captured`` emission
  time (``writers.py:245``) — shipped as ``instruments-service@46ba62b`` and covers
  the non-sports emit path for ALL three asset groups. This script is item (b): the
  one-off backfill that promotes historical blank rows to the canonical value so any
  downstream consumer using the canonical filter
  (``capture_status=='captured' AND data_type=='instruments'``) stops silently missing
  the pre-fix captures.

Filter (per the plan-of-record):
    ``date >= 2026-06-27``
    ``AND capture_status == 'captured'``
    ``AND (data_type is null OR data_type == '')``
    ``AND venue != ''``  (per-venue captures — NOT AG-grain rows)

Fix:
    ``data_type = 'instruments'``  (matches ``REFERENCE_DATA_TYPE`` in
    ``migrate_instruments_store_v9.py:126`` — the migration-time SSOT).

Idempotent: rows already carrying ``data_type='instruments'`` are left unchanged;
a re-run finds 0 rows to fix and exits clean.

Dry-run by default. ``--apply --confirm`` mutates the live ``_index`` (the same
guard the 2026-06-21 defi catalog data_type backfill script uses).

Post-run verification: reads the fresh ``_index`` back and reports the count of
blank captured rows on ``date >= 2026-06-27``; the exit is loud-fail if the
count is > 0 after ``--apply``.

Safety gate: the ``captured`` row count MUST be unchanged after the fix (we only
rewrite an unset column; we never delete or reclassify rows).

Usage::

    # inspect what will change on cefi (dry-run)
    cd instruments-service
    .venv/bin/python scripts/backfill_blank_instruments_data_type_2026_07_06.py --asset-group cefi

    # dry-run defi + tradfi (task is_cefi_manifest_blank_data_type_since_2026_06_29-006)
    .venv/bin/python scripts/backfill_blank_instruments_data_type_2026_07_06.py --asset-group defi
    .venv/bin/python scripts/backfill_blank_instruments_data_type_2026_07_06.py --asset-group tradfi

    # mutate the live _index for the given asset group
    .venv/bin/python scripts/backfill_blank_instruments_data_type_2026_07_06.py \\
        --asset-group defi --apply --confirm

SSOT: ``plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md``
todo ``[DATA] P2. Verify defi + tradfi are not affected. If the same regression
exists there, extend the (a) fix ... run the patch on their buckets too ...``.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from typing import Literal, cast

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_REFERENCE_DATA_TYPE = "instruments"  # mirrors migrate_instruments_store_v9.py:126
_CUTOFF_DATE = "2026-06-27"
_SUPPORTED_ASSET_GROUPS = ("cefi", "defi", "tradfi")


def _resolve_is_bucket(asset_group: str) -> str:
    # asset_group is validated against _SUPPORTED_ASSET_GROUPS by argparse choices;
    # narrow str → the bucket-naming Literal.
    ag = cast(Literal["cefi", "defi", "tradfi"], asset_group)
    return resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group=ag, deployment_env="prod"
    )


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, _INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, _INDEX_BLOB)
    return df


def _identify_blank_rows(df: pd.DataFrame, asset_group: str) -> pd.Index:
    """Return index of captured rows on ``date >= 2026-06-27`` with blank ``data_type``."""
    missing = [c for c in ("date", "capture_status", "data_type", "venue") if c not in df.columns]
    if missing:
        logger.error("Manifest missing required columns: %s", missing)
        return df.index[:0]

    dates = df["date"].astype(str)
    status = df["capture_status"].fillna("").astype(str)
    dtype = df["data_type"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)

    on_or_after = dates >= _CUTOFF_DATE
    is_captured = status == "captured"
    is_blank_dt = dtype == ""
    has_venue = venue != ""

    idx = df[on_or_after & is_captured & is_blank_dt & has_venue].index
    total_captured_recent = int((on_or_after & is_captured & has_venue).sum())
    logger.info(
        "Blank-data_type %s captured rows on %s+: %d of %d total venue-grain captured (%.1f%%)",
        asset_group,
        _CUTOFF_DATE,
        len(idx),
        total_captured_recent,
        100.0 * len(idx) / total_captured_recent if total_captured_recent else 0.0,
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    sub = df.loc[idx]
    by_venue = sub["venue"].value_counts()
    logger.info("Blank-data_type rows by venue (%d venues):\n%s", len(by_venue), by_venue.to_string())
    dates = sorted(sub["date"].astype(str).unique())
    if dates:
        logger.info("Blank-data_type row date coverage: %d dates (%s .. %s)", len(dates), dates[0], dates[-1])


def _apply_fix(df: pd.DataFrame, idx: pd.Index) -> tuple[pd.DataFrame, int, int]:
    """Rewrite ``data_type='instruments'`` on ``idx`` rows; return (fixed_df, captured_before, captured_after)."""
    fixed = df.copy()
    captured_before = int((fixed["capture_status"].fillna("").astype(str) == "captured").sum())

    fixed.loc[idx, "data_type"] = _REFERENCE_DATA_TYPE

    captured_after = int((fixed["capture_status"].fillna("").astype(str) == "captured").sum())
    return fixed, captured_before, captured_after


def _upload_manifest(bucket: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    client = get_storage_client(provider="gcp")
    client.upload_bytes(bucket, _INDEX_BLOB, buf.getvalue())
    logger.info(
        "Uploaded manifest: %d rows, %.1f KB → gs://%s/%s",
        len(df),
        len(buf.getvalue()) / 1024,
        bucket,
        _INDEX_BLOB,
    )


def _verify_post_run(bucket: str, asset_group: str) -> int:
    """Re-read the _index and count blank captured rows on ``date >= 2026-06-27``."""
    df = _load_manifest(bucket)
    residual = _identify_blank_rows(df, asset_group)
    return len(residual)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--asset-group",
        required=True,
        choices=_SUPPORTED_ASSET_GROUPS,
        help="Which non-sports IS PRD availability-index to promote (cefi | defi | tradfi).",
    )
    ap.add_argument("--apply", action="store_true", help="Mutate the live _index (requires --confirm)")
    ap.add_argument("--confirm", action="store_true", help="Confirm a live --apply write")
    args = ap.parse_args()

    if args.apply and not args.confirm:
        ap.error("--apply requires --confirm")

    apply = args.apply and args.confirm
    asset_group = args.asset_group

    bucket = _resolve_is_bucket(asset_group)
    logger.info("%s IS PRD availability-index bucket: gs://%s", asset_group, bucket)

    df = _load_manifest(bucket)
    blank_idx = _identify_blank_rows(df, asset_group)

    if len(blank_idx) == 0:
        logger.info(
            "Already clean — 0 blank %s captured rows on %s+; nothing to do.",
            asset_group,
            _CUTOFF_DATE,
        )
        return 0

    _report_distribution(df, blank_idx)

    fixed, captured_before, captured_after = _apply_fix(df, blank_idx)
    if captured_after != captured_before:
        logger.error(
            "SAFETY GATE FAILED: captured row count changed %d → %d after fix — aborting write",
            captured_before,
            captured_after,
        )
        return 4

    logger.info(
        "Safety gate OK: captured row count preserved at %d before and after the fix.", captured_before
    )

    if not apply:
        logger.info(
            "DRY-RUN: %d blank %s captured rows would be promoted to data_type='%s'. "
            "Re-run with --apply --confirm to mutate.",
            len(blank_idx),
            asset_group,
            _REFERENCE_DATA_TYPE,
        )
        return 0

    _upload_manifest(bucket, fixed)

    residual = _verify_post_run(bucket, asset_group)
    if residual > 0:
        logger.error(
            "Post-run verification FAILED: %d blank %s captured rows remain on %s+ (expected 0)",
            residual,
            asset_group,
            _CUTOFF_DATE,
        )
        return 5

    logger.info(
        "✅ Done — promoted %d rows to data_type='%s'. Post-run verify: 0 blank %s captured rows on %s+.",
        len(blank_idx),
        _REFERENCE_DATA_TYPE,
        asset_group,
        _CUTOFF_DATE,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
