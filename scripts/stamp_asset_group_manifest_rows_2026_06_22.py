#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: every market-data-tick-{cefi,defi,tradfi,sports,pred} _index has 0 blank/null
#   asset_group on CAPTURED market-data rows (verified live) AND the UTL manifest writer stamps
#   asset_group on every new capture (shipped in unified-trading-library — _resolve_asset_group),
#   so no new write regresses; tracked under data_completion_to_100_all_ag_2026_06_21.md.
"""Stamp the v9 ``asset_group`` ROW COLUMN on existing market-data-tick ``_index`` rows.

ROOT CAUSE (data_completion_to_100_all_ag_2026_06_21.md): ``asset_group`` was historically
OMITTED from the UTL ``AvailabilityRecord`` + serializer on the never-implemented assumption
the consolidator derives it from the GCS hive-partition key — it does NOT (the consolidator
unions per-VM shard columns by NAME, and per-VM shards are flat blobs). So every NEW capture
wrote a BLANK ``asset_group`` fleet-wide and the per-asset_group coverage rollup was corrupt.
The UTL writer fix (``ManifestWriterIngestMixin._resolve_asset_group``) closes it for NEW
writes; this one-off back-stamps the EXISTING blank rows.

THE BUCKET IS THE ASSET GROUP — every row in ``market-data-tick-{ag}-prd-…`` belongs to that
AG, so the stamp is unambiguous: blank/null/``"None"``/``"nan"`` ``asset_group`` → the bucket's
AG. A row that already carries a (different, non-blank) asset_group is LEFT UNTOUCHED and counted
(so a cross-AG mis-stamp would surface, never be silently overwritten).

GUARDS (refuse to write otherwise):
  * rowcount preserved EXACTLY (before == after — this tool only fills a column, never drops/adds).
  * captured-count preserved EXACTLY.
  * no row carries a non-blank asset_group that disagrees with the bucket AG (would be a real
    cross-AG contamination → abort + report, do NOT mask it).

DRY-RUN by default. ``--apply`` snapshots the live ``_index`` to
``_index/snapshots/pre_asset_group_stamp_{ag}_2026_06_22.parquet`` FIRST, then writes it back.
GCS I/O via UTL ``get_storage_client()`` (cloud-agnostic) — never gsutil.

SSOT: unified-trading-pm/plans/active/data_completion_to_100_all_ag_2026_06_21.md
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stamp_asset_group")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_BLANK_TOKENS = frozenset({"", "None", "nan", "NaN", "<NA>"})
# Captured market-data rows MUST resolve an asset_group; non-captured + feature/service rows
# are exempt per the UTL writer contract — but in a market-data-tick bucket the bucket AG is
# unambiguous, so we stamp ALL blank rows (captured + honest-absence) for denominator
# attribution. Feature/service rows do not live in these tick buckets.
_CAPTURED = "captured"


def _bucket_for_ag(ag: str) -> str:
    if ag == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag)


def _is_blank(series: pd.Series) -> pd.Series:
    """Boolean mask of blank/null/sentinel asset_group cells."""
    return series.isna() | series.astype(str).str.strip().isin(_BLANK_TOKENS)


def stamp(df: pd.DataFrame, ag: str) -> tuple[pd.DataFrame, dict[str, object]]:
    out = df.copy()
    n_before = len(out)
    cap_before = int((out["capture_status"].astype(str) == _CAPTURED).sum()) if "capture_status" in out else 0

    if "asset_group" not in out.columns:
        out["asset_group"] = ""

    ag_series = out["asset_group"]
    blank_mask = _is_blank(ag_series)
    n_blank = int(blank_mask.sum())

    # Non-blank rows whose AG disagrees with the bucket AG = real cross-AG contamination.
    nonblank = out[~blank_mask]
    mismatched = nonblank[nonblank["asset_group"].astype(str).str.strip() != ag]
    n_mismatch = len(mismatched)

    cap_blank = (
        int((out.loc[blank_mask, "capture_status"].astype(str) == _CAPTURED).sum()) if "capture_status" in out else 0
    )

    out.loc[blank_mask, "asset_group"] = ag

    cap_after = int((out["capture_status"].astype(str) == _CAPTURED).sum()) if "capture_status" in out else 0
    blank_after = int(_is_blank(out["asset_group"]).sum())

    stats: dict[str, object] = {
        "rows_before": n_before,
        "rows_after": len(out),
        "blank_asset_group_before": n_blank,
        "blank_asset_group_after": blank_after,
        "blank_captured_rows_stamped": cap_blank,
        "captured_before": cap_before,
        "captured_after": cap_after,
        "nonblank_mismatch": n_mismatch,
        "mismatch_values": (
            mismatched["asset_group"].astype(str).value_counts().head(8).to_dict() if n_mismatch else {}
        ),
        "asset_group_dist_after": out["asset_group"].astype(str).value_counts(dropna=False).head(8).to_dict(),
    }
    return out, stats


def run_ag(ag: str, *, apply: bool) -> int:
    bucket = _bucket_for_ag(ag)
    storage = get_storage_client()
    logger.info("── %s  gs://%s/%s ──", ag, bucket, _MANIFEST_BLOB)
    try:
        raw = storage.download_bytes(bucket, _MANIFEST_BLOB)
    except Exception as exc:  # shard-isolation: one bucket fail must not abort the sweep
        logger.warning("  skip — cannot download index (%s)", exc)
        return 0

    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = stamp(df, ag)
    for k, v in stats.items():
        logger.info("    %s = %s", k, v)

    # GUARD 1+2: rowcount + captured preserved EXACTLY (column-fill only).
    if stats["rows_after"] != stats["rows_before"]:
        logger.error("  ABORT %s: rowcount changed %s → %s", ag, stats["rows_before"], stats["rows_after"])
        return 1
    if stats["captured_after"] != stats["captured_before"]:
        logger.error("  ABORT %s: captured changed %s → %s", ag, stats["captured_before"], stats["captured_after"])
        return 1
    # GUARD 3: a non-blank row disagreeing with the bucket AG is real contamination — report, abort.
    if stats["nonblank_mismatch"]:
        logger.error(
            "  ABORT %s: %d rows carry a non-blank asset_group != %r (%s) — cross-AG contamination, "
            "NOT masking it; investigate before stamping",
            ag,
            stats["nonblank_mismatch"],
            ag,
            stats["mismatch_values"],
        )
        return 1

    if stats["blank_asset_group_before"] == 0:
        logger.info("  ✓ %s already fully stamped — nothing to write", ag)
        return 0

    if not apply:
        logger.info("  DRY RUN — live _index untouched (pass --apply to write, snapshots first)")
        return 0

    snap = f"_index/snapshots/pre_asset_group_stamp_{ag}_2026_06_22.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    storage.upload_bytes(bucket, snap, sbuf.getvalue())
    logger.info("  ✎ snapshot → gs://%s/%s", bucket, snap)

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, _MANIFEST_BLOB, buf.getvalue())
    logger.info(
        "  ✅ APPLIED %s — stamped %d blank rows → asset_group=%r (%d captured)",
        ag,
        stats["blank_asset_group_before"],
        ag,
        stats["blank_captured_rows_stamped"],
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--asset-group",
        action="append",
        choices=["cefi", "defi", "tradfi", "sports", "prediction"],
        help="AG(s) to process (repeatable). Default: all five.",
    )
    p.add_argument("--apply", action="store_true", help="Write back (snapshots first). Default: dry-run.")
    args = p.parse_args(argv)
    ags = args.asset_group or ["cefi", "defi", "tradfi", "sports", "prediction"]
    rc = 0
    for ag in ags:
        rc |= run_ag(ag, apply=args.apply)
    logger.info("done at %s", datetime.now(UTC).isoformat())
    return rc


if __name__ == "__main__":
    sys.exit(main())
