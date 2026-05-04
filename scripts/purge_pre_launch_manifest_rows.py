#!/usr/bin/env python3
"""Purge pre-launch manifest rows that should never have existed.

A pre-launch row is a manifest entry whose ``date`` is BEFORE the source's
coverage start (per UAC's ``SOURCE_COVERAGE_START`` /
``DATA_TYPE_COVERAGE_START`` SSOT).  These rows are illegitimate — the
source had no data on that date, so any ``capture_status=captured`` claim
is fictitious and the phantom audit will keep flipping them to
``attempted_failed``, which costs API quota every backfill cycle (VMs
re-fetch and re-fail).

Root cause: a writer somewhere recorded a sentinel manifest row without
calling ``clip_dates_to_source_coverage(source, start, end, data_type)``
at the date-iteration boundary.  Until that's patched (separate fix), this
script cleans up the existing manifest in-place.

Sports-only for now (the asset_group where pre-launch is most acute due
to per-data-type coverage windows).  CeFi/DeFi/TradFi/Prediction use
venue-level launch dates that are already enforced by
``orchestrator.is_venue_available()``.

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_pre_launch_manifest_rows.py --asset-group sports --dry-run
    .venv/bin/python scripts/purge_pre_launch_manifest_rows.py --asset-group sports

Idempotent.  Per-VM shards untouched (the consolidator daemon merges them
into canonical on its next cycle; if any pre-launch rows resurface they'll
be purged on the next run).
"""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import sys
import tempfile
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    SOURCE_COVERAGE_START,
    SPORTS_DATA_TYPE_TO_SOURCE,
    is_pre_launch_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

ASSET_GROUP_BUCKETS: dict[str, str] = {
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    # Other asset_groups have venue-level launch enforcement instead of the
    # per-data-type coverage windows; not currently in scope for this purge.
}

INDEX_PATH = "_index/availability_index.parquet"


def _classify_pre_launch(df: pd.DataFrame) -> pd.Series:
    """Return a boolean Series — True for rows whose (source, data_type, date)
    falls before the coverage window."""
    # Vectorise via map for performance — manifest can be 2M+ rows.
    dt_col = df["data_type"].fillna("").astype(str)
    date_col = df["date"].astype(str)
    # Sources that aren't in the SOURCE_COVERAGE_START registry can never be
    # pre-launch (we don't know when they started).  Map data_type → source
    # → ISO start; rows where the lookup fails are kept.
    return pd.Series(
        [is_pre_launch_date(dt, dd) for dt, dd in zip(dt_col, date_col, strict=True)],
        index=df.index,
    )


def _classify_superseded_top_level(df: pd.DataFrame) -> pd.Series:
    """Return a boolean Series — True for top-level no-league
    ``attempted_failed`` rows that have been superseded by per-league
    captures of the same ``(date, data_type)``.

    Pattern: orchestrator records ``record_failed("INJURIES", exc)`` with
    no ``league_id`` when an entity-level fetch errors. The next run
    succeeds per-league and emits ``manifest.add(..., league_id=X)`` for
    each league with data, but the original top-level failure row is
    never retracted. Result: a stale top-level ``attempted_failed`` row
    co-exists with per-league ``captured`` / ``empty_confirmed`` rows
    indefinitely.

    Verified 2026-05-04: 11,142 such superseded rows accumulated
    in the sports manifest.
    """
    # Index of rows that are top-level no-league attempted_failed.
    af_top_level = (
        (df["capture_status"] == "attempted_failed")
        & (df["league_id"].fillna("") == "")
        & (df["data_type"].fillna("") != "")
    )
    # Build set of (date, data_type) keys that have AT LEAST ONE per-league
    # captured / empty_confirmed row — these supersede the top-level failure.
    per_league = df[df["capture_status"].isin(["captured", "empty_confirmed"]) & (df["league_id"].fillna("") != "")]
    keys_per_league = set(zip(per_league["date"].astype(str), per_league["data_type"].astype(str), strict=True))
    af_keys = list(zip(df["date"].astype(str), df["data_type"].astype(str), strict=True))
    return af_top_level & pd.Series([k in keys_per_league for k in af_keys], index=df.index)


def _summarise(df: pd.DataFrame, label: str) -> None:
    logger.info("%s: %d rows", label, len(df))
    if not df.empty:
        by_dt = df.groupby("data_type").size().sort_values(ascending=False)
        logger.info("  by data_type:\n%s", by_dt.head(15).to_string())
        if "date" in df.columns:
            logger.info(
                "  date range: %s -> %s",
                df["date"].astype(str).min(),
                df["date"].astype(str).max(),
            )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--asset-group",
        required=True,
        choices=list(ASSET_GROUP_BUCKETS.keys()),
        help="Asset group whose canonical manifest to purge.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify pre-launch rows but do NOT modify the manifest.",
    )
    args = p.parse_args()

    bucket_name = ASSET_GROUP_BUCKETS[args.asset_group]
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)

    logger.info("=" * 60)
    logger.info("SOURCE_COVERAGE_START (UAC SSOT):")
    for k, v in sorted(SOURCE_COVERAGE_START.items()):
        logger.info("  %s: %s", k, v.isoformat())
    logger.info("Sports data_type → source (UAC SSOT, %d entries)", len(SPORTS_DATA_TYPE_TO_SOURCE))
    logger.info("=" * 60)

    blob = bucket.blob(INDEX_PATH)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(manifest_path)
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket_name, INDEX_PATH)

    pre_launch_mask = _classify_pre_launch(df)
    superseded_mask = _classify_superseded_top_level(df)
    drop_mask = pre_launch_mask | superseded_mask
    pre_launch = df[pre_launch_mask]
    superseded = df[superseded_mask]
    keep = df[~drop_mask]

    logger.info("=" * 60)
    _summarise(pre_launch, "Pre-launch rows (would be DELETED)")
    _summarise(superseded, "Superseded top-level no-league failure rows (would be DELETED)")
    logger.info("Keep rows: %d", len(keep))
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("--dry-run: NOT writing manifest. Re-run without --dry-run to delete.")
        return 0

    if drop_mask.sum() == 0:
        logger.info("No rows to purge. Manifest is clean.")
        return 0

    # Write the filtered manifest back via generation-match CAS so we don't
    # clobber a concurrent consolidator-daemon write.
    out = io.BytesIO()
    keep.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    blob.reload()
    generation = blob.generation
    logger.info(
        "Uploading purged manifest (%d rows, %d pre-launch + %d superseded removed) with if_generation_match=%s",
        len(keep),
        len(pre_launch),
        len(superseded),
        generation,
    )
    bucket.blob(INDEX_PATH).upload_from_string(
        out.getvalue(),
        content_type="application/octet-stream",
        if_generation_match=generation,
    )
    logger.info("Done at %s", datetime.now(UTC).isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
