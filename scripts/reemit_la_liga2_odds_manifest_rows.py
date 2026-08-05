#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + manifest-vs-GCS cross-reference shows 0 LA_LIGA_2 GCS-only ODDS cells
# Plan: /plans/active/issues/sports_rebuild_delta_la_liga2_data_loss_2026_08_05.md
"""Re-emit LA_LIGA_2 ODDS manifest rows for the 846 (date, league) cells with real GCS objects.

Finds all GCS objects at paths like::

    sports_reference/by_date/day=D/pipeline_mode=batch_footystats/
      entity=footystats_odds/fetched_at_hour=H/league=LA_LIGA_2/footystats_odds.parquet

and emits manifest rows with the canonical ``league_id=SEGUNDA_DIVISION``
(LA_LIGA_2 → SEGUNDA_DIVISION per UAC ``_LEAGUE_ALIASES`` in
``unified_api_contracts.canonical.domain.sports.provider_league_ids``).

The 846 cells were identified by the post-07-13 manifest rebuild delta
reconciliation (Track S2) as GCS-present but manifest-absent — genuine data
loss because LA_LIGA_2 was absent from the instruments-service league catalogue
used by the rebuild.

Idempotent: ``ManifestWriter`` deduplicates on the shard tuple. Re-running is
safe (duplicate writes are silently collapsed).

Usage::

    cd instruments-service
    .venv/bin/python scripts/reemit_la_liga2_odds_manifest_rows.py --dry-run
    .venv/bin/python scripts/reemit_la_liga2_odds_manifest_rows.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_type

from google.api_core.exceptions import NotFound
from google.cloud import storage
from unified_api_contracts.canonical.domain.sports.provider_league_ids import (
    canonicalize_league_id,
)
from unified_trading_library import ManifestWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-prd-central-element-323112"
ROOT = "sports_reference/by_date/"

# The canonical form of LA_LIGA_2 per UAC alias resolution.
CANONICAL_LEAGUE_ID = canonicalize_league_id("LA_LIGA_2")  # → "SEGUNDA_DIVISION"

DATA_TYPE = "ODDS"


def _list_dates(client: storage.Client) -> list[str]:
    """Return sorted ISO-date strings from the GCS bucket via delimiter listing.

    Excludes non-ISO-date partitions (e.g. ``day=all`` sentinel). Only proper
    YYYY-MM-DD partitions are returned.
    """
    bucket = client.bucket(BUCKET)
    iterator = bucket.list_blobs(prefix=ROOT, delimiter="/")
    # Exhaust the iterator so .prefixes is populated.
    list(iterator)
    days: list[str] = []
    for p in iterator.prefixes:
        seg = p.removeprefix(ROOT).removesuffix("/")
        if not seg.startswith("day="):
            continue
        candidate = seg.removeprefix("day=")
        try:
            date_type.fromisoformat(candidate)
        except ValueError:
            continue
        days.append(candidate)
    return sorted(days)


def _probe_la_liga2_cell(client: storage.Client, day: str) -> tuple[str, int] | None:
    """Check whether *day* has a LA_LIGA_2 ODDS parquet and return its row count.

    Returns ``(day, row_count)`` if a ``league=LA_LIGA_2/footystats_odds.parquet``
    blob exists under any ``fetched_at_hour`` for *day*, or ``None`` if no such
    parquet exists.
    """
    bucket = client.bucket(BUCKET)
    odds_prefix = f"{ROOT}day={day}/pipeline_mode=batch_footystats/entity=footystats_odds/"

    # List fetched_at_hour sub-directories.  GCS list_blobs with delimiter="/"
    # returns the blobs at the exact prefix level AND populates .prefixes with
    # the sub-directory keys.
    hour_iter = bucket.list_blobs(prefix=odds_prefix, delimiter="/")
    # Consume the iterator so .prefixes is fully populated.
    for _ in hour_iter:
        pass

    if not hour_iter.prefixes:
        return None

    for hour_pfx in hour_iter.prefixes:
        # Look for a parquet blob directly under league=LA_LIGA_2/.
        # We don't use delimiter here — just list with a long-enough prefix
        # that only the target blob matches.
        target_pfx = f"{hour_pfx}league=LA_LIGA_2/footystats_odds.parquet"
        blobs = list(bucket.list_blobs(prefix=target_pfx, max_results=1))
        for blob in blobs:
            if blob.name == target_pfx or blob.name.endswith("/league=LA_LIGA_2/footystats_odds.parquet"):
                row_count = _read_row_count(blob)
                return (day, row_count)

    return None


def _read_row_count(blob: storage.Blob) -> int:
    """Read row count from blob metadata, or infer from size.

    Falls back to -1 (unknown but non-empty) if metadata can't be read.
    """
    try:
        blob.reload()
        if blob.metadata and "row_count" in blob.metadata:
            return int(blob.metadata["row_count"])
        if blob.size is not None:
            if blob.size == 0:
                return 0
            if blob.size < 300:
                return 0
        return -1  # Unknown but non-empty
    except (NotFound, OSError):
        return -1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Probe only, do not write manifest rows")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers (default 16)")
    parser.add_argument("--day", type=str, default="", help="Probe a single date YYYY-MM-DD (smoke test)")
    args = parser.parse_args(argv)

    # ManifestWriter's storage client needs GCP_PROJECT_ID to resolve the
    # project.  Set it from the gcloud default if not already in the env.
    if not os.environ.get("GCP_PROJECT_ID") and os.environ.get("PROJECT_ID"):
        os.environ["GCP_PROJECT_ID"] = os.environ["PROJECT_ID"]

    logger.info("Canonical league: %s → %s", "LA_LIGA_2", CANONICAL_LEAGUE_ID)

    client = storage.Client(project="central-element-323112")

    if args.day:
        days = [args.day]
    else:
        logger.info("Listing dates in %s ...", BUCKET)
        days = _list_dates(client)
        logger.info("Found %d dates", len(days))

    # Probe for LA_LIGA_2 cells in parallel.
    found: list[tuple[str, int]] = []
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_probe_la_liga2_cell, client, d): d for d in days}
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            try:
                result = fut.result()
            except (RuntimeError, ValueError, OSError) as exc:
                logger.warning("day=%s probe failed: %s", day, exc)
                continue
            if result is not None:
                found.append(result)
            if i % 500 == 0:
                logger.info(
                    "Probe progress: %d/%d dates, %d LA_LIGA_2 cells found (%.1fs)",
                    i,
                    len(days),
                    len(found),
                    time.monotonic() - t0,
                )

    elapsed = time.monotonic() - t0
    logger.info("Probe complete: %d LA_LIGA_2 cells across %d dates (%.1fs)", len(found), len(days), elapsed)

    if not found:
        logger.warning("No LA_LIGA_2 ODDS cells found. Nothing to re-emit.")
        return 0

    # Summary: date range.
    found_dates = sorted(f[0] for f in found)
    logger.info("Date range: %s to %s", found_dates[0], found_dates[-1])

    if args.dry_run:
        logger.info("DRY RUN — would emit %d manifest rows. Sample:", len(found))
        for day, row_count in found[:10]:
            logger.info("  date=%s data_type=%s league=%s row_count=%d", day, DATA_TYPE, CANONICAL_LEAGUE_ID, row_count)
        if len(found) > 10:
            logger.info("  ... and %d more", len(found) - 10)
        return 0

    # Emit manifest rows via ManifestWriter.
    logger.info("Writing %d manifest rows (canonical league=%s) ...", len(found), CANONICAL_LEAGUE_ID)
    manifest = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=BUCKET,
        per_vm_shards=True,
    )

    for day, row_count in found:
        proc_date = date_type.fromisoformat(day)
        manifest.add(
            processing_date=proc_date,
            row_count=max(row_count, 1),  # at least 1 for a non-empty parquet
            data_type=DATA_TYPE,
            league_id=CANONICAL_LEAGUE_ID,
            venue="",
        )

    manifest.flush()
    logger.info("Manifest flushed: %d rows written for league=%s", len(found), CANONICAL_LEAGUE_ID)
    logger.info(
        "Consolidator will merge shards into _index/availability_index.parquet. "
        "Re-run the manifest-vs-GCS cross-reference to verify 0 GCS-only LA_LIGA_2 cells remain."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
