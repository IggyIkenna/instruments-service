#!/usr/bin/env python3
"""Rescan GCS sports FIXTURES parquets and emit per-(date, canonical_league_id) manifest rows.

SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md`` (Phase 5 fix).

**Why a new script?** ``rescan_sports_manifest.py`` (the existing rescan) dedups
by entity and writes ``league_id=""`` — it can't produce per-league drilldown
for the FIXTURES entity. ``migrate_sports_per_league.py`` physically split
FIXTURE_EVENTS / FIXTURE_LINEUPS / FIXTURE_STATS / PLAYER_STATS / INJURIES
into per-league parquets, but FIXTURES itself remains a single per-day file
containing all leagues (by API-Football numeric id). Consequence: the
manifest's per-league FIXTURES coverage was ~0.2% honest (EPL=5 rows across
all seasons vs ~2300 expected; USL_CHAMPIONSHIP=639 is correct because USL
IDs hit the canonical mapping reliably).

This script:

1. Walks every ``sports_reference/by_date/day=.../entity=fixtures/fixtures.parquet``
   on GCS.
2. For each blob, reads the parquet, joins ``af_league_id`` →
   canonical ``league_id`` via ``get_league_by_api_football_id()`` (UAC
   ``unified_api_contracts.sports.league_data``), groups by canonical
   league, and counts fixtures.
3. Emits one v5 manifest row per (date, league_id) with:
     - ``data_type="FIXTURES"``, ``venue=""`` (not a venue — per SSOT)
     - ``league_id=<canonical>``
     - ``instrument_count=<# of fixtures that league had that day>``
     - ``capture_status="captured"`` (rows exist on GCS)
     - ``schema_version=5``
4. Preserves non-FIXTURES rows (other sports entities + other services).

Designed for VM execution — do NOT run on a laptop over a multi-year window.

VM launch recipe
----------------

1. Refresh the SPORTS tarball::

    bash deployment-service/scripts/vm/create-code-tarballs.sh --category SPORTS

2. Launch a small-test VM first (single date)::

    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh \\
      --date 2024-09-01 --dry-run

   Verify the dry-run output lists the expected per-league counts, then re-run
   without ``--dry-run``. Confirm the manifest row count changes as expected
   via ``python instruments-service/scripts/verify_instrument_manifest_coverage.py``.

3. Full backfill (all dates)::

    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh --workers 16

   Singleton-locked: will refuse to launch if a same-prefix VM is RUNNING
   (per 2026-04-19 SFI thundering-herd incident).

4. Watch events::

    gcloud logging read \\
      "resource.type=gce_instance AND labels.vm_category=SPORTS" \\
      --limit 50 --format json

5. Success criterion: deployment-api ``/api/data-status/manifest?service=instruments-service``
   reports SPORTS FIXTURES completion_pct ≥ 90% on at least the PREDICTION 33 leagues.

Usage
-----

::

    # Dry run — show what would be written without touching the index
    python scripts/rescan_sports_fixtures_canonical.py --dry-run --date 2024-09-01

    # Single date, write index
    python scripts/rescan_sports_fixtures_canonical.py --date 2024-09-01

    # Full rescan (VM only — see VM launch recipe above)
    python scripts/rescan_sports_fixtures_canonical.py --workers 16
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import get_league_by_api_football_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_NOW = datetime.now(UTC).isoformat()

BUCKET_NAME = "instruments-store-sports-central-element-323112"
FIXTURES_PREFIX = "sports_reference/by_date/"
INDEX_BLOB = "_index/availability_index.parquet"


def _list_fixtures_blobs(bucket: storage.Bucket, date_str: str | None) -> list[storage.Blob]:
    """List every day=<D>/entity=fixtures/fixtures.parquet blob (optionally scoped to one date)."""
    if date_str:
        prefix = f"{FIXTURES_PREFIX}day={date_str}/entity=fixtures/"
    else:
        prefix = FIXTURES_PREFIX
    matches: list[storage.Blob] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith("/entity=fixtures/fixtures.parquet"):
            matches.append(blob)
    return matches


def _parse_date(blob_name: str) -> str | None:
    for p in blob_name.split("/"):
        if p.startswith("day="):
            return p[4:]
    return None


def _scan_blob(bucket: storage.Bucket, blob: storage.Blob) -> list[dict[str, object]]:
    """Read one fixtures.parquet and emit per-canonical-league manifest rows."""
    date_str = _parse_date(blob.name)
    if date_str is None:
        return []

    try:
        raw = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("Failed to read %s: %s", blob.name, exc)
        return []

    if df.empty or "af_league_id" not in df.columns:
        return []

    per_league: dict[str, int] = {}
    unmapped = 0
    for af_id, group in df.groupby("af_league_id"):
        try:
            af_id_int = int(af_id)
        except (ValueError, TypeError):
            unmapped += len(group)
            continue
        league = get_league_by_api_football_id(af_id_int)
        if league is None:
            unmapped += len(group)
            continue
        per_league[league.league_id] = per_league.get(league.league_id, 0) + len(group)

    entries: list[dict[str, object]] = []
    for lid, count in per_league.items():
        entries.append(
            {
                "date": date_str,
                "venue": "",
                "data_type": "FIXTURES",
                "service_name": "instruments-service",
                "instrument_count": int(count),
                "written_at": _NOW,
                "schema_version": 5,
                "timeframe": "",
                "league_id": lid,
                "chain": "",
                "instrument_type": "",
                "capture_status": "captured",
                "error_reason": "",
                "attempted_at": _NOW,
                "expected": True,
                "available": True,
            }
        )

    if unmapped:
        logger.debug("%s: %d fixtures unmapped (af_league_id not in LEAGUE_REGISTRY)", date_str, unmapped)

    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescan sports FIXTURES with canonical league mapping")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing index")
    parser.add_argument("--date", type=str, help="Scan a single date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers")
    parser.add_argument("--bucket", type=str, default=BUCKET_NAME, help="GCS bucket")
    args = parser.parse_args()

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    logger.info("Listing fixtures blobs in %s ...", args.bucket)
    blobs = _list_fixtures_blobs(bucket, args.date)
    logger.info("Found %d fixtures.parquet files", len(blobs))
    if not blobs:
        logger.warning("Nothing to do.")
        return

    all_entries: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_scan_blob, bucket, b): b for b in blobs}
        for done, future in enumerate(as_completed(futures), 1):
            try:
                all_entries.extend(future.result())
            except Exception as exc:
                logger.warning("scan failed: %s", exc)
            if done % 200 == 0:
                logger.info("Progress: %d / %d", done, len(blobs))

    logger.info(
        "Scan complete: %d per-(date, league_id) FIXTURES rows across %d blobs",
        len(all_entries),
        len(blobs),
    )

    if not all_entries:
        logger.warning("No entries produced. Aborting.")
        sys.exit(1)

    df = pd.DataFrame(all_entries)
    logger.info("Top-10 leagues by fixture count (Phase 5 target >= 200 per league/season):")
    top = df.groupby("league_id")["instrument_count"].sum().sort_values(ascending=False).head(10)
    print(top.to_string())

    if args.dry_run:
        logger.info("DRY RUN — not writing manifest.")
        return

    # Read existing index
    index_blob = bucket.blob(INDEX_BLOB)
    existing_entries: list[dict[str, object]] = []
    if index_blob.exists():
        logger.info("Reading existing manifest ...")
        existing_df = pd.read_parquet(io.BytesIO(index_blob.download_as_bytes()))
        # Keep everything except FIXTURES rows from instruments-service with non-empty league_id —
        # those are the rows we're replacing. Preserve legacy empty-league_id FIXTURES rows
        # (rescan_sports_manifest bootstrap output) so we don't lose the sparse-entity entries.
        mask_keep = ~(
            (existing_df.get("data_type") == "FIXTURES")
            & (existing_df.get("service_name") == "instruments-service")
            & (existing_df.get("league_id", pd.Series(dtype=str)).astype(str) != "")
        )
        existing_entries = existing_df[mask_keep].to_dict("records")
        logger.info(
            "Preserving %d existing rows; replacing %d FIXTURES per-league rows",
            len(existing_entries),
            len(existing_df) - len(existing_entries),
        )

    combined = pd.DataFrame(all_entries + existing_entries)
    local_out = "/tmp/_new_manifest.parquet"
    combined.to_parquet(local_out, index=False)
    bucket.blob(INDEX_BLOB).upload_from_filename(local_out)
    logger.info("Wrote %d rows to gs://%s/%s", len(combined), args.bucket, INDEX_BLOB)


if __name__ == "__main__":
    main()
