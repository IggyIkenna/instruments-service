#!/usr/bin/env python3
"""Reconcile manifest by adding captured rows for per-league parquets that exist on disk.

Companion to ``migrate_bare_to_per_league.py``. The migration writes
per-league parquet files but its second-pass league-set computation
re-uses an empty fixtures_map, so per-league manifest rows are only
emitted for data types where ``_resolve_league_for_row`` succeeds without
a fixtures lookup (footystats with ``competition_id`` in fixture_id,
INJURIES with direct ``league`` column, SFI with hex match_id mapping).

For data types that need a fixture_id → league_id join (FIXTURE_EVENTS /
LINEUPS / STATS / PLAYER_STATS / WEATHER) the per-league parquets exist
but the manifest still shows them as bare-only. This script:

1. Lists every blob under ``sports_reference/by_date/day=*/entity=*/league=*/``
2. Parses (date, entity, league_id) from the path
3. Cross-references against the canonical manifest
4. Adds a ``captured`` row for each (data_type, date, league_id) tuple
   missing from the manifest
5. Optionally drops the corresponding bare manifest row

Idempotent. Use ``--drop-bare`` to also delete the now-redundant bare row.
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import tempfile
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    SPORTS_DATA_TYPE_TO_FOLDER,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"
# Per-VM shard path for reconciliation rows. The consolidator daemon merges
# per_vm shards into canonical every ~60s; writing here ensures our adds
# survive consolidator overwrites (direct canonical writes do NOT survive).
PER_VM_SHARD_BLOB = "_index/per_vm/manifest-recon-from-per-league-parquets.parquet"

PER_LEAGUE_RE = re.compile(
    r"sports_reference/by_date/day=(?P<date>\d{4}-\d{2}-\d{2})"
    r"/entity=(?P<entity>[^/]+)/league=(?P<lid>[^/]+)/[^/]+\.parquet$"
)

# Reverse SPORTS_DATA_TYPE_TO_FOLDER: folder → data_type.
FOLDER_TO_DATA_TYPE = {v: k for k, v in SPORTS_DATA_TYPE_TO_FOLDER.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--drop-bare",
        action="store_true",
        help="Drop bare (league_id='') captured rows once a per-league row exists for the same (data_type, date)",
    )
    args = parser.parse_args()

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)

    logger.info("Listing per-league parquets under sports_reference/by_date/...")
    per_league_keys: set[tuple[str, str, str]] = set()
    n_blobs = 0
    for blob in bucket.list_blobs(prefix="sports_reference/by_date/"):
        n_blobs += 1
        if n_blobs % 50000 == 0:
            logger.info("  scanned %d blobs, %d per-league found", n_blobs, len(per_league_keys))
        m = PER_LEAGUE_RE.match(blob.name)
        if not m:
            continue
        date = m.group("date")
        entity = m.group("entity")
        lid = m.group("lid")
        data_type = FOLDER_TO_DATA_TYPE.get(entity)
        if data_type is None:
            continue
        per_league_keys.add((data_type, date, lid))
    logger.info("Total blobs scanned: %d", n_blobs)
    logger.info("Per-league parquets discovered: %d unique (data_type, date, league_id)", len(per_league_keys))

    logger.info("Reading canonical manifest...")
    blob = bucket.blob(INDEX_BLOB)
    blob.download_to_filename(f"{tempfile.gettempdir()}/canon_pre_recon.parquet")
    df = pd.read_parquet(f"{tempfile.gettempdir()}/canon_pre_recon.parquet")
    logger.info("Manifest rows: %d", len(df))

    captured = df[df["capture_status"] == "captured"]
    captured_per_league = captured[captured["league_id"].fillna("") != ""]
    existing_keys: set[tuple[str, str, str]] = set(
        zip(
            captured_per_league["data_type"].astype(str),
            captured_per_league["date"].astype(str),
            captured_per_league["league_id"].astype(str),
            strict=True,
        )
    )
    logger.info("Existing per-league captured manifest rows: %d", len(existing_keys))

    missing = per_league_keys - existing_keys
    logger.info("Missing manifest rows (parquet exists on disk, no captured row): %d", len(missing))

    if not missing:
        logger.info("Nothing to reconcile.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    new_rows: list[dict[str, object]] = []
    for data_type, date, lid in sorted(missing):
        new_rows.append(
            {
                "date": date,
                "venue": "",
                "data_type": data_type,
                "service_name": "instruments-service",
                "instrument_count": 0,
                "written_at": now_iso,
                "schema_version": 5,
                "timeframe": "",
                "league_id": lid,
                "chain": "",
                "instrument_type": "",
                "capture_status": "captured",
                "error_reason": "reconciled_from_existing_per_league_parquet",
                "attempted_at": now_iso,
                "expected": True,
                "available": True,
                "underlying": "",
                "feature_group": "",
                "model_family": "",
                "training_period": "",
                "strategy_id": "",
                "client_id": "",
                "instruction_type": "",
                "instrument_id": "",
            }
        )
    logger.info("Will add %d per-league captured rows", len(new_rows))

    n_drop = 0
    if args.drop_bare:
        # Drop bare rows for (data_type, date) where ANY per-league row now exists.
        per_league_dt_dates = {(d, dt) for (dt, d, _) in per_league_keys}
        bare_mask = (df["league_id"].fillna("") == "") & (df["capture_status"] == "captured")
        bare_dt_date_mask = bare_mask & df.apply(
            lambda r: (str(r["date"]), str(r["data_type"])) in per_league_dt_dates, axis=1
        )
        n_drop = int(bare_dt_date_mask.sum())
        logger.info("Will drop %d bare-row captures (per-league parquet now exists for same date+data_type)", n_drop)

    if args.dry_run:
        logger.info("DRY RUN — no manifest changes")
        return 0

    # Write the new per-league captured rows to a per-vm shard (NOT directly to
    # canonical). The consolidator daemon rebuilds canonical from per_vm
    # shards every ~60s, so direct canonical writes get overwritten. Per-vm
    # shards survive merges as the source of truth.
    new_df = pd.DataFrame(new_rows)
    out = io.BytesIO()
    new_df.to_parquet(out, index=False)
    out.seek(0)
    bucket.blob(PER_VM_SHARD_BLOB).upload_from_file(out, content_type="application/octet-stream")
    logger.info("Per-VM shard written: %d rows at gs://%s/%s", len(new_df), BUCKET, PER_VM_SHARD_BLOB)

    # Drop bare rows directly from canonical — these are stale-supersession rows
    # we want gone immediately. The consolidator's merge dedup logic respects
    # the latest written_at, but bare rows have league_id='' which is a
    # different shard key than per-league rows, so they don't auto-supersede.
    # Direct canonical drop + per-vm add ensures the next consolidator cycle
    # produces a clean canonical with per-league rows and no stale bare rows.
    if args.drop_bare and n_drop:
        df_clean = df[~bare_dt_date_mask]
        out2 = io.BytesIO()
        df_clean.to_parquet(out2, index=False)
        out2.seek(0)
        blob.upload_from_file(out2, content_type="application/octet-stream")
        logger.info("Canonical bare-row sweep: dropped %d rows (canonical now %d)", n_drop, len(df_clean))
    return 0


if __name__ == "__main__":
    sys.exit(main())
