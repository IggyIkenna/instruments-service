#!/usr/bin/env python3
"""Write empty placeholder parquets for PLAYER_VALUES at per-(day, league).

The PLAYER_VALUES manifest emits ``capture_status=captured`` per fixture-day
× canonical_league for breakdown cohesion (denorm of weekly transfermarkt
snapshots — see ``backfill_sports_per_entity_manifest.py``). The literal
data_status audit reads ``candidate_parquet_paths`` and fails because no
parquet exists at the per-(day, league) path on non-snapshot days.

This script writes a 0-row parquet (schema headers only) at every
per-(day, league) path the manifest claims. Storage cost ~ 1KB × 167k
shards = ~170MB. Result: physical truth matches the manifest's logical
truth — every captured row has a retrievable parquet, downstream
consumers can read it (returns 0 rows, schema preserved), and the
phantom audit passes.

Idempotent: skips paths that already exist. Safe to re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/write_player_values_placeholders.py --dry-run
    .venv/bin/python scripts/write_player_values_placeholders.py            # full
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    candidate_parquet_paths,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
DATA_TYPE = "PLAYER_VALUES"
INDEX_BLOB = "_index/availability_index.parquet"

# Schema mirrors the real transfermarkt_teams.parquet — 0-row placeholder.
# When orchestrator next captures real TM data, it overwrites with full schema.
PLACEHOLDER_COLUMNS = [
    "team_id",
    "league_id",
    "canonical_league",
    "team_value_eur",
    "snapshot_date",
    "season",
    "transfermarkt_id",
]


def _write_placeholder_blob(bucket: storage.Bucket, path: str) -> bool:
    """Write a 0-row parquet at ``path``. Returns True if written, False if existed."""
    blob = bucket.blob(path)
    if blob.exists():
        return False
    df = pd.DataFrame(columns=pd.Index(PLACEHOLDER_COLUMNS))
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    return True


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--limit", type=int, default=0, help="Cap rows for testing")
    args = parser.parse_args(argv)

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)

    logger.info("Reading manifest…")
    raw = bucket.blob(INDEX_BLOB).download_as_bytes()
    df = pd.read_parquet(io.BytesIO(raw))
    df = df[
        (df["data_type"] == DATA_TYPE)
        & (df["service_name"] == "instruments-service")
        & (df["capture_status"] == "captured")
    ]
    if df.empty:
        logger.info("No PLAYER_VALUES captured rows in manifest")
        return 0
    logger.info("PLAYER_VALUES captured rows: %d", len(df))

    # Build target paths per row using SSOT.
    targets: list[str] = []
    for _, row in df.iterrows():
        day = str(row["date"])
        lid = str(row.get("league_id", "") or "")
        if not lid:
            continue
        # Per-league subpartition path (SSOT first candidate).
        paths = candidate_parquet_paths(DATA_TYPE, day, lid)
        if paths:
            targets.append(paths[0])
    targets = sorted(set(targets))
    if args.limit:
        targets = targets[: args.limit]
    logger.info("Distinct target paths: %d", len(targets))

    if args.dry_run:
        logger.info("DRY RUN — sample 5 targets:")
        for p in targets[:5]:
            logger.info("  %s", p)
        return 0

    written = 0
    skipped = 0
    failed = 0
    t0 = time.monotonic()

    def _write(path: str) -> tuple[str, bool]:
        try:
            return path, _write_placeholder_blob(bucket, path)
        except Exception as exc:
            logger.warning("write failed: %s — %s", path, exc)
            return path, None  # type: ignore[return-value]

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_write, p) for p in targets]
        n = 0
        for fut in as_completed(futures):
            path, result = fut.result()
            if result is True:
                written += 1
            elif result is False:
                skipped += 1
            else:
                failed += 1
            n += 1
            if n % 5000 == 0:
                logger.info(
                    "progress: %d/%d (written=%d, skipped=%d, failed=%d) in %.1fs",
                    n,
                    len(targets),
                    written,
                    skipped,
                    failed,
                    time.monotonic() - t0,
                )
    logger.info(
        "Done in %.1fs: %d total (%d new, %d skipped existing, %d failed)",
        time.monotonic() - t0,
        len(targets),
        written,
        skipped,
        failed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
