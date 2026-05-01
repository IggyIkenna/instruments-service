#!/usr/bin/env python3
"""Canonicalize GCS sports parquets that live at ``league={NUMERIC}/`` paths.

For each per-league parquet under
``sports_reference/by_date/day=YYYY-MM-DD/entity={E}/league={NUMERIC}/{E}.parquet``
where the league directory is an all-digits api_football_id, copy the blob
to the corresponding canonical path
``sports_reference/by_date/day=YYYY-MM-DD/entity={E}/league={CANONICAL}/{E}.parquet``
and delete the numeric original.

Without this rename, per-league downstream readers that resolve via UAC
``candidate_parquet_paths(data_type, day, "EPL")`` get a path like
``league=EPL/...`` and 404 on the file actually sitting at ``league=39/...``.

Idempotent: skips paths whose target already exists.

Companion to ``canonicalize_manifest_league_ids.py`` (manifest pass).
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
from unified_api_contracts.sports import get_league_by_api_football_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"

# Match per-league subpartition with NUMERIC league directory.
NUMERIC_LEAGUE_RE = re.compile(
    r"^(?P<prefix>sports_reference/by_date/day=\d{4}-\d{2}-\d{2}/entity=[^/]+/)"
    r"league=(?P<numeric>\d+)/"
    r"(?P<filename>[^/]+\.parquet)$"
)


def _resolve_canonical(numeric: str) -> str | None:
    try:
        league = get_league_by_api_football_id(int(numeric))
    except Exception:
        return None
    return league.league_id if league is not None else None


def _rename_one(
    bucket: storage.Bucket,
    src_name: str,
    canonical: str,
    dry_run: bool,
) -> str:
    """Copy src to canonical path then delete src. Returns status string."""
    m = NUMERIC_LEAGUE_RE.match(src_name)
    if m is None:
        return "skip-no-match"
    target = f"{m.group('prefix')}league={canonical}/{m.group('filename')}"
    src = bucket.blob(src_name)
    tgt = bucket.blob(target)
    if tgt.exists():
        if dry_run:
            return "would-skip-target-exists"
        # Delete redundant numeric source — canonical already authoritative.
        src.delete()
        return "deleted-numeric-target-exists"
    if dry_run:
        return "would-copy"
    bucket.copy_blob(src, bucket, new_name=target)
    src.delete()
    return "copied-and-deleted"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-workers", type=int, default=32)
    args = parser.parse_args()

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)

    logger.info("Listing all sports_reference/by_date/ blobs...")
    work: list[tuple[str, str]] = []
    n_listed = 0
    unmapped_numerics: set[str] = set()
    for blob in bucket.list_blobs(prefix="sports_reference/by_date/"):
        n_listed += 1
        if n_listed % 50000 == 0:
            logger.info("  listed %d blobs, queued %d for rename", n_listed, len(work))
        m = NUMERIC_LEAGUE_RE.match(blob.name)
        if m is None:
            continue
        numeric = m.group("numeric")
        canonical = _resolve_canonical(numeric)
        if canonical is None:
            unmapped_numerics.add(numeric)
            continue
        work.append((blob.name, canonical))

    logger.info("Total blobs scanned: %d", n_listed)
    logger.info("Numeric-league blobs queued: %d", len(work))
    logger.info("Unmapped numeric IDs (skipped): %d  sample=%s", len(unmapped_numerics), sorted(unmapped_numerics)[:10])
    if not work:
        logger.info("Nothing to rename.")
        return 0

    counters: Counter[str] = Counter()
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(_rename_one, bucket, src, canonical, args.dry_run): src for src, canonical in work}
        for i, fut in enumerate(as_completed(futures)):
            try:
                status = fut.result()
            except Exception as exc:
                status = f"error: {type(exc).__name__}"
            counters[status] += 1
            if (i + 1) % 5000 == 0:
                logger.info("  progress: %d/%d done; counters=%s", i + 1, len(work), dict(counters))

    logger.info("=" * 60)
    logger.info("Final rename counters: %s", dict(counters))
    return 0


if __name__ == "__main__":
    sys.exit(main())
