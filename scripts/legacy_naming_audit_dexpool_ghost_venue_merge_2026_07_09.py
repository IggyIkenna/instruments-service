#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + a re-scan of every GHOST_TO_CANON prefix returns 0 objects
"""Merge legacy ghost-spelled DeFi venue GCS objects into their canonical-spelled
sibling under ``instrument_availability/by_date/`` (instruments-service reference
data, NOT MTDS market data).

## Real finding (2026-07-09 legacy-naming audit, generalizing the on-chain-perp
## bare-filename finding to CeFi/DeFi per the operator's 2026-07-09 decision)

A real, narrow-prefix GCS listing (not the manifest summary) of
``instrument_availability/by_date/day=*/venue=*/instruments.parquet`` across the
FULL real day-partition range (2020-01-20 .. 2026-07-09, 2,363 real day-prefixes in
``instruments-store-defi-prd-{pid}`` + 2,315 in the legacy env-less
``instruments-store-defi-{pid}``) found TWO real, coexisting venue-token spellings
for 10 of the 13 in-scope DEX-pool protocols (AAVE_V3's lending is also affected —
in scope per this audit's "DEX-pool protocols AND lending/staking venues" mandate):
a no-underscore "ghost" form (``UNISWAPV3-ARBITRUM``) and the underscore-canonical
form (``UNISWAP_V3-ARBITRUM``), written IN PARALLEL for the same real protocol x
chain x day, spanning **~4 years** (ghost writes ran 2022-03-27 .. ~2026-05-11,
then stopped — ``instruments_service/engine/orchestrator/writers.py``'s
``canonicalize_defi_venue_combined(venue_str)`` call, landed per
``defi_coverage_capability_alignment_2026_05_22.md``, already fixed this
GOING FORWARD; this script is the historical backfill that fix never covered).

Real per-venue ghost-day counts (full real scan, both buckets, 2026-07-09):

| Ghost venue            | Ghost days | Canon days (same bucket) | Notes |
|-------------------------|-----------:|--------------------------:|-------|
| AAVEV3-ARBITRUM          | 1,514 | 1,572 | |
| AAVEV3-AVALANCHE         | 1,514 | 1,572 | |
| AAVEV3-BASE              |   985 | 1,046 | |
| AAVEV3-BSC               |   831 |   891 | |
| AAVEV3-ETHEREUM          | 1,193 | 1,256 | |
| AAVEV3-LINEA             |   446 |   495 | |
| AAVEV3-OPTIMISM          | 1,514 |   108 | canon is a MINORITY of real history |
| AAVEV3-POLYGON           | 1,514 | 1,572 | |
| AERODROMEV3-BASE         |   710 |   792 | |
| CAMELOTV3-ARBITRUM       | 1,032 | 1,106 | |
| COMPOUNDV3-ARBITRUM      | 1,095 | 1,152 | |
| COMPOUNDV3-BASE          |   988 | 1,046 | |
| COMPOUNDV3-ETHEREUM      | 1,359 | 1,422 | |
| COMPOUNDV3-OPTIMISM      |   757 |   822 | |
| PANCAKESWAPV3-BASE       |   954 | 1,032 | |
| PANCAKESWAPV3-BSC        | 1,105 | 1,182 | |
| PANCAKESWAPV3-ETHEREUM   | 1,105 | 1,182 | |
| PANCAKESWAPV3-ZKSYNC     |   446 |     0 | 100% orphan under ghost spelling |
| SUSHISWAPV3-AVALANCHE    | 1,103 | 1,192 | |
| SUSHISWAPV3-BASE         |   245 |   883 | |
| SUSHISWAPV3-ETHEREUM     | 1,102 | 1,191 | |
| UNISWAPV2-ETHEREUM       | 2,189 | 2,243 | |
| UNISWAPV3-ARBITRUM       | 1,781 | 1,842 | |
| UNISWAPV3-BASE           |   974 | 1,006 | |
| UNISWAPV3-ETHEREUM       | 1,825 | 1,883 | |
| UNISWAPV3-OPTIMISM       | 1,634 | 1,691 | |
| UNISWAPV3-POLYGON        | 1,594 | 1,656 | |
| UNISWAPV4-ETHEREUM       |   459 |   525 | |
| VELODROMEV2-OPTIMISM (legacy env-less bucket ONLY) | 1,044 | 0 (either bucket) | 100% orphan, also WRONG bucket |

Total: **33,012 real ghost-venue objects** (31,968 in ``-prd-`` + 1,044 in the
legacy env-less bucket), of which **29,840 are same-day COLLISIONS** (a canonical
sibling already exists for that exact day) and **3,172 are PURE ORPHANS** (no
canonical sibling exists anywhere for that day — silently invisible to any reader
that only lists the canonical venue prefix, e.g. deployment-ui's pool-breakdown).

## Why this is a MERGE, not a rename (real content-diff evidence)

A real content diff (81 sampled ghost/canon same-day pairs, reading both parquets
and comparing on the ``raw_symbol`` identity column) found ghost and canonical are
**NOT byte-identical duplicates** for most pairs — each side commonly holds REAL
pools the other side is missing (e.g. real: ``UNISWAPV3-OPTIMISM`` day=2023-11-18
had 168 rows, canonical had 282, with 6 pools unique to ghost and 120 unique to
canonical; ``PANCAKESWAPV3-BSC`` day=2024-10-07 had 129 ghost rows vs 86 canonical,
59 unique to ghost). The canonical schema is also WIDER (51 real columns vs
ghost's 40 — ``canonical_instrument_id``, ``product_root``, ``available_at``, etc.
were added to the writer after most ghost objects were captured). A blind
copy-and-overwrite in EITHER direction would destroy real, non-redundant pool
history. This script instead reads both real objects, takes the FULL row union
keyed on the identity column (preferring the canonical row's values when both
sides captured the same real pool, since canonical carries the richer/current
schema — this matches the operator's already-settled "ghost is a typo, retired
not migrated" framing for the catalogue layer, extended here to mean "canonical
wins on conflict, but a ghost-only real capture is never silently dropped"), and
writes the union back under ONLY the canonical path.

## Mechanics (matches instrument_id_format_canonicalization_2026_07_08.md's
## already-decided "migration mechanics": rewrite/relabel in place from
## already-captured data, never a re-download; backup before mutating)

For each (bucket, ghost_venue, day):
  1. Download the real ghost object. If a canonical object exists for the same
     (bucket, day), download it too.
  2. BACKUP both real pre-migration objects (server-side copy, no egress) under
     ``_migration_backup/legacy_naming_audit_dexpool_2026_07_09/<run_id>/...``
     in the destination (``-prd-``) bucket, before any write.
  3. Compute the column-union schema; row-union keyed by the first present
     identity column (``raw_symbol`` else ``pool_address`` else ``instrument_key``),
     canonical row wins on conflict, ghost-only rows carried over with the
     canonical-only columns filled ``None`` (honest absence, not fabricated).
  4. Write the merged frame to the CANONICAL path in the PRD bucket (this also
     handles ``VELODROMEV2-OPTIMISM``'s real cross-bucket case: legacy env-less
     bucket -> ``-prd-`` bucket, since ``-prd-`` is confirmed the only bucket any
     current reader/writer resolves via ``_get_instruments_bucket`` ->
     ``resolve_bucket_name`` -- the env-less bucket has taken ZERO real writes
     since 2026-05-22, confirmed via a real day-partition high-water-mark check).
  5. Delete the real ghost object ONLY after the canonical write is confirmed
     (re-read row count >= max(ghost_rows, old_canon_rows)).

Idempotent: re-running finds 0 ghost objects once complete (the ghost venue
prefix is empty).

Usage::

    cd instruments-service
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py --dry-run
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py --apply --workers 24
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py --apply --limit 50  # smoke test

Real evidence + per-protocol write-back results: see the Progress Log in
``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
and ``instruments-service/docs/DEFI_INSTRUMENTS.md``.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO

import pandas as pd
import requests
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("legacy_naming_audit_dexpool")

PROJECT_ID = "central-element-323112"
PRD_BUCKET = f"instruments-store-defi-prd-{PROJECT_ID}"
LEGACY_BUCKET = f"instruments-store-defi-{PROJECT_ID}"
PREFIX = "instrument_availability/by_date/"
RUN_ID = "2026_07_09"
BACKUP_PREFIX = f"_migration_backup/legacy_naming_audit_dexpool_{RUN_ID}"

# Real ghost -> canonical venue-token map, derived from the real full-history
# GCS scan (2026-07-09) — every ghost token that was found to have at least one
# real object under `instrument_availability/by_date/venue=<ghost>/`. Kept as an
# explicit table (not re-derived via canonicalize_defi_venue_combined at scan
# time) so the migration scope is a reviewable, fixed list, not a moving target.
GHOST_TO_CANON: dict[str, str] = {
    "AAVEV3-ARBITRUM": "AAVE_V3-ARBITRUM",
    "AAVEV3-AVALANCHE": "AAVE_V3-AVALANCHE",
    "AAVEV3-BASE": "AAVE_V3-BASE",
    "AAVEV3-BSC": "AAVE_V3-BSC",
    "AAVEV3-ETHEREUM": "AAVE_V3-ETHEREUM",
    "AAVEV3-LINEA": "AAVE_V3-LINEA",
    "AAVEV3-OPTIMISM": "AAVE_V3-OPTIMISM",
    "AAVEV3-POLYGON": "AAVE_V3-POLYGON",
    "AERODROMEV3-BASE": "AERODROME_V3-BASE",
    "CAMELOTV3-ARBITRUM": "CAMELOT_V3-ARBITRUM",
    "COMPOUNDV3-ARBITRUM": "COMPOUND_V3-ARBITRUM",
    "COMPOUNDV3-BASE": "COMPOUND_V3-BASE",
    "COMPOUNDV3-ETHEREUM": "COMPOUND_V3-ETHEREUM",
    "COMPOUNDV3-OPTIMISM": "COMPOUND_V3-OPTIMISM",
    "PANCAKESWAPV3-BASE": "PANCAKESWAP_V3-BASE",
    "PANCAKESWAPV3-BSC": "PANCAKESWAP_V3-BSC",
    "PANCAKESWAPV3-ETHEREUM": "PANCAKESWAP_V3-ETHEREUM",
    "PANCAKESWAPV3-ZKSYNC": "PANCAKESWAP_V3-ZKSYNC",
    "SUSHISWAPV3-AVALANCHE": "SUSHISWAP_V3-AVALANCHE",
    "SUSHISWAPV3-BASE": "SUSHISWAP_V3-BASE",
    "SUSHISWAPV3-ETHEREUM": "SUSHISWAP_V3-ETHEREUM",
    "UNISWAPV2-ETHEREUM": "UNISWAP_V2-ETHEREUM",
    "UNISWAPV3-ARBITRUM": "UNISWAP_V3-ARBITRUM",
    "UNISWAPV3-BASE": "UNISWAP_V3-BASE",
    "UNISWAPV3-ETHEREUM": "UNISWAP_V3-ETHEREUM",
    "UNISWAPV3-OPTIMISM": "UNISWAP_V3-OPTIMISM",
    "UNISWAPV3-POLYGON": "UNISWAP_V3-POLYGON",
    "UNISWAPV4-ETHEREUM": "UNISWAP_V4-ETHEREUM",
    "VELODROMEV2-OPTIMISM": "VELODROME_V2-OPTIMISM",
}

_IDENTITY_COLS = ("raw_symbol", "pool_address", "instrument_key")


def _make_client(project: str, pool_maxsize: int) -> storage.Client:
    """A storage.Client with an enlarged urllib3 connection pool so a real
    ThreadPoolExecutor with workers > the default pool size (10) doesn't spend
    the run discarding+reopening connections."""
    client = storage.Client(project=project)
    adapter = requests.adapters.HTTPAdapter(pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
    # google-cloud-storage has no public pool-size knob; mounting a larger
    # adapter on the private `_http` session is the standard workaround.
    client._http.mount("https://", adapter)
    client._http.mount("http://", adapter)
    return client


@dataclass
class MergeResult:
    bucket: str
    ghost_venue: str
    canon_venue: str
    day: str
    ok: bool
    ghost_rows: int
    canon_rows_before: int
    merged_rows: int
    err: str = ""


def _identity_col(df: pd.DataFrame) -> str | None:
    for c in _IDENTITY_COLS:
        if c in df.columns:
            return c
    return None


def _read_parquet(bucket: storage.Bucket, path: str) -> pd.DataFrame | None:
    blob = bucket.blob(path)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(data))


def _merge_frames(ghost_df: pd.DataFrame, canon_df: pd.DataFrame | None) -> pd.DataFrame:
    """Column-union + row-union (canonical wins on conflict), never drops a real row."""
    if canon_df is None or canon_df.empty:
        return ghost_df.copy()
    id_col = _identity_col(canon_df) or _identity_col(ghost_df)
    if id_col is None or id_col not in ghost_df.columns or id_col not in canon_df.columns:
        # No shared identity column to dedupe on — concatenate honestly rather
        # than guess; downstream duplicate pools are still discoverable, never
        # silently dropped.
        all_cols = list(dict.fromkeys([*canon_df.columns, *ghost_df.columns]))
        return pd.concat([canon_df, ghost_df], ignore_index=True)[all_cols]
    all_cols = list(dict.fromkeys([*canon_df.columns, *ghost_df.columns]))
    canon_ids = set(canon_df[id_col].astype(str).str.lower())
    ghost_only = ghost_df[~ghost_df[id_col].astype(str).str.lower().isin(canon_ids)]
    merged = pd.concat([canon_df, ghost_only], ignore_index=True)
    for c in all_cols:
        if c not in merged.columns:
            merged[c] = None
    return merged[all_cols]


def _backup_path(dst_bucket_name: str, original_bucket: str, original_path: str) -> str:
    return f"{BACKUP_PREFIX}/{original_bucket}/{original_path}"


def _process_one(
    client: storage.Client,
    bucket_name: str,
    ghost_venue: str,
    canon_venue: str,
    day: str,
    *,
    apply: bool,
) -> MergeResult:
    src_bucket = client.bucket(bucket_name)
    dst_bucket = client.bucket(PRD_BUCKET)  # canonical destination is ALWAYS -prd-
    ghost_path = f"{PREFIX}day={day}/venue={ghost_venue}/instruments.parquet"
    canon_path = f"{PREFIX}day={day}/venue={canon_venue}/instruments.parquet"
    try:
        ghost_df = _read_parquet(src_bucket, ghost_path)
        if ghost_df is None:
            return MergeResult(bucket_name, ghost_venue, canon_venue, day, False, 0, 0, 0, "ghost object vanished")
        canon_df = _read_parquet(dst_bucket, canon_path)
        canon_rows_before = 0 if canon_df is None else len(canon_df)
        merged = _merge_frames(ghost_df, canon_df)

        if not apply:
            return MergeResult(
                bucket_name, ghost_venue, canon_venue, day, True, len(ghost_df), canon_rows_before, len(merged)
            )

        # Backup BEFORE mutating: server-side copy of the real pre-migration bytes.
        src_bucket.copy_blob(
            src_bucket.blob(ghost_path), dst_bucket, new_name=_backup_path(PRD_BUCKET, bucket_name, ghost_path)
        )
        if canon_df is not None:
            dst_bucket.copy_blob(
                dst_bucket.blob(canon_path), dst_bucket, new_name=_backup_path(PRD_BUCKET, PRD_BUCKET, canon_path)
            )

        buf = BytesIO()
        merged.to_parquet(buf, index=False)
        buf.seek(0)
        dst_bucket.blob(canon_path).upload_from_file(buf, content_type="application/octet-stream")

        # Verify before delete: re-read the just-written canonical object. Floor is the
        # UNIQUE identity-column count, not the raw row count — a real source file can
        # carry an internal duplicate identity value (e.g. a subgraph pagination overlap
        # re-listing the same pool address twice); the merge already correctly dedupes
        # on the identity column, so gating on raw row count would reject a genuinely
        # correct de-duplicated merge as a false-positive "regression."
        id_col = _identity_col(ghost_df)
        ghost_floor = len(ghost_df[id_col].astype(str).str.lower().unique()) if id_col else len(ghost_df)
        verify_df = _read_parquet(dst_bucket, canon_path)
        if verify_df is None or len(verify_df) < max(ghost_floor, canon_rows_before):
            return MergeResult(
                bucket_name,
                ghost_venue,
                canon_venue,
                day,
                False,
                len(ghost_df),
                canon_rows_before,
                0 if verify_df is None else len(verify_df),
                "post-write verify row-count regression — ghost NOT deleted",
            )

        src_bucket.blob(ghost_path).delete()
        return MergeResult(
            bucket_name, ghost_venue, canon_venue, day, True, len(ghost_df), canon_rows_before, len(merged)
        )
    except Exception as exc:  # broad-except-ok: per-shard failure isolation, real migration over 33k objects
        return MergeResult(bucket_name, ghost_venue, canon_venue, day, False, 0, 0, 0, str(exc))


def _list_ghost_days(client: storage.Client, bucket_name: str, ghost_venue: str) -> list[str]:
    """Real, scoped (single-venue-prefix) GCS listing — not a whole-corpus walk."""
    bucket = client.bucket(bucket_name)
    days: list[str] = []
    for blob in client.list_blobs(
        bucket, prefix=PREFIX, match_glob=f"{PREFIX}day=*/venue={ghost_venue}/instruments.parquet"
    ):
        # day=YYYY-MM-DD/venue=.../instruments.parquet
        part = blob.name.split("day=", 1)[1]
        days.append(part.split("/", 1)[0])
    return sorted(days)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="Write for real. Default is dry-run.")
    p.add_argument("--workers", type=int, default=24)
    p.add_argument("--limit", type=int, default=0, help="Cap total (venue,day) pairs processed (0 = unlimited).")
    p.add_argument(
        "--venue",
        action="append",
        default=None,
        help="Restrict to one or more ghost venue tokens (repeatable). Default: all GHOST_TO_CANON keys.",
    )
    args = p.parse_args()
    apply = bool(args.apply)

    client = _make_client(PROJECT_ID, pool_maxsize=max(32, args.workers * 2))
    ghost_venues = args.venue or list(GHOST_TO_CANON.keys())

    logger.info(
        "Real GCS listing (scoped per-venue prefix, not a whole-corpus walk) for %d ghost venues across 2 buckets",
        len(ghost_venues),
    )
    jobs: list[tuple[str, str, str, str]] = []  # (bucket, ghost, canon, day)
    t0 = time.time()
    list_targets = [(b, g) for b in (PRD_BUCKET, LEGACY_BUCKET) for g in ghost_venues]
    with ThreadPoolExecutor(max_workers=min(32, len(list_targets))) as ex:
        futs = {ex.submit(_list_ghost_days, client, b, g): (b, g) for b, g in list_targets}
        for fut in as_completed(futs):
            bucket_name, ghost = futs[fut]
            canon = GHOST_TO_CANON[ghost]
            for day in fut.result():
                jobs.append((bucket_name, ghost, canon, day))
    jobs.sort()
    if args.limit:
        jobs = jobs[: args.limit]
    logger.info("Found %d real (bucket,ghost,day) triples to process in %.1fs", len(jobs), time.time() - t0)
    if not jobs:
        logger.info("Nothing to migrate — already fully canonical. Exiting.")
        return 0

    mode = "APPLY (real writes)" if apply else "DRY-RUN (read-only)"
    logger.info("Mode: %s. run_id=%s backup_prefix=gs://%s/%s", mode, RUN_ID, PRD_BUCKET, BACKUP_PREFIX)

    ok = 0
    failed = 0
    total_ghost_rows = 0
    total_merged_rows = 0
    completed = 0
    t1 = time.time()
    per_venue_stats: dict[str, dict[str, int]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_process_one, client, b, g, c, d, apply=apply) for b, g, c, d in jobs]
        for fut in as_completed(futs):
            r = fut.result()
            completed += 1
            stats = per_venue_stats.setdefault(r.ghost_venue, {"ok": 0, "failed": 0, "ghost_rows": 0, "merged_rows": 0})
            if r.ok:
                ok += 1
                stats["ok"] += 1
                total_ghost_rows += r.ghost_rows
                total_merged_rows += r.merged_rows
                stats["ghost_rows"] += r.ghost_rows
                stats["merged_rows"] += r.merged_rows
            else:
                failed += 1
                stats["failed"] += 1
                logger.warning("FAILED %s/%s day=%s: %s", r.bucket, r.ghost_venue, r.day, r.err)
            if completed % 500 == 0 or completed == len(jobs):
                rate = completed / max(0.01, time.time() - t1)
                logger.info(
                    "  %d/%d processed (%.1f/sec, ok=%d failed=%d, %.0fs elapsed)",
                    completed,
                    len(jobs),
                    rate,
                    ok,
                    failed,
                    time.time() - t1,
                )

    logger.info("=== Per-venue summary ===")
    for venue, stats in sorted(per_venue_stats.items()):
        logger.info(
            "  %-24s ok=%-5d failed=%-4d ghost_rows=%-6d merged_rows=%-6d",
            venue,
            stats["ok"],
            stats["failed"],
            stats["ghost_rows"],
            stats["merged_rows"],
        )
    logger.info(
        "Done. mode=%s total=%d ok=%d failed=%d total_ghost_rows=%d total_merged_rows=%d elapsed=%.0fs at %s",
        mode,
        len(jobs),
        ok,
        failed,
        total_ghost_rows,
        total_merged_rows,
        time.time() - t0,
        datetime.now(UTC).isoformat(),
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
