#!/usr/bin/env python3
"""One-shot migration: backfill missing ``expiration`` in legacy TradFi options-chain parquets.

Background
----------
Phase 1B of tradfi_canonical_futures_contract_hard_required_fields_2026_05_13 flipped
``CanonicalOptionsChainEntry.expiration`` from nullable to required (UAC@dd407ae).  Any
options-chain parquets written *before* that schema change may have ``expiration=null``
rows.  This script locates those rows and attempts to backfill via OCC symbol parsing
(works for US equity options).  Rows that cannot be backfilled are reported and flagged
with ``LEGACY_MIGRATION_MISSING_EXPIRY`` for operator review.

What it does
------------
1. List all ``*.parquet`` objects under the tradfi options-chain partition prefix.
2. For each parquet: read into pandas, locate rows where ``expiration`` is null.
3. For each null-expiration row: attempt ``_parse_occ_expiry(symbol)`` — parses YYMMDD
   from the OCC 21-character symbol format used by US equity options.
4. If parsed: fill ``expiration`` in-place; mark row as REPAIRED.
5. If unparseable (CME futures options, non-OCC symbols): log as MISSING — operator must
   decide re-fetch vs accept ``LEGACY_MIGRATION_MISSING_EXPIRY``.
6. In --apply mode: rewrite the parquet to GCS using server-side CAS
   (``if_generation_match``) for idempotency.

Runbook execution SSOT
-----------------------
execution:
  owner: ikenna slot-5 / tradfi_canonical_futures_contract_hard_required_fields_2026_05_13
  cadence: one-shot (run once after Phase 1B lands workspace-wide)
  verifier: script exit code 0 + log line "Migration complete ok=N failed=0"
  last_executed: NEVER

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_tradfi_expiry_schema.py \\
        --bucket market-data-tick-tradfi-central-element-323112 --dry-run
    .venv/bin/python scripts/migrate_tradfi_expiry_schema.py \\
        --bucket market-data-tick-tradfi-central-element-323112 --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import NamedTuple

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# TradFi options-chain partition prefix (canonical hive layout).
# Pattern: raw_tick_data/by_date/day=YYYY-MM-DD/asset_group=tradfi/venue=.../
#           instrument_type=option/data_type=options_chain/
_TRADFI_OPTIONS_PREFIX = "raw_tick_data/by_date/"
_OPTIONS_DATA_TYPE = "data_type=options_chain"

# OCC 21-character option symbol: 6-char padded ticker + 6-char YYMMDD + C/P + 8-char strike*1000
# Example: "AAPL  241220C00200000"
_OCC_RE = re.compile(
    r"^(?P<ticker>.{6})"
    r"(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<cp>[CP])"
    r"(?P<strike>\d{8})$"
)

# UTC hour that OCC-style equity options expire (10:00 ET ≈ 15:00 UTC on expiry Friday)
_OCC_EXPIRY_UTC_HOUR = 15


class _MigrationResult(NamedTuple):
    blob_name: str
    null_rows: int
    repaired: int
    unresolvable: int
    error: str


def _parse_occ_expiry(symbol: str) -> datetime | None:
    """Parse expiration datetime from OCC 21-character option symbol format.

    Returns UTC-aware datetime at 15:00 UTC (standard OCC equity option expiry hour)
    or None if the symbol does not match OCC format.
    """
    if not symbol:
        return None
    # Try exact OCC format first (ticker padded to 6 chars with trailing spaces)
    m = _OCC_RE.match(symbol)
    if not m:
        # Some sources omit leading padding — try right-padded to 21 chars
        padded = symbol.ljust(21) if len(symbol) < 21 else symbol
        m = _OCC_RE.match(padded)
    if not m:
        return None
    try:
        yy, mm, dd = int(m.group("yy")), int(m.group("mm")), int(m.group("dd"))
        year = 2000 + yy
        return datetime(year, mm, dd, _OCC_EXPIRY_UTC_HOUR, 0, 0, tzinfo=UTC)
    except (ValueError, OverflowError):
        return None


def _has_null_expiration(df: pd.DataFrame) -> bool:
    if "expiration" not in df.columns:
        return False
    return bool(df["expiration"].isna().any())


def _process_parquet(
    bucket: storage.Bucket,
    blob_name: str,
    *,
    apply: bool,
) -> _MigrationResult:
    blob = bucket.blob(blob_name)
    try:
        raw = blob.download_as_bytes()
    except Exception as exc:  # broad-except-ok: per-shard isolation
        return _MigrationResult(blob_name, 0, 0, 0, f"download failed: {exc}")

    try:
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        return _MigrationResult(blob_name, 0, 0, 0, f"parquet read failed: {exc}")

    if not _has_null_expiration(df):
        return _MigrationResult(blob_name, 0, 0, 0, "")

    null_mask = df["expiration"].isna()
    null_count = int(null_mask.sum())
    repaired = 0
    unresolvable = 0

    for idx in df.index[null_mask]:
        symbol = str(df.at[idx, "symbol"]) if "symbol" in df.columns else ""
        expiry = _parse_occ_expiry(symbol)
        if expiry is not None:
            df.at[idx, "expiration"] = expiry
            repaired += 1
        else:
            unresolvable += 1
            logger.warning(
                "LEGACY_MIGRATION_MISSING_EXPIRY blob=%s idx=%s symbol=%r",
                blob_name,
                idx,
                symbol,
            )

    if not apply:
        logger.info(
            "DRY RUN blob=%s null=%d repaired=%d unresolvable=%d",
            blob_name,
            null_count,
            repaired,
            unresolvable,
        )
        return _MigrationResult(blob_name, null_count, repaired, unresolvable, "")

    if repaired == 0:
        # Nothing we can fix → don't rewrite
        return _MigrationResult(blob_name, null_count, 0, unresolvable, "")

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    generation = blob.generation  # captured before overwrite
    dst_blob = bucket.blob(blob_name)
    try:
        dst_blob.upload_from_file(
            buf,
            content_type="application/octet-stream",
            if_generation_match=generation,
        )
    except Exception as exc:
        return _MigrationResult(blob_name, null_count, 0, unresolvable, f"upload failed: {exc}")

    return _MigrationResult(blob_name, null_count, repaired, unresolvable, "")


def _is_options_chain_blob(name: str) -> bool:
    return name.endswith(".parquet") and _OPTIONS_DATA_TYPE in name and "asset_group=tradfi" in name


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True, help="GCS bucket name (tradfi tick data)")
    p.add_argument("--prefix", default=_TRADFI_OPTIONS_PREFIX, help="GCS prefix to scan")
    p.add_argument("--dry-run", action="store_true", help="Scan and report without writing")
    p.add_argument("--apply", action="store_true", help="Write repaired parquets to GCS")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--limit", type=int, default=0, help="Cap candidate count (0 = unlimited)")
    args = p.parse_args()

    if args.dry_run and args.apply:
        logger.error("--dry-run and --apply are mutually exclusive")
        return 2
    if not args.dry_run and not args.apply:
        logger.error("Specify --dry-run (read-only audit) or --apply (write repairs)")
        return 2

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(args.bucket)

    logger.info(
        "Scanning gs://%s/%s for null-expiration options-chain rows",
        args.bucket,
        args.prefix,
    )
    candidates: list[str] = []
    t0 = time.time()
    for blob in bucket.list_blobs(prefix=args.prefix):
        if _is_options_chain_blob(blob.name):
            candidates.append(blob.name)
            if args.limit and len(candidates) >= args.limit:
                break
    logger.info("Found %d candidate parquets in %.1fs", len(candidates), time.time() - t0)

    if not candidates:
        logger.info("No tradfi options-chain parquets found. Exiting.")
        return 0

    total_null = total_repaired = total_unresolvable = total_errors = 0
    completed = 0
    t1 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_process_parquet, bucket, name, apply=args.apply): name for name in candidates}
        for fut in as_completed(futs):
            result: _MigrationResult = fut.result()
            completed += 1
            total_null += result.null_rows
            total_repaired += result.repaired
            total_unresolvable += result.unresolvable
            if result.error:
                total_errors += 1
                logger.error("ERROR blob=%s: %s", result.blob_name, result.error)
            if completed % 50 == 0:
                rate = completed / max(0.001, time.time() - t1)
                logger.info(
                    "  %d/%d processed (%.1f/sec) null=%d repaired=%d missing=%d",
                    completed,
                    len(candidates),
                    rate,
                    total_null,
                    total_repaired,
                    total_unresolvable,
                )

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    logger.info(
        "Migration complete [%s] blobs=%d null_rows=%d repaired=%d unresolvable=%d errors=%d",
        mode,
        len(candidates),
        total_null,
        total_repaired,
        total_unresolvable,
        total_errors,
    )

    if total_unresolvable > 0:
        logger.warning(
            "%d rows could not be repaired — flag with LEGACY_MIGRATION_MISSING_EXPIRY "
            "and schedule re-fetch via Databento RDC or accept as schema-incomplete.",
            total_unresolvable,
        )

    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
