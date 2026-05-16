#!/usr/bin/env python3
"""Delete corrupt legacy kebab rows from lst-rates + oracle-prices canonical manifests.

Per ``plans/active/issues/lst_rates_oracle_prices_corrupt_kebab_rows_2026_05_16.md``
Option D: the 1,560 lst-rates + 1,926 oracle-prices rows with venue=={data_type
literal uppercased} are unrecoverable phantoms (real venue lost, chain empty,
no parquet on disk). Total 3,486 rows to drop.

Corruption signature (closed-set on venue + chain + instrument_type, NOT
data_type — because slot 4's canonicalize_defi_manifest_data_types script may
already have flipped the data_type kebab → snake form via per-VM shard, so
filtering on data_type kebab misses the rewritten rows):

    lst-rates bucket:
      venue   == "LST_RATES"
      chain   == ""
      data_type ∈ {"lst-rates", "lst_rates"}   ← either form
      instrument_type populated (e.g. "lst")

    oracle-prices bucket:
      venue   == "ORACLE_PRICES"
      chain   == ""
      data_type ∈ {"oracle-prices", "oracle_prices"}   ← either form
      instrument_type populated (e.g. "lst")  -- note: oracle rows also have instrument_type=lst per slot 2 audit

Usage:
    python scripts/reconcile_corrupt_kebab_rows_lst_rates_oracle_prices_2026_05_16.py --dry-run
    python scripts/reconcile_corrupt_kebab_rows_lst_rates_oracle_prices_2026_05_16.py --apply --confirm

Idempotent: re-runs find 0 corrupt rows after the first apply.

Closes:
    plans/active/issues/lst_rates_oracle_prices_corrupt_kebab_rows_2026_05_16.md

Execution ownership (Runbook SSOT):
  execution:
    owner: slot-4-ikenna (cross-slot pickup; corruption was indirectly amplified
           by slot 4's canonicalize script applied 19:44 UTC; same slot owns the
           cleanup)
    cadence: one-shot
    verifier: groupby venue on each bucket returns only real venues (LIDO /
              ETHERFI / COINBASE / LIDO / JITO / MARINADE for lst-rates; CHAINLINK /
              PYTH / UNISWAP_TWAP for oracle-prices)
    last_executed: 2026-05-16
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"

# (bucket-name-without-pid-suffix, corrupt_venue_literal, valid_data_types)
CORRUPT_SIGNATURES: list[tuple[str, str, frozenset[str]]] = [
    ("lst-rates", "LST_RATES", frozenset({"lst-rates", "lst_rates"})),
    ("oracle-prices", "ORACLE_PRICES", frozenset({"oracle-prices", "oracle_prices"})),
]


def _per_vm_shard_blob(corrupt_venue: str) -> str:
    safe = corrupt_venue.lower().replace("_", "-")
    return f"_index/per_vm/manifest-drop-corrupt-{safe}-rows.parquet"


def process_one(
    *,
    client: storage.Client,
    bucket_name: str,
    corrupt_venue: str,
    data_type_values: frozenset[str],
    apply: bool,
) -> int:
    """Return corrupt-row count for the bucket. Drops them if apply."""
    full_bucket = f"{bucket_name}-{PROJECT_ID}"
    bucket = client.bucket(full_bucket)
    blob = bucket.blob(INDEX_BLOB)
    if not blob.exists():
        logger.warning("[%s] manifest missing — skipping", full_bucket)
        return 0

    local_path = f"{tempfile.gettempdir()}/{bucket_name}_pre_corrupt_drop.parquet"
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("[%s] manifest rows: %d", full_bucket, len(df))

    required_cols = {"data_type", "venue", "chain"}
    missing = required_cols - set(df.columns)
    if missing:
        logger.error("[%s] manifest missing columns %s — skipping", full_bucket, sorted(missing))
        return 0

    dt = df["data_type"].astype(str)
    venue = df["venue"].astype(str)
    chain = df["chain"].fillna("").astype(str)

    corrupt_mask = (
        dt.isin(data_type_values)
        & (venue == corrupt_venue)
        & (chain == "")
    )
    corrupt_count = int(corrupt_mask.sum())
    logger.info(
        "[%s] corrupt rows (venue=%s, chain=empty, data_type∈%s): %d",
        full_bucket,
        corrupt_venue,
        sorted(data_type_values),
        corrupt_count,
    )

    if corrupt_count == 0:
        logger.info("[%s] nothing to drop — already clean", full_bucket)
        return 0

    sample_cols = [c for c in ("date", "venue", "chain", "data_type", "instrument_type", "capture_status", "written_at") if c in df.columns]
    logger.info(
        "[%s] sample 3 corrupt rows (cols=%s):\n%s",
        full_bucket,
        sample_cols,
        df.loc[corrupt_mask, sample_cols].head(3).to_string(index=False),
    )

    if not apply:
        return corrupt_count

    # Strategy: rather than write a "drop these rows" shard (per-VM shards are
    # additive last-writer-wins, so dropping requires REWRITING the entire
    # canonical manifest minus the corrupt rows). Write the cleaned snapshot
    # back to the canonical INDEX_BLOB.
    clean_df = df.loc[~corrupt_mask].copy()
    logger.info(
        "[%s] writing cleaned snapshot: %d rows (was %d, dropping %d)",
        full_bucket,
        len(clean_df),
        len(df),
        corrupt_count,
    )

    # Also drop any per-VM canonicalize shards that contain the corrupt rows so
    # the consolidator doesn't re-introduce them on next cycle.
    canon_shard_kebab = f"_index/per_vm/manifest-canonicalize-{bucket_name}-kebab-to-snake.parquet"
    canon_shard_blob = bucket.blob(canon_shard_kebab)
    if canon_shard_blob.exists():
        canon_local = f"{tempfile.gettempdir()}/{bucket_name}_canon_shard.parquet"
        canon_shard_blob.download_to_filename(canon_local)
        canon_df = pd.read_parquet(canon_local)
        canon_dt = canon_df.get("data_type", pd.Series([], dtype=str)).astype(str)
        canon_venue = canon_df.get("venue", pd.Series([], dtype=str)).astype(str)
        canon_chain = canon_df.get("chain", pd.Series([], dtype=str)).fillna("").astype(str)
        canon_corrupt = (
            canon_dt.isin(data_type_values)
            & (canon_venue == corrupt_venue)
            & (canon_chain == "")
        )
        canon_corrupt_count = int(canon_corrupt.sum())
        if canon_corrupt_count > 0:
            canon_clean = canon_df.loc[~canon_corrupt].copy()
            logger.info(
                "[%s] canonicalize shard had %d corrupt rows; rewriting clean (%d → %d rows)",
                full_bucket,
                canon_corrupt_count,
                len(canon_df),
                len(canon_clean),
            )
            cout = io.BytesIO()
            canon_clean.to_parquet(cout, index=False)
            cout.seek(0)
            canon_shard_blob.upload_from_file(cout, content_type="application/octet-stream")
            logger.info("[%s] canonicalize shard cleaned at %s", full_bucket, canon_shard_kebab)

    # Write the cleaned canonical manifest back. This overwrites the entire blob.
    out = io.BytesIO()
    clean_df.to_parquet(out, index=False)
    out.seek(0)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("[%s] canonical manifest rewritten: %d rows", full_bucket, len(clean_df))
    return corrupt_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report counts; no writes")
    parser.add_argument("--apply", action="store_true", help="rewrite canonical manifest minus corrupt rows")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="required with --apply (safety belt; writes the canonical INDEX_BLOB, not just a per-VM shard)",
    )
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        parser.error("pass --dry-run or --apply")
    if args.apply and not args.confirm:
        parser.error("--apply requires --confirm (this overwrites the canonical manifest)")

    client = storage.Client(project=PROJECT_ID)
    total = 0
    for bucket_name, corrupt_venue, data_type_values in CORRUPT_SIGNATURES:
        corrupt_count = process_one(
            client=client,
            bucket_name=bucket_name,
            corrupt_venue=corrupt_venue,
            data_type_values=data_type_values,
            apply=args.apply,
        )
        total += corrupt_count

    logger.info("=" * 60)
    logger.info("SUMMARY (%s):", "applied" if args.apply else "dry-run")
    logger.info("  Total corrupt rows: %d", total)
    if args.apply and total > 0:
        logger.info(
            "Verify post-apply: groupby venue on each manifest returns only real venues (no LST_RATES / ORACLE_PRICES literals)."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
