"""Dedupe manifest rows that represent the same shard under different schema versions.

Background — verified 2026-05-04 against
``gs://market-data-tick-cefi-central-element-323112/_index/availability_index.parquet``:

The CeFi manifest accumulated multiple representations of the same logical shard
across several schema versions:

  - ``instrument_type`` values: ``perpetual`` (current), ``PERPETUAL`` (legacy
    uppercase), ``""`` (empty schema-v4 sentinel)
  - ``instrument_id`` values: populated symbol (``BTCUSDT``) on schema-v5+ vs
    empty ``""`` on older venue-level rows
  - ``capture_status`` collisions: same shard with ``captured`` AND
    ``empty_confirmed`` (older sentinel) AND occasionally ``attempted_failed``

Result: BINANCE-FUTURES coverage probe shows 138% (37,669 of 229,618 unique
shards have >1 manifest row, ~16%). Same pattern applies to OKX-SWAP and other
high-traffic perp venues.

Resolution policy (canonical row picker):
  1. Group by ``(venue, date, data_type, instrument_id_normalized)`` where
     ``instrument_id_normalized = instrument_id or "<venue-level>"``.
  2. Within a group, pick the canonical row by status priority:
        captured > empty_confirmed > attempted_failed
  3. Tie-break within the same status by most recent ``attempted_at``.
  4. Drop everything else.

Idempotent — running twice is a no-op (after first run, every group has 1 row).

Usage::

    python scripts/dedupe_manifest_schema_drift.py \\
        --bucket market-data-tick-cefi-central-element-323112 --dry-run
    python scripts/dedupe_manifest_schema_drift.py \\
        --bucket market-data-tick-cefi-central-element-323112  # real run
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

_STATUS_PRIORITY = {"captured": 3, "empty_confirmed": 2, "attempted_failed": 1}


def _shard_key_columns(df: pd.DataFrame) -> list[str]:
    cols = ["venue", "date", "data_type"]
    sym_col = "instrument_id" if "instrument_id" in df.columns else ("symbol" if "symbol" in df.columns else None)
    if sym_col is not None:
        cols.append(sym_col)
    return cols


def _pick_canonical_vectorized(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """Vectorized canonical-row picker — sorts once, drops dupes by first keep.

    Equivalent to groupby(key).apply(pick_best) but ~50x faster on the full
    2.3M-row CeFi manifest. Drops the temporary _status_rank column on the way
    out so the output schema matches the input exactly.
    """
    df = df.copy()
    df["_status_rank"] = df["capture_status"].map(_STATUS_PRIORITY).fillna(0).astype(int)
    sort_cols = ["_status_rank"]
    sort_asc = [False]
    if "attempted_at" in df.columns:
        sort_cols.append("attempted_at")
        sort_asc.append(False)
    # Highest-priority row floats to the top of each key group.
    df = df.sort_values(sort_cols, ascending=sort_asc, kind="stable")
    df = df.drop_duplicates(subset=key_cols, keep="first")
    return df.drop(columns=["_status_rank"])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True)
    p.add_argument("--blob", default="_index/availability_index.parquet")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--venue",
        default="",
        help="Optional venue filter — only dedupe rows for this venue (faster smoke test).",
    )
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    uri = f"gs://{args.bucket}/{args.blob}"
    logger.info("Loading manifest from %s", uri)
    df = pd.read_parquet(uri)
    n_in = len(df)
    logger.info("Manifest rows: %d", n_in)

    if args.venue:
        mask = df["venue"] == args.venue
        scope = df[mask].copy()
        rest = df[~mask].copy()
        logger.info("Scoped to venue=%s: %d rows", args.venue, len(scope))
    else:
        scope = df.copy()
        rest = df.iloc[0:0].copy()

    key_cols = _shard_key_columns(scope)
    # Normalize blank instrument_id to a sentinel so empty venue-level rows
    # collapse with their populated-symbol counterparts only when truly equal.
    sym_col = key_cols[-1] if key_cols[-1] in ("instrument_id", "symbol") else None
    if sym_col is not None:
        scope["_sym_norm"] = scope[sym_col].astype(str).replace({"": pd.NA})
        # Group by everything except sym_col, plus the normalized sym (so empty
        # vs populated do NOT collapse together — preserve true distinction).
        grouping = ["venue", "date", "data_type", "_sym_norm"]
    else:
        grouping = ["venue", "date", "data_type"]

    counts = scope.groupby(grouping, dropna=False).size()
    n_groups = len(counts)
    n_dup_groups = (counts > 1).sum()
    n_dup_rows = int(counts[counts > 1].sum())
    n_to_drop = int(n_dup_rows - n_dup_groups)
    logger.info(
        "Grouped: %d unique shards, %d shards with >1 row (%.2f%%)",
        n_groups,
        n_dup_groups,
        100.0 * n_dup_groups / max(n_groups, 1),
    )
    logger.info("Total rows in dup groups: %d → would drop %d after dedup", n_dup_rows, n_to_drop)

    if n_to_drop == 0:
        logger.info("Nothing to dedupe. Exiting.")
        return 0

    # Status combinations for context.
    dup_groups = scope.merge(
        counts[counts > 1].rename("_n").reset_index(),
        on=grouping,
        how="inner",
    )
    combos = (
        dup_groups.groupby(grouping)["capture_status"]
        .apply(lambda s: tuple(sorted(s.tolist())))
        .value_counts()
        .head(10)
    )
    logger.info("Top dup status combos:\n%s", combos.to_string())

    # Pick canonical row per group (vectorized).
    logger.info("Picking canonical row per group (vectorized)...")
    deduped = _pick_canonical_vectorized(scope, grouping)
    deduped = deduped.drop(columns=[c for c in ("_sym_norm",) if c in deduped.columns])
    logger.info("Dedup result: %d rows (was %d in scope)", len(deduped), len(scope))

    # Recombine with the unscoped rest.
    out = pd.concat([deduped, rest], ignore_index=True) if len(rest) else deduped.reset_index(drop=True)
    n_out = len(out)
    logger.info("Total manifest rows after dedup: %d (was %d, removed %d)", n_out, n_in, n_in - n_out)

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified.")
        return 0

    # Write back via raw bytes upload (atomic on GCS).
    logger.info("Writing deduped manifest back to %s", uri)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    client = storage.Client()
    blob = client.bucket(args.bucket).blob(args.blob)
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    logger.info("Done. New blob size: %d bytes", buf.tell())
    return 0


if __name__ == "__main__":
    sys.exit(main())
