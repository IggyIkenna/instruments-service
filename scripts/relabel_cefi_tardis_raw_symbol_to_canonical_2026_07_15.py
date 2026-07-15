#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after relabel verified live in the cefi -prd _index + G4 re-measure
#              shows the Tardis-lane eu gap dropping under canonical keys.
"""One-time relabel/reconcile: CeFi Tardis-lane raw exchange symbol -> canonical instrument_key.

Provenance: plans/active/issues/cefi_mtds_writer_raw_symbol_vs_canonical_eu_namespace_mismatch_2026_07_15.md
todo 2 ("[SCRIPT] P0. Write a one-time relabel/reconcile script for the cefi prd
manifest..."). Root cause (todo 1, separate task): MTDS's ``TardisAdapter`` manifest
WRITE path stamps the raw vendor symbol (e.g. ``PF_ETHUSD``) as ``instrument_id``
instead of resolving through the catalogue to the canonical ``instrument_key``
(e.g. ``KRAKEN-FUTURES:PERPETUAL:ETH-USD@LIN``) the way the non-Tardis
``OnchainPerpBatchHandler`` lane already does. Every ``captured`` row for every
Tardis-sourced venue, for the corpus's entire history, carries a raw symbol —
this script fixes the EXISTING rows in place (bytes are correct, only the
manifest key is wrong); it does not touch the writer (that is todo 1) and does
not re-fetch anything.

Mapping source: instruments-service's OWN reference-data catalogue
(``instrument_availability/by_date/day={day}/venue={venue}/instruments.parquet``),
which already carries both ``raw_symbol`` and the canonical ``instrument_key``
per instrument (measured live 2026-07-15: catalogue's Tardis-adapter build site
already resolves both). Case-insensitive match on ``raw_symbol`` (measured: the
catalogue stores lowercase, e.g. ``btcusdt``; the manifest's raw ``instrument_id``
is uppercase, e.g. ``BTCUSDT`` — a pure casing mismatch, not missing data).

Scope (STRICT — all conditions must hold):

* ``asset_group=cefi``, ``capture_status=captured``.
* ``venue`` is one of the 18 Tardis-sourced venues that have a same-named
  catalogue snapshot (measured live 2026-07-15) — see ``CATALOGUE_VENUES``.
  The 5 non-Tardis ``OnchainPerpBatchHandler`` venues (HYPERLIQUID / ASTER /
  LIGHTER-ZKSYNC / PACIFICA-SOLANA / EXTENDED-STARKNET) are excluded by
  construction (not in the allow-list) — confirmed already canonical, out of
  this defect's scope per the issue doc's scope correction.
* A small number of legacy/ambiguous venue labels (bare ``BINANCE`` / ``KRAKEN``
  / ``BITFINEX`` / ``OKEX`` / blank / etc., measured live: 30 rows total) are
  NOT in ``CATALOGUE_VENUES`` and are deliberately left untouched — a distinct,
  separate legacy-venue-naming problem, out of scope for this raw-symbol fix.

Action, per blob (main index + every ``_index/per_vm/*.parquet`` shard):

1. RELABEL: for in-scope ``captured`` rows whose ``instrument_id`` resolves
   (case-insensitively) against the venue's catalogue ``raw_symbol`` map,
   overwrite ``instrument_id`` with the canonical ``instrument_key``. Rows that
   don't resolve (delisted/expired contracts not in today's active catalogue —
   measured live: ~7% of in-scope captured rows) are left untouched and counted
   separately — an honest reported gap, not a silent drop.
2. RECONCILE (main index only — the Layer-1 enumerator's ``expected_unattempted``
   skeleton lives only there, never in per-VM shards): drop any
   ``expected_unattempted`` row whose full shard key
   (``date``, ``venue``, ``data_type``, ``instrument_type``, ``instrument_id``)
   now EXACTLY matches a row just relabeled in ANY blob (main index or a per-VM
   shard) to that same canonical key — it is now a stale duplicate of a captured
   row. Exact-match only, no fuzzy join: an eu row whose ``instrument_type``
   casing/value doesn't line up is conservatively left in place (safe direction
   — under-reconcile, never accidentally drops a genuine gap).

SNAPSHOT-FIRST (mirrors ``purge_deribit_option_per_strike_trades_book5_2026_07_12.py``):
every blob this script rewrites gets a pre-write copy under ``_index/snapshots/``
before any write. Default mode is DRY-RUN (read-only); ``--apply`` mutates.

STOP-ON-SURPRISE: measured live 2026-07-15 against the ``-prd`` cefi bucket —
main index in-scope (captured, Tardis venue) candidates = 2,980,748 (out of
11,303,737 total rows); case-insensitive catalogue resolution = 2,765,389
(92.8%); per-VM shards add a modest few thousand more. Bound generously around
the measured live total across all blobs.

Usage::

    cd instruments-service

    # dry-run (default, read-only)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py

    # apply (snapshot + relabel + reconcile + verify gate)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime, timedelta

import pandas as pd
from unified_trading_library import gcs_describe_object, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
CATALOGUE_DATE_PREFIX = "instrument_availability/by_date"

# The 18 Tardis-sourced CeFi venues with a same-named catalogue snapshot —
# measured live 2026-07-15 (see module docstring). Deliberately an explicit
# allow-list, not "all cefi venues minus the 5 onchain ones": legacy/ambiguous
# venue labels (bare BINANCE, KRAKEN, BITFINEX, OKEX, CRYPTOFACILITIES, blank,
# ...) must NOT silently pass through a subtractive filter.
CATALOGUE_VENUES: frozenset[str] = frozenset(
    {
        "BINANCE-FUTURES",
        "BINANCE-SPOT",
        "BITFINEX-FUTURES",
        "BITFINEX-SPOT",
        "BITGET-FUTURES",
        "BITGET-SPOT",
        "BYBIT",
        "BYBIT-SPOT",
        "COINBASE-CDE",
        "COINBASE-FUTURES",
        "COINBASE-SPOT",
        "DERIBIT",
        "KRAKEN-FUTURES",
        "KRAKEN-SPOT",
        "OKX-FUTURES",
        "OKX-SPOT",
        "OKX-SWAP",
        "UPBIT",
    }
)

SHARD_KEY_COLS = ["date", "venue", "data_type", "instrument_type", "instrument_id"]

# STOP-ON-SURPRISE: measured live 2026-07-15, main index only, in-scope
# (captured, CATALOGUE_VENUES) candidate rows = 2,980,748. Per-VM shards add a
# small additional amount (live 9-shard fleet, few thousand rows each). Bound
# generously around the measured total across ALL blobs combined.
_EXPECTED_MIN = 2_000_000
_EXPECTED_MAX = 3_500_000

# How many days back to probe per venue if today's catalogue snapshot is
# missing (defensive — measured live 2026-07-15 every venue had today's day).
_CATALOGUE_DAY_LOOKBACK = 7


def _resolve_catalogue_day(bucket: str, venue: str) -> str | None:
    today = datetime.now(UTC).date()
    for offset in range(_CATALOGUE_DAY_LOOKBACK):
        day = (today - timedelta(days=offset)).isoformat()
        blob = f"{CATALOGUE_DATE_PREFIX}/day={day}/venue={venue}/instruments.parquet"
        if gcs_describe_object(f"gs://{bucket}/{blob}") is not None:
            return day
    return None


def _load_raw_symbol_map(storage: object, inst_bucket: str) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """Build ``{(venue, RAW_SYMBOL_UPPER): instrument_key}`` from today's live catalogue.

    Ambiguous entries (same (venue, upper(raw_symbol)) resolving to two
    different instrument_key values — measured live: 297 case-collisions) are
    excluded from the map entirely rather than guessed at.
    """
    raw_map: dict[tuple[str, str], str] = {}
    conflicted: set[tuple[str, str]] = set()
    day_used: dict[str, str] = {}
    for venue in sorted(CATALOGUE_VENUES):
        day = _resolve_catalogue_day(inst_bucket, venue)
        if day is None:
            logger.warning(
                "No catalogue snapshot found for venue=%s in the last %d days — skipping.",
                venue,
                _CATALOGUE_DAY_LOOKBACK,
            )
            continue
        day_used[venue] = day
        blob = f"{CATALOGUE_DATE_PREFIX}/day={day}/venue={venue}/instruments.parquet"
        raw = storage.download_bytes(inst_bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
        cdf = pd.read_parquet(io.BytesIO(raw), columns=["venue", "raw_symbol", "instrument_key"])
        for row_venue, row_raw, row_key in zip(cdf["venue"], cdf["raw_symbol"], cdf["instrument_key"], strict=True):
            key = (str(row_venue), str(row_raw).upper())
            if key in conflicted:
                continue
            if key in raw_map and raw_map[key] != row_key:
                conflicted.add(key)
                del raw_map[key]
                continue
            raw_map[key] = row_key
    logger.info(
        "Catalogue map built: %d venues, %d resolvable (venue, raw_symbol) pairs, %d ambiguous pairs excluded.",
        len(day_used),
        len(raw_map),
        len(conflicted),
    )
    return raw_map, day_used


def _relabel_blob(
    df: pd.DataFrame,
    raw_map: dict[tuple[str, str], str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Relabel in-scope captured rows in ``df``. Returns (df, new_keys, stats).

    ``new_keys`` is the de-duplicated ``SHARD_KEY_COLS`` frame for every row
    just relabeled — the caller unions this across every blob before the
    reconcile pass.
    """
    required = {"capture_status", "venue", "instrument_id", *SHARD_KEY_COLS}
    if not required.issubset(df.columns):
        return (
            df,
            df.iloc[0:0][SHARD_KEY_COLS]
            if set(SHARD_KEY_COLS).issubset(df.columns)
            else pd.DataFrame(columns=SHARD_KEY_COLS),
            {
                "candidates": 0,
                "relabeled": 0,
                "unresolved": 0,
            },
        )

    in_scope_venue = df["venue"].isin(CATALOGUE_VENUES)
    captured_mask = (df["capture_status"] == "captured") & in_scope_venue
    n_candidates = int(captured_mask.sum())
    if n_candidates == 0:
        return df, df.iloc[0:0][SHARD_KEY_COLS], {"candidates": 0, "relabeled": 0, "unresolved": 0}

    upper_iid = df["instrument_id"].fillna("").astype(str).str.upper()
    lookup_keys = list(zip(df["venue"].astype(str), upper_iid, strict=True))
    canon = pd.Series([raw_map.get(k) for k in lookup_keys], index=df.index)

    relabel_mask = captured_mask & canon.notna()
    n_relabeled = int(relabel_mask.sum())
    n_unresolved = n_candidates - n_relabeled

    df = df.copy()
    df.loc[relabel_mask, "instrument_id"] = canon.loc[relabel_mask]
    new_keys = df.loc[relabel_mask, SHARD_KEY_COLS].drop_duplicates().reset_index(drop=True)

    return df, new_keys, {"candidates": n_candidates, "relabeled": n_relabeled, "unresolved": n_unresolved}


def _reconcile_eu_duplicates(df: pd.DataFrame, all_new_keys: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop expected_unattempted rows whose shard key now matches a relabeled row."""
    if all_new_keys.empty or not set(SHARD_KEY_COLS).issubset(df.columns):
        return df, 0
    if "capture_status" not in df.columns:
        return df, 0

    eu_mask = df["capture_status"] == "expected_unattempted"
    if not eu_mask.any():
        return df, 0

    eu_keys = df.loc[eu_mask, SHARD_KEY_COLS].reset_index().rename(columns={"index": "_orig_index"})
    merged = eu_keys.merge(all_new_keys.drop_duplicates().assign(_hit=True), on=SHARD_KEY_COLS, how="left")
    hit = merged["_hit"].astype("boolean").fillna(False)
    drop_idx = merged.loc[hit, "_orig_index"].tolist()
    n_dropped = len(drop_idx)
    if n_dropped:
        df = df.drop(index=drop_idx)
    return df, n_dropped


def _load(storage: object, bucket: str, blob: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw))


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_relabel_tardis_raw_symbol_{label}_{run_ts}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    df = _load(storage, bucket, blob)
    _write(storage, bucket, snap_blob, df)
    return snap_blob


def _verify_gate(storage: object, bucket: str, blobs: list[str], raw_map: dict[tuple[str, str], str]) -> int:
    """Post-apply: 0 resolvable-raw-symbol captured rows and 0 eu/captured key collisions should remain."""
    logger.info("Verifying post-apply gate on gs://%s ...", bucket)
    residual_resolvable = 0
    residual_eu_collision = 0
    for blob in blobs:
        df = _load(storage, bucket, blob)
        if "capture_status" not in df.columns or "venue" not in df.columns:
            continue
        captured_mask = (df["capture_status"] == "captured") & df["venue"].isin(CATALOGUE_VENUES)
        upper_iid = df.loc[captured_mask, "instrument_id"].fillna("").astype(str).str.upper()
        lookup_keys = list(zip(df.loc[captured_mask, "venue"].astype(str), upper_iid, strict=True))
        still_resolvable = sum(1 for k in lookup_keys if k in raw_map)
        residual_resolvable += still_resolvable
        if still_resolvable:
            logger.error(
                "GATE FAIL: gs://%s/%s still has %d resolvable raw-symbol captured rows.",
                bucket,
                blob,
                still_resolvable,
            )

    # Re-check the main index specifically for stale eu/captured key collisions
    # (the reconcile pass is scoped there only).
    main_df = _load(storage, bucket, INDEX_BLOB)
    if set(SHARD_KEY_COLS).issubset(main_df.columns) and "capture_status" in main_df.columns:
        captured_keys = main_df.loc[main_df["capture_status"] == "captured", SHARD_KEY_COLS].drop_duplicates()
        eu_keys = main_df.loc[main_df["capture_status"] == "expected_unattempted", SHARD_KEY_COLS]
        merged = eu_keys.merge(captured_keys.assign(_hit=True), on=SHARD_KEY_COLS, how="left")
        residual_eu_collision = int(merged["_hit"].astype("boolean").fillna(False).sum())
        if residual_eu_collision:
            logger.error(
                "GATE FAIL: main index still has %d expected_unattempted rows colliding with a captured key.",
                residual_eu_collision,
            )

    if residual_resolvable or residual_eu_collision:
        return 1
    logger.info(
        "GATE PASSED: 0 resolvable raw-symbol captured rows remain; 0 eu/captured key collisions in the main index."
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Relabel + reconcile (snapshot first). Default: dry-run.")
    p.add_argument(
        "--bucket", default=None, help="Override the cefi market-data bucket (default: resolve_bucket_name prd)."
    )
    p.add_argument(
        "--instruments-bucket",
        default=None,
        help="Override the cefi instruments-store bucket (default: resolve_bucket_name prd).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = args.instruments_bucket or resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi"
    )
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    raw_map, day_used = _load_raw_symbol_map(storage, inst_bucket)
    if not raw_map:
        logger.error("Empty catalogue map — refusing to proceed (would resolve nothing).")
        return 1

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [INDEX_BLOB, *per_vm_names]

    dfs: dict[str, pd.DataFrame] = {}
    all_new_keys: list[pd.DataFrame] = []
    total_candidates = 0
    total_relabeled = 0
    total_unresolved = 0
    per_blob_stats: dict[str, dict[str, int]] = {}

    for blob in blobs:
        df = _load(storage, bucket, blob)
        df, new_keys, stats = _relabel_blob(df, raw_map)
        dfs[blob] = df
        if not new_keys.empty:
            all_new_keys.append(new_keys)
        per_blob_stats[blob] = stats
        total_candidates += stats["candidates"]
        total_relabeled += stats["relabeled"]
        total_unresolved += stats["unresolved"]
        logger.info(
            "gs://%s/%s: %d candidates, %d relabeled, %d unresolved (left as raw)",
            bucket,
            blob,
            stats["candidates"],
            stats["relabeled"],
            stats["unresolved"],
        )

    logger.info(
        "TOTAL across %d blob(s): %d candidates, %d relabeled, %d unresolved.",
        len(blobs),
        total_candidates,
        total_relabeled,
        total_unresolved,
    )

    if not (_EXPECTED_MIN <= total_candidates <= _EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: total candidate row count %d outside expected range [%d, %d]. Diagnose before --apply.",
            total_candidates,
            _EXPECTED_MIN,
            _EXPECTED_MAX,
        )
        return 1

    combined_new_keys = (
        pd.concat(all_new_keys, ignore_index=True).drop_duplicates()
        if all_new_keys
        else pd.DataFrame(columns=SHARD_KEY_COLS)
    )
    main_df, n_eu_dropped = _reconcile_eu_duplicates(dfs[INDEX_BLOB], combined_new_keys)
    dfs[INDEX_BLOB] = main_df
    logger.info("Reconcile: %d stale expected_unattempted rows now redundant (main index only).", n_eu_dropped)

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. Pass --apply to relabel %d rows + drop %d stale eu rows.",
            total_relabeled,
            n_eu_dropped,
        )
        return 0

    if total_relabeled == 0:
        logger.info("Nothing to relabel.")
        return 0

    for blob in blobs:
        _snapshot(storage, bucket, blob, run_ts)
        _write(storage, bucket, blob, dfs[blob])
        logger.info("gs://%s/%s: written (%d rows).", bucket, blob, len(dfs[blob]))

    rc = _verify_gate(storage, bucket, blobs, raw_map)
    if rc == 0:
        logger.info(
            "APPLY COMPLETE: %d rows relabeled to canonical instrument_key; %d stale eu duplicates dropped. "
            "Catalogue days used: %s",
            total_relabeled,
            n_eu_dropped,
            day_used,
        )
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
