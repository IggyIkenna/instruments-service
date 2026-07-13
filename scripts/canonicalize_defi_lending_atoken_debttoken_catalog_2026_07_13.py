#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   defi_lending_atoken_debttoken_instrument_split_2026_07_07.md Progress Log)
"""Migrate remaining stale DeFi lending catalog rows to the canonical A_TOKEN/DEBT_TOKEN shape.

AAVE_V3/SPARK/COMPOUND_V3/MORPHO/FLUID's reference-data adapters now emit
``instrument_type=A_TOKEN``/``DEBT_TOKEN`` per reserve/market (aave_v3.py, spark.py,
compound_v3.py, morpho.py, fluid.py — see defi_lending_atoken_debttoken_instrument_split_
2026_07_07.md). The 2026-07-13 incremental catalogue rollup (build_instrument_catalogue.py)
correctly aged every row the fixed writers stopped producing into ``available_to``-capped
(delisted) history — no live row anywhere in the catalogue is still mislabeled — but the
DELISTED rows themselves still carry the legacy shape (field ``LENDING``, and for
COMPOUND_V3/MORPHO/FLUID a legacy key segment too: ``:SUPPLY:``/``:BORROW:``/
``:LENDING_MARKET:``, none of which are real ``InstrumentType`` enum members). Any consumer
reading HISTORICAL instrument identity (backtests, PnL reconciliation) would still hit the
same crash-risk / mislabel class the live rows already fixed.

This is a pure RELABEL/SPLIT of the already-catalogued row(s), never a resynthesis from an
external source, so no field is guessed:

* AAVE_V3 (4 rows): key is already ``:A_TOKEN:``/``:DEBT_TOKEN:``-shaped (this protocol never
  had a key-shape bug) — 1:1, fix the ``instrument_type`` field to match the key's own type
  segment.
* COMPOUND_V3 (26 rows): key is ``:SUPPLY:``/``:BORROW:``-shaped — 1:1, rewrite the key
  segment (SUPPLY->A_TOKEN, BORROW->DEBT_TOKEN) and the field to match; the symbol segment
  (e.g. ``CUSDC``/``BORROWUSDC``) is untouched, verified identical to what the fixed writer
  emits for the same market today.
* MORPHO / FLUID (898 + 6 rows): key is ``:LENDING_MARKET:{pair_key}``-shaped, ONE row per
  market (both a supply leg and a borrow leg collapsed together) — 1:2 SPLIT into
  ``:A_TOKEN:A{pair_key}`` and ``:DEBT_TOKEN:DEBT{pair_key}``, matching the live writers'
  naming exactly (verified against real live A_TOKEN/DEBT_TOKEN pairs in the catalogue: every
  other column — venue, chain, available_from, available_to, raw_symbol, base_asset,
  canonical_instrument_id, margin_type, glued_pair_id, pool_address, mvp — is IDENTICAL
  between a market's A_TOKEN and DEBT_TOKEN row, so both new rows are exact copies of the old
  row's other columns).

Usage:
  python scripts/canonicalize_defi_lending_atoken_debttoken_catalog_2026_07_13.py --dry-run
  python scripts/canonicalize_defi_lending_atoken_debttoken_catalog_2026_07_13.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"

# venue prefix (the instrument_id's OWN embedded prefix, before the first ':' — NOT the
# catalogue's `venue` column, which is a separate, already-flagged bug for MORPHO/FLUID
# (bare "MORPHO"/"FLUID" instead of chain-suffixed) that this migration does not touch.
_ONE_TO_ONE_AAVE = "AAVE_V3"
_ONE_TO_ONE_COMPOUND = "COMPOUND_V3"
_SPLIT_PREFIXES = ("MORPHO-", "FLUID-")


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(run_ts: str) -> str:
    return f"prod/catalog.{run_ts}.atokendebttoken.bak.parquet"


def _id_prefix(instrument_id: str) -> str:
    return instrument_id.split(":", 1)[0]


def _migrate_aave(row: pd.Series) -> pd.Series:
    """1:1 — key already correct, fix the field to match the key's own type segment."""
    parts = row["instrument_id"].split(":")
    key_type = parts[1]
    if key_type not in ("A_TOKEN", "DEBT_TOKEN"):
        raise ValueError(f"AAVE_V3 stale row with unexpected key shape: {row['instrument_id']!r}")
    out = row.copy()
    out["instrument_type"] = key_type
    return out


def _migrate_compound(row: pd.Series) -> pd.Series:
    """1:1 — rewrite the SUPPLY/BORROW key segment + field, symbol segment untouched."""
    venue_prefix, old_type, symbol = row["instrument_id"].split(":", 2)
    if old_type == "SUPPLY":
        new_type = "A_TOKEN"
    elif old_type == "BORROW":
        new_type = "DEBT_TOKEN"
    else:
        raise ValueError(f"COMPOUND_V3 stale row with unexpected key shape: {row['instrument_id']!r}")
    out = row.copy()
    out["instrument_id"] = f"{venue_prefix}:{new_type}:{symbol}"
    out["instrument_type"] = new_type
    return out


def _migrate_split(row: pd.Series) -> list[pd.Series]:
    """1:2 — split a combined LENDING_MARKET row into its A_TOKEN + DEBT_TOKEN pair."""
    venue_prefix, old_type, pair_key = row["instrument_id"].split(":", 2)
    if old_type != "LENDING_MARKET":
        raise ValueError(f"MORPHO/FLUID stale row with unexpected key shape: {row['instrument_id']!r}")
    a_row = row.copy()
    a_row["instrument_id"] = f"{venue_prefix}:A_TOKEN:A{pair_key}"
    a_row["instrument_type"] = "A_TOKEN"
    debt_row = row.copy()
    debt_row["instrument_id"] = f"{venue_prefix}:DEBT_TOKEN:DEBT{pair_key}"
    debt_row["instrument_type"] = "DEBT_TOKEN"
    return [a_row, debt_row]


def migrate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return the migrated dataframe + a summary of rows touched per protocol."""
    id_prefix = df["instrument_id"].map(_id_prefix)

    aave_mask = id_prefix.str.startswith(f"{_ONE_TO_ONE_AAVE}-", na=False) & (df["instrument_type"] == "LENDING")
    compound_mask = id_prefix.str.startswith(f"{_ONE_TO_ONE_COMPOUND}-", na=False) & (
        df["instrument_type"] == "LENDING"
    )
    split_mask = id_prefix.str.startswith(_SPLIT_PREFIXES, na=False) & (df["instrument_type"] == "LENDING")

    stale_mask = aave_mask | compound_mask | split_mask
    keep = df[~stale_mask]

    new_rows: list[pd.Series] = []
    for _, row in df[aave_mask].iterrows():
        new_rows.append(_migrate_aave(row))
    for _, row in df[compound_mask].iterrows():
        new_rows.append(_migrate_compound(row))
    for _, row in df[split_mask].iterrows():
        new_rows.extend(_migrate_split(row))

    migrated = pd.concat([keep, pd.DataFrame(new_rows)], ignore_index=True) if new_rows else df.copy()
    summary = {
        "aave_v3_relabeled": int(aave_mask.sum()),
        "compound_v3_relabeled": int(compound_mask.sum()),
        "morpho_fluid_split_source_rows": int(split_mask.sum()),
        "morpho_fluid_split_output_rows": int(split_mask.sum()) * 2,
        "rows_before": len(df),
        "rows_after": len(migrated),
    }
    return migrated, summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    bucket = _catalogue_bucket()
    client = get_storage_client(project_id=None)
    logger.info("Reading gs://%s/%s", bucket, _CATALOG_BLOB)
    raw = client.download_bytes(bucket, _CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d catalogue rows", len(df))

    migrated, summary = migrate(df)
    for k, v in summary.items():
        logger.info("  %s = %d", k, v)

    if args.dry_run:
        logger.info("[dry-run] would write %d rows (was %d) — no cloud writes performed", len(migrated), len(df))
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_blob = _backup_path(run_ts)
    logger.info("Backing up current catalogue to gs://%s/%s", bucket, backup_blob)
    client.upload_bytes(bucket, backup_blob, raw)  # pyright: ignore[reportAttributeAccessIssue]

    buf = io.BytesIO()
    migrated.to_parquet(buf, index=False)
    buf.seek(0)
    logger.info("Writing migrated catalogue (%d rows) to gs://%s/%s", len(migrated), bucket, _CATALOG_BLOB)
    client.upload_bytes(bucket, _CATALOG_BLOB, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
