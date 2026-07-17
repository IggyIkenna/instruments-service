#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   data_status_page_ux_and_canonicalisation_2026_07_16.md P4-A "drain residual LENDING")
"""Drain the 893 residual ``instrument_type=LENDING`` rows from the DeFi catalogue.

**Why a DELETE and not another split — the important correction.** The obvious move
(and what the plan's todo suggests) is to re-run
``canonicalize_defi_lending_atoken_debttoken_catalog_2026_07_13.py``, "the same
pattern for the remaining 3". **That would corrupt the catalogue.** Verified by
running that script's own pure ``migrate()`` over live prod data: it would emit
**1,766 duplicate ``instrument_id``s** (880 A_TOKEN + 880 DEBT_TOKEN pairs), because
it has no dedup and the split twins **already exist** — the 2026-07-13 run added the
A_TOKEN/DEBT_TOKEN rows but never removed its LENDING source rows. So the residual
is not "unsplit markets"; it is **already-split markets whose stale original was left
behind**.

**Measured on real prod GCS 2026-07-17** (``defi prod/catalog.parquet``, 11,776 rows):

* Residual ``LENDING``: **893** — MORPHO 861, COMPOUND_V3 26, FLUID 6. (The todo's
  "MORPHO/FLUID/AAVE_PLASMA" list is stale on two counts: COMPOUND_V3 is in the
  residual, and no AAVE_PLASMA venue exists in the catalogue at all. AAVE_V3 is
  already fully drained — 0 LENDING rows.)
* **All 893 are delisted/aged** (``available_to`` non-blank; 0 active) — no LIVE row
  is mislabeled, matching the 2026-07-13 rollup's documented aging behaviour.
* **Every one already has its canonical twin**: MORPHO/FLUID 867/867 have an A_TOKEN
  twin; COMPOUND_V3 26/26 have theirs.
* **The twins carry the FULL history**: 867/867 (and 26/26) twins have
  ``available_from`` EARLIER-OR-EQUAL to their LENDING original — 0 twins start
  later. Byte-identical in practice (e.g. ``MORPHO-ETHEREUM:LENDING_MARKET:cbBTC-USDT:0x2b8019``
  from 2023-12-28 vs its ``:A_TOKEN:AcbBTC-USDT:0x2b8019`` twin from 2023-12-28).

So each LENDING row is fully redundant with a twin that is at least as complete, and
is actively harmful: ``LENDING`` is **not a real ``InstrumentType`` enum member**, so
any consumer reading historical instrument identity (backtests, PnL reconciliation)
hits the same crash-risk/mislabel class the live rows already had fixed. Worse, the
pair reads as "this market delisted on 2026-06-24 and a different one listed" when in
truth only our labelling changed.

**Safety — a row is deleted ONLY if its replacement provably supersedes it.** Per row
we derive the required twin id(s) from the row's OWN key shape and require that each
EXISTS and has ``available_from <=`` the LENDING row's. Any row failing that is KEPT
and reported (never deleted on assumption) — losing real lifecycle history would be a
far worse outcome than a stale label. Backup snapshot before write; post-verify by
re-reading from GCS.

Twin derivation (mirrors the 2026-07-13 writers' own naming, verified against real
live pairs):
  ``<prefix>:LENDING_MARKET:<pair>`` -> BOTH ``<prefix>:A_TOKEN:A<pair>``
                                       AND ``<prefix>:DEBT_TOKEN:DEBT<pair>``
  ``<prefix>:SUPPLY:<sym>``          -> ``<prefix>:A_TOKEN:<sym>``
  ``<prefix>:BORROW:<sym>``          -> ``<prefix>:DEBT_TOKEN:<sym>``

Usage:
  .venv/bin/python scripts/drain_residual_lending_rows_2026_07_17.py --dry-run
  .venv/bin/python scripts/drain_residual_lending_rows_2026_07_17.py --apply --confirm
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

CATALOG_BLOB = "prod/catalog.parquet"
BACKUP_PREFIX = "prod/catalog."
BACKUP_SUFFIX = ".lendingdrain.defi.bak.parquet"

_LEGACY_MARKERS: tuple[str, ...] = (":LENDING_MARKET:", ":SUPPLY:", ":BORROW:")


def _norm(series: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    return series.astype(str).fillna("").replace({"nan": "", "None": "", "<NA>": ""})


def required_twins(instrument_id: str) -> list[str] | None:
    """Twin id(s) that must supersede ``instrument_id``, or None if its key shape is
    not a recognised legacy lending shape (=> never delete it)."""
    if ":LENDING_MARKET:" in instrument_id:
        prefix, pair = instrument_id.split(":LENDING_MARKET:", 1)
        return [f"{prefix}:A_TOKEN:A{pair}", f"{prefix}:DEBT_TOKEN:DEBT{pair}"]
    if ":SUPPLY:" in instrument_id:
        prefix, sym = instrument_id.split(":SUPPLY:", 1)
        return [f"{prefix}:A_TOKEN:{sym}"]
    if ":BORROW:" in instrument_id:
        prefix, sym = instrument_id.split(":BORROW:", 1)
        return [f"{prefix}:DEBT_TOKEN:{sym}"]
    return None


def plan_drain(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """``(kept_frame, undeletable_lending_rows, stats)``. Pure — unit-testable."""
    itype = _norm(df["instrument_type"]).str.upper()
    lending = df[itype == "LENDING"]
    stats: dict[str, int] = {
        "rows_in": len(df),
        "lending_rows": len(lending),
        "deletable": 0,
        "kept_no_twin": 0,
        "kept_twin_starts_later": 0,
        "kept_unknown_shape": 0,
    }
    if lending.empty:
        stats["rows_out"] = len(df)
        return df, lending, stats

    by_id: dict[str, pd.Series] = {}  # type: ignore[type-arg]
    for _, row in df.iterrows():
        by_id[str(row["instrument_id"])] = row

    deletable_idx: list[int] = []
    blocked_idx: list[int] = []
    for idx, row in lending.iterrows():
        iid = str(row["instrument_id"])
        twins = required_twins(iid)
        if twins is None:
            stats["kept_unknown_shape"] += 1
            blocked_idx.append(idx)  # type: ignore[arg-type]
            continue
        missing = [t for t in twins if t not in by_id]
        if missing:
            stats["kept_no_twin"] += 1
            blocked_idx.append(idx)  # type: ignore[arg-type]
            continue
        lend_from = str(row["available_from"])
        # Every required twin must cover AT LEAST the original's history.
        if any(str(by_id[t]["available_from"]) > lend_from for t in twins):
            stats["kept_twin_starts_later"] += 1
            blocked_idx.append(idx)  # type: ignore[arg-type]
            continue
        deletable_idx.append(idx)  # type: ignore[arg-type]

    stats["deletable"] = len(deletable_idx)
    kept = df.drop(index=deletable_idx)
    stats["rows_out"] = len(kept)
    return kept, df.loc[blocked_idx], stats


def _summarize(df: pd.DataFrame, label: str) -> None:
    itype = _norm(df["instrument_type"]).str.upper()
    lending = df[itype == "LENDING"]
    per_venue = lending["venue"].astype(str).value_counts().to_dict() if len(lending) else {}
    logger.info(
        "%s: rows=%d  LENDING=%d %s  A_TOKEN=%d  DEBT_TOKEN=%d  dup_ids=%d",
        label,
        len(df),
        len(lending),
        per_venue,
        int((itype == "A_TOKEN").sum()),
        int((itype == "DEBT_TOKEN").sum()),
        int(df["instrument_id"].duplicated().sum()),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Report only (default).")
    parser.add_argument("--apply", action="store_true", help="Write the drained catalogue.")
    parser.add_argument("--confirm", action="store_true", help="Required alongside --apply.")
    args = parser.parse_args()

    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    storage = get_storage_client(project_id=None)
    logger.info("Mode=%s bucket=%s blob=%s", "APPLY" if apply else "DRY-RUN", bucket, CATALOG_BLOB)

    raw = storage.download_bytes(bucket, CATALOG_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    _summarize(df, "BEFORE")

    kept, blocked, stats = plan_drain(df)
    logger.info(
        "plan: rows_in=%d lending=%d deletable=%d kept_no_twin=%d kept_twin_starts_later=%d "
        "kept_unknown_shape=%d rows_out=%d",
        stats["rows_in"],
        stats["lending_rows"],
        stats["deletable"],
        stats["kept_no_twin"],
        stats["kept_twin_starts_later"],
        stats["kept_unknown_shape"],
        stats["rows_out"],
    )
    if len(blocked):
        logger.warning("%d LENDING rows are NOT superseded and are being KEPT (history preservation):", len(blocked))
        for _, row in blocked.head(10).iterrows():
            logger.warning(
                "  KEEP: %s (from=%s to=%s)", row["instrument_id"], row["available_from"], row["available_to"]
            )

    if stats["lending_rows"] == 0:
        logger.info("Idempotent no-op — 0 residual LENDING rows.")
        return 0
    if stats["deletable"] == 0:
        logger.info("Nothing safely deletable — no write.")
        return 0

    _summarize(kept, "AFTER (planned)")
    if not apply:
        logger.info("DRY-RUN (no writes). Re-run with --apply --confirm to drain the live catalogue.")
        return 0

    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{BACKUP_PREFIX}{timestamp}{BACKUP_SUFFIX}"
    storage.upload_bytes(bucket, backup_key, raw)
    logger.info("Backup written: gs://%s/%s (%d bytes)", bucket, backup_key, len(raw))

    buf = io.BytesIO()
    kept.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage.upload_bytes(bucket, CATALOG_BLOB, buf.getvalue())
    logger.info("APPLIED — gs://%s/%s rewritten (%d -> %d rows)", bucket, CATALOG_BLOB, stats["rows_in"], len(kept))

    verify_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, CATALOG_BLOB)))
    _summarize(verify_df, "AFTER (re-read from GCS)")
    residual = int((_norm(verify_df["instrument_type"]).str.upper() == "LENDING").sum())
    expected = stats["lending_rows"] - stats["deletable"]
    if residual != expected:
        logger.error("POST-VERIFY FAILED — %d LENDING rows remain (expected %d)", residual, expected)
        return 3
    logger.info(
        "✅ POST-VERIFY: %d LENDING rows remain (expected %d); rollback: gs://%s/%s",
        residual,
        expected,
        bucket,
        backup_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
