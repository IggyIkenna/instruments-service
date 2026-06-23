#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: the live tradfi _index/availability_index.parquet has been --apply'd (out-of-floor
#              tick/OHLCV expected_unattempted reclassed to empty_confirmed; derived 15m/24h EU
#              reclassed; VIX cash-index cells out of universe; captured + attempted_failed counts
#              verified UNCHANGED) AND the Barchart/massive VIX-index GCS objects are deleted.
"""correct_tradfi_universe_floor_clip_and_vix_index.py — one-off live tradfi ``_index`` universe correction.

Operator decision 2026-06-23 (tradfi universe-correction). The live tradfi
``_index/availability_index.parquet`` over-seeded the expected universe; this
**reclasses** the dead ``expected_unattempted`` cells to ``empty_confirmed`` with a
typed ``EXPECTED_*`` reason IN PLACE (snapshot + write-back, the same row-PRESERVING
pattern as ``market_tick_data_service/scripts/populate_v9_index_columns_inplace.py``
— rows are never dropped; the manifest keeps the audit trail and the honest-absence
``capture_status`` flip is the SSOT-canonical way to remove a cell from the fetchable
denominator).

Three corrections (each only ever moves ``expected_unattempted`` → ``empty_confirmed``):

  1. **Databento rolling-history floor.** ``expected_unattempted`` cells for a
     Databento-FETCHED data_type, dated OLDER than the data_type's billing-LEVEL
     floor, are UNFETCHABLE — the subscription only includes a trailing-from-today
     window per level (L0 16y: ``ohlcv_1s``/``ohlcv_1m``; L1 1y: ``trades``/``tbbo``;
     L2 1mo: ``mbp_10`` — SSOT = UAC ``databento_subscription_allowlist``). Fetching
     such a cell would trip ``assert_lookback_allowed`` (metered-billing guard) and it
     inflates the honest-coverage denominator with cells we can never fill. Reclass to
     ``empty_confirmed[EXPECTED_OUT_OF_COVERAGE_WINDOW]`` (the data_type is still valid
     + restorable post-cutover, just OUT of the current rolling coverage window).

  2. **Derived-OHLCV bars.** ``ohlcv_15m`` / ``ohlcv_24h`` are DERIVED by downstream
     aggregation (``aggregate_from_15s_efficient`` from 1s/1m), NOT fetched from
     Databento — so a databento/massive-sourced ``expected_unattempted`` 15m/24h cell
     is a phantom *fetch* target (a derived output is produced, not fetched). Reclass
     to ``empty_confirmed[EXPECTED_OUTSIDE_PROCESSING_SCOPE]`` (the cell exists in the
     catalogue but the FETCH path explicitly skips it — it is produced by the
     aggregation processing path instead). EXCEPTION: an FX-source 24h cell rides the
     Yahoo daily path (the one kept non-databento daily fetch) — source in
     {``yahoo``, ``fx``} is NOT reclassed.

  3. **VIX cash index.** The VIX *cash index* (``instrument_type=index`` named VIX,
     sourced Barchart/massive) is removed from the universe in favour of the VIX
     *futures* (VX, ``futures_chain`` / ``future``, Databento ``XCBF.PITCH`` — they
     trade more often, over a longer window, at finer granularity; the index is
     derivable from them and is not tradable). Any VIX cash-index ``expected_unattempted``
     cell is reclassed to ``empty_confirmed[EXPECTED_DEPRECATED_DATA_TYPE]``. Scoped to
     VIX so DXY / treasury-yield cash indices (no futures equivalent) are UNAFFECTED.
     The 135 VX *futures_chain* captured cells are NOT touched. (The underlying GCS
     index objects are deleted by ``delete_vix_cash_index_gcs_objects_2026_06_23.py``.)

GATE (refuse to --apply if violated): ``captured`` count UNCHANGED and
``attempted_failed`` count UNCHANGED. This correction ONLY ever moves
``expected_unattempted`` → ``empty_confirmed`` — it never touches a ``captured`` or
``attempted_failed`` cell, and the total row count is preserved (rows are reclassed in
place, not dropped). If either count changes the script aborts before writing.

Usage::

    python scripts/correct_tradfi_universe_floor_clip_and_vix_index.py --dry-run
    python scripts/correct_tradfi_universe_floor_clip_and_vix_index.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import Counter
from datetime import UTC, date, datetime

import pandas as pd

logger = logging.getLogger(__name__)

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SNAPSHOT_BLOB = "_index/snapshots/pre_floorclip_2026_06_23.parquet"

#: Databento-FETCHED tradfi data_types that carry a rolling-history floor (LEVEL → window).
#: ohlcv_1s/1m = L0 (16y), trades/tbbo = L1 (1y), mbp_10 = L2 (1mo). The allowlist resolves the
#: level + window from the underscore manifest spelling (it normalises ``_`` → ``-`` internally).
_DATABENTO_FETCHED: frozenset[str] = frozenset({"ohlcv_1s", "ohlcv_1m", "trades", "tbbo", "mbp_10"})
#: Derived OHLCV bars — produced by aggregation, never a Databento *fetch* target.
_DERIVED_OHLCV: frozenset[str] = frozenset({"ohlcv_15m", "ohlcv_24h"})
#: Sources that legitimately FETCH a daily (24h) bar (the one kept non-databento daily path).
_DERIVED_KEEP_SOURCES: frozenset[str] = frozenset({"yahoo", "fx"})

_REASON_FLOOR = "EXPECTED_OUT_OF_COVERAGE_WINDOW"
_REASON_DERIVED = "EXPECTED_OUTSIDE_PROCESSING_SCOPE"
_REASON_VIX = "EXPECTED_DEPRECATED_DATA_TYPE"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="tick-data", asset_group="tradfi")


def _floor_for(data_type: str, today: date) -> date | None:
    """Databento rolling-history floor for a FETCHED data_type, else None (derived/refdata)."""
    if data_type not in _DATABENTO_FETCHED:
        return None
    from unified_api_contracts.registry.databento_subscription_allowlist import earliest_allowed_start

    now = datetime(today.year, today.month, today.day, tzinfo=UTC)
    return earliest_allowed_start(data_type, now=now).date()


def _is_vix_index_row(it: str, iid: str, und: str, ba: str, venue: str) -> bool:
    """VIX cash-index discriminator: instrument_type==index AND VIX-named (id/underlying/base),
    OR a blank-id CBOE index legacy cell (CBOE's only cash index is VIX). A future named
    DXY/treasury index carries its own non-VIX id and is NOT matched."""
    if it.strip().lower() != "index":
        return False
    if any("VIX" in str(tok).upper() for tok in (iid, und, ba)):
        return True
    return venue.strip().upper() == "CBOE" and not str(iid).strip() and not str(und).strip() and not str(ba).strip()


def classify(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return EU-only boolean masks (eu_floor, derived_eu, vix_index_eu) — disjoint, EU-scoped."""
    cs = df["capture_status"].fillna("").astype(str)
    dt = df["data_type"].fillna("").astype(str)
    it = df["instrument_type"].fillna("").astype(str)
    iid = df["instrument_id"].fillna("").astype(str)
    und = df["underlying"].fillna("").astype(str)
    ba = (
        df["base_asset"].fillna("").astype(str)
        if "base_asset" in df.columns
        else pd.Series([""] * len(df), index=df.index)
    )
    ven = df["venue"].fillna("").astype(str)
    src = df["source"].fillna("").astype(str).str.lower()
    d = pd.to_datetime(df["date"], errors="coerce").dt.date
    d_ser = pd.Series(d, index=df.index)

    is_eu = cs == "expected_unattempted"
    today = datetime.now(UTC).date()
    dts: list[str] = [str(x) for x in dt.unique()]
    floors = {x: _floor_for(x, today) for x in dts}

    # 1. Floor drop: fetched data_type, EU, dated older than its rolling-history floor.
    older = pd.Series(False, index=df.index)
    for data_type, floor in floors.items():
        if floor is None:
            continue
        older = older | ((dt == data_type) & d_ser.notna() & (d_ser < floor))
    eu_floor = is_eu & older

    # 2. Derived EU: 15m/24h EU (any non-Yahoo/FX source) — a phantom fetch target.
    derived_eu = is_eu & dt.isin(_DERIVED_OHLCV) & (~src.isin(_DERIVED_KEEP_SOURCES))

    # 3. VIX cash-index EU: instrument_type==index named VIX (or blank-id CBOE index).
    vix_index_eu = is_eu & pd.Series(
        [_is_vix_index_row(a, b, c, e, v) for a, b, c, e, v in zip(it, iid, und, ba, ven, strict=True)],
        index=df.index,
    )

    # disjointness: floor takes precedence, then derived, then vix.
    derived_eu = derived_eu & ~eu_floor
    vix_index_eu = vix_index_eu & ~eu_floor & ~derived_eu
    return eu_floor, derived_eu, vix_index_eu


def _counts(df: pd.DataFrame) -> dict[str, int]:
    return {k: int(v) for k, v in Counter(df["capture_status"].fillna("").astype(str)).items()}


def run(*, apply: bool) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    st = get_storage_client()
    logger.info("tradfi universe-correction: bucket=%s blob=%s apply=%s", bucket, _MANIFEST_BLOB, apply)
    raw = st.download_bytes(bucket, _MANIFEST_BLOB)
    before = pd.read_parquet(io.BytesIO(raw))
    for c in (
        "data_type",
        "instrument_type",
        "instrument_id",
        "underlying",
        "source",
        "capture_status",
        "date",
        "error_reason",
        "venue",
    ):
        if c not in before.columns:
            before[c] = ""
    n0 = len(before)
    cs0 = _counts(before)
    logger.info("BEFORE: rows=%d capture_status=%s", n0, cs0)

    eu_floor, derived_eu, vix_eu = classify(before)
    after = before.copy()
    # reclass IN PLACE — capture_status -> empty_confirmed + typed error_reason. Rows preserved.
    after.loc[eu_floor, "capture_status"] = "empty_confirmed"
    after.loc[eu_floor, "error_reason"] = _REASON_FLOOR
    after.loc[derived_eu, "capture_status"] = "empty_confirmed"
    after.loc[derived_eu, "error_reason"] = _REASON_DERIVED
    after.loc[vix_eu, "capture_status"] = "empty_confirmed"
    after.loc[vix_eu, "error_reason"] = _REASON_VIX

    dt = before["data_type"].fillna("").astype(str)
    logger.info(
        "RECLASS EU->empty_confirmed: floor_clip=%d derived_15m_24h=%d vix_cash_index=%d (total=%d)",
        int(eu_floor.sum()),
        int(derived_eu.sum()),
        int(vix_eu.sum()),
        int((eu_floor | derived_eu | vix_eu).sum()),
    )
    logger.info("  floor_clip by data_type: %s", dict(Counter(dt[eu_floor])))
    logger.info("  derived by data_type:    %s", dict(Counter(dt[derived_eu])))
    cs1 = _counts(after)
    logger.info("AFTER:  rows=%d capture_status=%s", len(after), cs1)
    eu_b, eu_a = cs0.get("expected_unattempted", 0), cs1.get("expected_unattempted", 0)
    logger.info("EU universe: %d -> %d (reclassed %d)", eu_b, eu_a, eu_b - eu_a)

    # GATE: captured + attempted_failed UNCHANGED; total rows UNCHANGED.
    cap_delta = cs1.get("captured", 0) - cs0.get("captured", 0)
    af_delta = cs1.get("attempted_failed", 0) - cs0.get("attempted_failed", 0)
    rows_ok = len(after) == n0
    gate_ok = cap_delta == 0 and af_delta == 0 and rows_ok
    logger.info(
        "GATE: captured %d->%d (delta=%d) | attempted_failed %d->%d (delta=%d) | rows %d->%d => %s",
        cs0.get("captured", 0),
        cs1.get("captured", 0),
        cap_delta,
        cs0.get("attempted_failed", 0),
        cs1.get("attempted_failed", 0),
        af_delta,
        n0,
        len(after),
        "OK" if gate_ok else "VIOLATION",
    )
    if not gate_ok:
        logger.error(
            "GATE FAILED — captured/attempted_failed/row-count changed beyond the sanctioned EU->empty reclass."
        )
        return 2

    if not apply:
        logger.info("DRY RUN — live _index NOT modified. Re-run with --apply to write.")
        return 0

    if not st.blob_exists(bucket, _SNAPSHOT_BLOB):
        st.upload_bytes(bucket, _SNAPSHOT_BLOB, raw)
        logger.info("snapshot written: gs://%s/%s", bucket, _SNAPSHOT_BLOB)
    else:
        logger.info("snapshot already exists: gs://%s/%s (kept)", bucket, _SNAPSHOT_BLOB)

    buf = io.BytesIO()
    after.to_parquet(buf, index=False)
    st.upload_bytes(bucket, _MANIFEST_BLOB, buf.getvalue())
    logger.info("APPLIED — live _index written: gs://%s/%s (%d rows)", bucket, _MANIFEST_BLOB, len(after))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    args = p.parse_args(argv)
    return run(apply=args.apply)


if __name__ == "__main__":
    sys.exit(main())
