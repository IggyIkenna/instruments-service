#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: the live tradfi _index/availability_index.parquet has been --apply'd (EU floor-clipped,
#              VIX cash-index cells removed, derived-databento 15m/24h EU removed; verified captured +
#              attempted_failed UNCHANGED) AND the Barchart/massive VIX-index GCS objects are deleted.
"""correct_tradfi_universe_floor_clip_and_vix_index.py — one-off live tradfi ``_index`` universe correction.

Operator decision 2026-06-23 (tradfi universe-correction). The live tradfi
``_index/availability_index.parquet`` over-seeded the expected universe three ways;
this removes the dead cells IN PLACE (snapshot + write-back, the same row-preserving
pattern as ``market_tick_data_service/scripts/populate_v9_index_columns_inplace.py``):

  1. **Databento rolling-history floor.** ``expected_unattempted`` cells for a
     Databento-FETCHED data_type (``ohlcv_1s`` / ``ohlcv_1m``, L0 / 16-year window)
     dated OLDER than the level's floor are UNFETCHABLE — the subscription only
     includes trailing-from-today history, and a backfill of such a cell would trip
     ``assert_lookback_allowed`` (metered-billing guard). They inflate the
     honest-coverage denominator with cells we can never fill. The floor is the UAC
     SSOT ``databento_subscription_allowlist.earliest_allowed_start``.

  2. **VIX cash index.** The VIX *cash index* (``instrument_type=index`` whose
     instrument_id / underlying names VIX, sourced Barchart/massive) is DELETED
     entirely — keep the VIX *futures* (VX, ``futures_chain``/``future``, Databento)
     instead: the futures trade more often, over a longer window, at finer
     granularity, the index is derivable from them and is not tradable. Every VIX
     cash-index cell (any capture_status) is removed. Scoped to VIX so DXY / US
     treasury-yield cash indices (no futures equivalent) are UNAFFECTED.

  3. **Derived-databento EU.** ``ohlcv_15m`` / ``ohlcv_24h`` are DERIVED by
     downstream aggregation (``aggregate_from_15s_efficient``), NOT fetched — so a
     databento-SOURCED ``expected_unattempted`` 15m/24h cell is a phantom fetch
     target. Remove those (the FX-24h-via-Yahoo path is source=``yahoo``/``massive``,
     untouched).

GATE (refuse to --apply if violated): ``captured`` count UNCHANGED and
``attempted_failed`` count UNCHANGED — this script only ever REMOVES
``expected_unattempted`` cells plus the dead VIX cash-index cells (whose captured
rows are the Barchart-index series the operator is deleting from GCS in the same
correction). The captured rows that survive deletion are the VIX-INDEX captured
cells; the operator deletes the underlying GCS objects in a paired step, so this
GATE treats the VIX-index captured removal as the ONLY sanctioned captured-count
delta and reports it explicitly (``captured`` may DROP by exactly the VIX-index
captured count, never by anything else; ``attempted_failed`` likewise only by the
VIX-index attempted_failed count).

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
_SNAPSHOT_BLOB = "_index/snapshots/pre_universe_floor_clip_2026_06_23.parquet"

#: Databento-FETCHED tradfi data_types (floor-clipped). 15m/24h are DERIVED, FX 24h is Yahoo.
_DATABENTO_FETCHED: frozenset[str] = frozenset({"ohlcv_1s", "ohlcv_1m"})
#: Derived OHLCV bars — never a Databento fetch target.
_DERIVED_OHLCV: frozenset[str] = frozenset({"ohlcv_15m", "ohlcv_24h"})


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="tick-data", asset_group="tradfi")


def _blank(v: object) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "nan", "none")


def _floor_for(data_type: str, today: date) -> date | None:
    """Databento rolling-history floor for a FETCHED data_type, else None (derived/Yahoo)."""
    if data_type not in _DATABENTO_FETCHED:
        return None
    from unified_api_contracts.registry.databento_subscription_allowlist import earliest_allowed_start

    now = datetime(today.year, today.month, today.day, tzinfo=UTC)
    return earliest_allowed_start(data_type, now=now).date()


def _is_vix_index_row(it: str, iid: str, und: str, venue: str) -> bool:
    """VIX cash-index discriminator: instrument_type==index AND (VIX-named, OR a
    blank-id CBOE index cell — CBOE's only cash index is VIX; the legacy unstamped
    rows). A future named DXY/treasury index carries its own non-VIX id and is NOT
    matched, so those daily series are unaffected.
    """
    if it.strip().lower() != "index":
        return False
    if any("VIX" in str(tok).upper() for tok in (iid, und)):
        return True
    return venue.strip().upper() == "CBOE" and not str(iid).strip() and not str(und).strip()


def classify(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return boolean masks (eu_floor_drop, vix_index_drop, derived_db_eu_drop)."""
    dt = df["data_type"].fillna("").astype(str)
    it = df["instrument_type"].fillna("").astype(str)
    iid = df["instrument_id"].fillna("").astype(str)
    und = df["underlying"].fillna("").astype(str)
    ven = df["venue"].fillna("").astype(str)
    src = df["source"].fillna("").astype(str).str.lower()
    cs = df["capture_status"].fillna("").astype(str)
    d = pd.to_datetime(df["date"], errors="coerce").dt.date

    today = datetime.now(UTC).date()
    floors = {x: _floor_for(x, today) for x in dt.unique()}

    is_eu = cs == "expected_unattempted"
    # 1. EU floor drop: databento-fetched data_type, EU, dated older than its floor.
    older = pd.Series(False, index=df.index)
    for data_type, floor in floors.items():
        if floor is None:
            continue
        older = older | ((dt == data_type) & d.notna() & (pd.Series(d, index=df.index) < floor))
    eu_floor_drop = is_eu & older

    # 2. VIX cash-index drop: every cell (any status), instrument_type==index named VIX
    #    (or a blank-id CBOE index legacy cell).
    vix_index_drop = pd.Series(
        [_is_vix_index_row(a, b, c, v) for a, b, c, v in zip(it, iid, und, ven, strict=True)], index=df.index
    )

    # 3. Derived-databento EU drop: 15m/24h EU sourced databento (phantom fetch target).
    derived_db_eu_drop = is_eu & dt.isin(_DERIVED_OHLCV) & (src == "databento")

    return eu_floor_drop, vix_index_drop, derived_db_eu_drop


def _counts(df: pd.DataFrame) -> dict[str, int]:
    return {k: int(v) for k, v in Counter(df["capture_status"].fillna("").astype(str)).items()}


def run(*, apply: bool) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    st = get_storage_client()
    logger.info("tradfi universe-correction: bucket=%s blob=%s apply=%s", bucket, _MANIFEST_BLOB, apply)
    raw = st.download_bytes(bucket, _MANIFEST_BLOB)
    before = pd.read_parquet(io.BytesIO(raw))
    for c in ("data_type", "instrument_type", "instrument_id", "underlying", "source", "capture_status", "date"):
        if c not in before.columns:
            before[c] = ""
    n0 = len(before)
    cs0 = _counts(before)
    logger.info("BEFORE: rows=%d capture_status=%s", n0, cs0)

    eu_floor, vix_idx, derived_db = classify(before)
    drop_mask = eu_floor | vix_idx | derived_db
    after = before[~drop_mask].copy()

    # per-category breakdown
    vix_idx_cap = int((vix_idx & (before["capture_status"] == "captured")).sum())
    vix_idx_af = int((vix_idx & (before["capture_status"] == "attempted_failed")).sum())
    logger.info(
        "DROP: eu_floor=%d vix_cash_index=%d derived_databento_eu=%d (overlap-collapsed total=%d)",
        int(eu_floor.sum()),
        int(vix_idx.sum()),
        int(derived_db.sum()),
        int(drop_mask.sum()),
    )
    logger.info("  vix_cash_index breakdown: captured=%d attempted_failed=%d", vix_idx_cap, vix_idx_af)
    cs1 = _counts(after)
    logger.info("AFTER: rows=%d capture_status=%s", len(after), cs1)
    eu_before, eu_after = cs0.get("expected_unattempted", 0), cs1.get("expected_unattempted", 0)
    logger.info("EU universe: %d -> %d (removed %d)", eu_before, eu_after, eu_before - eu_after)

    # GATE: captured may only drop by the VIX-index captured count; attempted_failed only by VIX-index af.
    cap_delta = cs0.get("captured", 0) - cs1.get("captured", 0)
    af_delta = cs0.get("attempted_failed", 0) - cs1.get("attempted_failed", 0)
    gate_ok = (cap_delta == vix_idx_cap) and (af_delta == vix_idx_af)
    logger.info(
        "GATE: captured %d->%d (delta=%d, sanctioned vix-index=%d => %s); "
        "attempted_failed %d->%d (delta=%d, sanctioned=%d => %s)",
        cs0.get("captured", 0),
        cs1.get("captured", 0),
        cap_delta,
        vix_idx_cap,
        "OK" if cap_delta == vix_idx_cap else "VIOLATION",
        cs0.get("attempted_failed", 0),
        cs1.get("attempted_failed", 0),
        af_delta,
        vix_idx_af,
        "OK" if af_delta == vix_idx_af else "VIOLATION",
    )
    if not gate_ok:
        logger.error("GATE FAILED — captured/attempted_failed changed beyond the sanctioned VIX-index removal.")
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
