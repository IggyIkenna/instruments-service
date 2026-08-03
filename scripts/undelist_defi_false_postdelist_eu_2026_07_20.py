#!/usr/bin/env python3
# Epic: defi_consolidated_closeout
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 false EXPECTED_INSTRUMENT_DELISTED rows on now-live instruments + honest_cov re-measured
"""Un-delist defi manifest rows falsely stamped EXPECTED_INSTRUMENT_DELISTED.

The EXACT INVERSE of ``reclassify_defi_postdelist_eu_2026_06_24.py``. That script flipped
EU rows past their catalogue ``available_to`` → ``empty_confirmed(EXPECTED_INSTRUMENT_DELISTED)``.
After the ``available_to`` false-delisting fix (``instruments-service@c37d4f96``: DeFi drop-outs
never last-seen-delist) + a ``--mode full`` catalogue regen, those instruments are once again
catalogue-LIVE (``available_to`` reset to null), so their ``EXPECTED_INSTRUMENT_DELISTED`` cells
are now FALSE. This resets them to pending ``expected_unattempted`` (blank ``error_reason`` →
NOT skip-worthy per ``manifest_freshness``), so a re-capture pass re-attempts them and resolves
each to ``captured`` / ``EXPECTED_NOT_ENOUGH_TVL``.

INSTRUMENT_TYPE-AGNOSTIC on purpose: the false-delisting clusters are overwhelmingly non-POOL
(SPOT_ASSET / LENDING / A_TOKEN / DEBT_TOKEN), which the POOL-only ``validate_defi_no_delisted_on_live_pool``
gate does not cover. Selection is purely "manifest says DELISTED **and** the current catalogue
says the instrument is LIVE on that cell's date" — genuine truth-gated delistings (catalogue
``available_to`` still set, which the c37d4f96 fix preserves for real ``delisted_at``/``expiry``)
have ``date > available_to`` → NOT live → correctly LEFT delisted.

Mirrors ``reclassify``'s blob set (consolidated ``_index/availability_index.parquet`` +
``_index/per_vm/*.parquet``), backup-then-write, idempotent, ``--dry-run``/``--apply``.

Usage:
  python scripts/undelist_defi_false_postdelist_eu_2026_07_20.py --dry-run
  python scripts/undelist_defi_false_postdelist_eu_2026_07_20.py --apply

Order (load-bearing): (1) ``--mode full`` defi catalogue regen (available_to→null) FIRST;
(2) THIS un-delist; (3) re-capture the affected cells; (4) validate_defi_no_delisted_on_live_pool.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import EmptyConfirmedReason
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_DELISTED = EmptyConfirmedReason.EXPECTED_INSTRUMENT_DELISTED.value


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.undelist.bak.parquet"
    return f"{blob_name}.{run_ts}.undelist.bak"


def build_lifecycle_map(catalogue: pd.DataFrame) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    """``{key_lower: (available_from_ts, available_to_ts_or_NaT)}`` keyed by BOTH ``instrument_id``
    and ``raw_symbol`` (the manifest is 0x-keyed). ``available_to`` NaT = still live (open).

    When the SAME key resolves to multiple catalogue rows, keep the WIDEST live window
    (earliest ``available_from``, and an open/NaT ``available_to`` wins over any closed one) —
    conservative toward "live", so we never LEAVE a falsely-delisted cell stamped.
    """
    lmap: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    if catalogue.empty or "available_from" not in catalogue.columns:
        return lmap
    af = pd.to_datetime(catalogue["available_from"], errors="coerce")
    at = (
        pd.to_datetime(catalogue["available_to"], errors="coerce")
        if "available_to" in catalogue.columns
        else pd.Series(pd.NaT, index=catalogue.index)
    )
    for i in catalogue.index:
        af_ts = af[i]
        at_ts = at[i]
        for col in ("instrument_id", "raw_symbol"):
            v = str(catalogue.at[i, col] if col in catalogue.columns else "").strip().lower()
            if not v:
                continue
            prev = lmap.get(v)
            if prev is None:
                lmap[v] = (af_ts, at_ts)
                continue
            prev_af, prev_at = prev
            # earliest available_from
            new_af = af_ts if (pd.notna(af_ts) and (pd.isna(prev_af) or af_ts < prev_af)) else prev_af
            # open (NaT) available_to wins; else the LATEST close (widest window)
            new_at = pd.NaT if pd.isna(prev_at) or pd.isna(at_ts) else (at_ts if at_ts > prev_at else prev_at)
            lmap[v] = (new_af, new_at)
    return lmap


def _expected_true_value(df: pd.DataFrame) -> object:
    """The frame's OWN encoding of a truthy ``expected``.

    Measured on the live defi ``_index`` (2026-07-20): parquet type ``string``, values
    ``'true'`` / ``'false'`` / null — NOT booleans. Deriving the literal from an existing
    ``expected_unattempted`` row (falling back to any truthy value, then to ``'true'``)
    keeps the write type-compatible even if the encoding ever changes, instead of
    hard-coding an assumption that blows up at ``to_parquet`` time."""
    if "expected" not in df.columns:
        return "true"
    col = df["expected"]
    if "capture_status" in df.columns:
        eu = col[df["capture_status"].astype(str) == "expected_unattempted"].dropna()
        if not eu.empty:
            return eu.iloc[0]
    nn = col.dropna()
    for v in nn.head(200):
        if str(v).strip().lower() in {"true", "1", "t", "yes"}:
            return v
    return "true"


def undelist_frame(
    df: pd.DataFrame, lmap: dict[str, tuple[pd.Timestamp, pd.Timestamp]]
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Reset FALSE ``empty_confirmed(EXPECTED_INSTRUMENT_DELISTED)`` rows → pending EU. Pure + idempotent.

    A row is un-delisted iff it is DELISTED **and** the catalogue now says the instrument is
    LIVE on that cell's date: ``available_from <= date`` AND (``available_to`` is NaT OR
    ``date <= available_to``). This is the exact live-window ``reclassify``/the validator use,
    so genuine (still-``available_to``-closed) delistings are LEFT untouched.
    """
    stats = {"undelisted": 0, "untouched": 0}
    needed = ("capture_status", "instrument_id", "date")
    if df.empty or not all(c in df.columns for c in needed):
        stats["untouched"] = len(df)
        return df, stats
    is_delisted = df["capture_status"].astype(str) == "empty_confirmed"
    if "error_reason" in df.columns:
        is_delisted &= df["error_reason"].astype(str) == _DELISTED
    iid = df["instrument_id"].astype(str).str.lower()
    af = iid.map(lambda k: lmap.get(k, (pd.NaT, pd.NaT))[0])
    at = iid.map(lambda k: lmap.get(k, (pd.NaT, pd.NaT))[1])
    d = pd.to_datetime(df["date"], errors="coerce")
    listed = af.notna() & (d >= af)
    within = at.isna() | (d <= at)
    live_now = listed & within
    flip = is_delisted & live_now
    n = int(flip.sum())
    if n == 0:
        stats["untouched"] = len(df)
        return df, stats
    work = df.copy()
    work.loc[flip, "capture_status"] = "expected_unattempted"
    if "error_reason" in work.columns:
        work.loc[flip, "error_reason"] = ""
    if "expected" in work.columns:
        # ``expected`` is a STRING column in the manifest (parquet type=string, values
        # 'true'/'false'/None) — NOT a bool. Writing a Python bool raises
        # ``ArrowTypeError: Expected bytes, got a 'bool' object`` at to_parquet time
        # (hit for real on the 2026-07-20 apply; the backup-then-write ordering meant the
        # live _index was never overwritten). Write the value in the column's OWN encoding,
        # derived from the frame so we can never re-introduce a type mismatch.
        work.loc[flip, "expected"] = _expected_true_value(df)
    stats["undelisted"] = n
    return work, stats


def _process_blob(
    storage: object,
    bucket: str,
    blob_name: str,
    run_ts: str,
    lmap: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
    *,
    apply: bool,
) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = undelist_frame(df, lmap)
    if stats["undelisted"] == 0:
        return stats
    logger.info("  %s: un-delisted %d false EXPECTED_INSTRUMENT_DELISTED → pending EU", blob_name, stats["undelisted"])
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAttributeAccessIssue]
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        storage.upload_bytes(bucket, blob_name, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(out))
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply = bool(args.apply)

    bucket = _defi_bucket()
    storage = get_storage_client(project_id=None)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    cat_raw = storage.download_bytes(_catalogue_bucket(), "prod/catalog.parquet")  # pyright: ignore[reportAttributeAccessIssue]
    lmap = build_lifecycle_map(pd.read_parquet(io.BytesIO(cat_raw)))
    logger.info("Catalogue lifecycle map: %d keys; mode=%s", len(lmap), "APPLY" if apply else "DRY-RUN")

    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    total = 0
    for blob_name in blobs:
        total += _process_blob(storage, bucket, blob_name, run_ts, lmap, apply=apply).get("undelisted", 0)
    logger.info(
        "TOTAL un-delisted %d false EXPECTED_INSTRUMENT_DELISTED (mode=%s)", total, "APPLY" if apply else "DRY-RUN"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
