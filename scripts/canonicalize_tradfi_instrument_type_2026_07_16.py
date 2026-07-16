#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod tradfi `_index/availability_index.parquet` has 0 blank-`instrument_type`
#   rows with capture_status=captured (verified via --dry-run reporting 0 targets).
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false, reportArgumentType=false
# (one-shot migration over the untyped pandas / StorageClient row-dict surface — same pragma
#  set as the sibling migrate_instruments_store_v9.py / canonicalize_*_catalog_2026_07_*.py tools.)
"""canonicalize_tradfi_instrument_type_2026_07_16.py — one-off manifest-index instrument_type backfill.

Plan: ``unified-trading-pm/plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md`` P9 item 7
("TradFi instrument_type: migrate/stamp the 46.5M blank (__legacy__) rows to their canonical InstrumentType").

WRITER STATUS — already fixed, no code change shipped by this migration. Git archaeology (2026-07-16) traced
the root cause to the shared cefi/tradfi/defi manifest writer
(``instruments_service/engine/orchestrator/writers.py::_write_venue``), which hardcoded
``instrument_type=""`` on every ``manifest.record_captured(...)`` call regardless of the underlying
DataFrame's own (always-populated, since this repo's initial commit) ``instrument_type`` column — a pure
manifest-STAMPING bug, not a data-capture bug: the per-day-per-venue ``instruments.parquet`` objects
themselves have always carried the real per-record type. Two commits already fixed this on the live branch
before this migration ran:
  * ``b475ae8e`` (2026-06-17) — ``_derive_instrument_type()``: stamps the real type when a venue x date
    shard is single-type, "" when mixed/absent.
  * ``91fc7bd2`` (2026-07-07) — ``_split_by_instrument_type()``: supersedes the above — multi-type venues
    (CME/CBOE/ICE routinely capture FUTURE + OPTION + COMBO the same day) now emit ONE manifest row PER
    distinct type instead of collapsing to "" when mixed. This is the CURRENT code (verified by reading
    ``writers.py`` at HEAD 2026-07-16) — no further writer change is needed for tradfi.

REAL, LIVE-VERIFIED SCOPE (read directly off ``_index/availability_index.parquet`` for
``instruments-store-tradfi-prd-central-element-323112``, 2026-07-16 — always re-verify at run time, never
trust these numbers): the manifest index is only **17,083 ROWS** (one row per (date, venue[, instrument_type])
capture-shard) — NOT ~47M. The "46.5M of ~47.1M" figure quoted in the plan/mission is the SUM of each row's
``instrument_count``/``row_count`` (how many individual instrument DEFINITIONS were captured under that
shard) grouped by blank-vs-populated ``instrument_type``, not a manifest row count:
  * 15,017 of 17,083 rows carry a blank ``instrument_type``; their ``instrument_count`` sums to 46,552,151
    of the index's 47,189,078 total (98.6%) — matches the plan's "46.5M of ~47.1M" almost exactly.
  * Of those 15,017 blank rows, 10,542 are ``capture_status="captured"`` with a real backing GCS object —
    these are the rows this script derives a type for.
  * The remaining ~4,475 blank rows (82 attempted_failed + 3,883 empty_confirmed + 510 expected_unattempted)
    captured ZERO instruments and are left blank (honest — there is nothing to derive from an empty/failed
    shard).
  * Blank rows' ``written_at`` (manifest write time, NOT the ``date`` column which is the trading day —
    date range 2019-2026) spans 2026-04-03 to 2026-07-07 — i.e. they were all written by a historical
    backfill run that used the buggy pre-fix writer code; populated rows start 2026-06-18 (the day after
    the first partial fix landed).

DERIVATION (never fabricated — honest-absence rule): for each blank ``captured`` row, this script reads
THAT EXACT shard's own ``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet``
object (a targeted single-object read keyed by the manifest row's own (date, venue) — NOT a fresh
whole-corpus GCS walk/listing; single-walk discipline preserved) and re-derives ``instrument_type`` from
that object's OWN ``instrument_type`` column, using the SAME grouping logic the current writer uses
(mirrors ``writers._split_by_instrument_type``): every distinct non-blank type present in the shard's real
captured records becomes its own manifest row carrying that type's real per-type count, splitting a single
blank row into N typed rows when the shard mixes types. A shard whose object is unreadable/missing, or
whose object itself carries no non-blank ``instrument_type`` value, is left blank and counted as an
explicit residual in the run's log output — never guessed from venue name or any other heuristic (the
existing ``migrate_instruments_store_v9.py``'s venue-suffix heuristic only covers cefi ``-SPOT``/
``-FUTURES`` venue names and does not apply to tradfi venues at all).

GATE (refuse to --apply if violated): the blank-row count must not INCREASE, and not every target may go
unresolved (either would signal a real derivation bug or a systemic read failure). The manifest ROW COUNT
is expected to INCREASE by (distinct types - 1) for each split multi-type shard — that is the correct
shape (it is what the current writer would have produced had it not had this bug) — and each resolved
row's ``row_count``/``instrument_count`` is re-stamped from the REAL object it just read, which is honest
even when it differs from the shard's old (stale) manifest count.

KNOWN, PRE-EXISTING, UNRELATED FINDING (2026-07-16, live-verified — flagged for the operator, not fixed by
this script): a small number of shards' manifest ``row_count``/``instrument_count`` is stale relative to
the object's CURRENT content — e.g. tradfi CME 2026-06-28 carries a manifest count of 74,005 while the
object itself now holds only 2,826 rows (the object was legitimately overwritten by a later, narrower
capture without a manifest update). This script does NOT attempt to preserve the old, stale count — per
workspace convention ("trust the actual distribution, not the constant") it re-stamps each row from the
real object, which is strictly more correct. Every such shard is logged as a ``shard count DRIFT`` WARNING
with the before/after counts, and the aggregate is surfaced in the run's ``drift_shards``/``drift_magnitude``
stats — this is a separate, pre-existing manifest-vs-object staleness bug (likely not tradfi-specific) that
warrants its own follow-up investigation/migration, out of scope for an instrument_type-only fix.

Usage (run from the instruments-service repo root)::

    python scripts/canonicalize_tradfi_instrument_type_2026_07_16.py                 # dry-run (default)
    python scripts/canonicalize_tradfi_instrument_type_2026_07_16.py --workers 48     # dry-run, more parallel shard reads
    python scripts/canonicalize_tradfi_instrument_type_2026_07_16.py --apply          # write back (snapshots first)
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import Counter
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from unified_trading_library.cloud_interface.abstractions import StorageClient

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _shard_instrument_type_counts(storage: StorageClient, bucket: str, date: str, venue: str) -> dict[str, int] | None:
    """Read one shard's own ``instruments.parquet`` and return ``{instrument_type: count}``.

    The returned dict's values ALWAYS sum to the shard's real row count — a "" key is
    included when a (small) fraction of the shard's own records carry a genuinely blank
    ``instrument_type`` (a handful of Databento rows failed to classify at capture time,
    independent of this migration's manifest-stamping bug), so that fraction stays
    explicitly blank rather than being silently dropped (honest — never fabricated, and
    never lossy: the caller's coalesced-count GATE depends on this dict always summing to
    the shard's true total).

    Returns ``None`` when the object is missing/unreadable/unparseable, or carries a real
    row count of zero — the caller leaves the manifest row untouched (its original count
    preserved) in that case. Per-object isolation: any exception is caught and logged,
    never raised (mirrors the shard-level-failure-isolation convention used across this
    service's writers and precedent migration scripts).
    """
    path = f"instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet"
    try:
        raw = storage.download_bytes(bucket, path)
    except Exception as exc:  # broad-except-ok — per-object isolation (perf contract)
        logger.warning("shard object unreadable date=%s venue=%s: %s: %s", date, venue, type(exc).__name__, exc)
        return None
    try:
        df = pd.read_parquet(io.BytesIO(raw), columns=["instrument_type"])
    except ValueError:
        # Legacy schema missing the requested column — fall back to a full read.
        try:
            full = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:  # broad-except-ok — per-object isolation
            logger.warning("shard parse failed date=%s venue=%s: %s: %s", date, venue, type(exc).__name__, exc)
            return None
        if "instrument_type" not in full.columns:
            return None
        df = full[["instrument_type"]]
    except Exception as exc:  # broad-except-ok — per-object isolation
        logger.warning("shard parse failed date=%s venue=%s: %s: %s", date, venue, type(exc).__name__, exc)
        return None
    if df.empty:
        return None
    col = df["instrument_type"].astype("string").fillna("")
    return dict(Counter(col.tolist()))


def _coalesced_count(df: pd.DataFrame) -> pd.Series:
    """``row_count`` when >0, else ``instrument_count`` (mirrors the reader's own backfill semantics)."""
    rc = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
    ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(0)
    return rc.where(rc > 0, ic)


def derive(
    df: pd.DataFrame, *, workers: int, bucket: str, storage: StorageClient
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Pure(ish) transform — the only I/O is the per-shard read, isolated in ``_shard_instrument_type_counts``.

    Returns ``(new_index_df, stats)``. Never mutates ``df`` in place.
    """
    itype = df["instrument_type"].astype("string").fillna("")
    blank = itype.str.len() == 0
    captured = df["capture_status"].astype(str) == "captured"
    target_idx = df.index[blank & captured].tolist()

    logger.info(
        "targets: %d blank+captured rows (of %d blank overall, %d total index rows)",
        len(target_idx),
        int(blank.sum()),
        len(df),
    )

    shard_results: dict[int, dict[str, int] | None] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(
                _shard_instrument_type_counts,
                storage,
                bucket,
                str(df.at[i, "date"])[:10],
                str(df.at[i, "venue"]),
            ): i
            for i in target_idx
        }
        for done, fut in enumerate(as_completed(futs), start=1):
            i = futs[fut]
            shard_results[i] = fut.result()
            if done % 500 == 0:
                logger.info("  shards read: %d/%d", done, len(target_idx))

    new_rows: list[dict[str, object]] = []
    keep_mask = pd.Series(True, index=df.index)
    updated_in_place = 0
    split_shards = 0
    split_rows_created = 0
    unresolvable = 0
    manifest_counts = _coalesced_count(df)
    mismatch_shards = 0
    mismatch_magnitude = 0

    for i in target_idx:
        type_counts = shard_results.get(i)
        if not type_counts:
            unresolvable += 1
            continue
        derived_sum = sum(type_counts.values())
        manifest_count = int(manifest_counts.at[i])
        if derived_sum != manifest_count:
            mismatch_shards += 1
            mismatch_magnitude += manifest_count - derived_sum
            logger.warning(
                "shard count DRIFT date=%s venue=%s: manifest_count=%d real_derived_sum=%d diff=%d types=%s",
                str(df.at[i, "date"])[:10],
                str(df.at[i, "venue"]),
                manifest_count,
                derived_sum,
                manifest_count - derived_sum,
                type_counts,
            )
        keep_mask.at[i] = False
        base = df.loc[i].to_dict()
        if len(type_counts) == 1:
            updated_in_place += 1
        else:
            split_shards += 1
            split_rows_created += len(type_counts)
        for itype_val, cnt in type_counts.items():
            new_row = dict(base)
            new_row["instrument_type"] = itype_val
            new_row["row_count"] = cnt
            new_row["instrument_count"] = cnt
            new_rows.append(new_row)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        out = pd.concat([df.loc[keep_mask], new_df], ignore_index=True)
        for col in ("row_count", "instrument_count", "schema_version"):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")
    else:
        out = df.copy()

    stats: dict[str, object] = {
        "targets": len(target_idx),
        "updated_in_place": updated_in_place,
        "split_shards": split_shards,
        "split_rows_created": split_rows_created,
        "unresolvable_left_blank": unresolvable,
        "rows_before": len(df),
        "rows_after": len(out),
        "coalesced_sum_before": int(_coalesced_count(df).sum()),
        "coalesced_sum_after": int(_coalesced_count(out).sum()),
        "blank_before": int(blank.sum()),
        "blank_after": int((out["instrument_type"].astype("string").fillna("").str.len() == 0).sum()),
        "drift_shards": mismatch_shards,
        "drift_magnitude": mismatch_magnitude,
    }
    return out, stats


def _gate(stats: Mapping[str, object]) -> bool:
    """Refuse to --apply on signs of a REAL bug — never on honest manifest/object drift.

    A handful of shards' manifest ``row_count``/``instrument_count`` is provably STALE
    relative to the object's CURRENT content (confirmed live 2026-07-16: e.g. tradfi
    CME 2026-06-28 carries manifest_count=74005 while the object itself now holds only
    2,826 rows — the object was legitimately overwritten by a later, narrower capture
    without a manifest update, a pre-existing drift bug unrelated to instrument_type).
    Per workspace convention ("trust the actual distribution, not the constant"), this
    migration re-stamps each resolved row's count from the REAL object it just read —
    which is MORE correct than the stale value it replaces — so an aggregate coalesced-
    count DELTA is EXPECTED and is not, by itself, a gate failure (logged per-shard via
    "shard count DRIFT" WARNING lines + ``drift_shards``/``drift_magnitude`` in stats for
    the record). The gate instead refuses to apply on signs of an actual derivation bug:
    blank rows increasing (real data loss) or every single captured target going
    unresolved (a systemic read failure, not scattered honest absence).
    """
    blank_regressed = int(stats["blank_after"]) > int(stats["blank_before"])
    all_unresolved = int(stats["targets"]) > 0 and int(stats["unresolvable_left_blank"]) == int(stats["targets"])
    return not blank_regressed and not all_unresolved


def run(*, apply: bool, workers: int) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    storage = get_storage_client()
    logger.info(
        "tradfi instrument_type canonicalization: bucket=%s blob=%s apply=%s workers=%d",
        bucket,
        _INDEX_BLOB,
        apply,
        workers,
    )

    raw_bytes = storage.download_bytes(bucket, _INDEX_BLOB)
    before = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("BEFORE: rows=%d", len(before))

    out, stats = derive(before, workers=workers, bucket=bucket, storage=storage)
    logger.info("=== derivation stats === %s", stats)

    gate_ok = _gate(stats)
    logger.info("GATE (no blank-count regression, not all-unresolved): %s", "OK" if gate_ok else "VIOLATION")
    if stats["drift_shards"]:
        logger.warning(
            "honest manifest/object count DRIFT found on %s shards (pre-existing, unrelated to instrument_type — "
            "see 'shard count DRIFT' lines above): coalesced_sum_before=%s coalesced_sum_after=%s "
            "net_drift=%s — NOT a gate failure (re-stamping from the REAL object is correct; see docstring).",
            stats["drift_shards"],
            stats["coalesced_sum_before"],
            stats["coalesced_sum_after"],
            stats["drift_magnitude"],
        )
    if not gate_ok:
        logger.error(
            "GATE FAILED — blank_before=%s blank_after=%s unresolvable=%s/%s — aborting before write.",
            stats["blank_before"],
            stats["blank_after"],
            stats["unresolvable_left_blank"],
            stats["targets"],
        )
        return 2

    if not apply:
        logger.info("DRY RUN — live index NOT modified. Re-run with --apply to write.")
        return 0

    stamp = _utc_stamp()
    snap_blob = f"_index/snapshots/pre_tradfi_instrument_type_canon_2026_07_16_{stamp}.parquet"
    storage.upload_bytes(bucket, snap_blob, raw_bytes)
    logger.info("snapshot written: gs://%s/%s", bucket, snap_blob)

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, _INDEX_BLOB, buf.getvalue())
    logger.info("APPLIED — live index written: gs://%s/%s (%d rows)", bucket, _INDEX_BLOB, len(out))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    p.add_argument("--workers", type=int, default=32, help="Parallel per-shard GCS reads (default: 32)")
    args = p.parse_args(argv)
    return run(apply=args.apply, workers=args.workers)


if __name__ == "__main__":
    sys.exit(main())
