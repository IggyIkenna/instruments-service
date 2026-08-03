#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod cefi AND defi `_index/availability_index.parquet` both have 0 blank-`instrument_type`
#   rows with capture_status=captured (verified via --dry-run reporting 0 targets for both asset groups).
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false, reportArgumentType=false
# (one-shot migration over the untyped pandas / StorageClient row-dict surface — same pragma
#  set as the sibling migrate_instruments_store_v9.py / canonicalize_*_2026_07_*.py tools.)
"""canonicalize_cefi_defi_instrument_type_2026_07_17.py — cefi+defi manifest-index instrument_type backfill.

Plan: ``unified-trading-pm/plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md`` —
the cefi/defi counterpart of the completed tradfi migration
(``scripts/canonicalize_tradfi_instrument_type_2026_07_16.py``).

WRITER STATUS — already fixed, no code change shipped by this migration. The shared cefi/tradfi/defi
manifest writer (``instruments_service/engine/orchestrator/writers.py::_write_venue``) historically
hardcoded ``instrument_type=""`` on every ``manifest.record_captured(...)`` call regardless of the
underlying DataFrame's own always-populated ``instrument_type`` column. It was fixed by ``b475ae8e``
(2026-06-17) and superseded by ``91fc7bd2`` (2026-07-07, ``_split_by_instrument_type`` — one manifest row
PER distinct type). Only the tradfi BACKFILL ever ran; cefi + defi still carry the blank rows this script
resolves.

LIVE-VERIFIED SCOPE (read off prod ``_index/availability_index.parquet``, 2026-07-17 — always re-verify at
run time, never trust these numbers):
  * cefi: 79,943 index rows; 13,046 blank+``captured``; 31,884 blank overall.
  * defi: 175,172 index rows; 65,443 blank+``captured``; 108,324 blank overall.
  * Blank rows that are NOT ``captured`` (empty_confirmed / expected_unattempted / attempted_failed)
    captured ZERO instruments and are HONESTLY blank — this script never touches them.

=============================================================================================
TWO CEFI/DEFI-SPECIFIC HAZARDS THAT DO NOT EXIST IN TRADFI — why this is not a port of that script
=============================================================================================

**(1) The object path is DUAL-SHAPED, and the legacy shape is STALE.** Live probe (2026-07-17) shows every
asset group carries BOTH:
  * CANONICAL (what the current writer writes, via the source-aware pipeline_mode partition):
    ``instrument_availability/by_date/day={D}/pipeline_mode={PM}/asset_group={AG}/venue={V}/instruments.parquet``
  * LEGACY:
    ``instrument_availability/by_date/day={D}/venue={V}/instruments.parquet``
The tradfi script reads ONLY the legacy path. For cefi that path is provably STALE: on a 150-row live
sample it existed for just 8 targets and was SMALLER than the canonical object in 8/8 cases — e.g. cefi
DERIBIT 2019-03-30 canonical=295 rows (OPTION 289/FUTURE 4/PERPETUAL 2) vs legacy=6 rows (FUTURE 4/
PERPETUAL 2): the legacy object is missing all 289 OPTIONs. Deriving from it would have silently deleted
289 instruments' worth of manifest count on that one shard.
So this script reads the CANONICAL path FIRST and only falls back to the legacy path when the canonical
object does not exist (measured: defi genuinely needs that fallback — canonical existed for 67/120 sampled
genuine targets while legacy existed for 99/120, and the two AGREE where both exist, 127/143). The legacy
object is therefore never consulted for a shard whose canonical object exists — which is precisely the
tradfi bug (see the FINDING note at the bottom of this docstring).

**(2) Most blank cefi/defi rows are SUPERSEDED GHOSTS, not backfill targets.** Because ``instrument_type``
is part of the manifest ``row_key``, a shard re-captured by the FIXED writer emits a NEW typed row while
the old blank row survives forever alongside it at its own row_key. Live-measured on the same shard atom
``(date, venue, chain)``:
  * cefi: 7,055 of 13,046 blank+captured rows ALREADY have a typed sibling → only 5,991 are genuine.
  * defi: 58,329 of 65,443 already have a typed sibling → only 7,114 are genuine.
E.g. cefi UPBIT 2021-04-23 carries BOTH a blank row (row_count=7, written_at 2026-05-04, buggy writer) and
a ``SPOT_PAIR`` row (row_count=133, written_at 2026-06-26, fixed writer); the object holds 133 SPOT_PAIR
rows whose ``available_at`` matches the typed row exactly. Backfilling the blank row would MINT A DUPLICATE
of an already-correct row and double-count coverage. tradfi never hit this (its pre-migration snapshot
shows the blank rows had effectively no typed siblings), which is why the tradfi script has no such guard.
This script is therefore COLLISION-AWARE: a derived type that already has a row on that shard atom is
skipped (the existing fixed-writer row wins, untouched), and the resolved blank row is DROPPED rather than
duplicated. A derived type with no existing row is emitted as a new typed row — so a ghost whose object
carries a type its siblings do not cover still contributes that type instead of being silently lost.

DERIVATION (never fabricated — honest-absence rule): for each blank ``captured`` row, a TARGETED
single-object read of THAT shard's own ``instruments.parquet``, keyed by the manifest row's own
(date, venue, chain) — NOT a fresh whole-corpus GCS walk/listing (single-walk discipline preserved) —
re-derives ``instrument_type`` from the object's OWN column, mirroring ``writers._split_by_instrument_type``
(including its ``_LEGACY_INSTRUMENT_TYPE_ALIASES`` canonicalisation, so a legacy-lowercase value in an old
object can never mint a new permanently-duplicated row_key). PATH VENUE: DeFi manifest rows split the glued
orchestrator venue into ``venue=PROTOCOL`` + ``chain=CHAIN`` while the OBJECT PATH keeps the glued
``PROTOCOL-CHAIN`` form, so the path venue is re-glued as ``f"{venue}-{chain}"`` when ``chain`` is non-empty
(a no-op for cefi, whose rows carry ``chain=""`` — including the on-chain perp CLOBs LIGHTER-ZKSYNC /
EXTENDED-STARKNET, which are cefi venues whose glued name IS the manifest venue).
A shard whose object is missing/unreadable/empty, or carries no ``instrument_type`` column, is LEFT BLANK
and counted as an explicit residual — never guessed from the venue name or any other heuristic.

GATES (refuse to --apply if violated):
  * ``row_key`` UNIQUENESS — ``(date, venue, chain, instrument_type, data_type, pipeline_mode)`` must stay
    unique. Live-verified as a true invariant pre-migration (0 duplicates in BOTH cefi and defi), so this
    gate makes the ghost-duplication hazard (2) structurally impossible rather than merely handled.
  * blank rows must not INCREASE (real data loss).
  * not EVERY target may go unresolved (a systemic read failure, vs. scattered honest absence).
Rows are otherwise copied VERBATIM (only ``instrument_type``/``row_count``/``instrument_count`` are
written), which is the condition the cross-service shard-atom review depends on: every other axis
(``capture_status``/``attempted_at``/``written_at``/``pipeline_mode``/``asset_group``/``data_type``) is
invariant across split rows, so IS-index consumers that filter without ``instrument_type`` are unaffected.

FINDING — THE COMPLETED TRADFI MIGRATION CORRUPTED ITS OWN INDEX (surfaced by this work, NOT fixed here;
reported to the operator; repair is its own plan item):
Because it read the STALE legacy path (hazard 1), ``canonicalize_tradfi_instrument_type_2026_07_16.py``
re-stamped counts from a partial object and mis-attributed the shortfall to "pre-existing manifest/object
staleness" in its own ``shard count DRIFT`` warnings and docstring. Live-verified 2026-07-17 against its own
pre-migration snapshot (``_index/snapshots/pre_tradfi_instrument_type_canon_2026_07_16_20260716T143452Z.parquet``):
tradfi CME 2026-06-28 went from a single blank row of 74,005 to OPTION 2,566 + FUTURE 32 + COMBO 228 =
2,826, while the CANONICAL object for that shard holds exactly 74,005 (OPTION 69,212/COMBO 4,446/FUTURE
347) — matching the ORIGINAL manifest count precisely. The drift its docstring documents as pre-existing was
in fact manufactured by reading the wrong object. This script's canonical-path-first rule is the fix for that
class.

MAGNITUDE — CORRECTED 2026-07-17 (this block previously said "aggregate tradfi coalesced count fell
47,189,618 → 46,764,522 (425,096 instruments lost)"; that number was an OVERSTATEMENT and is retracted).
Measured per-atom against every canonical object during the repair
(``scripts/repair_tradfi_instrument_type_counts_2026_07_17.py``, which is the tradfi fix — NOT a re-run of
this script, whose blank-row targeting is a no-op on tradfi now that zero blank+captured rows remain): the
snapshot→live Σ ``instrument_count`` delta of -422,560 decomposes into -453,041 superseded-ghost rows the
consolidator legitimately deduped, +75,709 genuine post-snapshot re-captures, and +25,951 legitimate
re-stamps where the snapshot's count really was stale. The ONLY atom actually corrupted is CME 2026-06-28
(-71,179) — the single case where a canonical object exists AND diverges from the legacy one. Consistent with
this script's own sampling: where both objects exist they AGREE (149/149 on a random tradfi 150-target
sample), so the hazard is real but concentrated, not systemic.

Usage (run from the instruments-service repo root)::

    python scripts/canonicalize_cefi_defi_instrument_type_2026_07_17.py --asset-group cefi
    python scripts/canonicalize_cefi_defi_instrument_type_2026_07_17.py --asset-group defi --workers 48
    python scripts/canonicalize_cefi_defi_instrument_type_2026_07_17.py --asset-group cefi --apply --confirm
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
    from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_DEFAULT_PIPELINE_MODE = "batch_instruments_service"

# Mirrors instruments_service.engine.orchestrator.writers._LEGACY_INSTRUMENT_TYPE_ALIASES.
# Duplicated (not imported) deliberately: this is a frozen one-off whose derivation must stay
# pinned to the alias set as it stood at migration time even if the writer's map later changes.
_LEGACY_INSTRUMENT_TYPE_ALIASES: dict[str, str] = {
    "perpetual": "PERPETUAL",
    "spot": "SPOT_PAIR",
}

# The row_key axes that must stay unique across the index (live-verified: 0 duplicates in both
# cefi and defi pre-migration). `chain` is a real axis for defi and constant-"" for cefi.
_ROW_KEY_AXES = ("_d", "venue", "_c", "_t", "data_type", "pipeline_mode")


def _bucket(asset_group: str) -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=asset_group)


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _norm(value: object) -> str:
    """Normalise a manifest cell to a plain string ("" for NaN/None/"nan")."""
    if value is None:
        return ""
    text = str(value)
    if text in {"nan", "NaT", "None"}:
        return ""
    return text


def path_venue(venue: str, chain: str) -> str:
    """Re-glue the DeFi object-path venue (``PROTOCOL-CHAIN``) from the split manifest axes.

    The manifest stores DeFi as ``venue=AAVE_V3`` + ``chain=ETHEREUM`` (canonical v5 shard-key
    matrix) while the parquet PATH keeps the glued ``AAVE_V3-ETHEREUM`` form the orchestrator
    wrote. A no-op for cefi/tradfi rows, whose ``chain`` is "" — including the on-chain perp
    CLOBs (LIGHTER-ZKSYNC / EXTENDED-STARKNET), which are cefi venues carrying chain="" whose
    glued name IS the manifest venue.
    """
    return f"{venue}-{chain}" if chain else venue


def candidate_paths(*, asset_group: str, date: str, venue: str, chain: str, pipeline_mode: str) -> list[str]:
    """Object paths to try, CANONICAL FIRST — order is load-bearing.

    The legacy (``day=/venue=``) shape is a stale historical artifact for cefi (live-measured
    SMALLER than the canonical object in 8/8 sampled shards where both exist), so it is only ever
    consulted when the canonical (``day=/pipeline_mode=/asset_group=/venue=``) object is ABSENT —
    which defi genuinely needs (canonical existed for only 67/120 sampled genuine defi targets).
    Reading the legacy object in preference to an existing canonical one is exactly the bug that
    corrupted the completed tradfi migration (see module docstring).
    """
    pv = path_venue(venue, chain)
    pm = pipeline_mode or _DEFAULT_PIPELINE_MODE
    return [
        f"instrument_availability/by_date/day={date}/pipeline_mode={pm}/asset_group={asset_group}/venue={pv}/instruments.parquet",
        f"instrument_availability/by_date/day={date}/venue={pv}/instruments.parquet",
    ]


def _read_types(storage: StorageClient, bucket: str, path: str) -> dict[str, int] | None:
    """Read ONE object and return ``{instrument_type: count}``; ``None`` when unusable.

    Per-object isolation: any exception is caught and logged, never raised (mirrors the
    shard-level-failure-isolation convention used across this service's writers and the
    precedent migration scripts). ``None`` means missing/unreadable/unparseable/empty or no
    ``instrument_type`` column — the caller leaves the manifest row honestly blank.

    A genuinely blank ``instrument_type`` inside the object keeps its "" key so the returned
    dict ALWAYS sums to the object's real row count (the caller's drift accounting depends on
    it) and that fraction stays explicitly blank rather than being silently dropped.
    """
    try:
        raw = storage.download_bytes(bucket, path)
    except Exception:  # broad-except-ok — per-object isolation; a missing object is the norm here
        return None
    try:
        df = pd.read_parquet(io.BytesIO(raw), columns=["instrument_type"])
    except ValueError:
        # Legacy schema missing the requested column — fall back to a full read.
        try:
            full = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:  # broad-except-ok — per-object isolation
            logger.warning("shard parse failed path=%s: %s: %s", path, type(exc).__name__, exc)
            return None
        if "instrument_type" not in full.columns:
            logger.warning("shard has no instrument_type column path=%s — leaving blank", path)
            return None
        df = full[["instrument_type"]]
    except Exception as exc:  # broad-except-ok — per-object isolation
        logger.warning("shard parse failed path=%s: %s: %s", path, type(exc).__name__, exc)
        return None
    if df.empty:
        return None
    col = df["instrument_type"].astype("string").fillna("").astype(str)
    col = col.replace(_LEGACY_INSTRUMENT_TYPE_ALIASES)
    return dict(Counter(col.tolist()))


def shard_instrument_type_counts(
    storage: StorageClient, bucket: str, *, asset_group: str, date: str, venue: str, chain: str, pipeline_mode: str
) -> tuple[dict[str, int], str] | None:
    """Resolve one shard's real ``{instrument_type: count}`` from its OWN object.

    Tries :func:`candidate_paths` in order (canonical first) and returns the FIRST usable
    result plus the path it came from. ``None`` when no candidate is usable → the caller leaves
    the row honestly blank.
    """
    for path in candidate_paths(
        asset_group=asset_group, date=date, venue=venue, chain=chain, pipeline_mode=pipeline_mode
    ):
        counts = _read_types(storage, bucket, path)
        if counts:
            return counts, path
    return None


def _coalesced_count(df: pd.DataFrame) -> pd.Series:
    """``row_count`` when >0, else ``instrument_count`` (mirrors the reader's own backfill semantics)."""
    rc = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
    ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(0)
    return rc.where(rc > 0, ic)


def _axes(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the normalised row_key axis columns used for collision detection + the gate."""
    out = df.copy()
    out["_d"] = out["date"].astype(str).str[:10]
    out["_c"] = out["chain"].map(_norm)
    out["_t"] = out["instrument_type"].map(_norm)
    return out


def derive(
    df: pd.DataFrame, *, asset_group: str, workers: int, bucket: str, storage: StorageClient
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Pure(ish) transform — the only I/O is the per-shard read, isolated in ``shard_instrument_type_counts``.

    Returns ``(new_index_df, stats)``. Never mutates ``df`` in place.
    """
    work = _axes(df)
    blank = work["_t"].str.len() == 0
    captured = work["capture_status"].astype(str) == "captured"
    target_idx = work.index[blank & captured].tolist()

    # Every (shard atom, type) that ALREADY has a row — a derived type hitting one of these is a
    # superseded ghost, not a backfill target (see docstring hazard 2). Built from ALL rows, not
    # just captured ones: the row_key is status-independent, so any existing row is a collision.
    existing_keys: set[tuple[str, str, str, str]] = {
        (r["_d"], str(r["venue"]), r["_c"], r["_t"]) for _, r in work[~blank].iterrows()
    }

    logger.info(
        "targets: %d blank+captured rows (of %d blank overall, %d total index rows); %d typed rows already present",
        len(target_idx),
        int(blank.sum()),
        len(work),
        len(existing_keys),
    )

    shard_results: dict[int, tuple[dict[str, int], str] | None] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(
                shard_instrument_type_counts,
                storage,
                bucket,
                asset_group=asset_group,
                date=work.at[i, "_d"],
                venue=str(work.at[i, "venue"]),
                chain=work.at[i, "_c"],
                pipeline_mode=_norm(work.at[i, "pipeline_mode"]),
            ): i
            for i in target_idx
        }
        for done, fut in enumerate(as_completed(futs), start=1):
            shard_results[futs[fut]] = fut.result()
            if done % 2000 == 0:
                logger.info("  shards read: %d/%d", done, len(target_idx))

    new_rows: list[dict[str, object]] = []
    keep_mask = pd.Series(True, index=work.index)
    manifest_counts = _coalesced_count(work)
    updated_in_place = 0
    split_shards = 0
    split_rows_created = 0
    ghost_rows_dropped = 0
    ghost_types_skipped = 0
    unresolvable = 0
    legacy_path_used = 0
    drift_shards = 0
    drift_magnitude = 0

    for i in target_idx:
        resolved = shard_results.get(i)
        if resolved is None:
            unresolvable += 1
            continue
        type_counts, used_path = resolved
        if "/pipeline_mode=" not in used_path:
            legacy_path_used += 1

        date = work.at[i, "_d"]
        venue = str(work.at[i, "venue"])
        chain = work.at[i, "_c"]

        emitted: list[dict[str, object]] = []
        skipped_types = 0
        base = df.loc[i].to_dict()
        for itype_val, cnt in type_counts.items():
            if (date, venue, chain, itype_val) in existing_keys:
                # A correct, fixed-writer row for this exact (atom, type) already exists — it wins,
                # untouched. Emitting here would mint a duplicate row_key and double-count coverage.
                skipped_types += 1
                ghost_types_skipped += 1
                continue
            new_row = dict(base)
            new_row["instrument_type"] = itype_val
            new_row["row_count"] = cnt
            new_row["instrument_count"] = cnt
            emitted.append(new_row)
            existing_keys.add((date, venue, chain, itype_val))

        # The blank row is resolved either way: its content is now represented by the rows emitted
        # above and/or by the pre-existing typed rows it duplicated. Drop it.
        keep_mask.at[i] = False

        if not emitted:
            ghost_rows_dropped += 1
            continue

        derived_sum = sum(type_counts.values())
        manifest_count = int(manifest_counts.at[i])
        if skipped_types == 0 and derived_sum != manifest_count:
            drift_shards += 1
            drift_magnitude += manifest_count - derived_sum
            logger.warning(
                "shard count DRIFT date=%s venue=%s chain=%s: manifest_count=%d real_derived_sum=%d diff=%d types=%s path=%s",
                date,
                venue,
                chain,
                manifest_count,
                derived_sum,
                manifest_count - derived_sum,
                type_counts,
                used_path,
            )
        if len(emitted) == 1:
            updated_in_place += 1
        else:
            split_shards += 1
            split_rows_created += len(emitted)
        new_rows.extend(emitted)

    if new_rows:
        out = pd.concat([df.loc[keep_mask], pd.DataFrame(new_rows)], ignore_index=True)
    else:
        out = df.loc[keep_mask].copy().reset_index(drop=True)
    for col in ("row_count", "instrument_count", "schema_version"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")

    out_axes = _axes(out)
    dup_keys = int((out_axes.groupby(list(_ROW_KEY_AXES)).size() > 1).sum())
    out_blank = out_axes["_t"].str.len() == 0
    out_captured = out_axes["capture_status"].astype(str) == "captured"

    stats: dict[str, object] = {
        "targets": len(target_idx),
        "updated_in_place": updated_in_place,
        "split_shards": split_shards,
        "split_rows_created": split_rows_created,
        "ghost_rows_dropped": ghost_rows_dropped,
        "ghost_types_skipped": ghost_types_skipped,
        "unresolvable_left_blank": unresolvable,
        "legacy_path_used": legacy_path_used,
        "rows_before": len(df),
        "rows_after": len(out),
        "blank_before": int(blank.sum()),
        "blank_after": int(out_blank.sum()),
        "blank_captured_before": len(target_idx),
        "blank_captured_after": int((out_blank & out_captured).sum()),
        "coalesced_sum_before": int(_coalesced_count(df).sum()),
        "coalesced_sum_after": int(_coalesced_count(out).sum()),
        "duplicate_row_keys_after": dup_keys,
        "drift_shards": drift_shards,
        "drift_magnitude": drift_magnitude,
    }
    return out, stats


def gate(stats: Mapping[str, object]) -> bool:
    """Refuse to --apply on signs of a REAL bug.

    * ``duplicate_row_keys_after`` — the hard structural invariant (live-verified as 0 pre-migration
      in BOTH cefi and defi). A duplicate would mean a ghost got backfilled alongside the correct
      fixed-writer row, double-counting coverage. This is THE gate that makes the cefi/defi ghost
      hazard impossible rather than merely handled.
    * ``blank_after > blank_before`` — real data loss.
    * every target unresolved — a systemic read failure (vs. scattered honest absence, which is fine).

    An aggregate coalesced-count DELTA is NOT a gate failure: per workspace convention ("trust the
    actual distribution, not the constant") a resolved row's count is re-stamped from the REAL
    CANONICAL object, which is strictly more correct than a stale manifest constant. Dropped ghost
    rows also legitimately remove double-counted totals. Both are logged for the record.
    """
    return (
        int(stats["duplicate_row_keys_after"]) == 0
        and int(stats["blank_after"]) <= int(stats["blank_before"])
        and not (int(stats["targets"]) > 0 and int(stats["unresolvable_left_blank"]) == int(stats["targets"]))
    )


def run(*, asset_group: str, apply: bool, workers: int) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket(asset_group)
    storage = get_storage_client()
    logger.info(
        "%s instrument_type canonicalization: bucket=%s blob=%s apply=%s workers=%d",
        asset_group,
        bucket,
        _INDEX_BLOB,
        apply,
        workers,
    )

    raw_bytes = storage.download_bytes(bucket, _INDEX_BLOB)
    before = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("BEFORE: rows=%d", len(before))

    out, stats = derive(before, asset_group=asset_group, workers=workers, bucket=bucket, storage=storage)
    logger.info("=== derivation stats === %s", stats)

    gate_ok = gate(stats)
    logger.info(
        "GATE (row_key uniqueness, no blank regression, not all-unresolved): %s", "OK" if gate_ok else "VIOLATION"
    )
    if stats["unresolvable_left_blank"]:
        logger.warning(
            "%s captured rows left HONESTLY BLANK (object missing/unreadable/empty or no instrument_type "
            "column) — never guessed. These remain blank by design.",
            stats["unresolvable_left_blank"],
        )
    if stats["ghost_rows_dropped"]:
        logger.warning(
            "%s blank rows were SUPERSEDED GHOSTS (every derived type already had a correct typed row on "
            "the same shard atom) — dropped instead of duplicated; %s ghost types skipped in total.",
            stats["ghost_rows_dropped"],
            stats["ghost_types_skipped"],
        )
    if not gate_ok:
        logger.error(
            "GATE FAILED — dup_row_keys=%s blank_before=%s blank_after=%s unresolvable=%s/%s — aborting before write.",
            stats["duplicate_row_keys_after"],
            stats["blank_before"],
            stats["blank_after"],
            stats["unresolvable_left_blank"],
            stats["targets"],
        )
        return 2

    if not apply:
        logger.info("DRY RUN — live index NOT modified. Re-run with --apply --confirm to write.")
        return 0

    stamp = _utc_stamp()
    snap_blob = f"_index/snapshots/pre_{asset_group}_instrument_type_canon_2026_07_17_{stamp}.parquet"
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
    p.add_argument("--asset-group", required=True, choices=("cefi", "defi"), help="Which manifest index to migrate")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    p.add_argument("--confirm", action="store_true", default=False, help="Required alongside --apply")
    p.add_argument("--workers", type=int, default=32, help="Parallel per-shard GCS reads (default: 32)")
    args = p.parse_args(argv)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm (refusing to write the live index without it).")
        return 2
    return run(asset_group=args.asset_group, apply=args.apply, workers=args.workers)


if __name__ == "__main__":
    sys.exit(main())
