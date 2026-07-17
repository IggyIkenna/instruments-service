#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod tradfi `_index/availability_index.parquet` reconciles against the CANONICAL
#   by_date objects for every atom the 2026-07-16 migration re-stamped (verified via --dry-run
#   reporting 0 atoms needing repair), AND the parent issue doc
#   `plans/active/issues/tradfi_instrument_type_migration_read_stale_legacy_object_2026_07_17.md`
#   is closed.
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false, reportArgumentType=false
# (one-shot migration over the untyped pandas / StorageClient row-dict surface — same pragma
#  set as the sibling canonicalize_cefi_defi_instrument_type_2026_07_17.py.)
"""repair_tradfi_instrument_type_counts_2026_07_17.py — repair the tradfi index the 07-16 migration corrupted.

Issue: ``unified-trading-pm/plans/active/issues/tradfi_instrument_type_migration_read_stale_legacy_object_2026_07_17.md``

WHAT BROKE. ``scripts/canonicalize_tradfi_instrument_type_2026_07_16.py`` (run + flipped DONE 2026-07-16)
backfilled blank ``instrument_type`` by re-deriving each shard's type from that shard's own object — but it
resolved the object at the LEGACY path ``instrument_availability/by_date/day={D}/venue={V}/instruments.parquet``
instead of the CANONICAL, source-aware
``instrument_availability/by_date/day={D}/pipeline_mode={PM}/asset_group={AG}/venue={V}/instruments.parquet``.
Both exist; the legacy one is a stale partial. Because the script re-stamps ``row_count``/``instrument_count``
from whatever object it read, it wrote PARTIAL counts over correct ones, then logged the shortfall as a
"pre-existing" manifest-vs-object staleness bug in its own DRIFT warnings and docstring. That explanation is
false; the drift was manufactured by the migration. Live-verified 2026-07-17: tradfi CME 2026-06-28 CANONICAL
object = 74,005 rows (OPTION 69,212 / COMBO 4,446 / FUTURE 347) — exactly the ORIGINAL manifest count — while
the LEGACY object = 2,826 (OPTION 2,566 / COMBO 228 / FUTURE 32) — exactly what the migration wrote.

=============================================================================================
WHY THE TWO OBVIOUS REPAIRS ARE BOTH WRONG (each measured, 2026-07-17 — re-verify at run time)
=============================================================================================

**(1) Re-running the sibling ``canonicalize_cefi_defi_instrument_type_2026_07_17.py --asset-group tradfi``
is a NO-OP.** That script targets blank+``captured`` rows; tradfi now has ZERO of them (the migration typed
them all yesterday). The damage is WRONG COUNTS — and potentially MISSING TYPES — on rows that are already
typed, which a blank-row backfill does not look at.

**(2) Restoring the pre-migration snapshot LOSES real data.** Measured: 39 live rows carry a ``written_at``
NEWER than the snapshot's max (2026-07-16T13:41:12Z), spanning 21 (date, venue) atoms over 2026-07-15/16/17 —
and 14 of those atoms ALREADY EXIST in the snapshot, because the daily job RE-CAPTURES a rolling window of
recent days (e.g. CME 2026-07-15 OPTION: snapshot 68,500 → live 70,552, re-captured 07-17). So the naive
"restore the snapshot, keep only atoms absent from it" rule silently reverts 26 rows of fresher, more correct
data. A blind restore also re-blanks the ``instrument_type`` the migration legitimately fixed.

THE DESIGN (base = LIVE, not the snapshot). The snapshot is used ONLY to identify WHICH atoms the migration
touched — it is never a source of row content. For each such atom the CANONICAL object is the single source of
truth for both the type split and the counts:
  * an existing ``captured`` row whose type the object confirms → ONLY its ``row_count``/``instrument_count``
    are re-stamped; every other field (``written_at``/``attempted_at``/``capture_status``/``source``/…) is
    preserved VERBATIM, so provenance survives the repair.
  * a type the object carries but the index lacks → minted from the atom's most-recently-written ``captured``
    row as the metadata donor. This is what recovers a type the stale legacy object omitted ENTIRELY (proven
    in the sibling asset group: cefi DERIBIT 2019-03-30 canonical=295 rows incl. 289 OPTIONs vs legacy=6 with
    ZERO OPTIONs) — a counts-only repair would leave those instruments missing.
  * a ``captured`` row whose type the object does NOT carry → dropped (the object is the truth).
  * NON-``captured`` rows (``empty_confirmed``/``expected_unattempted``/``attempted_failed``) are NEVER
    touched: they captured zero instruments and are honestly blank.
Everything outside the touched-atom set is copied verbatim, which is why the 39 post-snapshot rows survive
without a special case — they are simply live rows nobody targets. The transform is IDEMPOTENT: a second run
re-derives the same distribution, finds it already matches, and reports 0 atoms repaired.

HONEST ABSENCE. An atom whose CANONICAL **and** LEGACY objects are both missing/unreadable/empty/type-less is
left EXACTLY as-is and counted as an explicit residual — never guessed, never blanked, never zeroed.
Canonical-first ordering is a correctness contract (it is the whole bug); the legacy path is consulted ONLY
when the canonical object is absent. Live-measured on a random 150-target sample: canonical present 149/150,
legacy 150/150, and where BOTH exist they AGREE 149/149 — so tradfi's damage is concentrated in the minority
of shards where they diverge, and the legacy fallback is safe for the one shard that needs it.

GATES (refuse to --apply if violated):
  * Σ ``instrument_count`` must strictly INCREASE — this is a repair; a decrease means it is re-breaking.
  * No ``captured`` (date, venue) atom may disappear.
  * Blank+``captured`` rows must not increase.
  * ``row_key`` duplicates must not increase beyond the MEASURED pre-existing baseline. NOTE: unlike cefi/defi
    (0 duplicates), tradfi carries 153 PRE-EXISTING duplicate row_keys — all KRX, every one an
    (``expected_unattempted``, ``empty_confirmed``) pair at instrument_count=0. They are untouched by this
    repair (non-captured) and are a separate pre-existing issue; gating on ==0 would refuse forever.

CONCURRENCY (measured 2026-07-17, and why the write is a CAS). The canonical index's GENERATION changes every
~60s: the consolidator's idle ``_touch_canonical_mtime`` does a server-side copy-to-self to refresh the
reader's freshness mtime. That copy carries the CURRENT bytes, so it cannot revert this repair — but a plain
``upload_bytes`` could still clobber a concurrent real writer. The write therefore uses
``download_bytes_with_generation`` + ``conditional_upload_bytes(if_generation_match=...)``, re-verifying that
the index CONTENT still matches the frame this run derived from before committing, and retrying on a lost race.

THE CONSOLIDATOR MARKER — PRESERVE, NEVER MINT. ``consolidator_content_write_at`` is the prune cutoff: it
means "the last REAL merge's shard-listing time". Stamping it to now() from a repair tool would arm the shard
reaper described in ``plans/active/issues/consolidator_content_write_marker_strip_silent_shard_reap_2026_07_17.md``
(P0, fixed in unified-trading-library@1e995f75). This script therefore carries EXISTING custom metadata
forward verbatim and never invents the key. Measured at authoring time: the tradfi canonical already carries
NO marker (yesterday's migration stripped it via a metadata-less ``upload_bytes``), which post-fix is the
fail-CLOSED self-healing state (merge everything, prune nothing) — so preserving "absent" is correct and safe.
Live-verified: the tradfi bucket's only ``_index/per_vm/`` shard is ``_legacy_seed.parquet``, which the
consolidator EXCLUDES whenever a canonical exists — so there is no pending shard able to resurrect the rows
this repair drops.

Usage (run from the instruments-service repo root)::

    python scripts/repair_tradfi_instrument_type_counts_2026_07_17.py                    # dry-run (default)
    python scripts/repair_tradfi_instrument_type_counts_2026_07_17.py --workers 48
    python scripts/repair_tradfi_instrument_type_counts_2026_07_17.py --apply --confirm
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
from typing import TYPE_CHECKING, NamedTuple

import pandas as pd

if TYPE_CHECKING:
    from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)

_ASSET_GROUP = "tradfi"
_INDEX_BLOB = "_index/availability_index.parquet"
_DEFAULT_PIPELINE_MODE = "batch_instruments_service"
_PRE_MIGRATION_SNAPSHOT = "_index/snapshots/pre_tradfi_instrument_type_canon_2026_07_16_20260716T143452Z.parquet"

# Mirrors instruments_service.engine.orchestrator.writers._LEGACY_INSTRUMENT_TYPE_ALIASES, and is a
# deliberate copy of the same constant in scripts/canonicalize_cefi_defi_instrument_type_2026_07_17.py.
# NOT imported from that script: it is a frozen, already-APPLIED one-off, and importing it would put this
# repair's derivation at the mercy of any later edit to it (and vice versa). Both must stay pinned to the
# alias set as it stood at migration time.
_LEGACY_INSTRUMENT_TYPE_ALIASES: dict[str, str] = {
    "perpetual": "PERPETUAL",
    "spot": "SPOT_PAIR",
}

# The manifest row_key. `chain` is constant-"" for tradfi but is kept as an explicit axis so the key
# matches the writer's / consolidator's own definition rather than a tradfi-shaped simplification.
_ATOM_AXES = ("_d", "venue", "_c", "data_type", "pipeline_mode")
_ROW_KEY_AXES = (*_ATOM_AXES, "_t")

_CAPTURED = "captured"


class Resolved(NamedTuple):
    """One atom's real type→count distribution, plus the object path it was read from."""

    counts: dict[str, int]
    path: str


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=_ASSET_GROUP)


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _norm(value: object) -> str:
    """Normalise a manifest cell to a plain string ("" for NaN/None/"nan")."""
    if value is None:
        return ""
    text = str(value)
    if text in {"nan", "NaT", "None", "<NA>"}:
        return ""
    return text


def candidate_paths(*, date: str, venue: str, pipeline_mode: str) -> list[str]:
    """Object paths to try, CANONICAL FIRST — the ordering IS the bug fix.

    The legacy ``day=/venue=`` shape is a stale partial left behind by the pipeline_mode partition
    migration. Consulting it in preference to an existing canonical object is exactly what corrupted
    the 2026-07-16 migration, so it is only ever read when the canonical object is ABSENT.
    """
    pm = pipeline_mode or _DEFAULT_PIPELINE_MODE
    return [
        f"instrument_availability/by_date/day={date}/pipeline_mode={pm}/asset_group={_ASSET_GROUP}/venue={venue}/instruments.parquet",
        f"instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet",
    ]


def _read_types(storage: StorageClient, bucket: str, path: str) -> dict[str, int] | None:
    """Read ONE object and return ``{instrument_type: count}``; ``None`` when unusable.

    Per-object isolation: any exception is caught and logged, never raised (shard-level failure
    isolation). ``None`` means missing/unreadable/unparseable/empty or no ``instrument_type`` column —
    the caller then leaves the atom untouched rather than guessing.

    A genuinely blank ``instrument_type`` inside the object keeps its "" key, so the returned dict
    ALWAYS sums to the object's real row count and that fraction stays explicitly blank.
    """
    try:
        raw = storage.download_bytes(bucket, path)
    except Exception:  # broad-except-ok — per-object isolation; a missing object is the norm here
        return None
    try:
        df = pd.read_parquet(io.BytesIO(raw), columns=["instrument_type"])
    except ValueError:
        try:
            full = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:  # broad-except-ok — per-object isolation
            logger.warning("shard parse failed path=%s: %s: %s", path, type(exc).__name__, exc)
            return None
        if "instrument_type" not in full.columns:
            logger.warning("shard has no instrument_type column path=%s — leaving atom untouched", path)
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


def resolve_atom(storage: StorageClient, bucket: str, *, date: str, venue: str, pipeline_mode: str) -> Resolved | None:
    """Resolve one atom's real ``{instrument_type: count}`` from its OWN object, canonical-path-first."""
    for path in candidate_paths(date=date, venue=venue, pipeline_mode=pipeline_mode):
        counts = _read_types(storage, bucket, path)
        if counts:
            return Resolved(counts=counts, path=path)
    return None


def _coalesced_count(df: pd.DataFrame) -> pd.Series:
    """``row_count`` when >0, else ``instrument_count`` (mirrors the reader's own backfill semantics)."""
    rc = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
    ic = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(0)
    return rc.where(rc > 0, ic)


def _axes(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the normalised row_key axis columns."""
    out = df.copy()
    out["_d"] = out["date"].map(_norm).str[:10]
    out["_c"] = out["chain"].map(_norm)
    out["_t"] = out["instrument_type"].map(_norm)
    out["_pm"] = out["pipeline_mode"].map(_norm)
    return out


def _sum_instrument_count(df: pd.DataFrame) -> int:
    return int(pd.to_numeric(df["instrument_count"], errors="coerce").fillna(0).sum())


def _dup_row_keys(df: pd.DataFrame) -> int:
    work = _axes(df)
    return int((work.groupby(list(_ROW_KEY_AXES)).size() > 1).sum())


def _captured_atoms(df: pd.DataFrame) -> set[tuple[str, str]]:
    work = _axes(df)
    cap = work["capture_status"].astype(str) == _CAPTURED
    return set(zip(work.loc[cap, "_d"], work.loc[cap, "venue"].astype(str), strict=True))


def target_atoms(snapshot: pd.DataFrame) -> set[tuple[str, str, str, str, str]]:
    """The atoms the 2026-07-16 migration re-stamped: its blank+``captured`` rows.

    Derived from the PRE-MIGRATION snapshot because the live index no longer identifies them (the
    migration typed every one, so live has zero blank+captured rows). The snapshot is used for this
    and nothing else — never as a source of row content.
    """
    work = _axes(snapshot)
    blank = work["_t"].str.len() == 0
    captured = work["capture_status"].astype(str) == _CAPTURED
    tgt = work[blank & captured]
    return set(
        zip(
            tgt["_d"],
            tgt["venue"].astype(str),
            tgt["_c"],
            tgt["data_type"].map(_norm),
            tgt["_pm"],
            strict=True,
        )
    )


def derive(
    live: pd.DataFrame,
    snapshot: pd.DataFrame,
    *,
    workers: int,
    bucket: str,
    storage: StorageClient,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Reconcile every migration-touched atom against its CANONICAL object. Never mutates ``live``."""
    atoms = target_atoms(snapshot)
    work = _axes(live)
    work["_ag_atom"] = list(
        zip(work["_d"], work["venue"].astype(str), work["_c"], work["data_type"].map(_norm), work["_pm"], strict=True)
    )
    logger.info(
        "migration-touched atoms (from snapshot blank+captured rows): %d; live rows=%d",
        len(atoms),
        len(live),
    )

    live_atoms = set(work["_ag_atom"])
    present = sorted(a for a in atoms if a in live_atoms)
    logger.info("of those, atoms still present in live: %d", len(present))

    results: dict[tuple[str, str, str, str, str], Resolved | None] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(resolve_atom, storage, bucket, date=a[0], venue=a[1], pipeline_mode=a[4]): a for a in present
        }
        for done, fut in enumerate(as_completed(futs), start=1):
            results[futs[fut]] = fut.result()
            if done % 2000 == 0:
                logger.info("  atoms read: %d/%d", done, len(present))

    keep_mask = pd.Series(True, index=work.index)
    new_rows: list[dict[str, object]] = []
    counts_restamped = 0
    rows_minted = 0
    rows_dropped = 0
    atoms_repaired = 0
    atoms_already_correct = 0
    unresolvable = 0
    legacy_path_used = 0
    delta_total = 0

    by_atom: dict[tuple[str, str, str, str, str], list[int]] = {}
    for idx, atom in zip(work.index, work["_ag_atom"], strict=True):
        by_atom.setdefault(atom, []).append(idx)

    for atom in present:
        resolved = results.get(atom)
        idxs = by_atom.get(atom, [])
        cap_idxs = [i for i in idxs if str(work.at[i, "capture_status"]) == _CAPTURED]
        if resolved is None:
            unresolvable += 1
            logger.warning(
                "atom UNRESOLVED (canonical AND legacy object absent/unusable) date=%s venue=%s — "
                "left EXACTLY as-is, never guessed",
                atom[0],
                atom[1],
            )
            continue
        if "/pipeline_mode=" not in resolved.path:
            legacy_path_used += 1
        if not cap_idxs:
            # Nothing captured to reconcile (the migration's rows lost a dedup, or the atom is
            # non-captured now). Leave it alone — minting here would invent coverage.
            continue

        derived = {t: c for t, c in resolved.counts.items() if t}
        blank_in_object = resolved.counts.get("", 0)
        if not derived:
            logger.warning(
                "atom date=%s venue=%s object carries ONLY blank instrument_type (%d rows) — left as-is",
                atom[0],
                atom[1],
                blank_in_object,
            )
            unresolvable += 1
            continue

        # One representative row per captured type. A same-type duplicate inside one atom would
        # otherwise survive untouched alongside the re-stamped row and mint a duplicate row_key,
        # so every extra is dropped here (tradfi's 153 pre-existing duplicates are all
        # NON-captured KRX pairs, so this is a guard, not a live code path — the dup gate proves it).
        existing_by_type: dict[str, int] = {}
        for i in cap_idxs:
            itype_existing = str(work.at[i, "_t"])
            if itype_existing in existing_by_type:
                keep_mask.at[i] = False
                rows_dropped += 1
                logger.warning(
                    "atom date=%s venue=%s: dropped DUPLICATE captured row for type=%s",
                    atom[0],
                    atom[1],
                    itype_existing,
                )
                continue
            existing_by_type[itype_existing] = i
        donor = max(cap_idxs, key=lambda i: (_norm(work.at[i, "written_at"]), str(work.at[i, "_t"])))

        before_sum = sum(int(pd.to_numeric(work.at[i, "instrument_count"], errors="coerce") or 0) for i in cap_idxs)
        changed = False

        for itype, cnt in derived.items():
            i = existing_by_type.get(itype)
            if i is None:
                base = live.loc[donor].to_dict()
                base["instrument_type"] = itype
                base["row_count"] = cnt
                base["instrument_count"] = cnt
                new_rows.append(base)
                rows_minted += 1
                changed = True
                logger.info(
                    "atom date=%s venue=%s: MINTED missing type=%s count=%d (stale object omitted it)",
                    atom[0],
                    atom[1],
                    itype,
                    cnt,
                )
                continue
            old = int(pd.to_numeric(work.at[i, "instrument_count"], errors="coerce") or 0)
            if old != cnt:
                counts_restamped += 1
                changed = True
            keep_mask.at[i] = False
            row = live.loc[i].to_dict()
            row["row_count"] = cnt
            row["instrument_count"] = cnt
            new_rows.append(row)

        for itype, i in existing_by_type.items():
            if itype not in derived:
                keep_mask.at[i] = False
                rows_dropped += 1
                changed = True
                logger.warning(
                    "atom date=%s venue=%s: DROPPED type=%s (canonical object does not carry it)",
                    atom[0],
                    atom[1],
                    itype,
                )

        after_sum = sum(derived.values())
        delta_total += after_sum - before_sum
        if changed:
            atoms_repaired += 1
        else:
            atoms_already_correct += 1

    out = (
        pd.concat([live.loc[keep_mask], pd.DataFrame(new_rows)], ignore_index=True)
        if new_rows
        else live.loc[keep_mask].copy().reset_index(drop=True)
    )
    for col in ("row_count", "instrument_count", "schema_version"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")

    out_axes = _axes(out)
    out_blank = out_axes["_t"].str.len() == 0
    out_cap = out_axes["capture_status"].astype(str) == _CAPTURED
    live_axes = _axes(live)
    live_blank = live_axes["_t"].str.len() == 0
    live_cap = live_axes["capture_status"].astype(str) == _CAPTURED

    lost = _captured_atoms(live) - _captured_atoms(out)
    stats: dict[str, object] = {
        "touched_atoms": len(atoms),
        "atoms_present_in_live": len(present),
        "atoms_repaired": atoms_repaired,
        "atoms_already_correct": atoms_already_correct,
        "counts_restamped": counts_restamped,
        "rows_minted": rows_minted,
        "rows_dropped": rows_dropped,
        "unresolvable_left_as_is": unresolvable,
        "legacy_path_used": legacy_path_used,
        "rows_before": len(live),
        "rows_after": len(out),
        "sum_instrument_count_before": _sum_instrument_count(live),
        "sum_instrument_count_after": _sum_instrument_count(out),
        "sum_instrument_count_delta": _sum_instrument_count(out) - _sum_instrument_count(live),
        "coalesced_sum_before": int(_coalesced_count(live).sum()),
        "coalesced_sum_after": int(_coalesced_count(out).sum()),
        "blank_captured_before": int((live_blank & live_cap).sum()),
        "blank_captured_after": int((out_blank & out_cap).sum()),
        "dup_row_keys_before": _dup_row_keys(live),
        "dup_row_keys_after": _dup_row_keys(out),
        "captured_atoms_before": len(_captured_atoms(live)),
        "captured_atoms_after": len(_captured_atoms(out)),
        "captured_atoms_lost": len(lost),
    }
    if lost:
        logger.error("captured atoms LOST by this transform (gate will refuse): %s", sorted(lost)[:20])
    return out, stats


def gate(stats: Mapping[str, object]) -> bool:
    """Refuse to --apply unless this is unambiguously a REPAIR.

    * Σ ``instrument_count`` must strictly INCREASE. This repair only ever replaces a stale-legacy
      partial count with the canonical object's real one, so a decrease means it is re-breaking the
      index rather than fixing it. (Contrast the sibling backfill, where a delta is expected — there,
      dropped ghost rows legitimately remove double-counted totals.)
    * No ``captured`` (date, venue) atom may disappear.
    * Blank+``captured`` rows must not increase.
    * ``row_key`` duplicates must not increase beyond the pre-existing baseline (tradfi carries 153;
      see module docstring). A NEW duplicate would mean a mint collided with an existing row.
    """
    return (
        int(stats["sum_instrument_count_after"]) > int(stats["sum_instrument_count_before"])
        and int(stats["captured_atoms_lost"]) == 0
        and int(stats["blank_captured_after"]) <= int(stats["blank_captured_before"])
        and int(stats["dup_row_keys_after"]) <= int(stats["dup_row_keys_before"])
    )


def _write_with_cas(
    storage: StorageClient,
    bucket: str,
    *,
    out: pd.DataFrame,
    base_bytes: bytes,
    generation: int,
    attempts: int = 5,
) -> bool:
    """CAS-write the index, re-verifying the base content each attempt.

    The canonical's GENERATION churns every ~60s (the consolidator's idle copy-to-self mtime touch),
    so a lost race is expected and retried. The retry re-downloads and REQUIRES the content to still
    equal the frame this run derived from — if a real writer changed the rows, we abort rather than
    clobber it.
    """
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    payload = buf.getvalue()

    meta_obj = storage.get_blob_metadata(bucket, _INDEX_BLOB)
    carried = dict(getattr(meta_obj, "metadata", None) or {})
    if carried:
        logger.info("carrying existing custom metadata forward verbatim: %s", sorted(carried))
    else:
        logger.info(
            "canonical carries NO custom metadata — preserving that (fail-CLOSED consolidator state). "
            "NEVER minting consolidator_content_write_at from a repair tool: it would arm the shard reaper."
        )

    cur_bytes, cur_gen = base_bytes, generation
    for attempt in range(1, attempts + 1):
        new_gen = storage.conditional_upload_bytes(bucket, _INDEX_BLOB, payload, if_generation_match=cur_gen)
        if new_gen is not None:
            logger.info("CAS write OK on attempt %d: generation %d -> %d", attempt, cur_gen, new_gen)
            if carried:
                # conditional_upload_bytes carries no metadata parameter, so preserving a marker
                # costs one follow-up PUT of the SAME payload. Measured at authoring time this is
                # dead code (the canonical carries no custom metadata); it exists so a future run
                # against a re-markered index cannot silently strip it. The follow-up is outside the
                # CAS, but it re-writes byte-identical content, so the only loss window is a real
                # writer landing in the milliseconds between — strictly better than dropping a marker.
                storage.upload_bytes(bucket, _INDEX_BLOB, payload, metadata=carried)
                logger.info("custom metadata re-applied after CAS write (same bytes): %s", sorted(carried))
            return True
        logger.warning("CAS precondition failed (attempt %d/%d) — re-verifying live content", attempt, attempts)
        cur_bytes, cur_gen = storage.download_bytes_with_generation(bucket, _INDEX_BLOB)
        if cur_bytes is None:
            logger.error("canonical vanished mid-write — aborting")
            return False
        # BYTE equality, not a row/sum comparison: the expected racer is the consolidator's
        # server-side copy-to-self, which is byte-identical by construction. Anything that changed
        # the bytes is a REAL writer whose work we must not clobber.
        if cur_bytes != base_bytes:
            logger.error(
                "live index CONTENT changed under us (a real writer landed) — ABORTING rather than "
                "clobbering it. Re-run the script; it is idempotent."
            )
            return False
    logger.error("exhausted %d CAS attempts — not written", attempts)
    return False


def run(*, apply: bool, workers: int) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    storage = get_storage_client()
    logger.info("tradfi instrument_type COUNT REPAIR: bucket=%s apply=%s workers=%d", bucket, apply, workers)

    raw_bytes, generation = storage.download_bytes_with_generation(bucket, _INDEX_BLOB)
    if raw_bytes is None:
        logger.error("canonical index absent: %s", _INDEX_BLOB)
        return 2
    live = pd.read_parquet(io.BytesIO(raw_bytes))
    snapshot = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, _PRE_MIGRATION_SNAPSHOT)))
    logger.info(
        "BEFORE: live rows=%d (generation=%d) | pre-migration snapshot rows=%d",
        len(live),
        generation,
        len(snapshot),
    )

    out, stats = derive(live, snapshot, workers=workers, bucket=bucket, storage=storage)
    logger.info("=== repair stats === %s", stats)

    gate_ok = gate(stats)
    logger.info("GATE (sum UP, no captured atom lost, no blank/dup regression): %s", "OK" if gate_ok else "VIOLATION")
    if stats["unresolvable_left_as_is"]:
        logger.warning(
            "%s atoms left EXACTLY as-is (canonical AND legacy object absent/unusable) — honest absence, never guessed.",
            stats["unresolvable_left_as_is"],
        )
    if not gate_ok:
        logger.error("GATE FAILED — aborting before write. stats=%s", stats)
        return 2

    if not apply:
        logger.info("DRY RUN — live index NOT modified. Re-run with --apply --confirm to write.")
        return 0

    snap_blob = f"_index/snapshots/pre_tradfi_count_repair_2026_07_17_{_utc_stamp()}.parquet"
    storage.upload_bytes(bucket, snap_blob, raw_bytes)
    logger.info("ROLLBACK snapshot written: gs://%s/%s", bucket, snap_blob)

    if not _write_with_cas(storage, bucket, out=out, base_bytes=raw_bytes, generation=generation):
        return 3
    logger.info("APPLIED — live index written: gs://%s/%s (%d rows)", bucket, _INDEX_BLOB, len(out))
    logger.info("ROLLBACK: restore gs://%s/%s over gs://%s/%s", bucket, snap_blob, bucket, _INDEX_BLOB)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    p.add_argument("--confirm", action="store_true", default=False, help="Required alongside --apply")
    p.add_argument("--workers", type=int, default=32, help="Parallel per-atom GCS reads (default: 32)")
    args = p.parse_args(argv)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm (refusing to write the live index without it).")
        return 2
    return run(apply=args.apply, workers=args.workers)


if __name__ == "__main__":
    sys.exit(main())
