#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false
# pyright: reportMissingParameterType=false, reportUnknownLambdaType=false
# (one-shot migration over the untyped pandas / StorageClient surface — same pragma set as the sibling
#  MTDS migrate_*_v9_canonical.py tools.)
"""Migrate an ``instruments-store-{ag}`` bucket → canonical v9 (single bundled walk, AG-parametric).

This is the **instruments-service analogue** of the per-AG MTDS ``migrate_*_v9_canonical.py`` scripts, and the
WRITE counterpart of the read-only ``plans/audit/results/cf_manifest_audit_2026_06_01.py``. It brings an
``instruments-store-{ag}`` bucket's ``_index/availability_index.parquet`` AND its object paths from the legacy v8
form to the canonical v9 form (CF-1…CF-14), in ONE walk per bucket.

OWNER: ``plans/active/instruments_manifest_canonicalisation_2026_06_01.md`` (E2 migrator, G1-V8 P0). The non-sports
buckets (cefi/defi/tradfi/prediction) are this plan's primary surface; the **sports** instruments-store rides
``sports_manifest_canonicalisation_2026_06_01.md`` (slot-4 sports MASTER) — this tool supports ``--asset-group
sports`` structurally but the sports owner verifies the CF-5 typed-reason relabel via the sports coverage oracle.

GROUNDED LAYOUT (probed 2026-06-07, gcloud storage ls — never a single shallow guess). All five buckets are
``instruments-store-{ag}-{env}-{pid}`` (``{ag}``: prediction→``pred``); the data-bearing trees:

  non-sports  ``instrument_availability/by_date/day={D}/venue={V}/instruments.parquet``
              FLAT: NO ``asset_group=``, NO ``pipeline_mode=``, NO ``data_type=`` segment. defi venue is the
              co-mingled ``{VENUE}-{CHAIN}`` form (e.g. ``CURVE-ETHEREUM``). The ``_index`` rows key on
              ``(date, venue, data_type)`` with **blank ``data_type``** (cefi/tradfi/defi) or
              ``prediction_canonical_question_group`` (pred).
  sports      ``sports_reference/by_date/day={D}/[pipeline_mode={M}/]entity={E}/league={L}/{folder}.parquet``
              ``entity=``/``league=`` (NOT ``venue=``/``chain=``); ``_index`` rows carry UPPERCASE typed
              ``data_type`` + already-typed ``error_reason`` (owned by the sports plan).

GROUNDED ``_index`` DATA-STATE (read the ACTUAL distribution — the v8 lesson; never a code constant):
  * cefi   30,803 rows : 100% v8 · no ``asset_group``/``source``/``transport`` col · blank ``pipeline_mode`` ·
                         blank ``data_type`` · **12,372 (40%) null ``capture_status`` — ALL ``instrument_count>0``**.
  * tradfi 20,388 rows : 99.2% v8 (170 v9) · 11,301 null ``capture_status`` (10,647 count>0, 654 count==0) ·
                         62 already-typed empties (EXPECTED_WEEKEND/HOLIDAY).
  * defi   125,242 rows: 100% v8 · 57,466 null ``capture_status`` (all count>0) · ``chain`` col populated.
  * pred   493 rows    : 100% v8 · all captured · ``data_type=prediction_canonical_question_group`` · LEAN schema
                         (no ``pipeline_mode``/``source``/``transport``/``asset_group`` cols → ADD them).

CANONICAL v9 TARGET (CF-1…CF-14):
  * CF-1  ``schema_version=9`` (every row).
  * CF-2  ``asset_group`` column (= the AG) + ``asset_group=`` path key; any legacy ``category`` column dropped.
  * CF-3  ``pipeline_mode`` column + ``pipeline_mode=`` path key. instruments-store rows are the IS catalogue's
          availability ledger → produced by instruments-service (``service_name`` confirms) → **reference
          provenance** ``batch_instruments_service`` for non-sports (NOT the market-data source — a Deribit
          *listing* is reference, not a Tardis tick). sports derives per (entity, data_type).
  * CF-4  ``source`` column (= ``instruments_service`` for non-sports; sports per-(venue,data_type) via
          ``default_source``).
  * CF-TRANSPORT ``transport`` column via ``default_transport_for_source`` (instruments_service → ``rest``).
  * CF-5  typed ``EmptyConfirmedReason`` on every empty cell (blank reason → ``SOURCE_RETURNED_ZERO``); the
          dishonest ``captured``-but-``instrument_count==0`` cells relabel → ``empty_confirmed`` (CF-11).
  * CF-7  canonical ``data_type`` (blank → ``instruments`` for non-sports; pred/sports keep their typed value).
  * CF-8  ``available_at`` per-row = ``written_at`` (the honest write-time poll proxy — no lookahead).
  * CF-9  env-split bucket via ``resolve_bucket_name`` (never an inline ``gs://`` f-string).
  * CF-10 ``capture_status`` derived HONESTLY from the writer's own ``instrument_count`` (never a silent
          placeholder): null/blank + count>0 → ``captured``; count==0 → ``empty_confirmed``. Object-backed
          capture is asserted by the writer's recorded count; a deeper object-presence phantom sweep against the
          NEW path shape is ``reconcile_phantom_manifest_rows_all.py``'s job (run AFTER, with covering templates).

The ``_index`` rewrite is **fully deterministic from the recorded columns** (no GCS object probe) → fully
unit-testable offline. The object-PATH rewrite (CF-2/CF-3 path keys) is a separate, idempotent, server-side
``gcs_copy_object`` phase (object-backed only) — additive; the legacy objects are deleted only at the gated E6
decommission (out of scope here).

GATING: DRY-RUN is the default and is NOT gated. ``--apply`` is G4-class — gated on the coordinator G0 +
``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05.md`` Phase 0 (writer code) GREEN + per-AG
pre-migration drain. RUN ON A VM when it touches prod GCS (``MANIFEST_PER_VM_SHARDS=true``; no fire-and-forget —
T+10min verify).

PHANTOM-SAFETY (HARD): before ANY ``--apply``, confirm the phantom-audit templates cover the NEW v9 path shape —
running a phantom ``--apply`` on uncovered templates flips real ``captured``→``attempted_failed``.

Perf contract (``codex/05-infrastructure/gcs-object-operations.md`` §): the object walk is parallel
(``ThreadPoolExecutor``), ``--workers``/``--start-date``/``--end-date`` shard the ``day=`` walk across VMs,
``gcs_copy_object`` is server-side (path-only, no egress), per-object ``try/except … continue`` isolation,
unbuffered progress (``python -u``, counter every ~2000).

Usage (run from the instruments-service repo root; the canonical bucket env vars
``DEPLOYMENT_ENV``/``GCP_PROJECT_ID`` must be set so ``resolve_bucket_name`` resolves the env-split bucket):
    # DRY-RUN (read-only) — report v8→v9 delta + sample rewritten rows for one AG (index-only shown):
    python -u scripts/migrate_instruments_store_v9.py --asset-group cefi --skip-objects
    # DRY-RUN incl. the object-path walk (lists objects, plans copies, writes nothing):
    python -u scripts/migrate_instruments_store_v9.py --asset-group cefi
    # APPLY (GATED, on a VM) — rewrite _index + copy objects to canonical paths:
    python -u scripts/migrate_instruments_store_v9.py --asset-group cefi --workers 64 --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from typing import Literal, cast

import pandas as pd
from unified_api_contracts import (
    EMPTY_CONFIRMED_REASONS,
    EmptyConfirmedReason,
    PipelineMode,
    default_source,
    default_transport_for_source,
    source_string_for,
)

logger = logging.getLogger("migrate_instruments_store_v9")

INDEX_REL = "_index/availability_index.parquet"
SNAPSHOT_DIR = "_index/snapshots"
AVAIL_PREFIX = "instrument_availability/by_date"
SPORTS_PREFIX = "sports_reference/by_date"
MANIFEST_SCHEMA_VERSION = 9

# Reference provenance — instruments-store rows are the IS catalogue's availability ledger (service_name=
# instruments-service). A venue *listing* is reference data, NOT a market-data tick, so the honest source is the
# instruments-service reference pipeline — NOT the AG market source (cefi→tardis etc., which is what
# derive_pipeline_mode_for_row returns and would be WRONG here).
REFERENCE_PIPELINE_MODE = PipelineMode.BATCH_INSTRUMENTS_SERVICE.value  # "batch_instruments_service"
REFERENCE_SOURCE = source_string_for(PipelineMode.BATCH_INSTRUMENTS_SERVICE) or "instruments_service"
REFERENCE_DATA_TYPE = "instruments"  # SOURCE_PRIORITY[("reference", "instruments")]
EMPTY_ZERO_REASON = EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value

VALID_ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")
NON_SPORTS = ("cefi", "defi", "tradfi", "prediction")

# Columns the canonical v9 instruments-store _index MUST carry (added with a blank default if absent — the lean
# pred schema lacks pipeline_mode/source/transport/asset_group).
_V9_TEXT_COLUMNS = (
    "data_type",
    "pipeline_mode",
    "source",
    "transport",
    "asset_group",
    "error_reason",
    "capture_status",
    "available_at",
)

_VALID_PIPELINE_MODES = frozenset(m.value for m in PipelineMode)


def resolve_instruments_store_bucket(asset_group: str) -> str:
    """Resolve the canonical env-split instruments-store bucket name (CF-9 — never an inline gs:// f-string).

    Mirrors instruments-service ``orchestrator.resolve_instruments_store_kind``: prediction uses the flat
    ``instruments-store-prediction`` kind; the other AGs use ``instruments-store`` + the AssetGroup.
    """
    from unified_trading_library.cloud_interface import resolve_bucket_name  # noqa: qg-deep-import

    if asset_group == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction", asset_group=None)
    # asset_group is validated against VALID_ASSET_GROUPS by argparse choices; narrow str → the bucket-naming Literal.
    ag = cast(Literal["cefi", "defi", "tradfi", "sports"], asset_group)
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=ag)


# ── PURE index transform (no I/O — the heart; fully unit-testable) ────────────────────────────────────────────


def _as_text(series: pd.Series) -> pd.Series:
    """Coerce a possibly-object/None/NA column to plain ``str`` with ``""`` for every missing form."""
    return series.astype("string").fillna("").astype(str)


def _ensure_v9_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add any missing v9 text column (blank default) + drop a legacy ``category`` column (CF-2)."""
    df = df.copy()
    if "category" in df.columns:
        df = df.drop(columns=["category"])
    for col in _V9_TEXT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
        df[col] = _as_text(df[col])
    if "instrument_count" not in df.columns:
        df["instrument_count"] = 0
    return df


def _honest_capture_status(df: pd.DataFrame, counts: pd.Series) -> dict[str, int]:
    """CF-10/CF-11: derive capture_status HONESTLY from the writer's recorded instrument_count (in place).

    null/blank + count>0 → captured (status simply never stamped); count==0 → empty_confirmed; an existing
    ``captured`` row with count==0 is dishonest (captured-but-empty) → relabel empty_confirmed. Never a silent
    placeholder. Returns stats.
    """
    cs = df["capture_status"]
    blank = cs.str.len() == 0
    cap_from_blank = blank & (counts > 0)
    empty_from_blank = blank & (counts <= 0)
    df.loc[cap_from_blank, "capture_status"] = "captured"
    df.loc[empty_from_blank, "capture_status"] = "empty_confirmed"
    cap_but_empty = (df["capture_status"] == "captured") & (counts <= 0)
    df.loc[cap_but_empty, "capture_status"] = "empty_confirmed"
    return {
        "null_capture_to_captured": int(cap_from_blank.sum()),
        "null_capture_to_empty": int(empty_from_blank.sum()),
        "captured_but_empty_to_empty": int(cap_but_empty.sum()),
    }


def _typed_empty_reasons(df: pd.DataFrame) -> dict[str, int]:
    """CF-5: every empty_confirmed cell carries a typed reason (blank → SOURCE_RETURNED_ZERO); a non-canonical
    legacy reason on an empty is re-typed; captured rows carry no reason. Returns stats."""
    empties = df["capture_status"] == "empty_confirmed"
    reason = df["error_reason"]
    untyped = empties & (~reason.isin(EMPTY_CONFIRMED_REASONS))
    df.loc[untyped, "error_reason"] = EMPTY_ZERO_REASON
    df.loc[df["capture_status"] == "captured", "error_reason"] = ""
    return {"empty_reason_typed": int(untyped.sum())}


def _reference_provenance(df: pd.DataFrame) -> None:
    """CF-3/CF-4/CF-TRANSPORT for the NON-sports reference buckets (uniform instruments-service provenance).

    Idempotent: a row that already carries a valid PipelineMode is preserved; only blank/invalid is set to the
    reference mode. source defaults to the mode's source string; transport from the source.
    """
    invalid_pm = ~df["pipeline_mode"].isin(_VALID_PIPELINE_MODES)
    df.loc[invalid_pm, "pipeline_mode"] = REFERENCE_PIPELINE_MODE
    blank_src = df["source"].str.len() == 0
    df.loc[blank_src, "source"] = df.loc[blank_src, "pipeline_mode"].map(_source_for_mode)
    df["transport"] = df["source"].map(lambda s: default_transport_for_source(s) if s else "")


def _source_for_mode(pipeline_mode: str) -> str:
    """source string for a pipeline_mode value (reference fallback when unmappable)."""
    try:
        return source_string_for(PipelineMode(pipeline_mode)) or REFERENCE_SOURCE
    except ValueError:
        return REFERENCE_SOURCE


def _sports_provenance(df: pd.DataFrame) -> None:
    """CF-3/CF-4/CF-TRANSPORT for the sports reference bucket — per (venue, data_type), via the sports SSOT.

    Cheap over millions of rows: resolve provenance once per UNIQUE (venue, data_type) pair, then map. The
    sports plan (slot-4) owns the precise CF-5 relabel; this keeps the structural columns honest.
    """
    from unified_trading_library import derive_pipeline_mode_for_row

    pairs = df[["venue", "data_type"]].drop_duplicates()
    pm_map: dict[tuple[str, str], str] = {}
    src_map: dict[tuple[str, str], str] = {}
    for venue, data_type in pairs.itertuples(index=False, name=None):
        key = (venue, data_type)
        mode = derive_pipeline_mode_for_row(venue, "sports", data_type)
        pm_value = mode.value if mode is not None else REFERENCE_PIPELINE_MODE
        pm_map[key] = pm_value
        src = default_source("sports", data_type) or _source_for_mode(pm_value)
        src_map[key] = src
    keys = list(zip(df["venue"], df["data_type"], strict=True))
    invalid_pm = ~df["pipeline_mode"].isin(_VALID_PIPELINE_MODES)
    df.loc[invalid_pm, "pipeline_mode"] = [pm_map[k] for k, bad in zip(keys, invalid_pm, strict=True) if bad]
    blank_src = df["source"].str.len() == 0
    df.loc[blank_src, "source"] = [src_map[k] for k, bad in zip(keys, blank_src, strict=True) if bad]
    df["transport"] = df["source"].map(lambda s: default_transport_for_source(s) if s else "")


def transform_index_v9(df: pd.DataFrame, asset_group: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Single-pass v9 canonicalisation of an instruments-store ``_index`` DataFrame. PURE (no I/O, no GCS).

    Returns ``(v9_df, stats)``. Idempotent: a second pass over a v9 frame is a no-op. Reads the ACTUAL
    ``schema_version`` distribution into ``stats`` (never trusts a constant — the v8 lesson).
    """
    n = len(df)
    sv_before = (
        pd.to_numeric(df["schema_version"], errors="coerce")
        if "schema_version" in df.columns
        else pd.Series([], dtype="float64")
    )
    stats: dict[str, int] = {
        "rows": n,
        "v8_before": int((sv_before == 8).sum()),
        "v9_before": int((sv_before == 9).sum()),
    }

    out = _ensure_v9_columns(df)
    # sports capture_status / error_reason / data_type are set authoritatively by the sports enumerator + coverage
    # oracle (instrument_count is NOT the sports captured-signal — 194k sports captured cells legitimately carry
    # instrument_count==0). So for sports this tool does STRUCTURAL columns only and preserves the semantic
    # columns for the sports owner (sports_manifest_canonicalisation_2026_06_01.md).
    is_sports = asset_group == "sports"

    # CF-1 schema_version
    out["schema_version"] = MANIFEST_SCHEMA_VERSION
    # CF-2 asset_group on rows
    blank_ag = out["asset_group"].str.len() == 0
    out.loc[blank_ag, "asset_group"] = asset_group
    stats["asset_group_set"] = int(blank_ag.sum())

    if not is_sports:
        counts = pd.to_numeric(out["instrument_count"], errors="coerce").fillna(0).astype("int64")
        # CF-7 canonical data_type (blank → reference 'instruments'; typed values preserved, incl. pred)
        blank_dt = out["data_type"].str.len() == 0
        out.loc[blank_dt, "data_type"] = REFERENCE_DATA_TYPE
        stats["data_type_set"] = int(blank_dt.sum())
        # CF-10/CF-11 honest capture_status (from the writer's recorded instrument_count)
        stats.update(_honest_capture_status(out, counts))
        # CF-5 typed empty reasons
        stats.update(_typed_empty_reasons(out))

    # CF-3/CF-4/CF-TRANSPORT provenance
    if is_sports:
        _sports_provenance(out)
    else:
        _reference_provenance(out)
    # CF-8 available_at = written_at (honest write-time poll proxy; no lookahead)
    if "written_at" in out.columns:
        blank_aa = out["available_at"].str.len() == 0
        out.loc[blank_aa, "available_at"] = _as_text(out.loc[blank_aa, "written_at"])
    stats["available_at_filled"] = int((out["available_at"].str.len() > 0).sum())
    stats["v9_after"] = n
    return out, stats


# ── PURE object-path transform (CF-2 / CF-3 path keys) ────────────────────────────────────────────────────────


def _kv(rel: str) -> dict[str, str]:
    return {s.split("=", 1)[0]: s.split("=", 1)[1] for s in rel.split("/") if "=" in s}


def canonical_object_rel(rel: str, asset_group: str) -> str | None:
    """Map a current instruments-store object rel-path → its canonical v9 rel, or ``None`` to skip.

    non-sports ``instrument_availability/by_date/day={D}/venue={V}/instruments.parquet``
        → insert ``pipeline_mode=batch_instruments_service/asset_group={ag}/`` after ``day={D}/`` (LEFT of
          ``venue=``; ``pipeline_mode`` left of ``asset_group`` per the canonical rule).
    sports     ``sports_reference/by_date/day={D}/entity={E}/league={L}/{folder}.parquet``
        → insert ``pipeline_mode={M}/`` after ``day={D}/`` (preserve entity=/league=), M from the entity SSOT.

    Idempotent: a rel already carrying ``/pipeline_mode=`` returns unchanged. Object-backed only (CF-10) —
    caller lists real objects.
    """
    if not rel.endswith(".parquet"):
        return None
    if "/pipeline_mode=" in rel:
        return rel  # already canonical (idempotent no-op)
    kv = _kv(rel)
    day = kv.get("day")
    if not day:
        return None
    if asset_group == "sports":
        if not rel.startswith(SPORTS_PREFIX + "/"):
            return None
        mode = _sports_path_mode(kv.get("entity", ""))
        return rel.replace(f"day={day}/", f"day={day}/pipeline_mode={mode}/", 1)
    if not rel.startswith(AVAIL_PREFIX + "/"):
        return None
    if not kv.get("venue"):
        return None
    insert = f"pipeline_mode={REFERENCE_PIPELINE_MODE}/asset_group={asset_group}/"
    return rel.replace(f"day={day}/", f"day={day}/{insert}", 1)


def _sports_path_mode(entity: str) -> str:
    """pipeline_mode value for a sports ``entity=`` folder (sports plan SSOT; reference fallback)."""
    from unified_trading_library import derive_pipeline_mode_for_row

    mode = derive_pipeline_mode_for_row("", "sports", entity) if entity else None
    return mode.value if mode is not None else REFERENCE_PIPELINE_MODE


# ── I/O — index ───────────────────────────────────────────────────────────────────────────────────────────────


def _write_parquet_obj(df: pd.DataFrame) -> io.BytesIO:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


def _snapshot_and_write_index(storage, bucket: str, raw_old: bytes, new_df: pd.DataFrame, stamp: str) -> None:
    """Snapshot the pre-migration _index then write the v9 frame back (apply-only)."""
    snap_rel = f"{SNAPSHOT_DIR}/pre_v9_migration_{stamp}.parquet"
    storage.upload_from_file_obj(bucket, snap_rel, io.BytesIO(raw_old), content_type="application/octet-stream")
    logger.info("snapshot written: gs://%s/%s", bucket, snap_rel)
    storage.upload_from_file_obj(bucket, INDEX_REL, _write_parquet_obj(new_df), content_type="application/octet-stream")
    logger.info("v9 _index written: gs://%s/%s (%d rows)", bucket, INDEX_REL, len(new_df))


def _log_index_report(stats: dict[str, int], new_df: pd.DataFrame, asset_group: str) -> None:
    logger.info("=== %s _index transform === %s", asset_group, stats)
    for col in ("schema_version", "capture_status", "data_type", "pipeline_mode", "source", "transport"):
        if col in new_df.columns:
            dist = new_df[col].value_counts(dropna=False)
            logger.info("  %s: %s", col, dict(list(dist.items())[:8]))
    sample = new_df.head(2).to_dict(orient="records")
    for row in sample:
        keep = {
            k: row[k]
            for k in (
                "date",
                "venue",
                "data_type",
                "capture_status",
                "schema_version",
                "asset_group",
                "pipeline_mode",
                "source",
                "transport",
            )
            if k in row
        }
        logger.info("  sample: %s", keep)


# ── I/O — object paths ────────────────────────────────────────────────────────────────────────────────────────


def _iter_days(start: str, end: str) -> Iterable[str]:
    cur, last = date.fromisoformat(start), date.fromisoformat(end)
    while cur <= last:
        yield cur.isoformat()
        cur += timedelta(days=1)


def _list_day_objects(storage, bucket: str, tree: str, day: str) -> list[str]:
    prefix = f"{tree}/day={day}/"
    return [b.name for b in storage.list_blobs(bucket, prefix=prefix) if b.name.endswith(".parquet")]


def _copy_object(bucket: str, rel: str, new_rel: str | None, *, dry_run: bool) -> tuple[int, int]:
    """(planned, moved). Idempotent server-side copy; skip if already canonical or dest exists."""
    from unified_trading_library.cloud_interface import gcs_copy_object, gcs_describe_object  # noqa: qg-deep-import

    if new_rel is None or new_rel == rel:
        return (0, 0)
    src_uri = f"gs://{bucket}/{rel}"
    dst_uri = f"gs://{bucket}/{new_rel}"
    if dry_run:
        return (1, 0)
    try:
        if gcs_describe_object(dst_uri) is not None:
            return (1, 0)  # idempotent
        gcs_copy_object(src_uri, dst_uri)
    except Exception as exc:  # per-object isolation (perf contract)
        logger.warning("copy failed %s -> %s: %s", src_uri, dst_uri, exc)
        return (1, 0)
    return (1, 1)


def _rewrite_objects(
    storage, bucket: str, asset_group: str, days: list[str], workers: int, dry_run: bool
) -> tuple[int, int]:
    tree = SPORTS_PREFIX if asset_group == "sports" else AVAIL_PREFIX
    objs: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 32))) as pool:
        for found in pool.map(lambda d: _list_day_objects(storage, bucket, tree, d), days):
            objs.extend(found)
    logger.info("%s object walk: %d objects (%s tree, dry_run=%s)", bucket, len(objs), tree, dry_run)

    planned = moved = done = samples = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = [
            pool.submit(_copy_object, bucket, o, canonical_object_rel(o, asset_group), dry_run=dry_run) for o in objs
        ]
        for fut in as_completed(futs):
            p, m = fut.result()
            planned += p
            moved += m
            done += 1
            if dry_run and p and samples < 3:
                samples += 1
                ex = next((o for o in objs if canonical_object_rel(o, asset_group) not in (None, o)), None)
                if ex:
                    logger.info("  PLAN %s\n    -> %s", ex, canonical_object_rel(ex, asset_group))
            if done % 2000 == 0:
                logger.info("  objects: %d/%d (planned=%d moved=%d)", done, len(objs), planned, moved)
    logger.info("%s object walk: planned=%d moved=%d", bucket, planned, moved)
    return planned, moved


# ── main ──────────────────────────────────────────────────────────────────────────────────────────────────────


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def migrate(asset_group: str, *, start_date: str, end_date: str, workers: int, apply: bool, skip_objects: bool) -> int:
    from unified_trading_library.cloud_interface import get_storage_client  # noqa: qg-deep-import

    dry_run = not apply
    bucket = resolve_instruments_store_bucket(asset_group)
    logger.info(
        "=== migrate_instruments_store_v9 === ag=%s bucket=%s apply=%s skip_objects=%s",
        asset_group,
        bucket,
        apply,
        skip_objects,
    )
    storage = get_storage_client()

    # Phase A — _index v9 rewrite (deterministic).
    raw_old = storage.download_bytes(bucket, INDEX_REL)
    df = pd.read_parquet(io.BytesIO(raw_old))
    new_df, stats = transform_index_v9(df, asset_group)
    _log_index_report(stats, new_df, asset_group)
    if apply:
        _snapshot_and_write_index(storage, bucket, raw_old, new_df, _utc_stamp())
    else:
        logger.info("DRY-RUN — _index NOT written (use --apply on a VM, GATED).")

    # Phase B — object-path v9 rewrite (CF-2/CF-3 path keys; additive server-side copy).
    if not skip_objects:
        days = list(_iter_days(start_date, end_date))
        _rewrite_objects(storage, bucket, asset_group, days, workers, dry_run)
    else:
        logger.info("--skip-objects — object-path rewrite skipped (index-only run).")

    logger.info("=== DONE %s (%s) ===", asset_group, "APPLIED" if apply else "DRY-RUN")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="instruments-store v9 single-walk canonicaliser (AG-parametric)")
    ap.add_argument("--asset-group", required=True, choices=VALID_ASSET_GROUPS)
    ap.add_argument("--start-date", default="2019-01-01", help="object-walk day shard start (inclusive)")
    ap.add_argument("--end-date", default=date.today().isoformat(), help="object-walk day shard end (inclusive)")
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--apply", action="store_true", help="GATED (G4): write the _index + copy objects on a VM")
    ap.add_argument("--skip-objects", action="store_true", help="index-only run (skip the object-path walk)")
    args = ap.parse_args(argv)
    return migrate(
        args.asset_group,
        start_date=args.start_date,
        end_date=args.end_date,
        workers=args.workers,
        apply=args.apply,
        skip_objects=args.skip_objects,
    )


if __name__ == "__main__":
    sys.exit(main())
