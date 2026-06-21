#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: sports legacy batch_instruments_service rows == 0 in the live _index
"""Canonicalise legacy ``batch_instruments_service`` SPORTS manifest rows.

AUDIT (2026-06-21): the consolidated sports IS manifest
``instruments-store-sports-prd/_index/availability_index.parquet`` (+ its per-VM
shards ``_index/per_vm/*.parquet``) carries ~2.2M legacy rows written 2026-04-23..28
with the OLD generic ``pipeline_mode='batch_instruments_service'`` (pre source-aware
G0 standardisation) and a BLANK ``error_reason`` on ``capture_status='empty_confirmed'``
cells. Current/correct runs write source-aware ``pipeline_mode='batch_<source>'`` (e.g.
``batch_api_football``) with TYPED ``EmptyConfirmedReason`` reasons. Two rooted problems:

1. **Blank reasons** — ``empty_confirmed`` legacy cells carry ``error_reason=''``
   (non-canonical; should be a typed ``EmptyConfirmedReason``). 100% of the blank-reason
   ``empty_confirmed`` rows are ``batch_instruments_service`` (verified 2026-06-21).
2. **Double-counting** — the manifest consolidator dedups last-write-wins BY MANIFEST KEY
   ``(date, venue, data_type, service_name + optional dims)``; the SAME logical cell exists
   twice (a legacy ``captured`` twin + a newer ``empty_confirmed`` twin) → captured-count
   inflation.

   DIAGNOSIS CORRECTION (2026-06-21 — verified against the live index, supersedes the
   original "pipeline_mode in the key" framing): ``pipeline_mode`` is NOT in the dedup key,
   so re-stamping it ALONE collapses NOTHING. The REAL splitter is a **NULL-vs-empty-string
   mismatch in the OPTIONAL dedup columns**: the legacy rows carry parquet NULL where the
   canonical/new rows carry ``""``; the consolidator coalesces NULL to a distinct sentinel
   (``__UTL_CONSOLIDATOR_NULL_4e8a2__``) so NULL != "" → the twins never merge. Normalising
   the LEGACY rows' NULL optional dims to ``""`` (step 1b) is what actually collapses them.

THE FIX (unified — canonicalise the legacy rows so they share the dedup key with their newer
twins; the consolidator then collapses them last-write-wins, fixing the double-count;
surviving rows carry typed reasons):

  * **Re-stamp ``pipeline_mode``** ``batch_instruments_service`` → ``batch_<source>`` where
    ``<source>`` is the row's canonical vendor, resolved (in priority order) from the row's
    own ``source`` column → the UAC SSOT ``get_source_for_data_type(data_type)``. When the
    source is genuinely blank AND the data_type is not source-attributable (the catalog/ref
    types LEAGUES/TRANSFERMARKT_LEAGUES/SFI_LEAGUES/VENUES/SFI_STANDINGS), the row is LEFT
    AS-IS (honest — never fabricate a ``batch_<>`` mode) and REPORTED. This canonicalises the
    G0 source-aware vocabulary (aligns legacy rows with what live VMs write) but is NOT itself
    the double-count fix.
  * **Normalise legacy optional-dim NULL -> ""** (step 1b) — the actual double-count fix
    (see DIAGNOSIS CORRECTION above). Scoped to legacy rows only (convergent with the
    canonical ``""`` convention); the broader NULL/"" inconsistency on NON-legacy rows is a
    separate canonicalisation tracked in the issue doc filed alongside this script.
  * **Fill blank ``error_reason``** on ``capture_status=='empty_confirmed'`` FIXTURES cells
    → ``EmptyConfirmedReason.EXPECTED_NO_FIXTURE`` (the canonical reason a FIXTURES cell is
    empty: no fixture scheduled; the dominant reason the non-blank FIXTURES cells already
    use). For NON-FIXTURES sports data_types the empty-cause is genuinely AMBIGUOUS — the
    non-blank cells of those data_types use a MIX of ``EXPECTED_NO_FIXTURE`` /
    ``EXPECTED_INSTRUMENT_NOT_LISTED`` / ``SOURCE_RETURNED_ZERO`` (no single canonical reason
    is derivable), so those blank reasons are LEFT untouched and REPORTED per the task's
    "if genuinely ambiguous, leave it and REPORT the count".

ROW-PRESERVING (never adds/drops a row — only re-stamps ``pipeline_mode``, normalises legacy
optional-dim NULL->"", and fills blank FIXTURES ``error_reason``); the consolidator collapses
the now-key-sharing duplicates on its next cycle. IDEMPOTENT — a second run finds 0 legacy
rows / 0 blank FIXTURES reasons / 0 legacy NULL optional dims → no write.

SCOPE: the consolidated ``_index`` ONLY. Per-VM shards are skipped — the live sports bucket's
only legacy shard is the pre-v9 ``_legacy_seed.parquet`` (24 cols, NO ``pipeline_mode``/
``source``), already represented in the canonical 41-col ``_index``; mutating it would drift
its schema (mirrors ``populate_sports_is_index_v9_2026_06_19``, which also touched only
``_index``). A future source-aware shard (carrying ``pipeline_mode``) IS processed.

DRY-RUN by default (prints legacy counts, the re-mappings, and the projected post-dedup
distinct-count — WITHOUT writing). ``--apply`` snapshots the live ``_index`` then writes the
consolidated index back.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_api_contracts.canonical.crosscutting.honest_coverage import EmptyConfirmedReason
from unified_api_contracts.canonical.domain.sports.league_data import get_source_for_data_type
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("canon_sports_legacy_pm")

_CANONICAL_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm"
# The OLD generic IS producer mode being canonicalised away (pre G0 source-aware).
_LEGACY_MODE = "batch_instruments_service"
# Known sports vendor tokens (lowercase) — a row's own ``source`` column already in this set
# is an acceptable ``batch_<source>`` value; mirrors populate_sports_is_index_v9.
_KNOWN_VENDORS = frozenset(
    {
        "api_football",
        "footystats",
        "understat",
        "transfermarkt",
        "soccer_football_info",
        "open_meteo",
        "odds_api",
        "mdps_odds_horizon_bucket",
    }
)
# Consolidator dedup key SSOT (unified_trading_library.manifest_consolidator):
#   _BASE_DEDUP_COLS + the optional dims present in the schema. pipeline_mode + source are
#   NOT in the key — that is WHY old+new co-exist, and why re-stamping collapses them.
_BASE_DEDUP_COLS = ("date", "venue", "data_type", "service_name")
_OPTIONAL_DEDUP_COLS = (
    "timeframe",
    "league_id",
    "chain",
    "instrument_type",
    "underlying",
    "feature_group",
    "model_family",
    "training_period",
    "strategy_id",
    "client_id",
    "instruction_type",
    "instrument_id",
)
# Only FIXTURES has a single canonical empty reason. Other sports data_types are ambiguous
# (mixed EXPECTED_NO_FIXTURE / EXPECTED_INSTRUMENT_NOT_LISTED / SOURCE_RETURNED_ZERO in the
# non-blank cells) — leave + report.
_FIXTURES_BLANK_REASON = EmptyConfirmedReason.EXPECTED_NO_FIXTURE.value


def _resolve_source(data_type: str, source: str) -> str:
    """Canonical vendor for a (data_type, source), or "" when not attributable.

    Priority: the row's own ``source`` if it is already a known vendor token →
    the UAC SSOT ``get_source_for_data_type``. "" means the row is a catalog/ref
    type with no source attribution (LEAGUES/VENUES/…); caller leaves it as-is.
    """
    s = (source or "").strip().lower()
    if s in _KNOWN_VENDORS:
        return s
    if data_type:
        mapped = get_source_for_data_type(data_type) or get_source_for_data_type(data_type.upper())
        if mapped:
            return mapped
    return ""


def _dedup_cols(columns: list[str]) -> list[str]:
    """The consolidator dedup key for this schema (base + present optional dims)."""
    return list(_BASE_DEDUP_COLS) + [c for c in _OPTIONAL_DEDUP_COLS if c in columns]


def canonicalize(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Re-stamp legacy pipeline_mode + fill blank FIXTURES empty_confirmed reasons.

    Returns the modified frame + a stats dict. ROW-PRESERVING.
    """
    out = df.copy()
    n = len(out)
    for col in ("pipeline_mode", "source", "data_type", "capture_status", "error_reason"):
        if col not in out.columns:
            out[col] = ""

    pm = out["pipeline_mode"].astype("string").fillna("")
    src = out["source"].astype("string").fillna("")
    dt = out["data_type"].astype("string").fillna("")
    legacy_mask = pm == _LEGACY_MODE

    # --- 1. Re-stamp pipeline_mode for legacy rows where source is resolvable. ---
    resolved = [
        _resolve_source(d, s) if lg else ""
        for d, s, lg in zip(dt.tolist(), src.tolist(), legacy_mask.tolist(), strict=True)
    ]
    resolved_ser = pd.Series(resolved, index=out.index, dtype="string")
    restamp_mask = legacy_mask & (resolved_ser.str.len() > 0)
    new_pm = pm.copy()
    new_pm.loc[restamp_mask] = "batch_" + resolved_ser.loc[restamp_mask]
    out["pipeline_mode"] = new_pm

    legacy_unresolved = int((legacy_mask & ~restamp_mask).sum())

    # --- 1b. Normalise legacy optional-dim NULL -> "" (the ACTUAL double-count fix). ---
    # DIAGNOSIS CORRECTION (2026-06-21): re-stamping pipeline_mode ALONE does NOT collapse
    # the duplicates — pipeline_mode is NOT in the consolidator dedup key. The real splitter
    # is a NULL-vs-empty-string mismatch in the OPTIONAL dedup columns: legacy rows carry
    # python ``None`` (parquet NULL) where the canonical/new rows carry ``""``. The
    # consolidator coalesces NULL to a distinct sentinel (``__UTL_CONSOLIDATOR_NULL_4e8a2__``)
    # so NULL != "" → the same logical cell survives twice (e.g. FIXTURES 2020-07-01/EPL:
    # legacy ``captured`` twin + new ``empty_confirmed`` twin). Normalising the LEGACY rows'
    # NULL optional dims to ``""`` makes them share the canonical dedup key with their new
    # twins → the consolidator collapses them last-write-wins (newer row, by attempted_at/
    # written_at). Scoped to legacy rows ONLY (convergent — matches what live VMs write);
    # the broader NULL/"" inconsistency on NON-legacy rows is a separate canonicalisation
    # (see the issue doc filed alongside this script).
    opt_present = [c for c in _OPTIONAL_DEDUP_COLS if c in out.columns]
    legacy_null_normalised = 0
    for col in opt_present:
        is_null = out[col].isna()
        fill_mask = legacy_mask & is_null
        cnt = int(fill_mask.sum())
        if cnt:
            legacy_null_normalised += cnt
            # object-dtype assignment so an existing string column accepts "".
            colvals = out[col].astype("object")
            colvals[fill_mask] = ""
            out[col] = colvals

    # --- 2. Fill blank error_reason on empty_confirmed FIXTURES. ---
    er = out["error_reason"].astype("string").fillna("").str.strip()
    cs = out["capture_status"].astype("string").fillna("")
    ec_blank = (cs == "empty_confirmed") & (er == "")
    fixtures_blank = ec_blank & (dt == "FIXTURES")
    non_fixtures_blank = ec_blank & (dt != "FIXTURES")
    new_er = out["error_reason"].astype("string").fillna("")
    new_er.loc[fixtures_blank] = _FIXTURES_BLANK_REASON
    out["error_reason"] = new_er

    stats = {
        "rows": n,
        "legacy_rows_found": int(legacy_mask.sum()),
        "pipeline_mode_restamped": int(restamp_mask.sum()),
        "legacy_unresolved_left_asis": legacy_unresolved,
        "legacy_null_optdim_normalised_cells": legacy_null_normalised,
        "ec_blank_reason_total": int(ec_blank.sum()),
        "fixtures_blank_reason_filled": int(fixtures_blank.sum()),
        "non_fixtures_blank_reason_left": int(non_fixtures_blank.sum()),
    }
    return out, stats


def _projected_distinct(df: pd.DataFrame) -> tuple[int, int]:
    """(rows, distinct-by-dedup-key) for the canonicalised frame — projects the
    consolidator collapse.

    Mirrors the consolidator's NULL-sentinel semantics: ``coalesce(col, sentinel)`` maps a
    parquet NULL to a distinct sentinel while an empty string ``""`` stays ``""`` — so NULL
    and "" are intentionally NOT equal (that mismatch is exactly the double-count we fix).
    pipeline_mode + source are NOT in the dedup key, so the re-stamp itself never changes
    this count; the NULL->"" optional-dim normalisation is what collapses the twins.
    """
    cols = [c for c in _dedup_cols(list(df.columns)) if c in df.columns]
    # NULL -> sentinel (consolidator coalesce); "" stays "" (drop_duplicates keeps them distinct).
    key = df[cols].astype("object").where(~df[cols].isna(), "__UTL_CONSOLIDATOR_NULL_4e8a2__").astype("string")
    return len(df), len(key.drop_duplicates())


def _process_blob(
    fs: gcsfs.GCSFileSystem, bucket: str, blob: str, *, apply: bool, stamp: str, required: bool
) -> dict[str, int]:
    path = f"{bucket}/{blob}"
    logger.info("Reading gs://%s", path)
    try:
        with fs.open(path, "rb") as fh:
            df = pd.read_parquet(fh)
    except (FileNotFoundError, OSError) as exc:
        if required:
            raise
        # A per-VM shard listed by ls() but unreadable (stale cache / 0-byte / deleted) —
        # the consolidator skips unreadable shards (``_download_valid_parquets``); so do we.
        logger.warning("  [%s] SKIP — unreadable shard: %s", blob, exc)
        return {"skipped_unreadable_shards": 1}

    # A per-VM shard with NO ``pipeline_mode`` column is a legacy pre-v9 (pre source-aware)
    # shard already represented in the canonical _index — DON'T mutate it (writing the
    # missing column back drifts its schema). The canonical _index is the authoritative
    # target; skip + report.
    if not required and "pipeline_mode" not in df.columns:
        logger.warning(
            "  [%s] SKIP — legacy pre-v9 shard (no pipeline_mode column, %d cols); already in canonical _index.",
            blob,
            len(df.columns),
        )
        return {"skipped_legacy_pre_v9_shards": 1}
    out, stats = canonicalize(df)

    if stats["legacy_rows_found"] == 0 and stats["fixtures_blank_reason_filled"] == 0:
        logger.info("  [%s] nothing to canonicalise (idempotent no-op).", blob)
        return stats

    rows, distinct = _projected_distinct(out)
    # FIXTURES captured-distinct sanity: after collapse, captured FIXTURES rows ~= distinct.
    fx = out[out["data_type"].astype("string") == "FIXTURES"]
    fx_captured = fx[fx["capture_status"].astype("string") == "captured"]
    fx_dist_cols = [c for c in _dedup_cols(list(out.columns)) if c in fx_captured.columns]
    fx_cap_distinct = (
        len(fx_captured[fx_dist_cols].astype("string").fillna("__NULL__").drop_duplicates())
        if len(fx_captured)
        else 0
    )

    for k, v in stats.items():
        logger.info("  [%s] %s = %d", blob, k, v)
    logger.info("  [%s] post-canon rows=%d projected_distinct_by_dedup_key=%d", blob, rows, distinct)
    logger.info("  [%s] FIXTURES captured rows=%d captured-distinct=%d", blob, len(fx_captured), fx_cap_distinct)
    logger.info(
        "  [%s] pipeline_mode dist (post): %s",
        blob,
        out["pipeline_mode"].astype("string").fillna("<NA>").value_counts(dropna=False).head(12).to_dict(),
    )

    if not apply:
        return stats

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    # snapshot the canonical _index ONCE (per-VM shards are tiny + reconstructable; snapshot
    # only the consolidated index, mirroring the populate-script snapshot discipline).
    if blob == _CANONICAL_BLOB:
        snap = f"{bucket}/_index/snapshots/pre_pipeline_mode_canon_{stamp}.parquet"
        sbuf = io.BytesIO()
        df.to_parquet(sbuf, index=False)
        sbuf.seek(0)
        with fs.open(snap, "wb") as fh:
            fh.write(sbuf.getvalue())
        logger.info("  Snapshot → gs://%s", snap)
    with fs.open(path, "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("  [%s] APPLIED (%d rows written back).", blob, stats["rows"])
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply", action="store_true", help="Write the canonicalised index + shards back (snapshots first)."
    )
    args = p.parse_args(argv)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    fs = gcsfs.GCSFileSystem()
    stamp = datetime.now(UTC).strftime("%Y%m%d")

    # SCOPE: the consolidated _index ONLY. Per-VM shards are NOT canonicalised here:
    # the live sports bucket's only shard is the legacy v8 ``_legacy_seed.parquet`` (24
    # columns — NO pipeline_mode/source/fixture_id at all), whose content is already
    # represented in the canonical 41-column _index. Writing the missing columns back into
    # that 24-col shard would drift its schema and, on the next consolidator merge
    # (union_by_name → NULL fills), risk re-introducing the very NULL/blank divergence we
    # remove. So we treat the consolidated _index as the authoritative target (mirrors
    # populate_sports_is_index_v9_2026_06_19, which also touched only _index). A future
    # source-aware shard (>= the v9 schema, carrying pipeline_mode) WILL be processed — the
    # guard below skips a shard lacking pipeline_mode and reports it.
    blobs = [_CANONICAL_BLOB]
    try:
        shards = [s.split(f"{bucket}/", 1)[1] for s in fs.ls(f"{bucket}/{_PER_VM_PREFIX}") if s.endswith(".parquet")]
        blobs.extend(sorted(shards))
        logger.info("Per-VM shards found: %d (processed only if source-aware schema present)", len(shards))
    except FileNotFoundError:
        logger.info("No %s dir — consolidated index only.", _PER_VM_PREFIX)

    totals: dict[str, int] = {}
    for blob in blobs:
        s = _process_blob(fs, bucket, blob, apply=args.apply, stamp=stamp, required=(blob == _CANONICAL_BLOB))
        for k, v in s.items():
            totals[k] = totals.get(k, 0) + v

    logger.info("=== TOTALS across %d blob(s) ===", len(blobs))
    for k, v in totals.items():
        logger.info("  %s = %d", k, v)
    if not args.apply:
        logger.info("DRY RUN — live _index + shards untouched. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
