#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + GCS orphan-sweep = 0 for oos reclassified targets
"""reclassify_oos_sports_expected_unattempted_2026_06_24.py — fix out-of-scope understat/footystats cells.

ROOT CAUSE (audit 2026-06-21 / task 064): the sports MTDS pre-flight seeded
``expected_unattempted`` for EVERY (source, league_id, data_type, date) triple
in the expected universe, including combinations that ``is_expected_for_source``
(UAC ``registry.sports_per_source_rules``) knows are permanently out of scope:

* ``understat`` — only covers the ~50 leagues in ``UNDERSTAT_COVERED_LEAGUES``;
  rows for all other leagues are mis-seeded as ``expected_unattempted`` (inflates
  the completion-% denominator).
* ``footystats`` — coverage start + season window gating; cells before the
  coverage start or outside the active season window are mis-seeded.

Additionally, live MTDS writes that hit the guard correctly emit
``attempted_failed`` instead of ``empty_confirmed`` for some of these cells when
the source guard fires mid-run — those phantom failures also need correcting.

FIX (this script): for each row where
  ``source in {"understat", "footystats"}``
  AND ``capture_status in {"expected_unattempted", "attempted_failed"}``:
  call ``is_expected_for_source(source, league_id, day, data_type=data_type)``
  — if ``(False, reason)``: flip to ``empty_confirmed(error_reason=reason)``.

IDEMPOTENT: rows already ``empty_confirmed`` with a canonical OOS reason are
skipped. ``captured`` rows and ``empty_confirmed`` rows with a non-OOS reason
are never touched.

SINGLE-WALK: one parquet read → classify pass → one write-back (dry-run does
not write the canonical index; writes only a GCS audit projection).

Usage::

    # Scan-only (default) — logs counts + writes audit projection to GCS
    DEPLOYMENT_ENV_SHORT=prd GCP_PROJECT_ID=central-element-323112 \\
    PROJECT_ID=central-element-323112 \\
    .venv/bin/python scripts/reclassify_oos_sports_expected_unattempted_2026_06_24.py

    # Apply (after CSV review + consolidator quiesced)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=oos-reclassify-$(date +%s) \\
    DEPLOYMENT_ENV_SHORT=prd GCP_PROJECT_ID=central-element-323112 \\
    PROJECT_ID=central-element-323112 \\
    .venv/bin/python scripts/reclassify_oos_sports_expected_unattempted_2026_06_24.py --apply

    # Restrict to one source (debug)
    .venv/bin/python scripts/reclassify_oos_sports_expected_unattempted_2026_06_24.py \\
        --sources understat
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import gcsfs
import pandas as pd
from unified_api_contracts.registry.sports_per_source_rules import is_expected_for_source
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MANIFEST_BLOB = "_index/availability_index.parquet"

_TARGET_SOURCES = frozenset({"understat", "footystats"})
_TARGET_STATUSES = frozenset({"expected_unattempted", "attempted_failed"})

# EmptyConfirmedReasons produced by is_expected_for_source — used for idempotent skip.
_OOS_REASONS = frozenset(
    {
        "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE",
        "EXPECTED_PRE_SOURCE_COVERAGE_START",
        "EXPECTED_PRE_SEASON",
        "EXPECTED_POST_SEASON",
        "EXPECTED_OUTSIDE_TRANSFER_WINDOW",
    }
)


def _parse_day(raw: object) -> date | None:
    """Convert a manifest date cell (str YYYY-MM-DD, datetime, or date) to date."""
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
    return None


def _build_flip_mask(df: pd.DataFrame, target_sources: frozenset[str]) -> pd.Series:
    """Return a boolean mask of rows to flip to empty_confirmed."""
    cs = df["capture_status"].astype("string").fillna("")
    src = df["source"].astype("string").fillna("")
    lg = df["league_id"].astype("string").fillna("")
    dt_col = df["data_type"].astype("string").fillna("")
    reason_col = df["error_reason"].astype("string").fillna("") if "error_reason" in df.columns else pd.Series("", index=df.index)

    # Candidate: right source + status, and NOT already correctly classified.
    candidate = (
        src.isin(target_sources)
        & cs.isin(_TARGET_STATUSES)
    ) | (
        src.isin(target_sources)
        & (cs == "empty_confirmed")
        & ~reason_col.isin(_OOS_REASONS)
    )

    # For already-empty_confirmed with OOS reason: skip (idempotent).
    already_correct = src.isin(target_sources) & (cs == "empty_confirmed") & reason_col.isin(_OOS_REASONS)
    candidate = candidate & ~already_correct

    cand_idx = df.index[candidate.values]
    if len(cand_idx) == 0:
        return pd.Series(False, index=df.index)

    # Build unique (source, league_id, data_type, date) tuples and call once per combo.
    date_col = df["date"] if "date" in df.columns else pd.Series(None, index=df.index)

    unique_keys: dict[tuple[str, str, str, date | None], tuple[bool, str | None]] = {}
    for idx in cand_idx:
        s = str(src.iloc[df.index.get_loc(idx)])
        lg_val = str(lg.iloc[df.index.get_loc(idx)])
        d = str(dt_col.iloc[df.index.get_loc(idx)])
        raw_date = date_col.iloc[df.index.get_loc(idx)]
        day = _parse_day(raw_date)
        key = (s, lg_val, d, day)
        if key not in unique_keys:
            if day is None:
                unique_keys[key] = (True, None)
            else:
                unique_keys[key] = is_expected_for_source(s, lg_val, day, data_type=d if d else None)

    mask = pd.Series(False, index=df.index)
    for idx in cand_idx:
        s = str(src.iloc[df.index.get_loc(idx)])
        lg_val = str(lg.iloc[df.index.get_loc(idx)])
        d = str(dt_col.iloc[df.index.get_loc(idx)])
        raw_date = date_col.iloc[df.index.get_loc(idx)]
        day = _parse_day(raw_date)
        key = (s, lg_val, d, day)
        is_exp, _reason = unique_keys[key]
        if not is_exp:
            mask.iloc[df.index.get_loc(idx)] = True

    return mask


def _get_flip_reason(df: pd.DataFrame, mask: pd.Series, target_sources: frozenset[str]) -> pd.Series:
    """For each True cell in mask, return the OOS reason from is_expected_for_source."""
    src = df["source"].astype("string").fillna("")
    lg = df["league_id"].astype("string").fillna("")
    dt_col = df["data_type"].astype("string").fillna("")
    date_col = df["date"] if "date" in df.columns else pd.Series(None, index=df.index)

    reasons = pd.Series("", index=df.index, dtype="string")
    cand_idx = df.index[mask.values]

    unique_keys: dict[tuple[str, str, str, date | None], tuple[bool, str | None]] = {}
    for idx in cand_idx:
        s = str(src.iloc[df.index.get_loc(idx)])
        lg_val = str(lg.iloc[df.index.get_loc(idx)])
        d = str(dt_col.iloc[df.index.get_loc(idx)])
        raw_date = date_col.iloc[df.index.get_loc(idx)]
        day = _parse_day(raw_date)
        key = (s, lg_val, d, day)
        if key not in unique_keys:
            if day is None:
                unique_keys[key] = (False, "EXPECTED_PRE_SOURCE_COVERAGE_START")
            else:
                unique_keys[key] = is_expected_for_source(s, lg_val, day, data_type=d if d else None)

    for idx in cand_idx:
        s = str(src.iloc[df.index.get_loc(idx)])
        lg_val = str(lg.iloc[df.index.get_loc(idx)])
        d = str(dt_col.iloc[df.index.get_loc(idx)])
        raw_date = date_col.iloc[df.index.get_loc(idx)]
        day = _parse_day(raw_date)
        key = (s, lg_val, d, day)
        _, reason = unique_keys[key]
        reasons.iloc[df.index.get_loc(idx)] = reason or ""

    return reasons


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write flipped manifest back. Default: dry-run.")
    p.add_argument(
        "--sources",
        type=str,
        default=",".join(sorted(_TARGET_SOURCES)),
        help=f"Comma-separated sources to target (default: {','.join(sorted(_TARGET_SOURCES))}).",
    )
    p.add_argument(
        "--max-flips",
        type=int,
        default=500_000,
        help="Halt safety cap (default 500k).",
    )
    args = p.parse_args()

    if not os.environ.get("DEPLOYMENT_ENV_SHORT"):
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing — would resolve wrong bucket.")
        return 1

    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not os.environ.get("VM_NAME")):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> per the manifest "
            "concurrency principle. Refusing to mutate without shard isolation."
        )
        return 1

    target_sources = frozenset(s.strip() for s in args.sources.split(",") if s.strip())
    if not target_sources.issubset(_TARGET_SOURCES):
        logger.error("--sources contains unknown source(s): %s", target_sources - _TARGET_SOURCES)
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    fs = gcsfs.GCSFileSystem()

    logger.info("Loading sports manifest from gs://%s/%s", bucket, _MANIFEST_BLOB)
    logger.info("Target sources: %s", sorted(target_sources))
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
    logger.info("Manifest rows: %d", len(df))

    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")
    if "source" not in df.columns:
        logger.error("Manifest lacks 'source' column — v9 schema required. Aborting.")
        return 1

    mask = _build_flip_mask(df, target_sources)
    n_to_flip = int(mask.sum())

    cs = df["capture_status"].astype("string").fillna("")
    src = df["source"].astype("string").fillna("")
    in_scope = src.isin(target_sources) & cs.isin(_TARGET_STATUSES)
    n_in_scope = int(in_scope.sum())
    n_already_correct = int(
        (src.isin(target_sources) & (cs == "empty_confirmed") & df["error_reason"].astype("string").fillna("").isin(_OOS_REASONS)).sum()
    )

    logger.info("=" * 60)
    logger.info("In-scope candidate rows (source+status match):  %d", n_in_scope)
    logger.info("Already correctly classified (idempotent skip): %d", n_already_correct)
    logger.info("Will flip to empty_confirmed:                   %d", n_to_flip)
    logger.info("=" * 60)

    if n_to_flip == 0:
        logger.info("Nothing to flip — manifest already clean for these sources.")
        return 0

    if n_to_flip > args.max_flips:
        logger.error(
            "n_to_flip=%d exceeds --max-flips=%d halt safety. Investigate before lifting the cap.",
            n_to_flip,
            args.max_flips,
        )
        return 2

    flip_df = df.loc[mask]
    by_source = flip_df.groupby("source", dropna=False).size()
    by_status = flip_df.groupby("capture_status", dropna=False).size()
    logger.info("Flip distribution by source:\n%s", by_source.to_string())
    logger.info("Flip distribution by current capture_status:\n%s", by_status.to_string())
    logger.info("=" * 60)

    # Compute per-row reasons before mutating.
    flip_reasons = _get_flip_reason(df, mask, target_sources)
    by_reason = flip_reasons[mask].value_counts()
    logger.info("Flip distribution by OOS reason:\n%s", by_reason.to_string())
    logger.info("=" * 60)

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = Path(tempfile.gettempdir()) / f"reclassify-oos-sports-{ts}.csv"
    audit_cols = ["date", "venue", "source", "data_type", "league_id", "capture_status", "error_reason", "attempted_at"]
    audit_existing = [c for c in audit_cols if c in flip_df.columns]
    flip_df[audit_existing].to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info("CSV audit written to %s (%d rows)", csv_path, n_to_flip)

    if not args.apply:
        logger.info("DRY RUN — manifest not modified. Re-run with --apply to flip.")
        return 0

    now_iso = datetime.now(UTC).isoformat()

    # Snapshot before mutating.
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    snap = f"{bucket}/_index/snapshots/pre_oos_reclassify_{stamp}.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot -> gs://%s", snap)

    df.loc[mask, "capture_status"] = "empty_confirmed"
    df.loc[mask, "error_reason"] = flip_reasons[mask]
    df.loc[mask, "attempted_at"] = now_iso

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("Uploading flipped manifest (%d rows total, %d flipped)", len(df), n_to_flip)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(out.read())
    logger.info("Done. CSV audit at %s", csv_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
