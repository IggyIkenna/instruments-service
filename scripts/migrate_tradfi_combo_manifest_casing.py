#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after --apply verified live in the tradfi -prd manifest index
#              (fresh-read gate: 0 residual COMBO-uppercase rows) and the
#              G1-ENUM tradfi present-set re-measure shows no regression.
"""One-time relabel: TradFi manifest index ``instrument_type=COMBO`` (legacy
uppercase) -> ``combo`` (canonical lowercase).

Provenance:
plans/active/issues/tradfi_combo_uppercase_casing_manifest_residual_2026_07_28.md.
Root cause: captures written before the 2026-06-22
``_canonical_writer_instrument_type`` writer-grain-alignment fix stamped the
uppercase ``COMBO`` form; captures written after stamp the canonical lowercase
``combo``. The manifest has carried both forms side-by-side ever since, with no
reconciliation pass to collapse the legacy rows onto the canonical casing.
Real census filed with the issue (2026-07-28, live ``-prd`` index,
5,456,407 total rows)::

    instrument_type=COMBO (uppercase, legacy):   1,314,705 rows
      empty_confirmed        718,567
      attempted_failed       360,571
      captured               235,321
      expected_unattempted        246
    instrument_type=combo (lowercase, canonical):   23,428 rows (100% captured)

This is a MANIFEST INDEX column-VALUE residual (the ``availability_index.parquet``
``instrument_type`` column) — NOT the GCS object-path casing issue that
``scripts/migrate_instrument_type_lowercase.py`` already covers (that script
rewrites hive-partitioned ``instrument_type=`` PATH segments in the actual data
files via ``google.cloud.storage`` directly; a different surface, and its
storage-client usage predates the current no-``google.cloud``-import standard —
do not copy it). This script targets the manifest index's own column value and
uses only the UTL cloud-interface wrappers.

Action: for every row where ``instrument_type == "COMBO"`` (case-sensitive),
relabel ``instrument_type`` to ``combo`` IN PLACE, across ALL FOUR
``capture_status`` values (this is a pure value-relabel — the row's meaning is
unchanged, only the string casing; the manifest reads already tolerate both
casings case-insensitively via ``bundle_instrument_type_for_leaf`` /
``grain_for_instrument_type``, so this is a cleanliness/consolidation pass, not
a correctness fix — P3, not urgent).

Safety (mirrors ``relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py`` +
the manifest consolidator's own CAS write path):

* Default mode is DRY-RUN (read-only, reports counts + a sample diff);
  ``--apply`` mutates.
* Pre-migration bytes are snapshotted to a timestamped
  ``_index/backups/availability_index.pre_combo_casing_relabel_{ts}.parquet``
  BEFORE any write to the live index (create-only-if-absent CAS — a fresh
  timestamped path never collides).
* The live-index write is a CONDITIONAL/CAS overwrite
  (``gcs_conditional_put`` / GCS ``if_generation_match``), never a blind
  overwrite: ``availability_index.parquet`` is actively read/written by the
  manifest consolidator and every writer/enumerator run. A concurrent
  modification during this script's write makes the CAS attempt fail; the
  script re-reads the new generation, re-computes the relabel against it, and
  retries (bounded) — it never silently reverts a concurrent writer's change,
  and if every retry is lost it ABORTS LOUDLY without writing.
* Post-apply verification re-reads the index FRESH (never trusts its own
  in-memory frame) and gates on: 0 residual ``COMBO``(upper) rows, and
  ``combo``(lower) row count == the pre-migration ``COMBO + combo`` sum.
* Idempotent: a second run against an already-migrated index finds 0
  candidates and exits 0 without writing anything.

Usage::

    cd instruments-service

    # dry-run (default, read-only)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/migrate_tradfi_combo_manifest_casing.py

    # apply (snapshot + CAS-protected relabel + fresh-read verify gate)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/migrate_tradfi_combo_manifest_casing.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import (
    gcs_conditional_put,
    gcs_read_object_with_generation,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

LEGACY_VALUE = "COMBO"
CANONICAL_VALUE = "combo"
CAPTURE_STATUSES = ("captured", "attempted_failed", "empty_confirmed", "expected_unattempted")

# STOP-ON-SURPRISE sanity window around the issue's measured 2026-07-28 census
# (1,314,705 candidates). A drift within this window is expected/normal (the
# manifest keeps moving) and is logged, not blocked — per the issue's explicit
# instruction to "use the REAL current counts, note the drift". Only a
# wildly-off count (a different order of magnitude — wrong bucket/column,
# logic bug) gets a loud WARNING before proceeding.
_SANITY_MIN = 500_000
_SANITY_MAX = 2_000_000

# CAS retry bound for the live-index write — mirrors
# ``manifest_consolidator.py``'s generation-match retry loop. The only
# concurrent writer in practice is the manifest consolidator's own periodic
# cycle, so contention is rare.
_CAS_ATTEMPTS = 5


def _relabel_combo_casing(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Relabel every ``instrument_type == "COMBO"`` row to ``"combo"`` IN PLACE,
    across all ``capture_status`` values.

    Pure value-relabel: row identity (date/venue/instrument_id/capture_status)
    is unchanged, only the ``instrument_type`` string casing. Idempotent — a
    second call on an already-migrated frame finds 0 candidates and returns the
    frame unchanged (same object, not copied, when there is nothing to do).

    Returns ``(relabeled_df, stats)``; ``stats`` carries the per-capture_status
    relabeled-row counts plus ``"total_relabeled"``.
    """
    stats: dict[str, int] = {"total_relabeled": 0}
    if "instrument_type" not in df.columns:
        return df, stats

    mask = df["instrument_type"] == LEGACY_VALUE
    stats["total_relabeled"] = int(mask.sum())
    if "capture_status" in df.columns:
        for status in CAPTURE_STATUSES:
            stats[status] = int((mask & (df["capture_status"] == status)).sum())

    if stats["total_relabeled"] == 0:
        return df, stats

    df = df.copy()
    df.loc[mask, "instrument_type"] = CANONICAL_VALUE
    return df, stats


def _census(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    """Per-capture_status row counts for both the legacy and canonical casing."""
    out: dict[str, dict[str, int]] = {LEGACY_VALUE: {}, CANONICAL_VALUE: {}}
    if "instrument_type" not in df.columns or "capture_status" not in df.columns:
        return out
    for value in (LEGACY_VALUE, CANONICAL_VALUE):
        sub = df.loc[df["instrument_type"] == value, "capture_status"]
        for status in CAPTURE_STATUSES:
            n = int((sub == status).sum())
            if n:
                out[value][status] = n
    return out


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply",
        action="store_true",
        help="Snapshot + CAS-protected relabel + fresh-read verify. Default: dry-run (read-only).",
    )
    p.add_argument(
        "--bucket",
        default=None,
        help="Override the tradfi market-data bucket (default: resolve_bucket_name prd).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="tradfi")
    main_uri = f"gs://{bucket}/{INDEX_BLOB}"
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    data, generation = gcs_read_object_with_generation(main_uri)
    if data is None:
        logger.error("Manifest index not found at %s — refusing to proceed.", main_uri)
        return 1

    df = pd.read_parquet(io.BytesIO(data))
    pre_census = _census(df)
    logger.info(
        "PRE-MIGRATION census %s (generation=%d, %d total rows): COMBO(upper)=%s combo(lower)=%s",
        main_uri,
        generation,
        len(df),
        pre_census[LEGACY_VALUE],
        pre_census[CANONICAL_VALUE],
    )

    relabeled_df, stats = _relabel_combo_casing(df)
    total_relabeled = stats["total_relabeled"]
    logger.info(
        "Relabel candidates: %d total (%s)",
        total_relabeled,
        {k: v for k, v in stats.items() if k != "total_relabeled"},
    )

    if total_relabeled == 0:
        logger.info("Nothing to relabel — manifest already fully canonical (idempotent no-op).")
        return 0

    if total_relabeled > len(df):
        # Logic-impossible — hard stop regardless of mode.
        logger.error(
            "STOP-ON-SURPRISE: candidate count %d exceeds total row count %d — refusing.",
            total_relabeled,
            len(df),
        )
        return 1
    if not (_SANITY_MIN <= total_relabeled <= _SANITY_MAX):
        logger.warning(
            "Candidate count %d is outside the issue's measured expectation window [%d, %d] "
            "(census filed 2026-07-28: 1,314,705) — the manifest has moved since filing. "
            "Proceeding with the REAL current count per the issue's explicit instruction.",
            total_relabeled,
            _SANITY_MIN,
            _SANITY_MAX,
        )

    sample = df.loc[df["instrument_type"] == LEGACY_VALUE].head(5)
    for _, row in sample.iterrows():
        logger.info(
            "  sample: date=%s venue=%s capture_status=%s instrument_type=COMBO -> combo",
            row.get("date"),
            row.get("venue"),
            row.get("capture_status"),
        )

    if not args.apply:
        logger.info("DRY-RUN (default) — no write. Pass --apply to relabel %d rows.", total_relabeled)
        return 0

    # Snapshot pre-migration bytes BEFORE touching the live index. Fresh
    # timestamped path -> create-only-if-absent CAS (if_generation_match=0)
    # can never legitimately collide.
    snapshot_blob = f"_index/backups/availability_index.pre_combo_casing_relabel_{run_ts}.parquet"
    snapshot_uri = f"gs://{bucket}/{snapshot_blob}"
    snap_generation = gcs_conditional_put(snapshot_uri, data, if_generation_match=0)
    if snap_generation is None:
        logger.error(
            "Snapshot write to %s failed (create-only-if-absent lost a race on a fresh timestamped "
            "path — unexpected). Aborting before touching the live index.",
            snapshot_uri,
        )
        return 1
    logger.info("Snapshot written: %s (generation=%d)", snapshot_uri, snap_generation)

    current_df = relabeled_df
    current_generation = generation
    current_total_relabeled = total_relabeled
    new_generation: int | None = None
    for attempt in range(_CAS_ATTEMPTS):
        buf = io.BytesIO()
        current_df.to_parquet(buf, index=False)
        payload = buf.getvalue()
        new_generation = gcs_conditional_put(main_uri, payload, if_generation_match=current_generation)
        if new_generation is not None:
            logger.info(
                "APPLY: wrote %d relabeled rows to %s (new generation=%d).",
                current_total_relabeled,
                main_uri,
                new_generation,
            )
            break
        if attempt == _CAS_ATTEMPTS - 1:
            logger.error(
                "CAS write lost the precondition race %d times in a row (concurrent modification of "
                "the live index) — ABORTING without overwriting. The concurrent writer's change is "
                "preserved. Re-run this script to retry against the new generation.",
                _CAS_ATTEMPTS,
            )
            return 1
        time.sleep(0.5 * (attempt + 1))
        logger.warning(
            "CAS precondition failed (attempt %d/%d) — re-reading the live index's fresh generation "
            "and re-relabeling against it.",
            attempt + 1,
            _CAS_ATTEMPTS,
        )
        fresh_data, current_generation = gcs_read_object_with_generation(main_uri)
        if fresh_data is None:
            logger.error("Live index disappeared mid-migration — aborting.")
            return 1
        fresh_df = pd.read_parquet(io.BytesIO(fresh_data))
        current_df, fresh_stats = _relabel_combo_casing(fresh_df)
        current_total_relabeled = fresh_stats["total_relabeled"]
        if current_total_relabeled == 0:
            logger.info("Concurrent writer already resolved every COMBO row — nothing left to do.")
            return 0

    if new_generation is None:
        # Unreachable (the loop above always returns or raises before falling
        # through), but keeps basedpyright happy about the narrowed type below.
        return 1

    # Verify from a FRESH read — never trust this script's own in-memory frame.
    verify_data, _verify_generation = gcs_read_object_with_generation(main_uri)
    if verify_data is None:
        logger.error("Post-apply verification read failed — index missing?!")
        return 1
    verify_df = pd.read_parquet(io.BytesIO(verify_data))
    post_census = _census(verify_df)
    residual_upper = sum(post_census[LEGACY_VALUE].values())
    lower_total = sum(post_census[CANONICAL_VALUE].values())
    expected_lower = sum(pre_census[LEGACY_VALUE].values()) + sum(pre_census[CANONICAL_VALUE].values())
    logger.info(
        "POST-MIGRATION fresh-read census: COMBO(upper)=%d combo(lower)=%d (expected=%d)",
        residual_upper,
        lower_total,
        expected_lower,
    )
    if residual_upper != 0 or lower_total != expected_lower:
        logger.error(
            "GATE FAIL: residual COMBO(upper)=%d, combo(lower)=%d (expected %d).",
            residual_upper,
            lower_total,
            expected_lower,
        )
        return 1
    logger.info(
        "GATE PASSED: 0 COMBO(uppercase) rows remain; combo(lowercase)=%d matches the pre-migration sum.",
        lower_total,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
