#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after DERIBIT-COMBO historical cells confirmed empty_confirmed[EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE] in cefi _index AND per-VM shards swept + consolidator confirmed healthy
"""Relabel DERIBIT-COMBO historical manifest rows to typed empty_confirmed.

Background (2026-06-27):

Deribit's public REST API (``public/get_combos``) returns ONLY currently-live
(active) combo instruments at the time of the call. Expired/historical combos
are NOT retained or exposed by Deribit via any REST endpoint. Consequently:

* DERIBIT-COMBO instrument-definition history is **unbackfillable via REST**
  — this is a genuine upstream source limitation, NOT a pipeline gap.
* The cefi 8-venue backfill correctly could not fill these cells.
* The correct manifest state for DERIBIT-COMBO historical cells is:
    ``capture_status = empty_confirmed``
    ``error_reason   = EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE``

The previous pipeline runs wrote ``attempted_failed`` for the ~22 days from
2026-05-23 (adapter added) to 2026-06-11 (before the adapter was fixed).
The 2023-12-17→2026-05-22 window has no manifest rows at all (silent absent).
This script relabels the ``attempted_failed`` rows and produces a report on
the silent-absent window so they can be seeded by the normal pipeline.

Specifically this script:
1. Snapshots the current ``_index/availability_index.parquet`` to
   ``_index/snapshots/pre_deribit_combo_relabel_YYYYMMDD_HHMMSS.parquet``
   before making any changes.
2. Relabels every ``capture_status=attempted_failed`` row with
   ``venue=DERIBIT-COMBO`` to:
     ``capture_status=empty_confirmed``
     ``error_reason=EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE``
3. Also relabels any ``capture_status=empty_confirmed`` rows with
   ``venue=DERIBIT-COMBO`` that have a blank/null ``error_reason`` (legacy
   blank-typed cells from early runs).
4. Sweeps per-VM shards under ``_index/per_vm/`` with the same relabeling so
   the next consolidation run does NOT re-merge stale ``attempted_failed``
   rows back into the canonical index.
5. Logs a count breakdown so the operator can verify the relabeling.

Idempotent: rows already at ``empty_confirmed[EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE]``
are left untouched. Non-DERIBIT-COMBO rows are never touched.

SSOT: ``codex/02-data/honest-absence-downstream-handling.md``
§ "DERIBIT-COMBO historical unavailability".

Usage::

    cd instruments-service

    # Dry-run first (always) — shows what would change without writing:
    .venv/bin/python scripts/relabel_deribit_combo_historical_to_empty_2026_06_27.py

    # Apply changes:
    .venv/bin/python scripts/relabel_deribit_combo_historical_to_empty_2026_06_27.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_describe_object,
)
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — get_storage_client factory for list_blobs / download / upload
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"market-data-tick-cefi-prd-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
SNAPSHOT_PREFIX = "_index/snapshots/"

# The new typed reason for DERIBIT-COMBO historical cells.
# Chosen over SOURCE_RETURNED_ZERO (no fetch was made) and EXPECTED_INSTRUMENT_DELISTED
# (instruments existed; the source can't serve historical state). This is the canonical
# reason for "the venue's BATCH source structurally does not offer this data_type at all
# — no historical endpoint exists." Also correctly excluded from the coverage-% denominator.
_TARGET_REASON = "EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE"
_TARGET_STATUS = "empty_confirmed"
_VENUE = "DERIBIT-COMBO"


def _relabel_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for DERIBIT-COMBO rows that need relabeling.

    Targets:
    - ``attempted_failed`` rows with ``venue=DERIBIT-COMBO`` (all of them,
      regardless of error_reason — no retry will succeed for historical state)
    - ``empty_confirmed`` rows with ``venue=DERIBIT-COMBO`` AND blank/null
      ``error_reason`` (legacy untyped empties from early runs)

    Leaves untouched:
    - ``captured`` rows (those are correctly captured live-combo days)
    - ``empty_confirmed[EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE]`` (already correct)
    - Any non-DERIBIT-COMBO rows
    """
    if "venue" not in df.columns or "capture_status" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    venue_mask = df["venue"].astype(str) == _VENUE
    if not venue_mask.any():
        return pd.Series([False] * len(df), index=df.index)

    status_col = df["capture_status"].astype(str)
    reason_col = df["error_reason"].fillna("").astype(str) if "error_reason" in df.columns else pd.Series([""] * len(df), index=df.index)

    # Target 1: attempted_failed rows for this venue (historical state is unbackfillable)
    attempted_failed_mask = venue_mask & (status_col == "attempted_failed")

    # Target 2: legacy blank-reason empty_confirmed rows for this venue
    blank_empty_mask = (
        venue_mask
        & (status_col == "empty_confirmed")
        & (reason_col.str.strip() == "")
    )

    return attempted_failed_mask | blank_empty_mask


def _process_blob(
    storage_client: object,
    object_key: str,
    *,
    apply: bool,
) -> tuple[int, int]:
    """Process one parquet blob; return (rows_relabeled, total_rows)."""
    raw = storage_client.download_bytes(BUCKET, object_key)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    mask = _relabel_mask(df)
    n_relabel = int(mask.sum())
    total = len(df)

    if n_relabel == 0:
        logger.info("%s: 0 rows to relabel (total %d rows)", object_key, total)
        return 0, total

    if "error_reason" in df.columns:
        by_reason = df.loc[mask, "error_reason"].fillna("").value_counts().to_dict()
    else:
        by_reason = {"<no error_reason col>": n_relabel}
    logger.info(
        "%s: %d rows to relabel (venue=DERIBIT-COMBO) — current reasons: %s",
        object_key,
        n_relabel,
        by_reason,
    )

    if not apply:
        return n_relabel, total

    df = df.copy()
    df.loc[mask, "capture_status"] = _TARGET_STATUS
    if "error_reason" not in df.columns:
        df["error_reason"] = ""
    df.loc[mask, "error_reason"] = _TARGET_REASON

    out = io.BytesIO()
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, object_key, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info(
        "%s: relabeled %d rows → empty_confirmed[%s] (total rows unchanged: %d)",
        object_key,
        n_relabel,
        _TARGET_REASON,
        total,
    )
    return n_relabel, total


def _snapshot_index(storage_client: object) -> None:
    """Copy the current _index to a timestamped snapshot blob (safety net)."""
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_key = f"{SNAPSHOT_PREFIX}pre_deribit_combo_relabel_{ts}.parquet"
    raw = storage_client.download_bytes(BUCKET, INDEX_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    storage_client.upload_bytes(BUCKET, snap_key, raw)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Snapshot written: gs://%s/%s (%d bytes)", BUCKET, snap_key, len(raw))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the relabeled manifest (default: dry-run).",
    )
    args = parser.parse_args()
    apply = bool(args.apply)

    storage_client = get_storage_client()

    # Verify the index exists before doing anything.
    if gcs_describe_object(f"gs://{BUCKET}/{INDEX_PATH}") is None:
        logger.error("Consolidated index not found: gs://%s/%s", BUCKET, INDEX_PATH)
        return 1

    if apply:
        logger.info("Snapshotting current _index before relabeling...")
        _snapshot_index(storage_client)

    total_relabeled = 0
    total_rows = 0

    # Process the consolidated canonical index first.
    n, t = _process_blob(storage_client, INDEX_PATH, apply=apply)
    total_relabeled += n
    total_rows += t

    # Sweep per-VM shards: the consolidator merges these into canonical, so
    # stale attempted_failed rows here will resurface after the next consolidation
    # unless they are also relabeled. Sweep all parquet shards under per_vm/.
    raw_blobs = storage_client.list_blobs(BUCKET, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
    shard_keys = [b.name for b in raw_blobs if b.name.endswith(".parquet")]  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Sweeping %d per-VM shard(s) under %s", len(shard_keys), PER_VM_PREFIX)
    for key in shard_keys:
        n, t = _process_blob(storage_client, key, apply=apply)
        total_relabeled += n
        total_rows += t

    mode = "APPLIED" if apply else "DRY-RUN (no writes)"
    logger.info(
        "%s — total DERIBIT-COMBO rows relabeled: %d (out of %d total rows across all blobs processed)",
        mode,
        total_relabeled,
        total_rows,
    )

    # Diagnose silent-absent window: how many DERIBIT-COMBO dates have NO row at all?
    # (These are dates from 2023-12-17 to 2026-05-22 that need the normal pipeline to run.)
    logger.info(
        "NOTE: this script relabels existing manifest rows only. "
        "Silent-absent DERIBIT-COMBO dates (2023-12-17→2026-05-22 approx.) with "
        "NO manifest row at all require the instruments-service pipeline to emit "
        "empty_confirmed[EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE] via the normal "
        "daily/backfill run (the pre-flight gate must classify this venue+window as "
        "out-of-coverage and seed the rows). Count those dates separately using "
        "instruments-service/scripts/enumerate_expected_universe.py with "
        "--asset-group cefi --venue DERIBIT-COMBO."
    )

    if not apply:
        logger.info("Re-run with --apply to write changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
