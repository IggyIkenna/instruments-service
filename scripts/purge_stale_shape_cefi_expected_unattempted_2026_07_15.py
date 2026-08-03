#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after purge verified live in the cefi _index (prd bucket) + G4 re-measure
#              confirms 0 non-canonical-shape expected_unattempted rows remain
"""Purge stale-shape cefi ``expected_unattempted`` manifest rows (G4 gate-closer).

Provenance: ``plans/active/issues/
cefi_mtds_writer_raw_symbol_vs_canonical_eu_namespace_mismatch_2026_07_15.md``
final P0 todo — "re-materialize the cefi expected-universe enumerator so every
``expected_unattempted`` row carries ONE canonical atom shape." Live manifest
inspection (2026-07-15) found the ``expected_unattempted`` side is a MIX:

* ~3,039,660 rows canonical-shaped (``VENUE:TYPE:BASE-QUOTE[@MARKER][-EXPIRY]``
  leaf, or ``instrument_id=""`` + ``underlying=<U>`` bundle).
* ~42,993 rows are LEGACY debris (``enumerator_run_id`` absent — written before
  that column existed, i.e. by an enumerator version that predates the
  shape-aware v2 seeder): ``instrument_type=""`` + a bare, sometimes-lowercase
  ``BASE-QUOTE`` id (e.g. ``BINANCE-FUTURES/book_snapshot_5`` -> ``ethusdt``,
  ``COINBASE/book_snapshot_5`` -> ``ETC-USD``) — a direct violation of the
  workspace HARD RULE "shard atom identical across writer/manifest/status/
  gate/UI".
* ~6,727 rows were an ACTIVE bug: the CURRENT enumerator run
  (``enum-universe-cefi-20260715-013053``) emitted futures_chain/options_chain
  BUNDLE rows with ``instrument_id=<underlying>`` / ``underlying=""`` (the raw
  ``_rollup_bundle_grain`` synthetic-entry shape, never translated to the MTDS
  writer's real bundle-capture convention). Fixed in
  ``scripts/enumerate_expected_universe.py`` (``_enumerate_v2_cefi`` bundle-aware
  ``seed_instrument_id``/``seed_underlying`` — see
  ``instruments-service@a2468dd9``); this script purges the ALREADY-WRITTEN
  stale rows the pre-fix code left behind (the code fix only stops FUTURE
  writes, it cannot retroactively fix rows already on disk).

Neither class of stale row can ever be closed by a correctly-canonicalizing
writer (the shard atom literally cannot match), so they sit permanently
``expected_unattempted`` and deflate honest-coverage even where the real cell
is either already captured under the canonical atom, or would be correctly
re-seeded by a fresh (post-fix) enumerator ``--apply-write`` pass. Purging is
LOW-RISK relative to a captured-row relabel (contrast
``relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py``, which mutates
CAPTURED data and remains ``--apply``-gated pending operator sign-off):
``expected_unattempted`` rows carry no capture evidence — they are a computed
denominator placeholder the next enumerator run regenerates from the catalogue
+ manifest present-set. Deleting a stale-shape row never destroys real data;
either the cell truly needs re-seeding (a fresh enumerator run supplies the
canonical-shape row) or the catalogue no longer lists the instrument (the
row should not exist at all).

SNAPSHOT-FIRST (operator instruction, standing pattern — mirrors
``purge_deribit_option_per_strike_trades_book5_2026_07_12.py`` /
``purge_prediction_index_final_residuals_2026_07_11.py``): every blob this
script rewrites gets a pre-purge copy under ``_index/snapshots/`` BEFORE any
delete.

Usage::

    cd instruments-service

    # dry-run (default, read-only)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_stale_shape_cefi_expected_unattempted_2026_07_15.py

    # apply (snapshot + delete + verify gate) — OPERATOR SIGN-OFF REQUIRED, see
    # this task's /blocked question before running with --apply.
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_stale_shape_cefi_expected_unattempted_2026_07_15.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

# Canonical LEAF shard-atom shape: VENUE:TYPE:BASE-QUOTE[@MARKER][-EXPIRY].
# Matches the MTDS writer's derive_row_instrument_id / build_instrument_id
# output (tardis_shared.py) for every leaf instrument_type (PERPETUAL / SPOT_PAIR
# / FUTURE / OPTION / EQUITY_PERP).
_LEAF_CANON_RE = re.compile(r"^[A-Za-z0-9_-]+:[A-Za-z_]+:[A-Za-z0-9-]+(@[A-Za-z]+)?(-[0-9]{8})?$")

# STOP-ON-SURPRISE: measured live 2026-07-15 in the prd cefi bucket canonical
# index = 49,720 non-canonical-shape expected_unattempted rows (42,993 legacy
# no-enumerator_run_id + 6,727 current-run bundle-shape bug, see module
# docstring). Bound generously around the measured live value.
_EXPECTED_MIN = 5_000
_EXPECTED_MAX = 250_000


def _is_canon_shape(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    """True where ``instrument_id``/``underlying`` already carry the canonical shard atom."""
    ids = df["instrument_id"].fillna("").astype(str)
    underlying = (
        df["underlying"].fillna("").astype(str) if "underlying" in df.columns else pd.Series("", index=df.index)
    )
    is_leaf_canon = ids.str.match(_LEAF_CANON_RE)
    is_bundle_canon = (ids == "") & (underlying != "")
    return (is_leaf_canon | is_bundle_canon).fillna(False)


def _target_mask(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    status = (
        df["capture_status"].fillna("").astype(str) if "capture_status" in df.columns else pd.Series("", index=df.index)
    )
    is_eu = status == "expected_unattempted"
    return (is_eu & ~_is_canon_shape(df)).fillna(False)


def _load(storage: object, bucket: str, blob: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw))


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_purge_stale_shape_eu_{label}_{run_ts}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    df = _load(storage, bucket, blob)
    _write(storage, bucket, snap_blob, df)
    return snap_blob


def _process_blob(storage: object, bucket: str, blob: str, run_ts: str, *, apply: bool) -> tuple[int, int]:
    df = _load(storage, bucket, blob)
    total_before = len(df)
    if "instrument_id" not in df.columns or "capture_status" not in df.columns:
        return 0, total_before

    mask = _target_mask(df)
    n_target = int(mask.sum())
    logger.info(
        "gs://%s/%s: %d / %d rows match (stale-shape expected_unattempted)",
        bucket,
        blob,
        n_target,
        total_before,
    )
    if n_target == 0:
        return 0, total_before

    if not apply:
        return n_target, total_before

    _snapshot(storage, bucket, blob, run_ts)
    df_purged = df.loc[~mask].reset_index(drop=True)
    _write(storage, bucket, blob, df_purged)
    logger.info("gs://%s/%s: deleted %d rows, %d remain", bucket, blob, n_target, len(df_purged))
    return n_target, total_before


def _verify_gate(storage: object, bucket: str, blobs: list[str]) -> int:
    logger.info("Verifying post-apply gate on gs://%s ...", bucket)
    residual_total = 0
    for blob in blobs:
        df_after = _load(storage, bucket, blob)
        if "instrument_id" not in df_after.columns or "capture_status" not in df_after.columns:
            continue
        residual = int(_target_mask(df_after).sum())
        residual_total += residual
        if residual:
            logger.error("GATE FAIL: gs://%s/%s still has %d target rows.", bucket, blob, residual)
    if residual_total:
        return 1
    logger.info("GATE PASSED: 0 stale-shape expected_unattempted rows remain in gs://%s.", bucket)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Delete matching rows (snapshot first). Default: dry-run.")
    p.add_argument("--bucket", default=None, help="Override bucket (default: resolve_bucket_name prd cefi bucket).")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [INDEX_BLOB, *per_vm_names]

    total_target = 0
    per_blob_counts: dict[str, int] = {}
    for blob in blobs:
        n_target, _total_before = _process_blob(storage, bucket, blob, run_ts, apply=False)
        per_blob_counts[blob] = n_target
        total_target += n_target

    logger.info("TOTAL target rows across %d blob(s): %d", len(blobs), total_target)

    if not (_EXPECTED_MIN <= total_target <= _EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: total target row count %d outside expected range [%d, %d]. Diagnose before --apply.",
            total_target,
            _EXPECTED_MIN,
            _EXPECTED_MAX,
        )
        return 1

    if not args.apply:
        logger.info("DRY-RUN (default) — no write. Pass --apply to delete %d rows.", total_target)
        return 0

    if total_target == 0:
        logger.info("Nothing to delete.")
        return 0

    deleted = 0
    for blob in blobs:
        if per_blob_counts.get(blob, 0) == 0:
            continue
        n_deleted, _total_before = _process_blob(storage, bucket, blob, run_ts, apply=True)
        deleted += n_deleted

    rc = _verify_gate(storage, bucket, blobs)
    if rc == 0:
        logger.info("APPLY COMPLETE: %d stale-shape expected_unattempted rows deleted.", deleted)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
