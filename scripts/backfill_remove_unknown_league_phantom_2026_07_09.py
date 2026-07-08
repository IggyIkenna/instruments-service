#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this run is confirmed applied in prod (verify via --verify-only)
"""Remove the sports ``league_id="UNKNOWN"`` phantom rows from the real catalogue + manifest.

Backfill for A1 (`instruments_docs_audit_outstanding_items_2026_07_08.md`,
`sports_manifest_unknown_league_id_2026_07_08.md`). Root cause (fixed same day in
``build_instrument_catalogue.build_sports_catalogue_from_manifest`` +
``enumerate_expected_universe._enumerate_v2_sports``): the catalogue roll-up was
unguarded against the ``"UNKNOWN"`` sentinel ``league_id``, minting a real,
persisted ``instrument_id="UNKNOWN"/league_id="UNKNOWN"`` catalogue row that the
v2 enumerator then read back and amplified into thousands of manifest
``expected_unattempted``/``empty_confirmed`` rows, recurring daily via the
``enum-universe-sports-*`` cron. The code fix stops NEW phantom rows; this script
removes the EXISTING ones.

Verified against real prod data (2026-07-09, ADC read):
  - ``prod/catalog.parquet``: 1 row (``instrument_id="UNKNOWN"``, ``league_id="UNKNOWN"``).
  - ``_index/availability_index.parquet``: 2,373 rows with ``league_id="UNKNOWN"``
    (1,352 ``expected_unattempted`` + 1,011 ``empty_confirmed`` + 10 ``captured``).
    All 10 ``captured`` rows have ``instrument_count=0`` and
    ``error_reason="reconciled_from_existing_per_league_parquet"``, written once
    2026-05-01T02:28:56Z — the frozen bootstrap rows the audit doc names (blocked
    from new writes by the ``_is_in_canonical_write_universe`` gate since
    2026-06-24). No row beyond these 10 carries ``capture_status="captured"``, so
    there is no real captured data under the sentinel to preserve.
  - ``_index/per_vm/*.parquet`` (2 shards: ``_legacy_seed.parquet``,
    ``hk_api_football_fixture_stats_20231019_ffaa82.parquet``): 0 rows with
    ``league_id="UNKNOWN"`` in either — no per-VM shard cleanup needed.

Safety: backup-then-write (``<blob>.<run_ts>.unknown_league_backfill.bak.parquet``,
matching the repo's established phantom-row-deletion pattern, e.g.
``delete_phantom_rows_from_shards.py``). HARD-ABORTS before any write if a
``captured`` row with ``instrument_count`` != 0 is found under the sentinel — that
would be real data, and only an operator-reviewed run should touch it.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    python scripts/backfill_remove_unknown_league_phantom_2026_07_09.py --dry-run
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    python scripts/backfill_remove_unknown_league_phantom_2026_07_09.py --apply
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    python scripts/backfill_remove_unknown_league_phantom_2026_07_09.py --verify-only
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_remove_unknown_league_phantom")

CATALOG_BLOB = "prod/catalog.parquet"
INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

#: Mirrors build_instrument_catalogue.SPORTS_LEAGUE_ID_SENTINELS /
#: enumerate_expected_universe._SPORTS_LEAGUE_ID_SENTINELS. Duplicated (not
#: imported) — this is a standalone, one-off entry point, not a shared package.
SENTINEL_LEAGUE_IDS = frozenset({"UNKNOWN"})


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.unknown_league_backfill.bak.parquet"
    return f"{blob_name}.{run_ts}.unknown_league_backfill.bak"


def _sentinel_mask(df: pd.DataFrame) -> pd.Series:
    lid = df["league_id"].fillna("").astype(str)
    return lid.str.upper().isin(SENTINEL_LEAGUE_IDS)


def _assert_no_real_captured_data(df: pd.DataFrame, mask: pd.Series, label: str) -> None:
    """Hard-abort if a to-be-deleted row looks like real captured data.

    Only ``capture_status="captured"`` rows with ``instrument_count == 0`` are
    the known-safe frozen bootstrap rows — anything else under the sentinel is
    unexpected and must stop the run for operator review, per the mandatory
    backfill instructions ("if you find real captured rows under UNKNOWN beyond
    the 10 frozen bootstrap ones, STOP and report").
    """
    if "capture_status" not in df.columns:
        return
    doomed = df.loc[mask]
    captured = doomed[doomed["capture_status"] == "captured"]
    if captured.empty:
        return
    bad = captured
    if "instrument_count" in captured.columns:
        counts = pd.to_numeric(captured["instrument_count"], errors="coerce").fillna(-1)
        bad = captured.loc[counts != 0]
    if not bad.empty:
        logger.error(
            "%s: %d 'captured' row(s) under a sentinel league_id have non-zero "
            "instrument_count — this looks like REAL data, not a frozen bootstrap "
            "artifact. Aborting without writing. Rows:\n%s",
            label,
            len(bad),
            bad.to_string(),
        )
        raise SystemExit(3)
    logger.info(
        "%s: %d 'captured' row(s) under a sentinel league_id, all instrument_count=0 "
        "(frozen bootstrap rows) — safe to delete.",
        label,
        len(captured),
    )


def _process_blob(
    storage: object,
    bucket: str,
    blob_name: str,
    run_ts: str,
    *,
    label: str,
    apply: bool,
) -> tuple[int, int]:
    """Returns (deleted_count, total_rows_before). No-op (0, 0) if the blob is absent."""
    if not storage.blob_exists(bucket, blob_name):  # type: ignore[attr-defined]
        logger.warning("%s: gs://%s/%s does not exist — skipping.", label, bucket, blob_name)
        return 0, 0

    raw = storage.download_bytes(bucket, blob_name)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    total_before = len(df)

    if "league_id" not in df.columns:
        logger.warning("%s: no league_id column — skipping.", label)
        return 0, total_before

    mask = _sentinel_mask(df)
    n_delete = int(mask.sum())
    logger.info("%s: %d / %d rows carry a sentinel league_id.", label, n_delete, total_before)
    if n_delete == 0:
        return 0, total_before

    _assert_no_real_captured_data(df, mask, label)

    if not apply:
        logger.info("%s: DRY-RUN — would delete %d rows. Pass --apply to write.", label, n_delete)
        return n_delete, total_before

    backup = _backup_path(blob_name, run_ts)
    storage.upload_bytes(bucket, backup, raw)  # type: ignore[attr-defined]
    logger.info("%s: backed up pre-deletion snapshot → gs://%s/%s", label, bucket, backup)

    filtered = df.loc[~mask].copy()
    out = io.BytesIO()
    filtered.to_parquet(out, index=False)
    out.seek(0)
    storage.upload_bytes(bucket, blob_name, out.read())  # type: ignore[attr-defined]
    logger.info("%s: rewrote %d rows (was %d) → gs://%s/%s", label, len(filtered), total_before, bucket, blob_name)
    return n_delete, total_before


def _verify(storage: object, bucket: str) -> int:
    """Confirm 0 sentinel rows remain anywhere (catalog, index, per-VM shards)."""
    remaining = 0
    for blob_name, label in [(CATALOG_BLOB, "catalog"), (INDEX_BLOB, "index")]:
        if not storage.blob_exists(bucket, blob_name):  # type: ignore[attr-defined]
            continue
        raw = storage.download_bytes(bucket, blob_name)  # type: ignore[attr-defined]
        df = pd.read_parquet(io.BytesIO(raw))
        if "league_id" not in df.columns:
            continue
        n = int(_sentinel_mask(df).sum())
        remaining += n
        logger.info("VERIFY %s: %d sentinel rows remaining (of %d total).", label, n, len(df))

    native = getattr(storage, "_client", None)
    if native is not None:
        blobs = list(native.bucket(bucket).list_blobs(prefix=PER_VM_PREFIX))
        for b in blobs:
            if not b.name.endswith(".parquet") or ".bak" in b.name:
                continue
            if not storage.blob_exists(bucket, b.name):  # type: ignore[attr-defined]
                # Stale listing race against a concurrent live writer — this
                # script never touches per-VM shards, so a 404 here means
                # something else moved/deleted it between list and read.
                logger.warning("VERIFY per-vm %s: listed but no longer exists — skipping.", b.name)
                continue
            raw = storage.download_bytes(bucket, b.name)  # type: ignore[attr-defined]
            df = pd.read_parquet(io.BytesIO(raw))
            if df.empty or "league_id" not in df.columns:
                continue
            n = int(_sentinel_mask(df).sum())
            remaining += n
            if n:
                logger.info("VERIFY per-vm %s: %d sentinel rows remaining.", b.name, n)

    logger.info("VERIFY: total sentinel rows remaining across all objects: %d", remaining)
    return remaining


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    bucket = _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)
    storage = get_storage_client()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    if args.verify_only:
        remaining = _verify(storage, bucket)
        return 0 if remaining == 0 else 1

    logger.info("=== Catalogue: gs://%s/%s ===", bucket, CATALOG_BLOB)
    cat_deleted, cat_total = _process_blob(storage, bucket, CATALOG_BLOB, run_ts, label="catalog", apply=args.apply)

    logger.info("=== Manifest index: gs://%s/%s ===", bucket, INDEX_BLOB)
    idx_deleted, idx_total = _process_blob(storage, bucket, INDEX_BLOB, run_ts, label="index", apply=args.apply)

    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  Catalogue: %d sentinel rows deleted (of %d total)", cat_deleted, cat_total)
    logger.info("  Manifest index: %d sentinel rows deleted (of %d total)", idx_deleted, idx_total)
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("DRY RUN — no writes performed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
