#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 rows with service_name=instruments-service + source=instruments_service +
#   data_type in (trades, odds_horizon_bucket, trades_inplay) remain in the sports market-data
#   availability index (verified post-apply)
"""Restamp source=/pipeline_mode= provenance for the 2,490 mis-stamped sports orphan rows.

``instruments-service/scripts/backfill_orphan_class_e_sports.py::record_cells()`` wrote 2,490
real, correctly-keyed manifest rows (``trades``=1,273 / ``odds_horizon_bucket``=1,106 /
``trades_inplay``=111) via ``resolve_source_and_mode()`` — a case-sensitivity gap meant the
lower-case GCS path-segment ``data_type`` never matched UAC ``SOURCE_PRIORITY``'s upper-case
sports keys, so every one of these rows fell through to the ``BATCH_INSTRUMENTS_SERVICE``
producer-fallback (``source="instruments_service"``, ``pipeline_mode="batch_instruments_service"``)
instead of their real vendor provenance. Root-caused + fixed 2026-08-03:

  - the case-sensitivity gap itself: fixed in ``resolve_source_and_mode()``
    (``instruments-service@d9994199`` — retries ``.upper()``).
  - ``TRADES``/``ODDS_HORIZON_BUCKET`` were already registered (upper-case) in UAC
    ``SOURCE_PRIORITY`` with real sources; only the case mismatch blocked resolution.
  - ``TRADES_INPLAY`` had NO ``SOURCE_PRIORITY`` entry under any casing — registered
    2026-08-03 (``unified-api-contracts@b27717b8``, ``odds_api``, same writer family as
    ``TRADES``).

Both fixes only change resolution for FUTURE writes/re-runs — the 2,490 ALREADY-RECORDED rows
keep their wrong provenance until restamped. This script is that one-off corrective restamp:
only the ``source``/``pipeline_mode`` columns change (``venue``/``league_id``/``row_count`` are
already correct per the root-cause finding — the cells themselves are real, correctly-keyed
captures, only the provenance columns are wrong).

Per-data_type target (matches ``resolve_source_and_mode()``'s now-fixed resolution exactly —
reused directly from that module, not re-derived, so this script can never drift from the
writer's own logic):

  - ``trades``              → source=odds_api,              pipeline_mode=batch_odds_api
  - ``odds_horizon_bucket``  → source=mdps_odds_horizon_bucket, pipeline_mode=batch_mdps_odds_horizon_bucket
  - ``trades_inplay``        → source=odds_api,              pipeline_mode=batch_odds_api

Same restamp pattern as ``restamp_is_sports_blank_source_2026_07_13.py`` (snapshot-before-write,
dry-run by default, direct index parquet read/write via the UTL StorageClient SDK) — this
script's surface is the ODDS/market-data sports bucket (``resolve_bucket_name(kind="market-data",
asset_group="sports")``), not the reference-data ``instruments-store`` surface that precedent
touched.

DRY-RUN by default. Use --apply to write back the restamped index. Run with the manifest
consolidator paused to avoid a read-modify-write race.

See ``sports_manifest_blank_venue_captured_rows_2026_07_27.md`` todo 2.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import tempfile
from pathlib import Path
from types import ModuleType

from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("restamp_sports_orphan_source_provenance")

_INDEX_BLOB = "_index/availability_index.parquet"
_SNAPSHOT_BLOB = "_index/snapshots/pre_orphan_source_provenance_restamp_{date}.parquet"

# The exact population the root-cause finding identified — captured (not honest-absence),
# blank-venue, IS-producer-fallback-stamped rows for these 3 data_types.
_TARGET_DATA_TYPES = ("trades", "odds_horizon_bucket", "trades_inplay")


def _resolve_expected_source_and_mode() -> ModuleType:
    """Load ``backfill_orphan_class_e_sports.resolve_source_and_mode`` — reused directly
    (not re-derived) so this restamp can never drift from the writer's own resolution."""
    stem = "backfill_orphan_class_e_sports"
    spec = importlib.util.spec_from_file_location(stem, Path(__file__).parent / f"{stem}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load sibling {stem}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(stem, mod)
    spec.loader.exec_module(mod)
    return mod


def _cp(src: str, dst: str) -> bool:
    """Copy between local paths and gs:// URIs via the UTL StorageClient SDK."""
    src_is_gcs = src.startswith("gs://")
    dst_is_gcs = dst.startswith("gs://")
    try:
        client = get_storage_client(provider="gcp")
        if src_is_gcs and dst_is_gcs:
            src_bucket, src_path = src.removeprefix("gs://").split("/", 1)
            dst_bucket, dst_path = dst.removeprefix("gs://").split("/", 1)
            client.copy_blob(src_bucket, src_path, dst_bucket, dst_path)
            return True
        if src_is_gcs and not dst_is_gcs:
            bucket, path = src.removeprefix("gs://").split("/", 1)
            client.download_file(bucket, path, dst)
            return True
        if dst_is_gcs and not src_is_gcs:
            bucket, path = dst.removeprefix("gs://").split("/", 1)
            client.upload_file(bucket, path, src)
            return True
        raise ValueError(f"_cp expects at least one of src/dst to be a gs:// URI: src={src!r} dst={dst!r}")
    except Exception:
        logger.exception("_cp failed: src=%s dst=%s", src, dst)
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write restamped index (default: dry-run)")
    ap.add_argument("--bucket", default="", help="Override bucket name (default: resolve_bucket_name market-data)")
    args = ap.parse_args()

    import pandas as pd

    orphan_mod = _resolve_expected_source_and_mode()

    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="sports")
    index_uri = f"gs://{bucket}/{_INDEX_BLOB}"
    logger.info("Downloading %s …", index_uri)

    with tempfile.TemporaryDirectory() as tmp:
        local_idx = str(Path(tmp) / "idx.parquet")
        if not _cp(index_uri, local_idx):
            logger.error("Failed to download %s", index_uri)
            return 1

        df = pd.read_parquet(local_idx)
        logger.info("Loaded %d rows", len(df))

        target = (
            (df["service_name"].fillna("").astype(str) == "instruments-service")
            & (df["source"].fillna("").astype(str) == "instruments_service")
            & (df["pipeline_mode"].fillna("").astype(str) == "batch_instruments_service")
            & (df["venue"].fillna("").astype(str) == "")
            & (df["capture_status"].fillna("").astype(str) == "captured")
            & (df["data_type"].astype(str).isin(_TARGET_DATA_TYPES))
        )
        logger.info("Rows matching the mis-stamped orphan population: %d", int(target.sum()))
        if target.sum() == 0:
            logger.info("Nothing to restamp — already GREEN")
            return 0

        by_data_type = df.loc[target, "data_type"].value_counts().to_dict()
        logger.info("  by data_type: %s", by_data_type)

        new_source = pd.Series(index=df.index, dtype=object)
        new_pipeline_mode = pd.Series(index=df.index, dtype=object)
        for dt in df.loc[target, "data_type"].unique():
            row_mask = target & (df["data_type"] == dt)
            source, pipeline_mode = orphan_mod.resolve_source_and_mode(str(dt), "")
            new_source[row_mask] = source
            new_pipeline_mode[row_mask] = pipeline_mode.value
            logger.info("  %s → source=%r pipeline_mode=%r (%d rows)", dt, source, pipeline_mode.value, row_mask.sum())

        # Only restamp rows where resolution actually produced a real (non-empty) source —
        # a future unregistered data_type sneaking into _TARGET_DATA_TYPES must not silently
        # restamp to blank.
        can_restamp = target & new_source.fillna("").astype(str).str.strip().ne("")
        skipped = int((target & ~can_restamp).sum())
        if skipped:
            logger.warning("Rows matched but NOT restampable (resolver returned blank source): %d", skipped)
        logger.info("Rows that will be restamped: %d", int(can_restamp.sum()))

        if args.apply:
            import datetime

            snap_blob = _SNAPSHOT_BLOB.format(date=datetime.datetime.now(tz=datetime.UTC).date().isoformat())
            snap_uri = f"gs://{bucket}/{snap_blob}"
            logger.info("Snapshotting current index → %s …", snap_uri)
            if not _cp(index_uri, snap_uri):
                logger.error("Snapshot failed — aborting")
                return 1

            df.loc[can_restamp, "source"] = new_source[can_restamp]
            df.loc[can_restamp, "pipeline_mode"] = new_pipeline_mode[can_restamp]
            local_out = str(Path(tmp) / "idx_restamped.parquet")
            df.to_parquet(local_out, index=False)
            logger.info("Writing restamped index → %s …", index_uri)
            if not _cp(local_out, index_uri):
                logger.error("Write-back failed")
                return 1
            logger.info("DONE — restamped %d rows", int(can_restamp.sum()))
        else:
            logger.info("DRY-RUN — rerun with --apply to write %d restamped rows", int(can_restamp.sum()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
