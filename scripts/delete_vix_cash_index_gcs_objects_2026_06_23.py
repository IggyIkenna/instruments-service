#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: the Barchart/massive VIX cash-index parquet objects (instrument_type=index, CBOE)
#              have been deleted from the live tradfi tick bucket (verified 0 remaining under the
#              instrument_type=index prefix across batch_massive/batch_barchart pipeline_modes).
"""delete_vix_cash_index_gcs_objects_2026_06_23.py — delete the VIX cash-index GCS objects.

Operator decision 2026-06-23: the VIX *cash index* is removed from the universe in favour of the
VIX *futures* (VX, XCBF.PITCH) — the futures carry the same/richer info (trade more often, longer
window, finer granularity; the index is derivable from them and is not tradable). The
VX-futures-vs-VIX-index sanity check confirmed they track (corr 0.95-0.98, steady ~1.7-2.1
vol-point contango basis). So after the manifest correction
(``correct_tradfi_universe_floor_clip_and_vix_index.py``) removed the VIX cash-index manifest
cells, this deletes the underlying Barchart/massive VIX-index parquet OBJECTS.

Target = every ``instrument_type=index`` object at ``venue=CBOE`` in the tradfi tick bucket (CBOE's
only cash index is VIX), across the batch_massive / batch_barchart / batch_databento pipeline_modes
(the index 15m/24h/1m series). Uses ``gcs_delete_object`` (never gsutil — the GCS-object-ops SSOT).
The VX *futures* objects live under ``instrument_type=futures_chain`` / ``=future`` and are NOT
touched.

Usage::

    python scripts/delete_vix_cash_index_gcs_objects_2026_06_23.py            # dry-run (list only)
    python scripts/delete_vix_cash_index_gcs_objects_2026_06_23.py --apply    # delete
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)

#: The cash-index discriminator segment in the canonical GCS path. CBOE's only cash index is VIX.
_INDEX_SEGMENT = "/instrument_type=index/"
_CBOE_SEGMENT = "/venue=CBOE/"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="tick-data", asset_group="tradfi")


def _list_vix_index_objects(bucket: str) -> list[str]:
    """Return every VIX cash-index object name (instrument_type=index AND venue=CBOE) in the bucket."""
    from unified_trading_library import get_project_id, get_storage_client

    st = get_storage_client()
    out: list[str] = []
    # The raw tick data lives under by_date/. ``list_blobs(bucket=, prefix=)`` returns blob objects
    # with ``.name`` (same pattern as dedup_phantom_after_recovery.py / the other IS shard scripts).
    for blob in st.list_blobs(bucket=bucket, prefix="raw_tick_data/by_date/"):
        name = blob.name
        if _INDEX_SEGMENT in name and _CBOE_SEGMENT in name and name.endswith(".parquet"):
            out.append(name)
    logger.info("project=%s bucket=%s — found %d VIX cash-index objects", get_project_id(), bucket, len(out))
    return out


def run(*, apply: bool) -> int:
    from unified_trading_library import gcs_delete_object

    bucket = _bucket()
    objects = _list_vix_index_objects(bucket)
    if not objects:
        logger.info("No VIX cash-index objects found — nothing to delete (already clean).")
        return 0
    # Show a sample so the operator can eyeball the target set.
    for name in objects[:10]:
        logger.info("  target: gs://%s/%s", bucket, name)
    if len(objects) > 10:
        logger.info("  ... and %d more", len(objects) - 10)

    if not apply:
        logger.info("DRY RUN — %d VIX cash-index objects would be deleted. Re-run with --apply.", len(objects))
        return 0

    deleted = 0
    for name in objects:
        gcs_delete_object(f"gs://{bucket}/{name}")
        deleted += 1
        if deleted % 100 == 0:
            logger.info("  deleted %d/%d", deleted, len(objects))
    logger.info("APPLIED — deleted %d VIX cash-index objects from gs://%s", deleted, bucket)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Delete (default: dry-run list)")
    args = p.parse_args(argv)
    return run(apply=args.apply)


if __name__ == "__main__":
    sys.exit(main())
