#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the CeFi by_date corpus is confirmed to have exactly one
#   file per (day, venue) and no new legacy-shorter-path files reappear across
#   a real production capture cycle.
"""Deduplicate CeFi by_date's two coexisting GCS path shapes.

Found 2026-07-13 while investigating why a full CeFi catalog regen walked
100,668 by_date parquets for only ~2,400 real distinct (day, venue) pairs per
venue (roughly 2x the naive expectation): every CeFi venue has TWO path shapes
for the same (day, venue) coexisting in GCS —

    NEW:    instrument_availability/by_date/day={D}/pipeline_mode=batch_instruments_service/asset_group=cefi/venue={V}/instruments.parquet
    LEGACY: instrument_availability/by_date/day={D}/venue={V}/instruments.parquet

The current, live write path (``process_write.py::_write_all_venues`` →
``get_data_sink(prefix="instrument_availability/by_date")`` with
``pipeline_mode=``/``asset_group=`` stamped per record) ONLY ever produces the
NEW shape — confirmed by reading the writer directly. The LEGACY shape is pure
historical residue from before that path convention existed; nothing writes it
today. A sample verified every LEGACY/NEW pair is byte-for-byte IDENTICAL
across all 5 previously-touched venues (BYBIT/KRAKEN-FUTURES/DERIBIT/
OKX-SWAP/OKX-FUTURES, 15 pairs sampled, 0 mismatches) — so this is pure
redundant storage + redundant catalog-rollup I/O, not a correctness bug (the
rollup's ``build_catalogue_dataframe`` aggregates by ``instrument_key``, so
reading both copies of an unchanged day just repeats work, it doesn't corrupt
lifecycle tracking).

This script quarantines (server-side copy to ``_migration_backup/`` + delete
the source — never a blind destructive delete) the LEGACY copy of every pair
verified byte-identical to its NEW counterpart, across ALL 24 CeFi venues (not
just the 5 touched by ``cefi_durability_force_converge_2026_07_10.py`` — this
path-shape duplication is a corpus-wide artifact, unrelated to that script's
instrument_key/expiry fixes). A pair whose two copies are NOT identical is
left untouched and reported for manual review — never guessed. A (day, venue)
with only ONE shape present (either one) is left alone; there is nothing to
deduplicate.

Usage::

    cd instruments-service
    .venv/bin/python scripts/cefi_legacy_path_dedup_2026_07_13.py --dry-run
    .venv/bin/python scripts/cefi_legacy_path_dedup_2026_07_13.py --apply --workers 32

SSOT: unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage as _gcs_sdk  # noqa: qg-deep-import — read-only bulk LISTING only (server-side

# match_glob filtering), matching the exact carve-out already established in
# cefi_durability_force_converge_2026_07_10.py — see that module's docstring.
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BY_DATE_PREFIX = "instrument_availability/by_date/"
QUARANTINE_PREFIX = "_migration_backup/cefi_legacy_path_dedup_2026_07_13/"
_DAY_VENUE_RE = re.compile(r"day=(\d{4}-\d{2}-\d{2})/(?:.*/)?venue=([^/]+)/instruments\.parquet$")


def _list_all_current(bucket: str) -> list[str]:
    """One bulk listing pass across every venue's current instruments.parquet."""
    gcs_client = _gcs_sdk.Client()
    gcs_bucket = gcs_client.bucket(bucket)
    out: list[str] = []
    for b in gcs_client.list_blobs(gcs_bucket, prefix=BY_DATE_PREFIX, match_glob="**/venue=*/instruments.parquet"):
        out.append(str(b.name))
    return out


def _group_by_day_venue(blobs: list[str]) -> dict[tuple[str, str], dict[str, str]]:
    """Group blob paths by (day, venue), classifying each into 'new' or 'legacy' shape."""
    groups: dict[tuple[str, str], dict[str, str]] = {}
    for blob in blobs:
        m = _DAY_VENUE_RE.search(blob)
        if m is None:
            continue
        day, venue = m.group(1), m.group(2)
        shape = "new" if "pipeline_mode=" in blob else "legacy"
        groups.setdefault((day, venue), {})[shape] = blob
    return groups


def _dedup_one_pair(st, bucket: str, day: str, venue: str, new_blob: str, legacy_blob: str, apply: bool) -> str:
    """Quarantine + delete legacy_blob iff byte-identical to new_blob. Returns an outcome tag."""
    new_raw = st.download_bytes(bucket, new_blob)
    legacy_raw = st.download_bytes(bucket, legacy_blob)
    if new_raw != legacy_raw:
        return "mismatch_left_untouched"
    if not apply:
        return "would_quarantine"
    backup_dest = QUARANTINE_PREFIX + legacy_blob
    if not st.blob_exists(bucket, backup_dest):
        st.upload_bytes(bucket, backup_dest, legacy_raw)
    verify = st.download_bytes(bucket, backup_dest)
    if len(verify) != len(legacy_raw):
        return "verify_size_mismatch_source_kept"
    st.delete_blob(bucket, legacy_blob)
    return "quarantined"


def run(apply: bool, workers: int) -> None:
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    st = get_storage_client()
    logger.info("bucket=%s mode=%s", bucket, "APPLY" if apply else "DRY-RUN")

    logger.info("=== listing (one server-side bulk pass across all venues) ===")
    t0 = time.time()
    all_current = _list_all_current(bucket)
    logger.info("listed %d current instruments.parquet file(s) in %.1fs", len(all_current), time.time() - t0)

    groups = _group_by_day_venue(all_current)
    pairs = [
        (day, venue, shapes["new"], shapes["legacy"])
        for (day, venue), shapes in groups.items()
        if "new" in shapes and "legacy" in shapes
    ]
    single_shape = sum(1 for shapes in groups.values() if len(shapes) == 1)
    logger.info(
        "%d (day, venue) group(s) total: %d with BOTH shapes (candidates), %d with only one shape (left alone)",
        len(groups),
        len(pairs),
        single_shape,
    )

    results: dict[str, int] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_dedup_one_pair, st, bucket, day, venue, new_blob, legacy_blob, apply): (day, venue)
            for day, venue, new_blob, legacy_blob in pairs
        }
        for i, fut in enumerate(as_completed(futures), start=1):
            day, venue = futures[fut]
            try:
                outcome = fut.result()
            except Exception:
                logger.exception("FAILED dedup day=%s venue=%s", day, venue)
                outcome = "error"
            results[outcome] = results.get(outcome, 0) + 1
            if i % 2000 == 0 or i == len(pairs):
                elapsed = time.time() - t0
                logger.info("[%d/%d] elapsed=%.1fs outcomes=%s", i, len(pairs), elapsed, results)

    logger.info("DONE: candidates=%d outcomes=%s elapsed=%.1fs", len(pairs), results, time.time() - t0)
    if results.get("mismatch_left_untouched"):
        logger.warning(
            "%d pair(s) had NON-identical new/legacy content — left untouched, needs manual review",
            results["mismatch_left_untouched"],
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry-run/report only).")
    ap.add_argument("--workers", type=int, default=32, help="Thread-pool size (default 32).")
    args = ap.parse_args(argv)
    run(args.apply, args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
