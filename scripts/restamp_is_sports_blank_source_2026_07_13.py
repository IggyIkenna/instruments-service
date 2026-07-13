#!/usr/bin/env python3
# Epic: sports_manifest_canonicalisation
# Lifecycle: oneoff
# Delete-when: after blank source='' rows in instruments-store-sports-prd are fixed (post-run 2026-07-13)
"""Restamp source= column for blank-source rows in the IS sports manifest.

CF-4 gap (tracked across sports_manifest_canonicalisation_2026_06_01.md E8 audit
touches 9-18): 796,523 rows in `instruments-store-sports-prd-central-element-323112`
have blank `source=`. Root-caused this session (2026-07-13):

  - 19,274 of those rows ALSO have blank `pipeline_mode` — cannot be restamped
    deterministically (no pipeline_mode to derive source from); these are the
    same population as the CF-3 blank-pipeline_mode gap, E4-migration scope,
    left untouched by this script.
  - The remaining 777,249 rows have a non-blank `pipeline_mode` (batch_api_football
    732,804 / batch_footystats 27,313 / batch_understat 17,101 /
    batch_soccer_football_info 31) and can be deterministically restamped via
    `source_string_for(pipeline_mode)` — the same derivation
    `ManifestWriter._stamp_producer_source()` (unified-trading-library@ca5f1dbd,
    manifest_record_expected_empty_blank_source_2026_07_08) already applies at
    WRITE time for any row landing since 2026-07-08T23:28:03Z. Confirmed via a
    time-based read of the live index: 0 of the 19,274 blank-pipeline_mode rows
    and 0 of the pre-fix blank-source rows carry attempted_at after that
    timestamp; a residual 2,902 post-fix blank-source rows DO exist
    (batch_api_football 2,842 / batch_footystats 60, attempted_at up to
    2026-07-10T14:53Z) tracing to `sports_reference_core.py`'s
    `note_empty()`/`emit_empty_gaps_for_entity()` (record_empty() call sites
    that never passed source= explicitly) — the ca5f1dbd auto-stamp SHOULD
    cover these going forward once the live/cron process running these
    callsites has picked up the current unified-trading-library pin; this
    script's --apply also restamps the already-landed 2,902 as one-off cleanup
    (same deterministic derivation, no live-write risk).

This is the exact same restamp pattern already shipped + run successfully for
the MTDS sports surface (market-tick-data-service/scripts/
restamp_mtds_sports_blank_source_2026_06_29.py, mtds@bae321ca, CF-4 GREEN
post-run) — this script is its IS-surface counterpart.

DRY-RUN by default. Use --apply to write back the updated index. Run with the
manifest consolidator paused to avoid a read-modify-write race.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("restamp_is_sports_blank_source")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_INDEX_BLOB = "_index/availability_index.parquet"
_SNAPSHOT_BLOB = "_index/snapshots/pre_blank_source_restamp_{date}.parquet"


def _cp(src: str, dst: str) -> bool:
    res = subprocess.run(["gcloud", "storage", "cp", src, dst], capture_output=True, text=True, check=False)
    return res.returncode == 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write restamped index (default: dry-run)")
    ap.add_argument("--bucket", default=_BUCKET, help="Override bucket name")
    ap.add_argument("--project-id", default="central-element-323112")
    args = ap.parse_args()

    import pandas as pd
    from unified_api_contracts import PipelineMode, source_string_for

    bucket = args.bucket
    index_uri = f"gs://{bucket}/{_INDEX_BLOB}"
    logger.info("Downloading %s …", index_uri)

    with tempfile.TemporaryDirectory() as tmp:
        local_idx = str(Path(tmp) / "idx.parquet")
        if not _cp(index_uri, local_idx):
            logger.error("Failed to download %s", index_uri)
            return 1

        df = pd.read_parquet(local_idx)
        logger.info("Loaded %d rows", len(df))

        # Identify rows with blank source but non-blank pipeline_mode
        mask_blank = df["source"].fillna("").astype(str).str.strip() == ""
        mask_has_pm = df["pipeline_mode"].fillna("").astype(str).str.strip() != ""
        target = mask_blank & mask_has_pm
        logger.info("Rows with blank source + non-blank pipeline_mode: %d", target.sum())
        logger.info(
            "Rows with blank source + blank pipeline_mode (NOT touched — E4-migration scope): %d",
            int((mask_blank & ~mask_has_pm).sum()),
        )

        if target.sum() == 0:
            logger.info("Nothing to restamp — CF-4 already GREEN")
            return 0

        # Build source string for each unique pipeline_mode
        pm_to_source: dict[str, str] = {}
        for pm_val in df.loc[target, "pipeline_mode"].unique():
            try:
                src = source_string_for(PipelineMode(pm_val)) or ""
            except (ValueError, KeyError):
                src = ""
            pm_to_source[str(pm_val)] = src
            logger.info("  %s → source='%s'", pm_val, src)

        new_source = df.loc[target, "pipeline_mode"].map(pm_to_source).fillna("")
        # Only restamp rows where we derived a non-empty source
        can_restamp = target & new_source.astype(str).str.strip().ne("")
        logger.info("Rows that can be restamped (non-empty derived source): %d", can_restamp.sum())
        logger.info("Rows with no derivable source (left blank): %d", (target & ~can_restamp).sum())

        if args.apply:
            import datetime

            snap_blob = _SNAPSHOT_BLOB.format(date=datetime.datetime.now(tz=datetime.UTC).date().isoformat())
            snap_uri = f"gs://{bucket}/{snap_blob}"
            logger.info("Snapshotting current index → %s …", snap_uri)
            if not _cp(index_uri, f"gs://{bucket}/{snap_blob}"):
                logger.error("Snapshot failed — aborting")
                return 1

            df.loc[can_restamp, "source"] = new_source[can_restamp]
            local_out = str(Path(tmp) / "idx_restamped.parquet")
            df.to_parquet(local_out, index=False)
            logger.info("Writing restamped index → %s …", index_uri)
            if not _cp(local_out, index_uri):
                logger.error("Write-back failed")
                return 1
            logger.info("DONE — restamped %d rows", int(can_restamp.sum()))
        else:
            logger.info(
                "DRY-RUN — would restamp %d rows (pipeline_mode dist: %s) — rerun with --apply to write",
                int(can_restamp.sum()),
                df.loc[can_restamp, "pipeline_mode"].value_counts().to_dict(),
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
