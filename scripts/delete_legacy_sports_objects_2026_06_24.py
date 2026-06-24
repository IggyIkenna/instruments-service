#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the legacy (no-pipeline_mode) sports objects + sports_reference_v1_archive are
#   deleted (twin-verified) and the campaign plan
#   sports_canonical_universe_and_apifootball_reference_expansion_2026_06_24 archives.
"""Twin-verified delete of LEGACY sports GCS objects (E8) — the canonical layer is complete + CF-GREEN.

The migrate_sports_canonical_v9 --apply inserted ``pipeline_mode=`` into every object path (additive
copy). The pre-migration LEGACY twins (same path WITHOUT ``pipeline_mode=``) remain as dead weight.
This deletes them ONLY when their canonical ``pipeline_mode=`` twin exists (twin-verified — never an
unconditional delete). Also deletes ``sports_reference_v1_archive/`` (verified canon-only=0 — its
af_fixture_ids are a subset of the canonical layer; its xg/stats columns were null).

NOT touched (separate handling): ``day=all`` (teams/venues — needs FLAT reconcile first), the no-env
legacy bucket, bare-for-old-days remnants.

DRY-RUN by default (prints deletable / skipped-no-twin counts). ``--apply`` deletes. Uses
``gcs_delete_object``-equivalent via gcsfs (script-only). The canonical twin IS the preserved copy,
so no separate snapshot is needed for the twins; the archive is its own historical snapshot.
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("delete_legacy_sports_objects")

_BY_DATE = "sports_reference/by_date/"
_ARCHIVE = "sports_reference_v1_archive/"


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _canonical_twin(rel: str, pm_for_entity) -> str | None:
    """Legacy rel -> canonical twin rel (insert pipeline_mode= after day=). None if not mappable."""
    if not rel.endswith(".parquet"):
        return None
    if "/pipeline_mode=" in rel:
        return None  # already canonical (not a legacy object)
    # parse day= and entity=
    parts = {seg.split("=", 1)[0]: seg.split("=", 1)[1] for seg in rel.split("/") if "=" in seg}
    day = parts.get("day")
    entity = parts.get("entity")
    if not day or not entity:
        return None
    mode = pm_for_entity(entity).value
    marker = f"day={day}/"
    if marker not in rel:
        return None
    return rel.replace(marker, f"day={day}/pipeline_mode={mode}/", 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Delete (default: dry-run count)")
    ap.add_argument(
        "--include-archive",
        action="store_true",
        help="Also delete sports_reference_v1_archive/ (verified canon-only=0)",
    )
    ap.add_argument("--bucket", default=None)
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Bucket: %s  apply=%s include_archive=%s", bucket, args.apply, args.include_archive)

    from unified_api_contracts import pipeline_mode_for_sports_entity  # noqa: imports-inside-functions

    import time  # noqa: imports-inside-functions
    from concurrent.futures import ThreadPoolExecutor  # noqa: imports-inside-functions

    import gcsfs  # noqa: imports-inside-functions

    fs = gcsfs.GCSFileSystem()

    # MEMORY-SAFE + RATE-SAFE: stream PER-DAY (never load the whole 1.57M-object tree into memory —
    # that OOM'd exit 144 + the unbounded fs.rm saturated GCS + stalled the consolidator 2026-06-24).
    # Per day: list that day's objects (small), twin-check within the day, delete bounded + paced.
    day_prefixes = [p for p in fs.ls(f"{bucket}/{_BY_DATE}") if "/day=" in p]
    logger.info("by_date: %d day partitions to stream", len(day_prefixes))

    def _delete_one(o: str) -> bool:
        try:
            fs.rm(o)
            return True
        except Exception as exc:  # noqa: BLE001 — per-object isolation
            logger.warning("delete failed %s: %s", o, exc)
            return False

    tot_legacy = tot_deletable = tot_no_twin = tot_deleted = 0
    no_twin_sample: list[str] = []
    for i, day_p in enumerate(day_prefixes):
        objs = [o for o in fs.find(day_p) if o.endswith(".parquet")]
        canon = {o for o in objs if "/pipeline_mode=" in o}
        legacy = [o for o in objs if "/pipeline_mode=" not in o]
        tot_legacy += len(legacy)
        day_deletable: list[str] = []
        for o in legacy:
            rel = o.split(f"{bucket}/", 1)[1]
            twin = _canonical_twin(rel, pipeline_mode_for_sports_entity)
            if twin and f"{bucket}/{twin}" in canon:
                day_deletable.append(o)
            else:
                tot_no_twin += 1
                if len(no_twin_sample) < 5:
                    no_twin_sample.append(rel)
        tot_deletable += len(day_deletable)
        if args.apply and day_deletable:
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                tot_deleted += sum(ex.map(_delete_one, day_deletable))
            time.sleep(0.2)  # pace: don't saturate GCS / starve the */1 consolidator
        if (i + 1) % 200 == 0:
            logger.info(
                "  %d/%d days | legacy=%d deletable=%d deleted=%d no_twin=%d",
                i + 1,
                len(day_prefixes),
                tot_legacy,
                tot_deletable,
                tot_deleted,
                tot_no_twin,
            )

    logger.info("by_date legacy: DELETABLE=%d NO-TWIN(skip)=%d deleted=%d", tot_deletable, tot_no_twin, tot_deleted)
    if no_twin_sample:
        logger.warning("NO-TWIN sample (NOT deleted): %s", no_twin_sample)

    # archive — stream-delete too
    if args.include_archive:
        arc = [o for o in fs.find(f"{bucket}/{_ARCHIVE}") if o.endswith(".parquet")]
        logger.info(
            "archive: %d parquet objects (verified canon-only=0)%s",
            len(arc),
            " -> deleting" if args.apply else " (dry-run)",
        )
        if args.apply and arc:
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                tot_deleted += sum(ex.map(_delete_one, arc))

    if not args.apply:
        logger.info("DRY RUN — no delete. DELETABLE(by_date)=%d. Re-run with --apply.", tot_deletable)
        return 0
    logger.info("DELETED %d legacy objects total", tot_deleted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
