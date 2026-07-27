"""Wipe pre-2020-06-06 (pre-floor) sports FIXTURES_SCHEDULE/FIXTURES_OUTCOMES objects.

# Epic: sports_master_closeout_2026_07_21 (2020-06 data floor + pre-floor wipe)
# Lifecycle: one-off remediation executor — DELETE after the fixtures pre-floor wipe is verified.
# Delete-when: a fresh census confirms 0 pre-floor entity=fixtures_schedule|fixtures_outcomes
#   objects remain under sports_reference/by_date/.

OPERATOR RULING 2026-07-21 (authoritative, codex/02-data/sports-2020-06-data-floor.md, decision 14
2026-07-23): sports odds tick data starts 2020-06-06; everything pre-floor is fabrication-by-
construction and is WIPED from GCS + manifest. This targets the population the floor doc's own
"ALSO STILL OPEN" bullet names as distinct from the already-executed 2026-07-21
`wipe_pre_floor_sports_2026_07_21.py` campaign: the ~83,541-row `FIXTURES_SCHEDULE`/
`FIXTURES_OUTCOMES` population found by the 2026-07-22 orphan-sweep audit
(plans/active/issues/sports_pre_floor_fixtures_orphan_misclassification_2026_07_22.md), which was
misclassified `E_orphan_real` due to a since-fixed `SPORTS_DATA_TYPE_TO_SOURCE` registry gap
(unified-api-contracts@46d865df).

WHY A FRESH ENUMERATION, NOT THE DURABLE ORPHAN-SWEEP PARQUET: the audit parquet at
gs://.../_index/audit/orphan_sweep_sports.parquet was regenerated 2026-07-25 (after the registry
fix landed) and now correctly classifies these rows C3_pre_launch_window -- a class the sweep
script's report deliberately excludes (it only ever writes B/B2/E rows). So the durable parquet no
longer contains this population; re-deriving the delete list from it is not possible without a full
re-classification pass. Measured 2026-07-27: the objects are still physically present in GCS (a
delimiter probe of day=2018-01-01/2019-06-15/2020-01-01/2020-06-05 found ONLY
entity=fixtures_schedule and entity=fixtures_outcomes under each -- no other entity survives at
these pre-floor days, confirming this population is exactly and only the target).

MEASURED DRIFT FROM THE ORIGINALLY-CITED COUNT: the plan/issue docs cite 83,541 rows from the
2026-07-21 audit. A fresh count at execution time (2026-07-27) found only 30,182 real GCS objects
(15,233 fixtures_schedule + 14,949 fixtures_outcomes) across the same 872 pre-floor day dirs. The
gap is not explained by this script (the durable audit parquet that would show the original
breakdown was itself overwritten 2026-07-25, before this run) -- per the data domain's own
"trust the actual distribution, not the constant" principle, this script deletes the MEASURED
current population, not the stale cited figure. See the plan's evidence line for the reconciliation
note.

SCOPE — bounded, not a whole-corpus walk: 872 pre-floor day dirs exist under
sports_reference/by_date/ (2018-01-01..2020-06-05; the 2014-2017 span was already wiped by the
2026-07-21 campaign). Objects are enumerated per pre-floor day dir via a bucket delimiter listing
(list of day= prefixes) + a per-day-dir prefix listing (bounded to ~872 calls, not a bucket-wide
walk), then filtered to keep only entity=fixtures_schedule|fixtures_outcomes paths — never blind-
deleting everything under a pre-floor day dir, since that would exceed this todo's scope (only the
two named data_types are ruled here; any other entity type surviving under a pre-floor day belongs
to a different track).

SAFETY:
  * day< FLOOR is re-asserted per object at listing time AND at delete time (matches the
    2026-07-21 script's triple-check discipline).
  * --snapshot writes the full pre-floor object-name list first (recovery record) before any
    delete — mirrors the mirrored Track F `derived_features` purge procedure this todo cites.
  * §3a reversibility-qualified carve-out (codex/02-data/gcs-and-manifest-delete-safety-
    protocol.md): this bucket's soft-delete retention was FRESH-CHECKED in this same execution
    session via gcs_bucket_soft_delete_retention_seconds() = 604800s (7 days) — re-check fresh at
    execution time, never reuse a prior session's citation.
  * deletes go through the UTL gcs_delete_object helper (no subprocess gcloud/gsutil), thread pool.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor

from unified_trading_library import (
    gcs_bucket_soft_delete_retention_seconds,
    gcs_delete_object,
    get_storage_client,
    resolve_bucket_name,
)

FLOOR = dt.date(2020, 6, 6)
ROOT_PREFIX = "sports_reference/by_date/"
TARGET_ENTITIES = ("entity=fixtures_schedule/", "entity=fixtures_outcomes/")
_DAY_DIR_RE = re.compile(r"day=(\d{4}-\d{2}-\d{2})/?$")
_DAY_RE = re.compile(r"/day=(\d{4}-\d{2}-\d{2})/")


def parse_day(path: str) -> dt.date | None:
    m = _DAY_RE.search(path if path.endswith("/") else path + "/")
    if not m:
        return None
    try:
        return dt.date.fromisoformat(m.group(1))
    except ValueError:
        return None


def list_pre_floor_day_dirs(bucket: str) -> list[str]:
    """Delimiter listing of day= dirs directly under ROOT_PREFIX (bounded, sanctioned)."""
    client = get_storage_client()
    bucket_handle = client.bucket(bucket)
    iterator = bucket_handle.list_blobs(prefix=ROOT_PREFIX, delimiter="/")
    prefixes: list[str] = []
    for _ in iterator:  # exhaust the iterator so .prefixes is populated
        pass
    for p in iterator.prefixes:
        m = _DAY_DIR_RE.search(p)
        if not m:
            continue
        d = dt.date.fromisoformat(m.group(1))
        if d < FLOOR:
            prefixes.append(p)
    return sorted(prefixes)


def collect_target_objects(bucket: str, pre_floor_day_dirs: list[str]) -> list[str]:
    client = get_storage_client()
    names: list[str] = []
    for i, day_prefix in enumerate(pre_floor_day_dirs, 1):
        for b in client.list_blobs(bucket, prefix=day_prefix):
            name = str(b.name)
            if not any(f"/{e}" in f"/{name}" for e in TARGET_ENTITIES):
                continue
            d = parse_day(name)
            if d is not None and d < FLOOR:  # re-assert the cutoff per object
                names.append(name)
        if i % 100 == 0:
            print(
                f"  scanned {i}/{len(pre_floor_day_dirs)} pre-floor day dirs, {len(names):,} target objects so far",
                flush=True,
            )
    return names


def do_delete(bucket: str, names: list[str], workers: int) -> dict[str, int]:
    verdicts: dict[str, int] = {"DELETED": 0, "ERROR": 0}

    def _one(name: str) -> str:
        d = parse_day(name)
        if d is None or d >= FLOOR:
            return "SKIP_NOT_PRE_FLOOR"
        if not any(f"/{e}" in f"/{name}" for e in TARGET_ENTITIES):
            return "SKIP_NOT_TARGET_ENTITY"
        try:
            gcs_delete_object(f"gs://{bucket}/{name}")
            return "DELETED"
        except Exception as exc:
            return f"ERROR:{str(exc)[:80]}"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, v in enumerate(ex.map(_one, names), 1):
            key = v.split(":")[0]
            verdicts[key] = verdicts.get(key, 0) + 1
            if i % 5000 == 0:
                print(f"  {i:,}/{len(names):,} {verdicts}", flush=True)
    return verdicts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True, help="json path for the pre-floor object-name snapshot")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: census only)")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")

    retention = gcs_bucket_soft_delete_retention_seconds(bucket)
    print(f"=== fresh soft-delete retention check :: {bucket} -> {retention}s ===", flush=True)
    if retention < 604800:
        print(f"!!! retention {retention}s < 604800s (7d) — does NOT qualify for the §3a carve-out. ABORT.", flush=True)
        return 1

    print(
        f"=== pre-floor fixtures wipe {'APPLY' if args.apply else 'CENSUS'} :: gs://{bucket}/{ROOT_PREFIX} floor={FLOOR} entities={TARGET_ENTITIES} ===",
        flush=True,
    )

    day_dirs = list_pre_floor_day_dirs(bucket)
    print(f"  pre-floor day dirs (<{FLOOR}): {len(day_dirs):,}", flush=True)
    if not day_dirs:
        print(">>> no pre-floor day dirs found — nothing to do", flush=True)
        return 0

    names = collect_target_objects(bucket, day_dirs)
    print(f"\n  >>> pre-floor target OBJECTS to wipe: {len(names):,}", flush=True)

    with open(args.snapshot, "w", encoding="utf-8") as f:
        json.dump(
            {
                "bucket": bucket,
                "root_prefix": ROOT_PREFIX,
                "target_entities": list(TARGET_ENTITIES),
                "floor": FLOOR.isoformat(),
                "pre_floor_object_count": len(names),
                "pre_floor_objects": names,
            },
            f,
        )
    print(f"  snapshot written: {args.snapshot} ({len(names):,} paths)", flush=True)

    if not args.apply:
        print("\n(CENSUS only — nothing deleted. Re-run with --apply to delete.)", flush=True)
        return 0

    print("\n  DELETING…", flush=True)
    verdicts = do_delete(bucket, names, args.workers)
    print(f"\n=== WIPE RESULT: {verdicts} ===", flush=True)
    return 0 if verdicts.get("ERROR", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
