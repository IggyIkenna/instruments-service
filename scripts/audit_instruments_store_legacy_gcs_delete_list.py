#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: instruments-store legacy-shape audit confirmed + any SAFE-TO-DELETE delete-list
#              approved+run by operator (instruments_mtds_subset_consistency_remediation_2026_06_17.md)
"""READ-ONLY audit: classify every object in each instruments-store bucket by path shape and
derive a per-AG legacy delete-list parquet (legacy_path, canonical_twin_path, twin_exists,
classification, reason, bytes) — the instruments-store analogue of the market-data
``audit_legacy_gcs_dup_delete_list.py`` (e2e-testing/scripts/defi/).

IMPORTANT — instruments-store is REFERENCE data, NOT market-data tick data. Its canonical GCS
layout has **NO `pipeline_mode=` and NO `asset_group=` hive keys** (one AG per bucket; no
batch/live mode concept for a reference snapshot). The canonical shapes are:

  * ``prod/catalog.parquet``                                            (rolled-up catalogue)
  * ``instrument_availability/by_date/day={D}/.../instruments.parquet`` (per-shard availability;
      cefi/defi/tradfi/pred partition by ``venue=``; sports by ``league=``/``venue=``)
  * control prefixes: ``_catalogue/`` ``_index/`` ``_cache/`` ``_backups/`` ``_vm_staging/``
      ``_audits/`` ``_smoke_test/`` ``availability_index/`` ``sports_reference*/``

So the market-data ``pipeline_mode``-twin model DOES NOT apply. A "legacy" object here is a
DATA parquet living OUTSIDE the canonical ``instrument_availability/by_date/`` (underscore-hive)
+ ``prod/`` shapes — specifically:

  * bare top-level ``day={D}/venue=.../...parquet``                 (no ``instrument_availability/``)
  * dash-separator ``instrument_availability/by-date/day-{D}/{slug}/...`` (legacy odds-api shape)

Twin-derivation + existence check: the only mappable-by-rename legacy shape would be the bare
top-level ``day=`` (→ prepend ``instrument_availability/by_date/`` + normalise the hive). The
dash-separator odds shape is UNMAPPABLE (different data source/schema — odds-api odds vs
api-football fixtures — and a non-translatable league slug), so it is honest-absence residue:
excluded from the delete-list, reported only.

READ-ONLY: lists + reads only. Writes the audit parquet to ``gs://<bucket>/_index/audit/`` only.
NEVER deletes/moves an object or touches a live ``_index/availability_index.parquet``.

Usage::

    GCP_PROJECT_ID=central-element-323112 \
      python scripts/audit_instruments_store_legacy_gcs_delete_list.py [--ag cefi,...] [--no-write]
"""

from __future__ import annotations

import argparse
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("instruments_store_legacy_audit")

# instruments-store asset_groups → resolve_bucket_name asset_group arg. Sports + the four
# AG-keyed kinds resolve via kind="instruments-store"; prediction uses the flat
# kind="instruments-store-prediction" (no per-AG key) — both resolved here uniformly.
_AGS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Canonical DATA prefixes (an object under one of these is canonical, never legacy).
_CANON_DATA_PREFIXES: tuple[str, ...] = ("instrument_availability/by_date/", "prod/")
# Control / index / reference prefixes (never a data-shape concern; left untouched).
_CONTROL_PREFIXES: tuple[str, ...] = (
    "_catalogue/",
    "_index/",
    "_cache/",
    "_backups/",
    "_vm_staging/",
    "_audits/",
    "_smoke_test/",
    "availability_index/",
    "sports_reference",
    "instrument_catalogue",
)


def _bucket_for(asset_group: str) -> str:
    kind = "instruments-store-prediction" if asset_group == "prediction" else "instruments-store"
    ag_arg = None if asset_group == "prediction" else asset_group
    return resolve_bucket_name(cloud="gcp", kind=kind, asset_group=ag_arg)


@dataclass
class AgResult:
    ag: str
    bucket: str = ""
    total_parquet: int = 0
    canonical_data: int = 0
    control: int = 0
    legacy_bare_day: int = 0
    legacy_dash_separator: int = 0
    safe_to_delete: int = 0
    migrate_first: int = 0
    unmappable: int = 0
    safe_bytes: int = 0
    migrate_bytes: int = 0
    unmappable_bytes: int = 0
    rows: list[dict[str, object]] = field(default_factory=list)


def _classify_shape(name: str) -> str:
    if any(name.startswith(p) for p in _CANON_DATA_PREFIXES):
        return "canonical_data"
    if any(name.startswith(p) for p in _CONTROL_PREFIXES):
        return "control"
    if name.startswith("instrument_availability/by-date/"):
        return "legacy_dash_separator"
    if name.startswith("day=") or name.startswith("category=") or name.startswith("asset_group="):
        return "legacy_bare_day"
    return "other"


def _derive_bare_day_twin(name: str, canon_names: set[str]) -> tuple[str, bool, str]:
    """Map a bare top-level ``day={D}/...`` legacy object to its canonical twin.

    Canonical = ``instrument_availability/by_date/`` + the same ``day=.../...`` tail. The hive
    keys are already ``=``-separated in the bare shape, so the only transform is the prefix.
    Twin existence = membership in the listed canonical name-set (authoritative, no per-object
    stat). Returns ``(twin_path, twin_exists, reason)``. A bare-day object whose tail uses a
    DIFFERENT partition axis than canonical (e.g. ``venue=BETFAIR`` where canonical is
    ``league=``) yields a twin path that simply will not exist → MIGRATE-FIRST, never deleted.
    """
    twin = f"instrument_availability/by_date/{name}"
    return twin, (twin in canon_names), "bare_day_prefix_rehome"


def audit_ag(asset_group: str, workers: int) -> AgResult:
    storage = get_storage_client()
    bucket = _bucket_for(asset_group)
    res = AgResult(ag=asset_group, bucket=bucket)

    # Single full listing — instruments-store buckets are small enough (≤~900k objects on
    # sports, dominated by control prefixes we skip) for one walk; canonical name-set built
    # in the same pass so the twin-check is in-memory. The UCI client yields BlobMetadata
    # (``.name`` / ``.size``) — cloud-agnostic, no direct google.cloud.storage handle.
    canon_names: set[str] = set()
    legacy_objs: list[tuple[str, int, str]] = []

    for blob in storage.list_blobs(bucket):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        res.total_parquet += 1
        shape = _classify_shape(name)
        if shape == "canonical_data":
            res.canonical_data += 1
            canon_names.add(name)
        elif shape == "control":
            res.control += 1
        elif shape == "legacy_bare_day":
            res.legacy_bare_day += 1
            legacy_objs.append((name, blob.size or 0, shape))
        elif shape == "legacy_dash_separator":
            res.legacy_dash_separator += 1
            legacy_objs.append((name, blob.size or 0, shape))
        else:  # "other" parquet outside every known prefix — treat as unmappable legacy
            legacy_objs.append((name, blob.size or 0, "other"))

    for name, size, shape in legacy_objs:
        if shape == "legacy_bare_day":
            twin, exists, _reason = _derive_bare_day_twin(name, canon_names)
            if exists:
                res.safe_to_delete += 1
                res.safe_bytes += size
                res.rows.append(
                    {
                        "legacy_path": name,
                        "canonical_twin_path": twin,
                        "twin_exists": True,
                        "classification": "SAFE-TO-DELETE",
                        "reason": "canonical_twin_verified",
                        "bytes": size,
                    }
                )
            else:
                # bare-day with a different partition axis than canonical → no rename twin.
                res.migrate_first += 1
                res.unmappable += 1
                res.unmappable_bytes += size
                res.rows.append(
                    {
                        "legacy_path": name,
                        "canonical_twin_path": twin,
                        "twin_exists": False,
                        "classification": "UNMAPPABLE",
                        "reason": "bare_day_no_canonical_twin_different_partition_axis",
                        "bytes": size,
                    }
                )
        else:
            # dash-separator odds-api shape / other → different data source+schema, no
            # deterministic canonical-rename → honest-absence residue, never deleted.
            res.migrate_first += 1
            res.unmappable += 1
            res.unmappable_bytes += size
            res.rows.append(
                {
                    "legacy_path": name,
                    "canonical_twin_path": "",
                    "twin_exists": False,
                    "classification": "UNMAPPABLE",
                    "reason": (
                        "dash_separator_legacy_odds_source_not_canonical_twin"
                        if shape == "legacy_dash_separator"
                        else "parquet_outside_canonical_prefixes"
                    ),
                    "bytes": size,
                }
            )
    return res


def write_audit_parquet(asset_group: str, bucket: str, rows: list[dict[str, object]]) -> str:
    storage = get_storage_client()
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=["legacy_path", "canonical_twin_path", "twin_exists", "classification", "reason", "bytes"]
        )
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    dest = f"_index/audit/instruments_store_legacy_delete_list_{asset_group}.parquet"
    storage.upload_from_file_obj(bucket, dest, buf)  # type: ignore[attr-defined]
    return f"gs://{bucket}/{dest}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ag", default="", help="comma list of asset_groups (default all)")
    ap.add_argument("--no-write", action="store_true", help="skip writing the audit parquet")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    ags = args.ag.split(",") if args.ag else list(_AGS)

    summary: dict[str, AgResult] = {}
    with ThreadPoolExecutor(max_workers=min(args.workers, len(ags))) as pool:
        futs = {pool.submit(audit_ag, ag, args.workers): ag for ag in ags}
        for fut in as_completed(futs):
            res = fut.result()
            summary[res.ag] = res

    for ag in ags:
        res = summary[ag]
        dest = "(skipped)"
        if not args.no_write:
            dest = write_audit_parquet(ag, res.bucket, res.rows)
        logger.info(
            "RESULT %s (%s): parquet=%d canonical=%d control=%d "
            "legacy[bare_day=%d dash=%d] SAFE-TO-DELETE=%d (%.3fGB) "
            "UNMAPPABLE=%d (%.3fGB) audit=%s",
            ag,
            res.bucket,
            res.total_parquet,
            res.canonical_data,
            res.control,
            res.legacy_bare_day,
            res.legacy_dash_separator,
            res.safe_to_delete,
            res.safe_bytes / 1e9,
            res.unmappable,
            res.unmappable_bytes / 1e9,
            dest,
        )

    ts = sum(r.safe_to_delete for r in summary.values())
    sb = sum(r.safe_bytes for r in summary.values())
    tu = sum(r.unmappable for r in summary.values())
    ub = sum(r.unmappable_bytes for r in summary.values())
    logger.info(
        "AGGREGATE: SAFE-TO-DELETE=%d (%.3fGB reclaimable) UNMAPPABLE-residue=%d (%.3fGB, NOT deleted)",
        ts,
        sb / 1e9,
        tu,
        ub / 1e9,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
