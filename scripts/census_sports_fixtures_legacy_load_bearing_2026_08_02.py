# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports_legacy_fixtures_path_migration_2026_07_24.md's Phase-1 todo is archived
"""Phase-1 census for `sports_legacy_fixtures_path_migration_2026_07_24.md`.

Resolves the plan's two open ambiguities for the captured/post-floor legacy
`entity=fixtures/` population (data_type="FIXTURES", date >= 2020-06-06):

1. Load-bearing vs. redundant — does the canonical `entity=fixtures_schedule/`
   path genuinely have NO data for a given (date, league_id), making the
   legacy fallback the only real source, vs. canonical already having real
   data (the fallback resolves via canonical and never touches legacy)?
2. Stale-label vs. real-legacy-path — does a `data_type="FIXTURES"` manifest
   row claiming `capture_status="captured"` actually correspond to a real
   object at the bare `entity=fixtures/` path, or is the label stale (a
   leftover from the pre-manifest-atom-migration writer, per
   `restamp_fixtures_manifest_legacy_atom_2026_07_24.py`'s docstring)?

Method (single-walk discipline): ONE `read_availability_index` call (slim
columns, date-floor filter pushdown) narrows the full corpus down to the
captured/post-floor (date, league_id) population, split into:

  - `redundant_manifest`: canonical ALSO manifest-captured for the same key
    — no GCS read needed, the manifest alone proves canonical has data.
  - `candidates`: canonical NOT manifest-captured — the genuinely ambiguous
    subset. Per finding 2 in the plan, a manifest row alone is not proof —
    for EVERY candidate this script performs a REAL GCS object read (exists
    + non-zero-row parquet, not just a blob-existence probe) against both
    the exact legacy path and the exact canonical path for that (date,
    league_id), classifying into:

      - `load_bearing`:      legacy real, canonical genuinely absent
      - `redundant_gcs`:     legacy real, canonical ALSO real (manifest was
                              stale/lagging on the canonical side)
      - `stale_label`:       legacy NOT real (the FIXTURES manifest row's
                              "captured" label doesn't correspond to a real
                              object at the bare path)
      - `read_error`:        a GCS transport error on both retries — NOT
                              folded into any of the above (would mis-count
                              a transient failure as a real classification)

Bounded, read-only. No manifest writes, no GCS writes, no whole-corpus GCS
walk (only the single manifest read; the GCS reads below are per-candidate,
targeted exact-object probes, not a listing walk).

Usage
-----
::

  # Manifest-only pass (fast): report population sizes without any GCS I/O.
  python scripts/census_sports_fixtures_legacy_load_bearing_2026_08_02.py --manifest-only

  # Full census (manifest + per-candidate real GCS confirmation):
  python scripts/census_sports_fixtures_legacy_load_bearing_2026_08_02.py
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from unified_trading_library import get_storage_client, read_availability_index, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _sports_ref_canonical_blob_path,
    _sports_ref_legacy_blob_path,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FLOOR_DATE = "2020-06-06"
_LEGACY_DATA_TYPE = "FIXTURES"
_CANONICAL_DATA_TYPE = "FIXTURES_SCHEDULE"
_LEGACY_ENTITY = "fixtures"
_CANONICAL_ENTITY = "fixtures_schedule"


def _read_manifest_pairs(bucket: str) -> tuple[set[tuple[str, str]], set[tuple[str, str]], int]:
    """One manifest read (single-walk discipline) -> (legacy, canonical) captured (date, league_id) sets."""
    logger.info("Reading manifest (slim columns, floor-filtered) from gs://%s/ ...", bucket)
    df: pd.DataFrame = read_availability_index(
        bucket,
        columns=["date", "data_type", "source", "capture_status", "league_id"],
        filters=[("date", ">=", _FLOOR_DATE)],
    )
    logger.info("  %d total rows read (date >= %s)", len(df), _FLOOR_DATE)
    af = df[df["source"] == "api_football"]

    def _pairs(data_type: str) -> set[tuple[str, str]]:
        sub = af[(af["data_type"] == data_type) & (af["capture_status"] == "captured")]
        sub = sub.dropna(subset=["date", "league_id"])
        return {(str(d), str(lid)) for d, lid in zip(sub["date"], sub["league_id"], strict=False) if str(lid)}

    legacy_pairs = _pairs(_LEGACY_DATA_TYPE)
    canonical_pairs = _pairs(_CANONICAL_DATA_TYPE)
    logger.info(
        "  legacy FIXTURES captured pairs=%d, canonical FIXTURES_SCHEDULE captured pairs=%d",
        len(legacy_pairs),
        len(canonical_pairs),
    )
    return legacy_pairs, canonical_pairs, len(df)


def _blob_real(client: object, bucket: str, path: str, *, retries: int = 3) -> bool | None:
    """Real GCS read: exists AND parses to a non-zero-row parquet. None on a persistent transport error."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            if not client.blob_exists(bucket, path):  # type: ignore[attr-defined]
                return False
            data = client.download_bytes(bucket=bucket, blob_path=path)  # type: ignore[attr-defined]
            frame = pd.read_parquet(io.BytesIO(data))
            return len(frame) > 0
        except Exception as exc:  # GCS transport boundary, retried below
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    logger.warning("read_error after %d attempts on gs://%s/%s: %s", retries, bucket, path, last_exc)
    return None


def _classify_candidate(client: object, bucket: str, date: str, league_id: str) -> tuple[str, str, str]:
    legacy_path = _sports_ref_legacy_blob_path(date, _LEGACY_ENTITY, league=league_id)
    canonical_path = _sports_ref_canonical_blob_path(date, _CANONICAL_ENTITY, league=league_id)

    legacy_real = _blob_real(client, bucket, legacy_path)
    if legacy_real is None:
        return date, league_id, "read_error"
    if not legacy_real:
        return date, league_id, "stale_label"

    canonical_real = _blob_real(client, bucket, canonical_path)
    if canonical_real is None:
        return date, league_id, "read_error"
    return date, league_id, "redundant_gcs" if canonical_real else "load_bearing"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", default="central-element-323112", help="GCP project ID")
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument("--manifest-only", action="store_true", help="Skip the per-candidate real GCS confirmation pass.")
    ap.add_argument(
        "--out-load-bearing",
        default="scripts/_sports_fixtures_legacy_load_bearing_2026_08_02.parquet",
        help="Output parquet listing the exact load-bearing (date, league_id) pairs.",
    )
    ap.add_argument(
        "--out-stale-label",
        default="scripts/_sports_fixtures_legacy_stale_label_2026_08_02.parquet",
        help="Output parquet listing the exact stale-label (date, league_id) pairs (audit trail).",
    )
    ap.add_argument(
        "--out-report",
        default="scripts/_sports_fixtures_legacy_census_report_2026_08_02.json",
        help="Output JSON summary report.",
    )
    args = ap.parse_args()

    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("GCP_PROJECT_ID", args.project)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    logger.info("bucket=%s floor=%s", bucket, _FLOOR_DATE)

    legacy_pairs, canonical_pairs, total_rows_read = _read_manifest_pairs(bucket)
    redundant_manifest = legacy_pairs & canonical_pairs
    candidates = sorted(legacy_pairs - canonical_pairs)

    report: dict[str, object] = {
        "bucket": bucket,
        "floor_date": _FLOOR_DATE,
        "total_manifest_rows_read": total_rows_read,
        "legacy_captured_pairs": len(legacy_pairs),
        "canonical_captured_pairs": len(canonical_pairs),
        "redundant_manifest_count": len(redundant_manifest),
        "candidate_count": len(candidates),
    }

    if args.manifest_only:
        print(json.dumps(report, indent=2))
        Path(args.out_report).write_text(json.dumps(report, indent=2))
        return 0

    client = get_storage_client()
    load_bearing: list[tuple[str, str]] = []
    redundant_gcs: list[tuple[str, str]] = []
    stale_label: list[tuple[str, str]] = []
    read_errors: list[tuple[str, str]] = []

    logger.info(
        "Confirming %d candidates via real per-object GCS reads (workers=%d) ...", len(candidates), args.workers
    )
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_classify_candidate, client, bucket, d, lid): (d, lid) for d, lid in candidates}
        for fut in as_completed(futures):
            done += 1
            if done % 500 == 0 or done == len(candidates):
                print(f"  {done}/{len(candidates)} candidates confirmed", file=sys.stderr)
            date, league_id, verdict = fut.result()
            if verdict == "load_bearing":
                load_bearing.append((date, league_id))
            elif verdict == "redundant_gcs":
                redundant_gcs.append((date, league_id))
            elif verdict == "stale_label":
                stale_label.append((date, league_id))
            else:
                read_errors.append((date, league_id))

    report.update(
        {
            "load_bearing_count": len(load_bearing),
            "redundant_gcs_discovered_count": len(redundant_gcs),
            "redundant_total_count": len(redundant_manifest) + len(redundant_gcs),
            "stale_label_count": len(stale_label),
            "read_error_count": len(read_errors),
            "read_errors_sample": read_errors[:20],
        }
    )
    print(json.dumps(report, indent=2))
    Path(args.out_report).write_text(json.dumps(report, indent=2))

    load_bearing_df = pd.DataFrame(load_bearing, columns=["date", "league_id"])
    load_bearing_df.to_parquet(args.out_load_bearing, index=False)
    logger.info("wrote %d load-bearing (date, league_id) pairs -> %s", len(load_bearing_df), args.out_load_bearing)

    stale_label_df = pd.DataFrame(stale_label, columns=["date", "league_id"])
    stale_label_df.to_parquet(args.out_stale_label, index=False)
    logger.info("wrote %d stale-label (date, league_id) pairs -> %s", len(stale_label_df), args.out_stale_label)

    if read_errors:
        logger.warning(
            "%d candidate(s) could not be confirmed after retries (read_error) — re-run the census to "
            "resolve them before treating the load-bearing count as final.",
            len(read_errors),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
