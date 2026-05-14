"""Cross-asset-group honest coverage measurement.

Reads the availability manifest for every asset_group, computes honest
coverage at three aggregation levels:
  - per asset_group (workspace-wide rollup)
  - per (asset_group, venue)
  - per (asset_group, venue, data_type)

Coverage formula:  captured / (captured + empty_confirmed + attempted_failed + expected_unattempted)

i.e. the fraction of *all known manifest rows* that were actually captured.
This is the correct honest baseline — it measures the pipeline's ability to
turn expected shard slots into captured data, WITHOUT inflating the denominator
with days before source-coverage-start (those are not in the manifest at all).

Output: gs://central-element-323112-honest-coverage/{YYYY-MM-DD}/coverage.json
SSOT: codex/03-deployment/data-status-ui-surface.md (Phase 2E/2F target).

execution:
  owner: Cron VM via deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh
  cadence: daily
  verifier: gs://central-element-323112-honest-coverage/{date}/coverage.json
             exists and parses without error
  last_executed: NEVER

Usage:
  python measure_honest_coverage.py [--asset-group cefi|defi|tradfi|sports|prediction|all]
  python measure_honest_coverage.py --output-path /tmp/coverage.json   # local probe
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import date, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Manifest bucket per asset_group (prod; scripts/ excluded from inline-URI QG ratchet).
# Pattern: market-data-tick-{ag_short}-prd-{project_id}/_index/availability_index.parquet
_MANIFEST_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-prd-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-prd-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-prd-{PROJECT_ID}",
    "sports": f"market-data-tick-sports-prd-{PROJECT_ID}",
    "prediction": f"market-data-tick-pred-prd-{PROJECT_ID}",
}

_OUTPUT_BUCKET = f"{PROJECT_ID}-honest-coverage"
_KNOWN_ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")
_CAPTURE_STATUSES = ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")


def _read_manifest(asset_group: str) -> pd.DataFrame | None:
    bucket_name = _MANIFEST_BUCKETS[asset_group]
    uri = f"gs://{bucket_name}/_index/availability_index.parquet"
    logger.info("Reading manifest for %s: %s", asset_group, uri)
    try:
        df = pd.read_parquet(uri)
        logger.info("  %s manifest rows: %s", asset_group, f"{len(df):,}")
        return df
    except Exception as exc:
        logger.warning("  SKIP %s — manifest not accessible: %s", asset_group, exc)
        return None


def _count_statuses(df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in _CAPTURE_STATUSES:
        counts[status] = int((df["capture_status"] == status).sum())
    total = sum(counts.values())
    counts["total"] = total
    counts["coverage_pct"] = round(counts["captured"] / total * 100, 2) if total else 0.0
    return counts


def _compute_coverage(
    dfs: dict[str, pd.DataFrame],
) -> dict[str, object]:
    by_asset_group: dict[str, object] = {}
    by_venue: dict[str, dict[str, object]] = {}
    by_venue_data_type: dict[str, dict[str, dict[str, object]]] = {}

    for ag, df in dfs.items():
        # level 1 — per asset_group
        by_asset_group[ag] = _count_statuses(df)

        # level 2 — per (ag, venue)
        venue_group: dict[str, object] = {}
        for venue, vdf in df.groupby("venue"):
            venue_group[str(venue)] = _count_statuses(vdf)
        by_venue[ag] = venue_group

        # level 3 — per (ag, venue, data_type)
        vdt_group: dict[str, dict[str, object]] = defaultdict(dict)
        for (venue, data_type), vtdf in df.groupby(["venue", "data_type"]):
            vdt_group[str(venue)][str(data_type)] = _count_statuses(vtdf)
        by_venue_data_type[ag] = dict(vdt_group)

    return {
        "by_asset_group": by_asset_group,
        "by_venue": by_venue,
        "by_venue_data_type": by_venue_data_type,
    }


def _write_output(payload: dict[str, object], output_path: str | None) -> None:
    blob_bytes = json.dumps(payload, indent=2).encode()

    if output_path:
        with open(output_path, "wb") as f:
            f.write(blob_bytes)
        logger.info("Wrote coverage JSON to %s", output_path)
        return

    run_date = payload["date"]
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(_OUTPUT_BUCKET)
    blob = bucket.blob(f"{run_date}/coverage.json")
    blob.upload_from_string(blob_bytes, content_type="application/json")
    logger.info("Wrote gs://%s/%s/coverage.json", _OUTPUT_BUCKET, run_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure cross-asset-group honest coverage")
    parser.add_argument(
        "--asset-group",
        default="all",
        choices=[*_KNOWN_ASSET_GROUPS, "all"],
        help="Asset group to measure (default: all)",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Local file path for output (default: write to GCS)",
    )
    args = parser.parse_args()

    asset_groups = list(_KNOWN_ASSET_GROUPS) if args.asset_group == "all" else [args.asset_group]

    dfs: dict[str, pd.DataFrame] = {}
    for ag in asset_groups:
        df = _read_manifest(ag)
        if df is not None and not df.empty:
            dfs[ag] = df

    if not dfs:
        logger.error("No manifests loaded — nothing to measure")
        sys.exit(1)

    coverage = _compute_coverage(dfs)

    now_utc = datetime.utcnow()
    payload: dict[str, object] = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": date.today().isoformat(),
        "asset_groups_measured": list(dfs.keys()),
        **coverage,
    }

    _write_output(payload, args.output_path)

    # Print per-asset-group summary to stdout for event-stream visibility
    print(f"\n=== Honest Coverage — {now_utc.strftime('%Y-%m-%d %H:%M')} UTC ===")
    for ag, counts in payload["by_asset_group"].items():  # type: ignore[union-attr]
        pct = counts["coverage_pct"]  # type: ignore[index]
        cap = counts["captured"]  # type: ignore[index]
        total = counts["total"]  # type: ignore[index]
        print(f"  {ag:12s}: {pct:6.2f}%  ({cap:,}/{total:,} captured)")


if __name__ == "__main__":
    main()
