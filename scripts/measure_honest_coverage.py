"""Cross-asset-group honest coverage measurement.

Reads the availability manifest for every asset_group, computes honest
coverage at three aggregation levels:
  - per asset_group (workspace-wide rollup)
  - per (asset_group, venue)
  - per (asset_group, venue, data_type)

Coverage formula:  captured / (captured + attempted_failed + expected_unattempted)

i.e. the fraction of *reachable* shard slots that were captured.
empty_confirmed rows (non-trading days, pre-genesis chain dates, source-confirmed
gaps) are excluded from the denominator — they represent legitimate absence, not
pipeline failures.  The old all-shards formula is preserved as
``all_shards_coverage_pct`` for reference.

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
from datetime import UTC, date, datetime
from typing import cast

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Manifest bucket candidates per asset_group (scripts/ excluded from inline-URI QG ratchet).
# During the bucket-SSOT env-tiering migration (bucket_name_ssot_canonicalisation Phase 2.6)
# the LIVE bucket differs per asset_group: CeFi tick still writes the legacy FLAT bucket
# (get_write_bucket_name → cloud_constants legacy prefixes), while DeFi on-chain handlers
# already write the env-tiered `-prd` bucket (different write path). So we cannot assume a
# single naming scheme. List both candidates and read whichever actually holds the data
# (most manifest rows). Self-corrects after Phase 2.6 consolidates everything onto `-prd`.
# See plans/active/issues/cefi_tick_bucket_ssot_divergence_2026_05_25.md.
_MANIFEST_BUCKET_CANDIDATES: dict[str, tuple[str, ...]] = {
    "cefi": (f"market-data-tick-cefi-prd-{PROJECT_ID}", f"market-data-tick-cefi-{PROJECT_ID}"),
    "defi": (f"market-data-tick-defi-prd-{PROJECT_ID}", f"market-data-tick-defi-{PROJECT_ID}"),
    "tradfi": (f"market-data-tick-tradfi-prd-{PROJECT_ID}", f"market-data-tick-tradfi-{PROJECT_ID}"),
    "sports": (f"market-data-tick-sports-prd-{PROJECT_ID}", f"market-data-tick-sports-{PROJECT_ID}"),
    "prediction": (f"market-data-tick-pred-prd-{PROJECT_ID}", f"market-data-tick-prediction-{PROJECT_ID}"),
}

_OUTPUT_BUCKET = f"{PROJECT_ID}-honest-coverage"
_KNOWN_ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")
_CAPTURE_STATUSES = ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")


def _read_manifest(asset_group: str) -> pd.DataFrame | None:
    """Read the live availability manifest for an asset_group.

    Tries each candidate bucket (env-tiered `-prd` + legacy flat) and returns
    the one with the most rows — the bucket the writers are actively filling
    under the in-flight env-tiering migration. Logs which candidate won so a
    stale/empty `-prd` read can't silently under-count a flat-bucket backfill.
    """
    best_df: pd.DataFrame | None = None
    best_uri: str | None = None
    for bucket_name in _MANIFEST_BUCKET_CANDIDATES[asset_group]:
        uri = f"gs://{bucket_name}/_index/availability_index.parquet"
        try:
            df = pd.read_parquet(uri)
        except Exception as exc:
            logger.info("  %s candidate not accessible (%s): %s", asset_group, uri, exc)
            continue
        logger.info("  %s candidate %s: %s rows", asset_group, uri, f"{len(df):,}")
        if best_df is None or len(df) > len(best_df):
            best_df, best_uri = df, uri
    if best_df is None:
        logger.warning("  SKIP %s — no candidate manifest accessible", asset_group)
        return None
    logger.info("  %s manifest selected: %s (%s rows)", asset_group, best_uri, f"{len(best_df):,}")
    return best_df


def _count_statuses(df: pd.DataFrame) -> dict[str, int | float]:
    counts: dict[str, int | float] = {}
    for status in _CAPTURE_STATUSES:
        counts[status] = int((df["capture_status"] == status).sum())
    total = sum(int(v) for v in counts.values())
    counts["total"] = total
    # Reachable denominator excludes empty_confirmed (legitimate absence).
    reachable = counts["captured"] + counts["attempted_failed"] + counts["expected_unattempted"]
    counts["coverage_pct"] = round(counts["captured"] / reachable * 100, 2) if reachable else 100.0
    counts["all_shards_coverage_pct"] = round(counts["captured"] / total * 100, 2) if total else 0.0
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

    now_utc = datetime.now(UTC)
    payload: dict[str, object] = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": date.today().isoformat(),
        "asset_groups_measured": list(dfs.keys()),
        **coverage,
    }

    _write_output(payload, args.output_path)

    # Print per-asset-group summary to stdout for event-stream visibility
    print(f"\n=== Honest Coverage — {now_utc.strftime('%Y-%m-%d %H:%M')} UTC ===")
    print("  (reachable: captured / (captured + attempted_failed + expected_unattempted))")
    ag_counts = cast(dict[str, dict[str, int | float]], payload["by_asset_group"])
    for ag, counts in ag_counts.items():
        pct = counts["coverage_pct"]
        cap = counts["captured"]
        af = counts["attempted_failed"]
        eu = counts["expected_unattempted"]
        reachable = cap + af + eu
        print(f"  {ag:12s}: {pct:6.2f}%  ({cap:,}/{reachable:,} reachable)")


if __name__ == "__main__":
    main()
