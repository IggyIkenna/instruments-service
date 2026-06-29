# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
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

Bucket selection (Bug 1 fix):
  Prefer the bucket whose _index/availability_index.parquet blob was MOST RECENTLY
  modified (blob.updated timestamp), not the bucket with the most rows. This prevents
  picking stale non-prd buckets (35.8M rows, 20 days old) over fresh prd buckets
  (5.2M rows, live data).

Manifest merge (Bug 2 fix):
  After picking the freshest bucket as PRIMARY, the secondary bucket is also read and
  merged. If the ``date`` column is present in both DataFrames, shards are deduplicated
  on (date, venue, data_type) keeping the PRIMARY row's capture_status (prd wins).
  This ensures the full expected_unattempted skeleton (from the legacy non-prd bucket)
  is combined with fresh captured/attempted_failed/empty_confirmed from prd.
  Use ``--no-merge`` to disable this merging and fall back to freshest-wins-only.

Usage:
  python measure_honest_coverage.py [--asset-group cefi|defi|tradfi|sports|prediction|all]
  python measure_honest_coverage.py --output-path /tmp/coverage.json   # local probe
  python measure_honest_coverage.py --no-merge                         # freshest-wins only
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
# single naming scheme. List both candidates and read whichever is MOST RECENTLY UPDATED
# (blob.updated timestamp — not row count). Self-corrects after Phase 2.6 consolidates
# everything onto `-prd`.
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
_INDEX_BLOB_PATH = "_index/availability_index.parquet"
_READ_COLUMNS = ["capture_status", "venue", "data_type", "date", "instrument_id"]
# Preferred shard key (instrument-level dedup); fallback used when instrument_id absent.
_SHARD_KEY = ["date", "venue", "data_type"]
_SHARD_KEY_WITH_IID = ["date", "venue", "instrument_id", "data_type"]
# Priority order for deduplication: lower index = higher priority.
_STATUS_PRIORITY: dict[str, int] = {
    "captured": 0,
    "attempted_failed": 1,
    "empty_confirmed": 2,
    "expected_unattempted": 3,
}


def _get_blob_updated(client: storage.Client, bucket_name: str) -> datetime | None:
    """Return the UTC-aware updated timestamp of the availability index blob, or None."""
    try:
        blob = client.bucket(bucket_name).get_blob(_INDEX_BLOB_PATH)
        if blob is None:
            return None
        return blob.updated  # already UTC-aware
    except Exception as exc:
        logger.info("  blob timestamp lookup failed for %s: %s", bucket_name, exc)
        return None


def _read_parquet_safe(
    bucket_name: str,
) -> pd.DataFrame | None:
    """Read the availability index parquet for a bucket, returning None on failure."""
    uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
    try:
        # Read ONLY the columns the coverage compute uses plus ``date`` for dedup.
        # The cefi availability_index is ~35.8M rows × many columns — loading the full
        # frame OOM-killed even a 32 GiB VM (rc=137); 4 string/date columns stay bounded
        # as the index grows. SSOT for used columns: _count_statuses + _compute_coverage.
        df = pd.read_parquet(uri, columns=_READ_COLUMNS)
    except Exception as exc:
        # ``date`` column may not be present in older bucket layouts — retry without it.
        try:
            df = pd.read_parquet(uri, columns=["capture_status", "venue", "data_type"])
        except Exception as exc2:
            logger.info("  candidate not accessible (%s): %s / %s", uri, exc, exc2)
            return None
    return df


def _read_parquet_eu_only(bucket_name: str) -> pd.DataFrame | None:
    """Read only expected_unattempted rows for memory-bounded prd+oracle merge.

    Uses pyarrow push-down filter so the cefi oracle (~35.8M rows) is never fully
    materialised — only the ~4.1M eu skeleton rows are loaded.
    """
    uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
    eu_filter = [("capture_status", "==", "expected_unattempted")]
    try:
        return pd.read_parquet(uri, columns=_READ_COLUMNS, filters=eu_filter)
    except Exception as exc:
        try:
            return pd.read_parquet(
                uri,
                columns=["capture_status", "venue", "data_type", "date"],
                filters=eu_filter,
            )
        except Exception as exc2:
            logger.info("  eu-only read failed for %s: %s / %s", uri, exc, exc2)
            return None


def _merge_manifests(
    df_primary: pd.DataFrame,
    df_secondary: pd.DataFrame,
) -> pd.DataFrame:
    """Merge primary and secondary manifests, preferring primary's capture_status per shard.

    Strategy:
    - If ``date`` is present in both DataFrames, deduplicate on the shard key keeping
      primary's row first (prd wins over legacy stale rows).  When ``instrument_id`` is
      present in both frames the full shard key ``(date, venue, instrument_id, data_type)``
      is used so different instruments on the same date/venue/data_type are not collapsed.
      Falls back to ``(date, venue, data_type)`` with a warning when instrument_id is absent.
    - If ``date`` is absent in either, concatenate without dedup (worst case = double-count
      for overlapping shards; documented behaviour — caller should check the log warning).

    Returns a merged DataFrame with a superset of shards from both buckets.
    """
    has_date = "date" in df_primary.columns and "date" in df_secondary.columns

    if not has_date:
        logger.warning(
            "  MERGE: 'date' column absent in one or both buckets — concatenating without dedup "
            "(double-count possible for overlapping shards). Use --no-merge to suppress."
        )
        return pd.concat([df_primary, df_secondary], ignore_index=True)

    has_iid = "instrument_id" in df_primary.columns and "instrument_id" in df_secondary.columns
    shard_key = _SHARD_KEY_WITH_IID if has_iid else _SHARD_KEY
    if not has_iid:
        logger.warning(
            "  MERGE: 'instrument_id' absent in one or both buckets — deduplicating on %s only "
            "(instruments sharing a date/venue/data_type are collapsed into one row).",
            _SHARD_KEY,
        )

    # Add priority column so sort-then-drop_duplicates keeps the best status per shard.
    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()
    df_primary["_priority"] = df_primary["capture_status"].map(
        lambda s: _STATUS_PRIORITY.get(s, 99)
    )
    df_secondary["_priority"] = df_secondary["capture_status"].map(
        lambda s: _STATUS_PRIORITY.get(s, 99)
    )

    combined = pd.concat([df_primary, df_secondary], ignore_index=True)
    # Sort ascending by priority so drop_duplicates(keep='first') keeps the best status.
    combined = combined.sort_values("_priority")
    combined = combined.drop_duplicates(subset=shard_key, keep="first")
    combined = combined.drop(columns=["_priority"])
    combined = combined.reset_index(drop=True)
    logger.info(
        "  MERGE: primary=%d rows + secondary=%d rows → merged=%d rows (dedup on %s)",
        len(df_primary),
        len(df_secondary),
        len(combined),
        shard_key,
    )
    return combined


def _read_manifest(asset_group: str, *, merge: bool = True) -> pd.DataFrame | None:
    """Read the live availability manifest for an asset_group.

    Bug 1 fix: compare blob.updated timestamps instead of row counts to select the
    FRESHEST bucket as primary. The stale non-prd bucket has 35.8M rows (written
    2026-06-08) which previously beat the live prd bucket (5.2M rows, fresh). Timestamp
    comparison correctly picks prd.

    Bug 2 fix (when merge=True): after picking the freshest bucket as primary, also read
    the secondary bucket and merge the two DataFrames. Non-prd holds the full
    expected_unattempted skeleton that prd lacks; merging gives accurate denominator
    counts without double-counting (dedup on day/venue/data_type preferring prd status).
    """
    candidates = _MANIFEST_BUCKET_CANDIDATES[asset_group]

    # Step 1: get blob timestamps for all candidates in parallel (serial loop is fine
    # since we only have 2 candidates per asset_group).
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket_info: list[tuple[str, datetime | None, pd.DataFrame | None]] = []
    for bucket_name in candidates:
        updated = _get_blob_updated(gcs_client, bucket_name)
        df = _read_parquet_safe(bucket_name)
        uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
        if df is None:
            logger.info("  %s candidate %s: not accessible", asset_group, uri)
        else:
            ts_str = updated.isoformat() if updated else "unknown"
            logger.info(
                "  %s candidate %s: %d rows, blob.updated=%s",
                asset_group,
                uri,
                len(df),
                ts_str,
            )
        bucket_info.append((bucket_name, updated, df))

    # Step 2: rank by blob.updated (newest first); fall back to row count if timestamps unavailable.
    accessible = [(name, ts, df) for name, ts, df in bucket_info if df is not None]
    if not accessible:
        logger.warning("  SKIP %s — no candidate manifest accessible", asset_group)
        return None

    def _sort_key(item: tuple[str, datetime | None, pd.DataFrame]) -> tuple[int, int]:
        _, ts, df = item
        # Primary sort: newest timestamp (negative epoch seconds); if ts is None use 0.
        ts_score = -int(ts.timestamp()) if ts is not None else 0
        # Secondary sort: row count as tiebreaker (more rows = better).
        return (ts_score, -len(df))

    accessible.sort(key=_sort_key)
    primary_name, primary_ts, primary_df = accessible[0]
    primary_uri = f"gs://{primary_name}/{_INDEX_BLOB_PATH}"
    primary_ts_str = primary_ts.isoformat() if primary_ts is not None else "unknown"
    logger.info(
        "  %s manifest SELECTED (freshest): %s (%d rows, blob.updated=%s)",
        asset_group,
        primary_uri,
        len(primary_df),
        primary_ts_str,
    )
    for name, ts, df in accessible[1:]:
        uri = f"gs://{name}/{_INDEX_BLOB_PATH}"
        ts_str = ts.isoformat() if ts is not None else "unknown"
        logger.info(
            "  %s manifest NOT SELECTED (older): %s (%d rows, blob.updated=%s)",
            asset_group,
            uri,
            len(df),
            ts_str,
        )

    result_df = primary_df

    if merge and len(accessible) > 1:
        # Re-read secondary as eu_only (pyarrow push-down filter) before merging.
        # The non-prd oracle can be 35.8M rows; only ~4.1M are expected_unattempted.
        # Reading eu_only keeps peak memory bounded while providing the full skeleton.
        for secondary_name, _ts, _secondary_full in accessible[1:]:
            secondary_eu = _read_parquet_eu_only(secondary_name)
            if secondary_eu is not None:
                result_df = _merge_manifests(result_df, secondary_eu)
            else:
                logger.warning(
                    "  %s eu-only read failed for secondary %s — skipping merge",
                    asset_group,
                    secondary_name,
                )

    return result_df


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
    parser.add_argument(
        "--no-merge",
        action="store_true",
        default=False,
        help=(
            "Disable prd/non-prd manifest merging. Falls back to freshest-wins only. "
            "Use when you want to measure a single bucket in isolation without combining "
            "the expected_unattempted skeleton from the secondary bucket."
        ),
    )
    args = parser.parse_args()

    asset_groups = list(_KNOWN_ASSET_GROUPS) if args.asset_group == "all" else [args.asset_group]
    merge = not args.no_merge

    dfs: dict[str, pd.DataFrame] = {}
    for ag in asset_groups:
        df = _read_manifest(ag, merge=merge)
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
