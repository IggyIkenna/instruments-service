#!/usr/bin/env python3
"""Reconcile phantom-captured manifest rows for ANY asset_group.

A phantom row claims ``capture_status=captured`` in the manifest but no
parquet exists at the canonical GCS path. This blocks the orchestrator's
``_should_skip_shard`` pre-flight (which trusts the manifest) into
permanently skipping the shard — every backfill VM exits doing nothing.

Pattern matches the 2026-04-29 sports phantom incident. This script ports
the same audit/flip logic to CeFi, DeFi, and Sports manifests, using a
single bulk-listing strategy: list once per ``(date, venue, data_type)``
prefix triple, then check each captured manifest row for membership.

Idempotent: ``attempted_failed`` rows are skipped, real captures are
left at ``captured``, only true phantoms get flipped.

Usage::

    cd instruments-service
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group cefi --dry-run
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group defi
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group sports --venues BINANCE-FUTURES,BINANCE-SPOT

The script reads/writes the canonical
``gs://market-data-tick-{asset_group}-{pid}/_index/availability_index.parquet``
(or ``instruments-store-sports-{pid}`` for sports). Per-VM shards untouched.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Asset-group → (canonical bucket, manifest blob, day-list prefix templates).
#
# The hive-key for asset-group has TWO live values in GCS due to the
# 2026-04 vocabulary rename:
#   - ``category=`` (legacy, dominant for pre-2024 + still emitted by
#     unmigrated writers)
#   - ``asset_group=`` (new canonical, emitted by all post-rename writers)
# A phantom audit MUST probe BOTH or we false-positive every legacy row.
# The 2026-05-01 incident (181k false phantoms on CeFi) was caused by
# probing only the new key.  We now list under each candidate prefix and
# treat the row as real if ANY prefix has at least one parquet.
ASSET_GROUP_CONFIG: dict[str, dict[str, list[str] | str]] = {
    "cefi": {
        "bucket": f"market-data-tick-cefi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # 4 path shapes coexist on disk; ALL must be probed:
        #   (a) raw_tick_data/by_date prefix + asset_group= hive (canonical)
        #   (b) raw_tick_data/by_date prefix + category= hive (legacy)
        #   (c) top-level + asset_group= hive (Tardis adapter via build_partition_path)
        #   (d) top-level + category= hive (older Tardis adapter)
        # Earlier audit only probed (a) + (b) and false-positived 130k rows
        # whose data lives at (c)/(d). 2026-05-03: extended to all 4 shapes.
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=cefi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=cefi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=cefi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=cefi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "defi": {
        "bucket": f"market-data-tick-defi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # DeFi layout has venue + chain (no instrument_type segment in older
        # paths). Probe new + legacy hive keys + no-asset-group + top-level
        # (no raw_tick_data/by_date/ prefix) variants.
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            # 2024-05-era DeFi paths skipped the asset-group hive segment.
            "raw_tick_data/by_date/day={date}/venue={venue}/chain={chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            # Legacy ``venue=PROTOCOL-CHAIN`` overload — pre-2026-04-29
            # EIGENLAYER restaking + a few other DeFi adapters wrote
            # ``venue=EIGENLAYER-ETHEREUM`` (no separate ``chain=`` segment).
            # ``rebuild_defi_manifest.py`` decomposes that back to
            # ``(venue=EIGENLAYER, chain=ETHEREUM)`` in manifest rows; the
            # audit must also probe the combined layout so those rows aren't
            # false-flagged as phantoms.  Verified 2026-05-04: 597 EIGENLAYER
            # restaking rewards live at this layout.
            "raw_tick_data/by_date/day={date}/asset_group=defi/venue={venue}-{chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=defi/venue={venue}-{chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "sports": {
        "bucket": f"instruments-store-sports-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # Sports has its own SSOT (per-league + bare paths) — handled
        # separately via the unified UAC dispatcher below.
        "prefix_tpls": [""],
    },
    "tradfi": {
        "bucket": f"market-data-tick-tradfi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=tradfi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=tradfi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=tradfi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=tradfi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "prediction": {
        "bucket": f"market-data-tick-prediction-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=prediction/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=prediction/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=prediction/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=prediction/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
}


def _venue_level_prefixes(asset_group: str, row: pd.Series) -> list[str]:
    """Return one prefix per ``(date, venue[, chain], hive-vocab)`` for
    a manifest row.

    We list ONCE per venue-level prefix, then substring-match
    ``data_type={dt}/`` and ``instrument_type={it}/`` (case-insensitive)
    in the returned keys.  This is robust to:

    1. **Hive-key vocabulary drift** — ``category=`` (legacy) and
       ``asset_group=`` (post-rename) coexist; both probed.
    2. **instrument_type casing** — manifest holds ``PERPETUAL`` /
       ``perpetual`` interchangeably; disk only has lowercase. Membership
       check is case-insensitive.
    3. **Empty ``instrument_type``** — schema-4 manifest rows omit the
       segment. We accept ANY parquet under
       ``venue/.../data_type={dt}/`` as evidence of capture.
    4. **Path-prefix drift** (2026-05-03) — Tardis-adapter writes via
       ``build_partition_path`` lived at top-level ``day={D}/...`` while
       orchestrator-direct writes used ``raw_tick_data/by_date/day={D}/...``.
       Both shapes coexist on disk; pre-2026-05-03 audits only probed the
       prefixed shape and false-positived 130k CeFi rows.
    """
    cfg = ASSET_GROUP_CONFIG[asset_group]
    base_fields = {
        "date": str(row["date"]),
        "venue": str(row.get("venue", "") or ""),
        "chain": str(row.get("chain", "") or ""),
    }
    tpls = cfg["prefix_tpls"]
    if isinstance(tpls, str):
        tpls = [tpls]
    out: list[str] = []
    for t in tpls:
        # Truncate template at the first hive segment AFTER venue (and
        # after chain for DeFi). The remainder narrows to one venue-day.
        stripped = t.split("instrument_type=")[0]
        # ``stripped`` may contain ``{venue}`` / ``{chain}`` / ``{date}``
        # placeholders only; safe to format with base_fields.
        out.append(stripped.format(**base_fields))
    return out


def _audit_sports(
    bucket: storage.Bucket,
    df: pd.DataFrame,
    captured_idx: pd.Index,
    workers: int,
) -> dict[int, bool]:
    """Sports uses per-league + bare path layout — delegate to UAC SSOT."""
    from unified_api_contracts.sports import candidate_parquet_paths

    # Bulk-list per day (sports has shared sports_reference/by_date/day=*/ path).
    days = sorted({str(d) for d in df.loc[captured_idx, "date"].unique()})
    logger.info("sports phantom: listing %d unique days", len(days))
    day_blobs: dict[str, set[str]] = {}

    def _list_day(day: str) -> tuple[str, set[str]]:
        prefix = f"sports_reference/by_date/day={day}/"
        return day, {b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(_list_day, d) for d in days]):
            day, blobs = fut.result()
            day_blobs[day] = blobs

    # Probe each captured row.
    real_or_phantom: dict[int, bool] = {}  # idx -> True if real
    for idx in captured_idx:
        row = df.loc[idx]
        date = str(row["date"])
        data_type = str(row.get("data_type", "") or "")
        league_id = str(row.get("league_id", "") or "")
        candidates = candidate_parquet_paths(data_type, date, league_id)
        blobs = day_blobs.get(date, set())
        real_or_phantom[idx] = any(c in blobs for c in candidates)
    return real_or_phantom


def _audit_generic(
    asset_group: str,
    bucket: storage.Bucket,
    df: pd.DataFrame,
    captured_idx: pd.Index,
    workers: int,
) -> dict[int, bool]:
    """CeFi/DeFi/TradFi/Prediction use venue+data_type prefix layout.

    Strategy: list ONCE per unique ``(date, venue[, chain], hive-vocab)``
    prefix, then for each manifest row substring-match
    ``data_type={dt}/`` and (if specified) ``instrument_type={it}/``
    (case-insensitive) in the key set.  Robust to hive-key drift,
    instrument_type casing drift, and schema-4 empty-instrument_type
    rows — see ``_venue_level_prefixes`` for rationale.

    Phantom = no key in any candidate prefix's listing matches the
    row's ``data_type`` (and ``instrument_type`` if non-empty).
    """
    # Each captured row maps to a list of candidate venue-level prefixes.
    prefixes_by_idx: dict[int, list[str]] = {}
    for idx in captured_idx:
        prefixes_by_idx[idx] = _venue_level_prefixes(asset_group, df.loc[idx])
    unique_prefixes = sorted({p for plist in prefixes_by_idx.values() for p in plist if p})
    logger.info(
        "%s phantom: %d unique (date, venue[, chain], hive-vocab) prefixes to list",
        asset_group,
        len(unique_prefixes),
    )

    prefix_keys: dict[str, set[str]] = {}

    def _list(prefix: str) -> tuple[str, set[str]]:
        try:
            keys = {b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")}
            return prefix, keys
        except Exception as exc:
            logger.warning("list error for %s: %s", prefix, exc)
            return prefix, set()

    completed = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_list, p) for p in unique_prefixes]
        for fut in as_completed(futs):
            prefix, keys = fut.result()
            prefix_keys[prefix] = keys
            completed += 1
            if completed % 500 == 0:
                rate = completed / max(0.01, time.time() - t0)
                logger.info(
                    "  %d/%d prefixes listed (%.1f/sec, ETA %.1fs)",
                    completed,
                    len(unique_prefixes),
                    rate,
                    (len(unique_prefixes) - completed) / max(0.01, rate),
                )

    real_or_phantom: dict[int, bool] = {}
    for idx, plist in prefixes_by_idx.items():
        row = df.loc[idx]
        data_type = str(row.get("data_type", "") or "")
        raw_it = str(row.get("instrument_type", "") or "")
        dt_needle = f"data_type={data_type}/"
        # Case-insensitive instrument_type needle. Empty manifest value
        # means "any instrument_type counts" (schema-4 rows).
        it_needles_lower = [f"instrument_type={raw_it.lower()}/"] if raw_it else []
        is_real = False
        for prefix in plist:
            keys = prefix_keys.get(prefix, set())
            for k in keys:
                if dt_needle not in k:
                    continue
                if it_needles_lower:
                    k_lower = k.lower()
                    if not any(it in k_lower for it in it_needles_lower):
                        continue
                is_real = True
                break
            if is_real:
                break
        real_or_phantom[idx] = is_real
    return real_or_phantom


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset-group", required=True, choices=list(ASSET_GROUP_CONFIG.keys()))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--venues", type=str, default="", help="Comma-separated venues to scope (default: all)")
    p.add_argument("--data-types", type=str, default="", help="Comma-separated data_types to scope")
    p.add_argument("--workers", type=int, default=32)
    args = p.parse_args()

    cfg = ASSET_GROUP_CONFIG[args.asset_group]
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(cfg["bucket"])
    blob = bucket.blob(cfg["index"])

    logger.info("Loading manifest from gs://%s/%s", cfg["bucket"], cfg["index"])
    # Per-invocation temp file so concurrent runs (one per asset_group) don't
    # clobber each other's downloads. Bandit B108: use tempfile, not /tmp.
    with tempfile.NamedTemporaryFile(
        prefix=f"recon-{args.asset_group}-", suffix=".parquet", delete=False
    ) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        try:
            os.unlink(manifest_path)
        except OSError:
            pass
    logger.info("Manifest rows: %d", len(df))

    captured_mask = df["capture_status"].fillna("") == "captured"
    if args.venues:
        wanted_venues = {v.strip() for v in args.venues.split(",") if v.strip()}
        captured_mask = captured_mask & df["venue"].isin(wanted_venues)
    if args.data_types:
        wanted_dts = {d.strip() for d in args.data_types.split(",") if d.strip()}
        captured_mask = captured_mask & df["data_type"].isin(wanted_dts)

    captured_idx = df[captured_mask].index
    logger.info("Captured rows in scope: %d", len(captured_idx))
    if len(captured_idx) == 0:
        logger.info("Nothing to audit. Exiting.")
        return 0

    if args.asset_group == "sports":
        real_or_phantom = _audit_sports(bucket, df, captured_idx, args.workers)
    else:
        real_or_phantom = _audit_generic(args.asset_group, bucket, df, captured_idx, args.workers)

    phantom_idx = [i for i, real in real_or_phantom.items() if not real]
    real_count = sum(1 for r in real_or_phantom.values() if r)
    logger.info("=" * 60)
    logger.info("Audit summary:")
    logger.info("  Real captures:    %d", real_count)
    logger.info("  Phantom captures: %d  ← will flip to attempted_failed", len(phantom_idx))
    logger.info("=" * 60)

    if not phantom_idx:
        logger.info("No phantoms found. Manifest is clean.")
        return 0

    # Show phantom distribution
    phantom_df = df.loc[phantom_idx]
    by_dt = phantom_df.groupby(["data_type"]).size().sort_values(ascending=False)
    logger.info("Phantom distribution by data_type (top 15):\n%s", by_dt.head(15).to_string())
    if "venue" in phantom_df.columns:
        by_v = phantom_df.groupby(["venue"]).size().sort_values(ascending=False)
        logger.info("Phantom distribution by venue (top 15):\n%s", by_v.head(15).to_string())

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified.")
        return 0

    # Flip phantoms in-place.
    now_iso = datetime.now(UTC).isoformat()
    df.loc[phantom_idx, "capture_status"] = "attempted_failed"
    df.loc[phantom_idx, "error_reason"] = "phantom_captured_no_parquet_at_canonical_path"
    df.loc[phantom_idx, "attempted_at"] = now_iso

    # Write back.
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("Uploading reconciled manifest (%d rows, %d phantoms flipped)", len(df), len(phantom_idx))
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
