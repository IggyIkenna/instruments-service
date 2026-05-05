#!/usr/bin/env python3
"""Aggregate legacy per-strike ES options parquets into canonical chain-bundled shards.

Source (WRONG path + WRONG granularity, written by the 2026-04-19 migration):
    gs://market-data-tick-tradfi-{pid}/raw_tick_data/by_date/day={D}/
        category=tradfi/venue=CME/instrument_type=future/data_type={trades|ohlcv_1m}/
        {ROOT}{EXP}_{C|P}{STRIKE}_migrated_*.parquet  -- one file per strike

Target (canonical):
    gs://market-data-tick-tradfi-{pid}/raw_tick_data/by_date/day={D}/
        asset_group=tradfi/venue=CME/instrument_type=options_chain/data_type={trades|ohlcv_1m}/
        underlying={ROOT}/ticks.parquet  -- one file per chain root

Scope: ES weekly + monthly + AM-settled options. Filename roots in scope:
    ES, EW, EW1, EW2, EW3, EW4, E1A, E2A, E3A, E4A, E5A, EOM
Anything else (6AG4 = AUD options, etc.) is skipped.

Mapping legacy strike-prefix -> chain root: strip the trailing 2 chars
(CME expiry letter F-Z + 1-digit year). Examples:
    E1AN0 C3090 -> root E1A (E1A weekly, July 2020, call strike 3090)
    ESM4   P4500 -> root ES  (ES quarterly, June 2024, put strike 4500)
    EW1M4  C5000 -> root EW1 (EW1 weekly week-1, June 2024, call 5000)
    EOMN0  C3000 -> root EOM (EOM monthly, July 2020, call 3000)

Aggregation, NOT re-fetch: the Databento trades-for-options quota was the
original reason this data wasn't fetched canonically; the migration already
paid for it. We just reshape per-strike -> chain-bundled.

Usage (run on a same-region asia-northeast1-c VM for 18x faster GCS list):
    python3 aggregate_legacy_es_opt_trades.py \\
        --asset-group tradfi \\
        --start-date 2020-01-01 \\
        --end-date 2022-12-31 \\
        --data-types trades ohlcv_1m \\
        --workers 32 \\
        --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage
from unified_trading_library.events import log_event, setup_events
from unified_trading_library.manifest_writer import ManifestWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_ES_OPTION_ROOTS = frozenset({"ES", "EW", "EW1", "EW2", "EW3", "EW4", "E1A", "E2A", "E3A", "E4A", "E5A", "EOM"})

# ROOT + month-letter (FGHJKMNQUVXZ) + year-digit (0-9) + _C/_P + STRIKE + _migrated_*.parquet
# Group 1 = strike-prefix (root + expiry letter + year digit).
_LEGACY_OPTION_FILENAME_RE = re.compile(r"^([A-Z][A-Z0-9]+[FGHJKMNQUVXZ][0-9])_[CP]\d+_migrated_[^/]+\.parquet$")

_LEGACY_PREFIX_TEMPLATE = (
    "raw_tick_data/by_date/day={day}/category=tradfi/venue=CME/instrument_type=future/data_type={data_type}/"
)
_CANONICAL_PATH_TEMPLATE = (
    "raw_tick_data/by_date/day={day}/asset_group=tradfi/venue=CME/"
    "instrument_type=options_chain/data_type={data_type}/underlying={root}/ticks.parquet"
)


def _root_from_strike_prefix(strike_prefix: str) -> str | None:
    """Extract chain root from a strike-prefix like 'E1AN0' -> 'E1A'.

    Returns None if the resulting root is not in the ES options scope set.
    """
    if len(strike_prefix) < 3:
        return None
    root = strike_prefix[:-2]
    return root if root in _ES_OPTION_ROOTS else None


def _iter_dates(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _list_legacy_blobs(
    bucket: storage.Bucket,
    day: str,
    data_type: str,
) -> list[storage.Blob]:
    prefix = _LEGACY_PREFIX_TEMPLATE.format(day=day, data_type=data_type)
    return list(bucket.list_blobs(prefix=prefix))


def _group_blobs_by_root(blobs: list[storage.Blob]) -> dict[str, list[storage.Blob]]:
    groups: dict[str, list[storage.Blob]] = defaultdict(list)
    for blob in blobs:
        leaf = blob.name.rsplit("/", 1)[-1]
        m = _LEGACY_OPTION_FILENAME_RE.match(leaf)
        if not m:
            continue
        root = _root_from_strike_prefix(m.group(1))
        if root is None:
            continue
        groups[root].append(blob)
    return groups


def _read_blob_to_df(blob: storage.Blob, scratch: Path) -> pd.DataFrame:
    local = scratch / blob.name.replace("/", "__")
    blob.download_to_filename(str(local))
    df = pq.read_table(str(local)).to_pandas()
    local.unlink(missing_ok=True)
    return df


def _aggregate_one_shard(
    bucket: storage.Bucket,
    day: str,
    data_type: str,
    root: str,
    blobs: list[storage.Blob],
    scratch: Path,
    workers: int,
    dry_run: bool,
) -> tuple[int, pd.DataFrame]:
    """Read N per-strike blobs, concat, write canonical chain-bundled parquet.

    Returns (input_blob_count, merged_df). On dry-run merged_df is empty.
    """
    if not blobs:
        return (0, pd.DataFrame())
    if dry_run:
        return (len(blobs), pd.DataFrame())

    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_read_blob_to_df, blob, scratch) for blob in blobs]
        for fut in as_completed(futures):
            df = fut.result()
            if not df.empty:
                frames.append(df)

    if not frames:
        return (len(blobs), pd.DataFrame())

    merged = pd.concat(frames, ignore_index=True)
    if "ts_event" in merged.columns:
        merged = merged.sort_values("ts_event", kind="mergesort").reset_index(drop=True)

    target_path = _CANONICAL_PATH_TEMPLATE.format(day=day, data_type=data_type, root=root)
    out_local = scratch / f"out_{day}_{data_type}_{root}.parquet"
    merged.to_parquet(str(out_local), index=False)
    bucket.blob(target_path).upload_from_filename(str(out_local))
    out_local.unlink(missing_ok=True)
    return (len(blobs), merged)


def _process_day(
    bucket: storage.Bucket,
    writer: ManifestWriter,
    day: str,
    data_type: str,
    scratch: Path,
    workers: int,
    dry_run: bool,
) -> dict[str, tuple[int, int]]:
    blobs = _list_legacy_blobs(bucket, day, data_type)
    groups = _group_blobs_by_root(blobs)
    out: dict[str, tuple[int, int]] = {}
    for root, group_blobs in sorted(groups.items()):
        in_count, merged_df = _aggregate_one_shard(
            bucket=bucket,
            day=day,
            data_type=data_type,
            root=root,
            blobs=group_blobs,
            scratch=scratch,
            workers=workers,
            dry_run=dry_run,
        )
        row_count = len(merged_df)
        out[root] = (in_count, row_count)
        if dry_run:
            logger.info(
                "[DRY] day=%s dt=%s root=%s -> would aggregate %d files",
                day,
                data_type,
                root,
                in_count,
            )
            continue
        if row_count == 0:
            logger.warning(
                "day=%s dt=%s root=%s: %d input blobs, all empty -> recording empty_confirmed",
                day,
                data_type,
                root,
                in_count,
            )
            writer.record_empty(
                row_key={"date": day, "venue": "CME"},
                instrument_type="options_chain",
                data_type=data_type,
                underlying=root,
                attempted_at=datetime.now(UTC),
            )
            log_event(
                "OPTIONS_CHAIN_AGGREGATED_EMPTY",
                metadata={"day": day, "data_type": data_type, "root": root, "blobs": in_count},
            )
            continue

        writer.record_captured(
            row_key={"date": day, "venue": "CME"},
            df=merged_df,
            category="tradfi",
            instrument_type="options_chain",
            data_type=data_type,
            underlying=root,
            row_count=row_count,
            attempted_at=datetime.now(UTC),
        )
        logger.info(
            "day=%s dt=%s root=%s: aggregated %d files -> %d rows",
            day,
            data_type,
            root,
            in_count,
            row_count,
        )
        log_event(
            "OPTIONS_CHAIN_AGGREGATED",
            metadata={
                "day": day,
                "data_type": data_type,
                "root": root,
                "input_blobs": in_count,
                "output_rows": row_count,
            },
        )
    return out


def _bucket_name_for(asset_group: str, project_id: str) -> str:
    return f"market-data-tick-{asset_group.lower()}-{project_id}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--asset-group", default="tradfi", choices=["tradfi"])
    parser.add_argument("--project-id", default="central-element-323112")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument(
        "--data-types",
        nargs="+",
        default=["trades", "ohlcv_1m"],
        choices=["trades", "ohlcv_1m"],
    )
    parser.add_argument("--workers", type=int, default=32, help="Threads per-day for parquet downloads")
    parser.add_argument("--dry-run", action="store_true", help="List + group only, no writes")
    args = parser.parse_args()

    setup_events(service_name="instruments-service", correlation_id="aggregate-legacy-es-opt")
    log_event(
        "AGGREGATE_LEGACY_ES_OPT_STARTED",
        metadata={
            "asset_group": args.asset_group,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "data_types": args.data_types,
            "dry_run": args.dry_run,
        },
    )

    bucket_name = _bucket_name_for(args.asset_group, args.project_id)
    client = storage.Client(project=args.project_id)
    bucket = client.bucket(bucket_name)

    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket_name,
        per_vm_shards=True,
    )

    scratch = Path(tempfile.mkdtemp(prefix="agg_es_opt_"))
    logger.info("scratch=%s bucket=%s", scratch, bucket_name)

    totals: dict[str, dict[str, int]] = {dt: {"days": 0, "shards": 0, "rows": 0} for dt in args.data_types}
    days = _iter_dates(args.start_date, args.end_date)
    logger.info("processing %d days", len(days))

    for day in days:
        for data_type in args.data_types:
            shards = _process_day(
                bucket=bucket,
                writer=writer,
                day=day,
                data_type=data_type,
                scratch=scratch,
                workers=args.workers,
                dry_run=args.dry_run,
            )
            if shards:
                totals[data_type]["days"] += 1
                totals[data_type]["shards"] += len(shards)
                totals[data_type]["rows"] += sum(rows for _, rows in shards.values() if rows > 0)

    writer.close()

    logger.info("done. totals=%s scratch=%s", totals, scratch)
    log_event("AGGREGATE_LEGACY_ES_OPT_FINISHED", metadata={"totals": totals})


if __name__ == "__main__":
    main()
