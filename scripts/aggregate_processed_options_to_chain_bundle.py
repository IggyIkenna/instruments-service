#!/usr/bin/env python3
"""Aggregate per-strike MDPS processed_candles into chain-bundled parquets.

Source (current MDPS output — per strike, suboptimal for chain-level features):
    gs://market-data-tick-{ag}-{pid}/processed_candles/by_date/day={D}/
        timeframe={tf}/data_type={dt}/venue={V}/{INSTRUMENT_ID}.parquet
    e.g. CME:OPTION:ESH4-C5000.parquet, CME:OPTION:ESH4-P5050.parquet, ...

Target (chain-bundled — ONE file per chain root, columns include strike + put_call):
    gs://market-data-tick-{ag}-{pid}/processed_candles/by_date/day={D}/
        timeframe={tf}/data_type={dt}/instrument_type=options_chain/
        venue={V}/underlying={ROOT}/ticks.parquet

Schema additions over base candle: ``strike`` (float), ``put_call`` (str: 'C'/'P'),
``expiry_code`` (str: e.g. 'H4' = Mar 2024), ``instrument_id`` (preserved per row).

Idempotent: if target parquet already exists, skips unless --force. Safe to re-run
after partial writes — concat+sort+drop_duplicates on (instrument_id, timestamp).

Reasoning: features-volatility-service IV/skew/put-call-ratio calculators need to
read an entire chain in one open. ONE bundled file per (date, root) is also ~50x
fewer GCS LIST + GET calls than per-strike. MDPS itself emits per-strike for now;
this script is an additive post-processing step. Eventually MDPS will chain-bundle
natively (master plan §9 P1.5).

Filename parsing — Databento canonical instrument_id for CME options:
    CME:OPTION:{ROOT}{EXPIRY}-{C|P}{STRIKE}
    e.g. CME:OPTION:ESH4-C5000 → root=ES, expiry=H4, put_call=C, strike=5000
         CME:OPTION:EWM4-P5050 → root=EW, expiry=M4, put_call=P, strike=5050
         CME:OPTION:E1AN0-C3090 → root=E1A, expiry=N0, put_call=C, strike=3090

CME expiry letters (month codes): F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug
                                  U=Sep V=Oct X=Nov Z=Dec

Usage:
    python3 aggregate_processed_options_to_chain_bundle.py \\
        --asset-group tradfi \\
        --start-date 2020-01-01 \\
        --end-date 2026-05-05 \\
        --timeframes 1m 5m 15m 1h \\
        --data-types ohlcv_1m ohlcv_15m \\
        --workers 32 \\
        --dry-run

Run on a same-region asia-northeast1-c VM (cross-region GCS list 18x slower).
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
from unified_api_contracts import PipelineMode
from unified_trading_library import ManifestWriter, log_event, setup_events

# Map asset_group → batch tick source per UAC SOURCE_PRIORITY: tradfi options
# come from databento (CME / NQ direct feed); cefi options come from tardis
# (multi-venue WS aggregator). Used at the ``record_captured`` callsite below
# to tag the aggregated chain-bundle row with the underlying tick source so
# batch-vs-live recon queries can pivot cleanly on pipeline_mode.
_AGGREGATE_OPTIONS_PIPELINE_MODE_BY_ASSET_GROUP: dict[str, PipelineMode] = {
    "tradfi": PipelineMode.BATCH_DATABENTO,
    "cefi": PipelineMode.BATCH_TARDIS,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# Databento CME canonical option-id pattern + legacy migrated form.
# Group 1 = full chain prefix incl. expiry (e.g. ESH4 / EWM4 / E1AN0)
# Group 2 = C or P
# Group 3 = strike (digits, possibly with dot — Databento uses ints for ES strikes)
#
# Two forms accepted:
# - canonical: ``ESH4-C5000`` / ``CME:OPTION:E1AN0-P3090``
# - legacy:    ``E1AG4_C4765`` / ``E1AG4_C4765_migrated_20260419T132320Z`` —
#   produced by the 2026-04-19 migration job that flat-aggregated raw ticks
#   into per-strike candles. Optional ``_migrated_<UTC-TS>`` suffix tolerated.
_OPTION_ID_RE = re.compile(
    r"^(?:CME:OPTION:)?([A-Z][A-Z0-9]{1,4}[FGHJKMNQUVXZ][0-9])[-_]([CP])(\d+(?:\.\d+)?)"
    r"(?:_migrated_[A-Z0-9]+)?$"
)

# Strip the trailing 2 chars (expiry letter + year digit) to get chain root.
_EXPIRY_CHAR_LEN = 2

_PROCESSED_PREFIX_TEMPLATE = "processed_candles/by_date/day={day}/timeframe={tf}/data_type={dt}/venue={venue}/"
_BUNDLED_PATH_TEMPLATE = (
    "processed_candles/by_date/day={day}/timeframe={tf}/data_type={dt}/"
    "instrument_type=options_chain/venue={venue}/underlying={root}/ticks.parquet"
)


def parse_option_filename(name: str) -> tuple[str, str, str, float] | None:
    """Parse an MDPS option-candle filename into (root, expiry_code, put_call, strike).

    Returns None for non-options or unparseable names.
    """
    leaf = name.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
    m = _OPTION_ID_RE.match(leaf)
    if not m:
        return None
    chain_prefix = m.group(1)
    put_call = m.group(2)
    strike = float(m.group(3))
    if len(chain_prefix) <= _EXPIRY_CHAR_LEN:
        return None
    root = chain_prefix[:-_EXPIRY_CHAR_LEN]
    expiry_code = chain_prefix[-_EXPIRY_CHAR_LEN:]
    return root, expiry_code, put_call, strike


def _iter_dates(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _list_per_strike_blobs(
    bucket: storage.Bucket,
    day: str,
    timeframe: str,
    data_type: str,
    venue: str,
) -> list[storage.Blob]:
    prefix = _PROCESSED_PREFIX_TEMPLATE.format(day=day, tf=timeframe, dt=data_type, venue=venue)
    return [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]


def _group_by_chain_root(
    blobs: list[storage.Blob],
) -> dict[str, list[tuple[storage.Blob, str, str, str, float]]]:
    """Group blobs by chain root. Returns {root: [(blob, root, expiry, pc, strike), ...]}.

    Non-option blobs (futures, bare ticks) are skipped.
    """
    groups: dict[str, list[tuple[storage.Blob, str, str, str, float]]] = defaultdict(list)
    for blob in blobs:
        parsed = parse_option_filename(blob.name)
        if parsed is None:
            continue
        root, expiry, pc, strike = parsed
        groups[root].append((blob, root, expiry, pc, strike))
    return groups


def _read_blob_with_meta(
    blob: storage.Blob,
    root: str,
    expiry: str,
    put_call: str,
    strike: float,
    scratch: Path,
) -> pd.DataFrame:
    local = scratch / blob.name.replace("/", "__")
    blob.download_to_filename(str(local))
    df = pq.read_table(str(local)).to_pandas()
    local.unlink(missing_ok=True)
    if df.empty:
        return df
    df = df.copy()
    df["strike"] = strike
    df["put_call"] = put_call
    df["expiry_code"] = expiry
    if "instrument_id" not in df.columns:
        leaf = blob.name.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
        df["instrument_id"] = leaf
    return df


def _aggregate_root_shard(
    bucket: storage.Bucket,
    day: str,
    timeframe: str,
    data_type: str,
    venue: str,
    root: str,
    blobs_meta: list[tuple[storage.Blob, str, str, str, float]],
    scratch: Path,
    workers: int,
    dry_run: bool,
    force: bool,
) -> tuple[int, int]:
    """Read N per-strike blobs for a chain root, concat, write bundled parquet.

    Returns (input_blob_count, output_row_count). On dry-run row_count is -1.
    """
    target_path = _BUNDLED_PATH_TEMPLATE.format(day=day, tf=timeframe, dt=data_type, venue=venue, root=root)

    if not force and not dry_run and bucket.blob(target_path).exists():
        logger.debug("skip existing: %s", target_path)
        return (len(blobs_meta), -2)

    if dry_run:
        return (len(blobs_meta), -1)

    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_read_blob_with_meta, blob, r, exp, pc, strike, scratch)
            for blob, r, exp, pc, strike in blobs_meta
        ]
        for fut in as_completed(futures):
            df = fut.result()
            if not df.empty:
                frames.append(df)

    if not frames:
        return (len(blobs_meta), 0)

    merged = pd.concat(frames, ignore_index=True)
    sort_cols = [c for c in ("timestamp", "strike", "put_call") if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    if all(c in merged.columns for c in ("instrument_id", "timestamp")):
        merged = merged.drop_duplicates(subset=["instrument_id", "timestamp"], keep="last")

    out_local = scratch / f"out_{day}_{timeframe}_{data_type}_{root}.parquet"
    merged.to_parquet(str(out_local), index=False)
    bucket.blob(target_path).upload_from_filename(str(out_local))
    out_local.unlink(missing_ok=True)
    return (len(blobs_meta), len(merged))


def _process_day_timeframe_dt(
    bucket: storage.Bucket,
    writer: ManifestWriter,
    asset_group: str,
    day: str,
    timeframe: str,
    data_type: str,
    venue: str,
    scratch: Path,
    workers: int,
    dry_run: bool,
    force: bool,
) -> dict[str, tuple[int, int]]:
    blobs = _list_per_strike_blobs(bucket, day, timeframe, data_type, venue)
    groups = _group_by_chain_root(blobs)
    out: dict[str, tuple[int, int]] = {}
    for root, blobs_meta in sorted(groups.items()):
        in_count, row_count = _aggregate_root_shard(
            bucket=bucket,
            day=day,
            timeframe=timeframe,
            data_type=data_type,
            venue=venue,
            root=root,
            blobs_meta=blobs_meta,
            scratch=scratch,
            workers=workers,
            dry_run=dry_run,
            force=force,
        )
        out[root] = (in_count, row_count)
        if dry_run:
            logger.info(
                "[DRY] %s tf=%s dt=%s root=%s -> would aggregate %d files",
                day,
                timeframe,
                data_type,
                root,
                in_count,
            )
            continue
        if row_count == -2:
            continue  # skip existing
        if row_count == 0:
            logger.warning(
                "%s tf=%s dt=%s root=%s: %d input blobs but all empty — skipping bundled write",
                day,
                timeframe,
                data_type,
                root,
                in_count,
            )
            log_event(
                "OPTIONS_CANDLE_BUNDLE_EMPTY",
                metadata={
                    "day": day,
                    "tf": timeframe,
                    "dt": data_type,
                    "root": root,
                    "blobs": in_count,
                },
            )
            continue

        writer.record_captured(
            row_key={"date": day, "venue": venue},
            df=pd.DataFrame(),
            asset_group=asset_group,
            instrument_type="options_chain",
            data_type=data_type,
            timeframe=timeframe,
            underlying=root,
            row_count=row_count,
            attempted_at=datetime.now(UTC),
            pipeline_mode=_AGGREGATE_OPTIONS_PIPELINE_MODE_BY_ASSET_GROUP[asset_group],
        )
        logger.info(
            "%s tf=%s dt=%s root=%s: aggregated %d files -> %d rows",
            day,
            timeframe,
            data_type,
            root,
            in_count,
            row_count,
        )
        log_event(
            "OPTIONS_CANDLE_BUNDLED",
            metadata={
                "day": day,
                "tf": timeframe,
                "dt": data_type,
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
    parser.add_argument("--asset-group", default="tradfi", choices=["cefi", "tradfi"])
    parser.add_argument("--project-id", default="central-element-323112")
    parser.add_argument("--venue", default="CME", help="Venue partition value (e.g. CME)")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--timeframes", nargs="+", default=["1m", "5m", "15m", "1h", "4h", "24h"])
    parser.add_argument(
        "--data-types",
        nargs="+",
        default=["ohlcv_1m", "ohlcv_15m"],
        help="Candle data types to bundle",
    )
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Overwrite existing bundled files")
    args = parser.parse_args()

    setup_events(
        service_name="instruments-service",
        correlation_id="aggregate-processed-options",
    )
    log_event(
        "AGGREGATE_PROCESSED_OPTIONS_STARTED",
        metadata={
            "asset_group": args.asset_group,
            "venue": args.venue,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "timeframes": args.timeframes,
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
    scratch = Path(tempfile.mkdtemp(prefix="agg_options_candles_"))
    logger.info("scratch=%s bucket=%s venue=%s", scratch, bucket_name, args.venue)

    totals: dict[str, dict[str, int]] = {
        f"{tf}/{dt}": {"shards": 0, "rows": 0} for tf in args.timeframes for dt in args.data_types
    }

    days = _iter_dates(args.start_date, args.end_date)
    logger.info("processing %d days x %d tfs x %d dts", len(days), len(args.timeframes), len(args.data_types))

    for day in days:
        for timeframe in args.timeframes:
            for data_type in args.data_types:
                key = f"{timeframe}/{data_type}"
                shards = _process_day_timeframe_dt(
                    bucket=bucket,
                    writer=writer,
                    asset_group=args.asset_group,
                    day=day,
                    timeframe=timeframe,
                    data_type=data_type,
                    venue=args.venue,
                    scratch=scratch,
                    workers=args.workers,
                    dry_run=args.dry_run,
                    force=args.force,
                )
                if shards:
                    totals[key]["shards"] += len(shards)
                    totals[key]["rows"] += sum(rows for _, rows in shards.values() if rows > 0)

    writer.close()
    logger.info("done. totals=%s", totals)
    log_event("AGGREGATE_PROCESSED_OPTIONS_FINISHED", metadata={"totals": totals})


if __name__ == "__main__":
    main()
