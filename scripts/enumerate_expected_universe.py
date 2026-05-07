#!/usr/bin/env python3
"""enumerate_expected_universe.py — Phase 3.D.4 backward-fill (writegate honest-coverage).

Enumerates the expected universe per asset_group, finds (shard_key, day) tuples
with NO manifest row, writes ``record_expected_empty(reason=EXPECTED_*)`` rows
via per-VM shard isolation.

Closes the rollup-vs-drilldown denominator divergence per
`unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md`
§ "Rollup-vs-drilldown denominator divergence (codified 2026-05-07)" by
ensuring every expected (shard_key, day) tuple has a manifest row.

Sister script to ``reconcile_expected_absence_reasons.py`` (which handles
legacy null-reason rows ALREADY in the manifest). This script handles the
complementary case: tuples that have NO manifest row at all.

Default scan-only (CSV report). ``--apply-write`` requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=...`` per the per-VM shard
isolation rule. ``--max-writes-per-run`` default 100k halt safety.

Per-asset-group implementation status (2026-05-07):

* TradFi: FULL — calendar pre-skip via UAC ``non_trading_day_reason``.
* DeFi:   FULL — chain pre-genesis + protocol pre-launch via UAC
  ``CHAIN_GENESIS_DATES`` + ``PROTOCOL_LAUNCH_DATES``.
* Sports: PARTIAL — pre-source-coverage-start via UAC
  ``SOURCE_COVERAGE_START``. Per-league enumeration deferred (needs
  sports leagues catalog read).
* CeFi:   STUB — needs instruments-service catalog with per-instrument
  lifecycle (``available_from`` / ``available_to`` / ``expiry``).
  See plan Phase 3.D.4 CeFi sub-task.
* Prediction: STUB — blocked on UAC ``PREDICTION_GROUPS`` registry which
  is empty pending the canonical_question_group SSOT
  (``predictions_master_2026_05_07.plan.md``).

Example::

    # Scan-only (TradFi)
    python scripts/enumerate_expected_universe.py --asset-group tradfi

    # Apply-write (DeFi)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=enum-universe-defi-$(date +%s) \\
    python scripts/enumerate_expected_universe.py \\
        --asset-group defi --apply-write --max-writes-per-run 50000
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage
from unified_api_contracts import DATA_TYPES_BY_ASSET_GROUP, VENUES_BY_ASSET_GROUP
from unified_api_contracts.registry.chain_env import (
    CHAIN_GENESIS_DATES,
    PROTOCOL_LAUNCH_DATES,
)
from unified_api_contracts.registry.venue_launch_dates import (
    CEFI_VENUE_LAUNCH_DATES,
    PREDICTION_VENUE_LAUNCH_DATES,
)
from unified_api_contracts.registry.venue_trading_calendar import (
    is_non_trading_day,
    non_trading_day_reason,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    "prediction": f"market-data-tick-prediction-{PROJECT_ID}",
}
MANIFEST_BLOB = "_index/availability_index.parquet"
DEFAULT_START_DATE = "2018-01-01"


@dataclass(frozen=True)
class ExpectedRow:
    """One row in the expected universe — either present in the manifest
    already (in which case the enumerator skips it) or missing (in which
    case the enumerator writes ``record_expected_empty(reason=...)``)."""

    asset_group: str
    venue: str
    chain: str
    data_type: str
    instrument_type: str
    instrument_id: str
    league_id: str
    date: str
    reason: str  # one of EMPTY_CONFIRMED_REASONS


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log (mirrors RECONCILER_* shape)."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


# ---------------------------------------------------------------------------
# Per-asset-group enumerators
# ---------------------------------------------------------------------------


def _enumerate_tradfi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Calendar pre-skip days × (venue, data_type) cross-product.

    For each TradFi venue, for each calendar non-trading day in window,
    yield one row per data_type with reason = EXPECTED_HOLIDAY / WEEKEND.
    """
    venues = VENUES_BY_ASSET_GROUP.get("tradfi", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("tradfi", [])
    if not venues or not data_types:
        logger.warning("TradFi venues/data_types empty — nothing to enumerate")
        return

    days = pd.date_range(start, end, freq="D")
    for venue in venues:
        venue_str = str(venue)
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            if not is_non_trading_day(venue_str, iso):
                continue
            reason = non_trading_day_reason(venue_str, iso) or "EXPECTED_HOLIDAY"
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="tradfi",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason=reason,
                )


def _enumerate_defi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Chain pre-genesis + protocol pre-launch days × data_types.

    For each (chain, protocol) in PROTOCOL_LAUNCH_DATES:
      * effective_start = max(chain_genesis, protocol_launch)
      * for each day in [start, effective_start - 1]:
          - day < chain_genesis  -> EXPECTED_PRE_GENESIS_CHAIN
          - day < protocol_launch -> EXPECTED_INSTRUMENT_NOT_LISTED
    """
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("defi", [])
    if not data_types:
        logger.warning("DeFi data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    for (chain, protocol), launch_date_str in PROTOCOL_LAUNCH_DATES.items():
        chain_upper = chain.upper()
        chain_genesis = CHAIN_GENESIS_DATES.get(chain_upper)
        if chain_genesis is None:
            logger.warning("Skipping (%s, %s): no chain genesis date in UAC", chain, protocol)
            continue
        # Effective start = max(chain_genesis, protocol_launch).
        effective_start = max(chain_genesis, launch_date_str)
        eff_ts = pd.Timestamp(effective_start)
        if pd.Timestamp(start) >= eff_ts:
            continue  # all days in window are post-launch — nothing to backfill
        # Yield rows for [start, min(end, effective_start - 1day)].
        last_day = min(end_ts, eff_ts - pd.Timedelta(days=1))
        days = pd.date_range(start, last_day, freq="D")
        venue_label = f"{protocol.upper()}-{chain_upper}"  # canonical venue shape
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            reason = "EXPECTED_PRE_GENESIS_CHAIN" if iso < chain_genesis else "EXPECTED_INSTRUMENT_NOT_LISTED"
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="defi",
                    venue=venue_label,
                    chain=chain_upper,
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason=reason,
                )


def _enumerate_sports(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-source-coverage-start days × data_types (per source).

    Per-league enumeration is deferred (v2 — needs sports leagues catalog).
    For now enumerates per-source pre-coverage dates which is the
    largest absent slice.
    """
    from unified_api_contracts.sports import SOURCE_COVERAGE_START  # type: ignore[attr-defined]

    data_types = DATA_TYPES_BY_ASSET_GROUP.get("sports", [])
    if not data_types:
        logger.warning("Sports data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    for source_key, coverage_start in SOURCE_COVERAGE_START.items():
        if coverage_start is None:
            continue
        if pd.Timestamp(start) >= pd.Timestamp(coverage_start):
            continue  # source covers entire window — nothing pre-coverage
        last_day = min(end_ts, pd.Timestamp(coverage_start) - pd.Timedelta(days=1))
        days = pd.date_range(start, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="sports",
                    venue=str(source_key),  # in sports the "venue" axis is the source key
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_SOURCE_COVERAGE_START",
                )


def _enumerate_cefi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-venue-launch days × data_types per CeFi venue.

    For each CeFi venue with a launch date in UAC ``CEFI_VENUE_LAUNCH_DATES``
    that is after the window start, yield rows for every
    ``(venue, data_type, day)`` tuple where ``day < launch_date``. Reason:
    ``EXPECTED_PRE_VENUE_LAUNCH``. Sister of the DeFi pre-genesis-chain branch
    above (chain genesis vs venue launch — same shape, different SSOT).

    **What this DOES NOT cover (deferred to v2 with a per-instrument catalog
    read):** ``EXPECTED_INSTRUMENT_NOT_LISTED`` / ``EXPECTED_INSTRUMENT_DELISTED``
    per-(venue, instrument_id, day) rows. Per-instrument lifecycle requires
    a ``gs://instruments-store-cefi-…`` catalog walk that's not wired here.
    Tracked as a P1 follow-up in writegate plan Phase 3.D.4 CeFi sub-task.

    The shard-key matrix declares CeFi spot/perp shards as
    ``(asset_group, venue, data_type, instrument_type, instrument_id, day)``.
    For pre-venue-launch dates ALL instruments are absent (the venue did not
    exist), so we use sentinel values ``instrument_type=""`` +
    ``instrument_id=""`` — the ``(venue, data_type, day)`` tuple alone is the
    correct atom for "no instruments existed yet" semantics. The reader-side
    classifier treats these venue-level rows as covering all per-instrument
    rows for that ``(venue, data_type, day)``.
    """
    venues = VENUES_BY_ASSET_GROUP.get("cefi", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("cefi", [])
    if not venues or not data_types:
        logger.warning("CeFi venues/data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    start_ts = pd.Timestamp(start)
    for venue in venues:
        venue_str = str(venue)
        launch_str = CEFI_VENUE_LAUNCH_DATES.get(venue_str)
        if launch_str is None:
            logger.info(
                "CeFi venue %s: no launch date in UAC CEFI_VENUE_LAUNCH_DATES; "
                "skipping pre-launch enumeration",
                venue_str,
            )
            continue
        launch_ts = pd.Timestamp(launch_str)
        if start_ts >= launch_ts:
            continue  # entire window is post-launch — nothing to backfill
        last_day = min(end_ts, launch_ts - pd.Timedelta(days=1))
        days = pd.date_range(start_ts, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="cefi",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_VENUE_LAUNCH",
                )


def _enumerate_prediction(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-venue-launch days × data_types per Prediction venue.

    Same shape as the CeFi enumerator — for each Prediction venue with a
    launch date in UAC ``PREDICTION_VENUE_LAUNCH_DATES`` after the window
    start, yield rows for every ``(venue, data_type, day)`` tuple where
    ``day < launch_date``. Reason: ``EXPECTED_PRE_VENUE_LAUNCH``.

    **What this DOES NOT cover (deferred to v2 once UAC ``PREDICTION_GROUPS``
    canonical_question_group registry lands per
    ``predictions_master_2026_05_07.plan.md``):** per-canonical-group market
    lifecycle bounds (``market_created_at`` / ``settlement_time``) which would
    yield ``EXPECTED_INSTRUMENT_NOT_LISTED`` / ``EXPECTED_INSTRUMENT_DELISTED``
    rows for individual canonical question groups. The pre-venue-launch slice
    is the largest absent universe by date count and is independently useful;
    per-canonical-group enumeration adds finer detail on top.
    """
    venues = VENUES_BY_ASSET_GROUP.get("prediction", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("prediction", [])
    if not venues or not data_types:
        logger.warning("Prediction venues/data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    start_ts = pd.Timestamp(start)
    for venue in venues:
        venue_str = str(venue)
        launch_str = PREDICTION_VENUE_LAUNCH_DATES.get(venue_str)
        if launch_str is None:
            logger.info(
                "Prediction venue %s: no launch date in UAC PREDICTION_VENUE_LAUNCH_DATES; "
                "skipping pre-launch enumeration",
                venue_str,
            )
            continue
        launch_ts = pd.Timestamp(launch_str)
        if start_ts >= launch_ts:
            continue
        last_day = min(end_ts, launch_ts - pd.Timedelta(days=1))
        days = pd.date_range(start_ts, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="prediction",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_VENUE_LAUNCH",
                )


_ENUMERATORS: dict[str, object] = {
    "tradfi": _enumerate_tradfi,
    "defi": _enumerate_defi,
    "sports": _enumerate_sports,
    "cefi": _enumerate_cefi,
    "prediction": _enumerate_prediction,
}


# ---------------------------------------------------------------------------
# Manifest IO
# ---------------------------------------------------------------------------


def _download_manifest(bucket_name: str, asset_group: str) -> tuple[pd.DataFrame, str]:
    """Bulk-download the canonical manifest. Returns (df, local_path)."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix=f"enum-univ-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_present_set(df: pd.DataFrame, asset_group: str) -> set[tuple[str, ...]]:
    """Build the set of (venue, chain, data_type, ..., date) tuples already in manifest."""
    if df.empty:
        return set()
    cols = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    available = [c for c in cols if c in df.columns]
    if "date" not in df.columns:
        logger.warning("Manifest missing 'date' column — cannot build present-set")
        return set()
    df_subset = df[available].fillna("").astype(str)
    return {tuple(row) for row in df_subset.itertuples(index=False, name=None)}


def _row_key(row: ExpectedRow, available_cols: list[str]) -> tuple[str, ...]:
    """Build the manifest-aligned row key from an ExpectedRow."""
    field_map = {
        "venue": row.venue,
        "chain": row.chain,
        "data_type": row.data_type,
        "instrument_type": row.instrument_type,
        "instrument_id": row.instrument_id,
        "league_id": row.league_id,
        "date": row.date,
    }
    return tuple(field_map.get(c, "") for c in available_cols)


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Enumerate expected universe and write record_expected_empty rows "
            "for tuples with no manifest row. Phase 3.D.4 — see module "
            "docstring."
        ),
    )
    p.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(ASSET_GROUP_BUCKETS.keys()),
        help="Asset group manifest to enumerate.",
    )
    p.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help=f"Window start (default: {DEFAULT_START_DATE}).",
    )
    p.add_argument(
        "--end-date",
        default=datetime.now(UTC).strftime("%Y-%m-%d"),
        help="Window end (default: today UTC).",
    )
    p.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical bucket (default: per-asset-group SSOT).",
    )
    p.add_argument(
        "--apply-write",
        action="store_true",
        help="Default scan-only. Pass to actually write to per-VM manifest shard.",
    )
    p.add_argument(
        "--max-writes-per-run",
        type=int,
        default=1_000_000,
        help=(
            "Halt-safety cap (default 1M, bumped 2026-05-07 after defi scan-only run "
            "exceeded the prior 100k default). Aborts if scan finds more than this."
        ),
    )
    p.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report dir (default: tempfile.gettempdir()).",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    asset_group: str = args.asset_group
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[asset_group]
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"enum-universe-{asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"enum-universe-{asset_group}-{run_ts}.csv"

    _emit_event(
        "ENUMERATOR_STARTED",
        enumerator="enumerate_expected_universe",
        asset_group=asset_group,
        bucket=bucket_name,
        start_date=args.start_date,
        end_date=args.end_date,
        apply_write=args.apply_write,
        max_writes_per_run=args.max_writes_per_run,
        run_id=run_id,
    )

    if args.apply_write:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-write requires MANIFEST_PER_VM_SHARDS=true (per-VM shard "
                "isolation rule, codex/02-data/availability-manifest-and-data-status.md)."
            )
            _emit_event(
                "ENUMERATOR_FAILED",
                reason="missing_per_vm_shards_env",
                run_id=run_id,
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-write requires VM_NAME=<unique-tag>.")
            _emit_event("ENUMERATOR_FAILED", reason="missing_vm_name_env", run_id=run_id)
            return 4

    # Step 1: download manifest, build present-set.
    start = time.time()
    df, local_manifest = _download_manifest(bucket_name, asset_group)
    try:
        present_set = _build_present_set(df, asset_group)
        logger.info("Manifest present-set size: %d", len(present_set))

        # Determine which manifest columns exist for present-set comparison.
        possible_cols = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
        available_cols = [c for c in possible_cols if c in df.columns]

        # Step 2: enumerate expected universe; filter to absent tuples.
        enumerator = _ENUMERATORS[asset_group]
        absent_rows: list[ExpectedRow] = []
        scan_start = time.time()
        for expected in enumerator(args.start_date, args.end_date):  # type: ignore[operator]
            key = _row_key(expected, available_cols)
            if key in present_set:
                continue
            absent_rows.append(expected)
            if len(absent_rows) > args.max_writes_per_run:
                logger.error(
                    "Halt-safety triggered: would-write %d > max_writes_per_run %d. "
                    "Increase --max-writes-per-run after operator review.",
                    len(absent_rows),
                    args.max_writes_per_run,
                )
                _emit_event(
                    "ENUMERATOR_FAILED",
                    reason="max_writes_exceeded",
                    candidates=len(absent_rows),
                    cap=args.max_writes_per_run,
                    run_id=run_id,
                )
                return 5
        scan_secs = time.time() - scan_start
        logger.info(
            "Enumeration complete: %d candidate rows in %.1fs",
            len(absent_rows),
            scan_secs,
        )

        if not absent_rows:
            logger.info("Nothing to backfill. Manifest already covers the expected universe.")
            _emit_event(
                "ENUMERATOR_COMPLETED",
                asset_group=asset_group,
                candidates=0,
                written=0,
                run_id=run_id,
            )
            return 0

        # Distribution by reason.
        reason_counts: dict[str, int] = {}
        for r in absent_rows:
            reason_counts[r.reason] = reason_counts.get(r.reason, 0) + 1
        logger.info("Distribution by reason:")
        for reason, count in sorted(reason_counts.items(), key=lambda kv: -kv[1]):
            logger.info("  %s: %d", reason, count)

        # CSV audit.
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(asdict(absent_rows[0]).keys()))
            writer.writeheader()
            writer.writerows(asdict(r) for r in absent_rows)
        logger.info("Would-write report: %s (%d rows)", report_path, len(absent_rows))

        if not args.apply_write:
            logger.info("Scan-only mode; not writing manifest. Pass --apply-write to commit.")
            _emit_event(
                "ENUMERATOR_COMPLETED",
                asset_group=asset_group,
                candidates=len(absent_rows),
                written=0,
                report_path=str(report_path),
                run_id=run_id,
            )
            return 0

        # Step 3: write per-VM shard. Mirrors reconcile_expected_absence_reasons.py
        # write pattern: build a DataFrame of new rows, upload as a single
        # parquet to _index/per_vm/{vm_name}.parquet. The consolidator daemon
        # merges per-VM shards into the canonical manifest with
        # last-writer-wins on identical row_key.
        # Using DataFrame.to_parquet (not per-row record_expected_empty) avoids
        # thousands of CAS round-trips per the reconciler precedent.
        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        attempted_at_iso = datetime.now(UTC).isoformat()

        # Build the new-row DataFrame. Schema must align with the existing
        # canonical manifest df we just read (so the consolidator can merge).
        # Start from the manifest's columns (via df.columns); fill new rows
        # with our values; default to "" for any column we don't populate.
        new_rows_records: list[dict[str, object]] = []
        for r in absent_rows:
            record: dict[str, object] = {
                "asset_group": asset_group,
                "venue": r.venue,
                "chain": r.chain,
                "data_type": r.data_type,
                "instrument_type": r.instrument_type,
                "instrument_id": r.instrument_id,
                "league_id": r.league_id,
                "date": r.date,
                "capture_status": "empty_confirmed",
                "error_reason": r.reason,
                "attempted_at": attempted_at_iso,
                "row_count": 0,
                "service_name": "instruments-service",
                "enumerator_run_id": run_id,
            }
            new_rows_records.append(record)

        new_df = pd.DataFrame(new_rows_records)
        # Align columns with the canonical manifest where they overlap; fill
        # any missing columns with type-appropriate nulls so the parquet schema
        # lines up cleanly with the canonical (consolidator merge requires
        # identical column types per pyarrow concat — empty-string defaults
        # for int64/float64 columns caused the 2026-05-07 ArrowTypeError on
        # instrument_count, see issues/manifest_consolidator_arrow_type_error_2026_05_07.md).
        manifest_cols = list(df.columns)
        for col in manifest_cols:
            if col not in new_df.columns:
                canonical_dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(canonical_dtype):
                    # Use pandas nullable Int64 — pyarrow writes it as int64 with nulls.
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Int64")
                elif pd.api.types.is_float_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Float64")
                elif pd.api.types.is_bool_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="boolean")
                else:
                    # string / object / datetime — empty string is a safe default
                    # (the canonical's read path tolerates it for non-numeric cols).
                    new_df[col] = ""
        # Reorder to match manifest column order.
        new_df = new_df.reindex(columns=manifest_cols + [c for c in new_df.columns if c not in manifest_cols])

        with tempfile.NamedTemporaryFile(
            prefix=f"enum-univ-out-{asset_group}-",
            suffix=".parquet",
            delete=False,
        ) as tf:
            out_path = tf.name
        try:
            new_df.to_parquet(out_path, index=False)
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(bucket_name)
            out_blob = bucket.blob(per_vm_blob)
            out_blob.upload_from_filename(out_path)
            logger.info("Uploaded per-VM shard to gs://%s/%s", bucket_name, per_vm_blob)
        finally:
            try:
                os.unlink(out_path)
            except OSError:
                pass

        elapsed = time.time() - start
        _emit_event(
            "ENUMERATOR_COMPLETED",
            asset_group=asset_group,
            candidates=len(absent_rows),
            written=len(new_rows_records),
            elapsed_secs=round(elapsed, 1),
            report_path=str(report_path),
            per_vm_blob=per_vm_blob,
            run_id=run_id,
        )
        logger.info(
            "Wrote %d rows to per-VM shard gs://%s/%s for VM=%s in %.1fs. "
            "Consolidator will merge into canonical manifest within ~5min.",
            len(new_rows_records),
            bucket_name,
            per_vm_blob,
            vm_name,
            elapsed,
        )
        return 0
    finally:
        try:
            os.unlink(local_manifest)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
