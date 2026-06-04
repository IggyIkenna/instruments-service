#!/usr/bin/env python3
"""Roll-up producer — derive the lifecycle instrument catalogue from the per-date definitions.

The v2 expected-universe enumerator (``enumerate_expected_universe.py --enumerator-version v2``)
consumes a ``catalog.parquet`` of :class:`InstrumentCatalogEntry` rows — ONE row per instrument,
each carrying an ``available_from`` / ``available_to`` lifecycle window — so it can emit
``EXPECTED_INSTRUMENT_NOT_LISTED`` (``date < available_from``) and ``EXPECTED_INSTRUMENT_DELISTED``
(``date > available_to``). That file is a *cumulative, all-instruments-ever* lifecycle catalogue,
NOT a current snapshot.

Until now nothing produced it on a recurring basis (slot-3 finding, 2026-06-04): it was an
operator-supplied static snapshot, stale two ways at once — missing newly-listed instruments AND
wrong about what is still alive (a since-delisted instrument shown alive → its cells stay
``expected_unattempted`` forever instead of ``DELISTED``).

This script makes the catalogue a **derivative of the maintained per-date definitions**. The
instruments-service already writes
``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet`` daily (the
point-in-time, reproducible-batch "what existed on date t" slice). For each instrument:

* ``available_from`` = the first day it appears across the ``by_date/`` snapshots;
* ``available_to``  = the last day it appears, OR ``None`` when it is present on the latest
  snapshot day (still active / no upper bound).

So the data already exists and the catalogue is a roll-up of it — correct + self-refreshing, with
no separate artifact to drift.

Output path (operator decision 2026-06-04, the path the launcher + enumerator already expect):
``gs://{get_write_bucket_name("instruments", ag)}/{DEPLOYMENT_ENV}/catalog.parquet`` — i.e.
``instruments-store-{ag_short}-{env_short}-{pid}/{env}/catalog.parquet``.

Monotonic-guard promotion (operator decision 2026-06-04): regenerate → write to a TEMP object →
assert the new row count is ``>=`` the current catalogue (instrument rows grow monotonically: new
listings add rows; delisted rows persist with ``available_to`` set) → on pass **promote** (copy
temp over the canonical object + delete temp); on a ``<`` regression KEEP the previous good
catalogue + alert, do NOT overwrite. ``--allow-catalogue-shrink`` overrides the ratchet for a
legitimate corrective shrink.

v9, NOT v10 — this is part of the v9 canonicalisation; no schema-version bump is introduced.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 1 — [CODE] P0).
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd
from unified_trading_library import (
    StorageClient,
    get_config,
    get_storage_client,
    get_write_bucket_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: GCS prefix the per-date instrument definitions live under, for cefi / defi /
#: tradfi / prediction. (Sports fixtures use ``sports_reference/by_date`` with an
#: ``entity=`` key — overridable via ``--by-date-prefix``; the sports slice is
#: tracked as Phase 3 of the plan.)
DEFAULT_BY_DATE_PREFIX = "instrument_availability/by_date"

#: The canonical catalogue filename the launcher + v2 enumerator read.
CATALOG_FILENAME = "catalog.parquet"

#: Columns the enumerator's ``_catalog_from_dataframe`` consumes. ``instrument_id``
#: is written as the canonical column (the helper also accepts ``instrument_key``).
CATALOG_COLUMNS: tuple[str, ...] = (
    "instrument_id",
    "instrument_type",
    "venue",
    "chain",
    "league_id",
    "available_from",
    "available_to",
    "market_created_at",
    "settlement_time",
)

#: Per-date parquet columns holding the instrument identifier (first match wins).
_ID_COLUMNS: tuple[str, ...] = ("instrument_key", "instrument_id")

#: Extract the ISO ``day=`` partition from a ``by_date`` blob path.
_DAY_RE = re.compile(r"(?:^|/)day=(\d{4}-\d{2}-\d{2})(?:/|$)")


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log (mirrors the enumerator's ENUMERATOR_* shape)."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


# ---------------------------------------------------------------------------
# Pure roll-up math (no I/O — unit-tested directly)
# ---------------------------------------------------------------------------


@dataclass
class _InstrumentAggregate:
    """Lifecycle accumulator for a single instrument across the per-date snapshots."""

    first_day: date
    last_day: date
    meta_day: date
    meta: dict[str, str | None]


def _row_id(row: dict[str, object]) -> str | None:
    """Return the instrument identifier for a per-date row, or None when absent/blank."""
    for col in _ID_COLUMNS:
        raw = row.get(col)
        if raw is None:
            continue
        try:
            if pd.isna(raw):  # pyright: ignore[reportArgumentType]
                continue
        except (TypeError, ValueError):
            pass
        text = str(raw).strip()
        if text:
            return text
    return None


def _opt_field(row: dict[str, object], col: str) -> str | None:
    """Return a string field from a per-date row, or None for missing/NaN."""
    raw = row.get(col)
    if raw is None:
        return None
    try:
        if pd.isna(raw):  # pyright: ignore[reportArgumentType]
            return None
    except (TypeError, ValueError):
        pass
    return str(raw)


def _str_field(row: dict[str, object], col: str) -> str:
    """Return a string field from a per-date row, or "" for missing/NaN."""
    return _opt_field(row, col) or ""


def build_catalogue_dataframe(snapshots: Iterable[tuple[date, pd.DataFrame]]) -> pd.DataFrame:
    """Roll the per-date instrument definitions up into one lifecycle catalogue.

    Args:
        snapshots: iterable of ``(day, frame)`` pairs — one per
            ``by_date/day={day}/.../instruments.parquet`` slice. Each frame holds
            that day's instrument definitions (one row per instrument).

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`, one row per distinct instrument:
        ``available_from`` = first day present (ISO ``YYYY-MM-DD``); ``available_to``
        = last day present, or ``None`` when the instrument is present on the latest
        snapshot day across all slices (still active). Metadata columns
        (instrument_type / venue / chain / league_id / market_created_at /
        settlement_time) are taken from the instrument's most-recent snapshot row.
        Rows are sorted by ``instrument_id`` for deterministic output.
    """
    aggregates: dict[str, _InstrumentAggregate] = {}
    all_days: set[date] = set()

    for day, frame in snapshots:
        all_days.add(day)
        if frame.empty:
            continue
        records: list[dict[str, object]] = frame.to_dict("records")  # pyright: ignore[reportAssignmentType]
        for row in records:
            iid = _row_id(row)
            if iid is None:
                continue
            existing = aggregates.get(iid)
            if existing is None:
                aggregates[iid] = _InstrumentAggregate(
                    first_day=day,
                    last_day=day,
                    meta_day=day,
                    meta=_extract_meta(row),
                )
                continue
            if day < existing.first_day:
                existing.first_day = day
            if day > existing.last_day:
                existing.last_day = day
            # Metadata follows the most-recent definition of the instrument.
            if day >= existing.meta_day:
                existing.meta_day = day
                existing.meta = _extract_meta(row)

    if not all_days:
        return pd.DataFrame(columns=list(CATALOG_COLUMNS))

    latest_day = max(all_days)

    rows: list[dict[str, str | None]] = []
    for iid in sorted(aggregates):
        agg = aggregates[iid]
        available_to = None if agg.last_day >= latest_day else agg.last_day.isoformat()
        rows.append(
            {
                "instrument_id": iid,
                "instrument_type": agg.meta["instrument_type"] or "",
                "venue": agg.meta["venue"] or "",
                "chain": agg.meta["chain"] or "",
                "league_id": agg.meta["league_id"] or "",
                "available_from": agg.first_day.isoformat(),
                "available_to": available_to,
                "market_created_at": agg.meta["market_created_at"],
                "settlement_time": agg.meta["settlement_time"],
            }
        )

    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


def _extract_meta(row: dict[str, object]) -> dict[str, str | None]:
    """Pull the catalogue metadata fields out of a per-date instrument row."""
    return {
        "instrument_type": _str_field(row, "instrument_type"),
        "venue": _str_field(row, "venue"),
        "chain": _str_field(row, "chain"),
        "league_id": _str_field(row, "league_id"),
        "market_created_at": _opt_field(row, "market_created_at"),
        "settlement_time": _opt_field(row, "settlement_time"),
    }


# ---------------------------------------------------------------------------
# Monotonic-guard decision (pure — unit-tested directly)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GuardDecision:
    """Outcome of the monotonic row-count guard."""

    accept: bool
    reason: str


def evaluate_monotonic_guard(
    new_count: int,
    current_count: int | None,
    *,
    allow_shrink: bool,
) -> GuardDecision:
    """Decide whether a freshly-rolled catalogue may replace the current one.

    Instrument rows grow monotonically — new listings add rows and delisted rows
    persist (with ``available_to`` stamped) — so a strictly smaller row count
    signals an incomplete / buggy regeneration, not a real change.

    Args:
        new_count: row count of the freshly-rolled catalogue.
        current_count: row count of the current canonical catalogue, or None when
            no catalogue exists yet (first run).
        allow_shrink: when True, a legitimate corrective shrink is permitted.

    Returns:
        A :class:`GuardDecision`; ``accept=False`` means KEEP the previous
        catalogue and do NOT overwrite.
    """
    if current_count is None:
        return GuardDecision(accept=True, reason="no_prior_catalogue")
    if new_count >= current_count:
        return GuardDecision(accept=True, reason="monotonic_ok")
    if allow_shrink:
        return GuardDecision(accept=True, reason="shrink_overridden")
    return GuardDecision(accept=False, reason="shrink_blocked")


# ---------------------------------------------------------------------------
# GCS I/O
# ---------------------------------------------------------------------------


def _iter_by_date_snapshots(
    storage: StorageClient,
    bucket: str,
    prefix: str,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield ``(day, frame)`` for every ``by_date`` instruments parquet under ``prefix``."""
    walk_prefix = prefix.rstrip("/") + "/"
    for blob in storage.list_blobs(bucket, prefix=walk_prefix):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        match = _DAY_RE.search(name)
        if match is None:
            logger.warning("Skipping blob with no day= partition: %s", name)
            continue
        day = date.fromisoformat(match.group(1))
        payload = storage.download_bytes(bucket, name)
        frame = pd.read_parquet(io.BytesIO(payload))
        yield day, frame


def _read_current_row_count(storage: StorageClient, bucket: str, blob_path: str) -> int | None:
    """Return the row count of the current canonical catalogue, or None when absent."""
    if not storage.blob_exists(bucket, blob_path):
        return None
    payload = storage.download_bytes(bucket, blob_path)
    current = pd.read_parquet(io.BytesIO(payload))
    return len(current)


def _catalogue_object_paths(env: str) -> tuple[str, str]:
    """Return ``(canonical_blob, temp_blob)`` for the env tier."""
    canonical = f"{env}/{CATALOG_FILENAME}"
    temp = f"{env}/_catalogue_staging/{CATALOG_FILENAME}"
    return canonical, temp


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a catalogue DataFrame to parquet bytes."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def promote_catalogue(
    storage: StorageClient,
    bucket: str,
    env: str,
    df: pd.DataFrame,
    *,
    allow_shrink: bool,
    dry_run: bool,
) -> int:
    """Apply the monotonic-guard promotion. Returns a process exit code (0 = ok)."""
    canonical_blob, temp_blob = _catalogue_object_paths(env)
    new_count = len(df)
    current_count = _read_current_row_count(storage, bucket, canonical_blob)
    decision = evaluate_monotonic_guard(new_count, current_count, allow_shrink=allow_shrink)

    logger.info(
        "Monotonic guard: new=%d current=%s decision=%s (%s)",
        new_count,
        current_count,
        "ACCEPT" if decision.accept else "REJECT",
        decision.reason,
    )

    if not decision.accept:
        _emit_event(
            "CATALOGUE_SHRINK_BLOCKED",
            bucket=bucket,
            env=env,
            new_count=new_count,
            current_count=current_count,
            hint="re-run a complete regeneration, or pass --allow-catalogue-shrink for a corrective shrink",
        )
        logger.error(
            "CATALOGUE_SHRINK_BLOCKED: new=%d < current=%d — keeping previous good catalogue at gs://%s/%s "
            "(pass --allow-catalogue-shrink to override for a legitimate corrective shrink)",
            new_count,
            current_count,
            bucket,
            canonical_blob,
        )
        return 1

    if dry_run:
        logger.info(
            "[dry-run] would promote %d-row catalogue to gs://%s/%s",
            new_count,
            bucket,
            canonical_blob,
        )
        return 0

    payload = _to_parquet_bytes(df)
    # Temp-first so a crash mid-write never corrupts the canonical object.
    storage.upload_bytes(bucket, temp_blob, payload, content_type="application/octet-stream")
    storage.copy_blob(bucket, temp_blob, bucket, canonical_blob)
    storage.delete_blob(bucket, temp_blob)
    _emit_event(
        "CATALOGUE_PROMOTED",
        bucket=bucket,
        env=env,
        rows=new_count,
        path=f"gs://{bucket}/{canonical_blob}",
        guard_reason=decision.reason,
    )
    logger.info("Promoted %d-row catalogue to gs://%s/%s", new_count, bucket, canonical_blob)
    return 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_rollup(
    asset_group: str,
    *,
    allow_shrink: bool,
    dry_run: bool,
    by_date_prefix: str = DEFAULT_BY_DATE_PREFIX,
    storage: StorageClient | None = None,
) -> int:
    """Roll up the per-date definitions for ``asset_group`` and promote the catalogue."""
    run_id = f"catalogue-rollup-{asset_group}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    storage = storage or get_storage_client()
    bucket = get_write_bucket_name("instruments", asset_group)
    env = get_config("DEPLOYMENT_ENV", "prod")
    _emit_event(
        "CATALOGUE_ROLLUP_STARTED",
        run_id=run_id,
        asset_group=asset_group,
        bucket=bucket,
        env=env,
        by_date_prefix=by_date_prefix,
    )
    logger.info(
        "Rolling up %s catalogue from gs://%s/%s/ (env=%s)",
        asset_group,
        bucket,
        by_date_prefix.rstrip("/"),
        env,
    )

    df = build_catalogue_dataframe(_iter_by_date_snapshots(storage, bucket, by_date_prefix))
    logger.info("Rolled up %d distinct instruments", len(df))

    code = promote_catalogue(
        storage,
        bucket,
        env,
        df,
        allow_shrink=allow_shrink,
        dry_run=dry_run,
    )
    _emit_event(
        "CATALOGUE_ROLLUP_COMPLETED" if code == 0 else "CATALOGUE_ROLLUP_FAILED",
        run_id=run_id,
        asset_group=asset_group,
        rows=len(df),
        exit_code=code,
    )
    return code


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Roll the per-date instrument definitions up into the lifecycle catalogue.",
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=["cefi", "defi", "tradfi", "sports", "prediction"],
        help="Asset group to roll up (lowercase).",
    )
    parser.add_argument(
        "--by-date-prefix",
        default=DEFAULT_BY_DATE_PREFIX,
        help=(
            "GCS prefix the per-date instrument definitions live under "
            f"(default: {DEFAULT_BY_DATE_PREFIX!r}; sports fixtures use sports_reference/by_date)."
        ),
    )
    parser.add_argument(
        "--allow-catalogue-shrink",
        action="store_true",
        help="Override the monotonic guard for a legitimate corrective shrink (removing a bad instrument row).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Roll up + evaluate the guard, but do NOT write the catalogue.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = _parse_args(argv)
    asset_group: str = args.asset_group
    allow_shrink: bool = args.allow_catalogue_shrink
    dry_run: bool = args.dry_run
    by_date_prefix: str = args.by_date_prefix
    return run_rollup(
        asset_group,
        allow_shrink=allow_shrink,
        dry_run=dry_run,
        by_date_prefix=by_date_prefix,
    )


if __name__ == "__main__":
    sys.exit(main())
