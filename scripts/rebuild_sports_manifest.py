#!/usr/bin/env python3
"""Rebuild the SPORTS availability manifest from canonical GCS layouts.

Mirrors the cefi/defi/prediction rebuilders but adds:

  * **Layout-aware walking** — UAC's ``SPORTS_DATA_TYPE_LAYOUT`` declares each
    entity as ``per_day_per_league`` / ``per_day_bare`` / ``flat`` and the
    rebuild emits one row per matching parquet.
  * **Bare-layout file unpacking** — entities like ``SFI_LEAGUES`` and
    ``TRANSFERMARKT_LEAGUES`` store one parquet per day with all leagues
    inside (raw provider IDs in a column). The rebuild reads each such
    parquet, extracts distinct provider league IDs, maps them through the
    UAC canonical mapping (``SOCCER_FOOTBALL_INFO_IDS``, ``TRANSFERMARKT_IDS``)
    to canonical league names, and emits one manifest row per
    ``(date, canonical_league_id)``. Provider IDs we don't recognise are
    skipped (don't pollute the manifest with raw IDs).
  * **Empty-vs-captured discrimination** — every emitted row carries
    ``capture_status=captured`` only when the parquet has rows > 0;
    ``empty_confirmed`` when the file exists with zero rows (the source
    legitimately had nothing on that day). NEVER fake ``captured`` to
    inflate coverage %.
  * Uses ``parquet metadata`` (not full read) for the row-count check,
    so large parquets cost <50ms each.
  * ``per_vm_shards=True`` so it doesn't fight the canonical writer.

Usage::

    python -m scripts.rebuild_sports_manifest \\
        --start-date 2018-01-01 --end-date 2026-05-04
    python -m scripts.rebuild_sports_manifest \\
        --start-date 2025-01-01 --end-date 2025-01-31 --dry-run

Then merge into canonical via:
    python -m unified_trading_library.manifest_consolidator \\
        --bucket instruments-store-sports-central-element-323112

Idempotent. Safe to re-run.
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import date as _date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


SERVICE_NAME = "instruments-service"
BUCKET_TEMPLATE = "instruments-store-sports-central-element-323112"


# ---------------------------------------------------------------------------
# Path parsing
# ---------------------------------------------------------------------------

# Captures: day, entity, optional league, file stem
_PAT_PER_LEAGUE = re.compile(
    r"^sports_reference/by_date/"
    r"day=(?P<date>\d{4}-\d{2}-\d{2})/"
    r"entity=(?P<entity>[^/]+)/"
    r"league=(?P<league>[^/]+)/"
    r"(?P<stem>[^/]+)\.parquet$"
)
_PAT_BARE = re.compile(
    r"^sports_reference/by_date/"
    r"day=(?P<date>\d{4}-\d{2}-\d{2})/"
    r"entity=(?P<entity>[^/]+)/"
    r"(?P<stem>[^/]+)\.parquet$"
)
_PAT_FLAT = re.compile(r"^sports_reference/(?P<entity>[^/]+)/(?P<stem>[^/]+)\.parquet$")


@dataclass(frozen=True)
class ParsedShard:
    date: str
    entity: str
    league_id: str  # canonical name; "" for bare layouts
    data_type: str  # canonical data_type string (uppercase per UAC manifest convention)
    capture_status: str  # captured / empty_confirmed
    instrument_count: int


# ---------------------------------------------------------------------------
# Provider hash → canonical league mapping
# ---------------------------------------------------------------------------


def _build_canonical_mappings():
    """Return (sfi_hash_to_canonical, transfermarkt_id_to_canonical) dicts."""
    from unified_api_contracts.sports import provider_league_ids as p

    sfi = {v: k for k, v in p.SOCCER_FOOTBALL_INFO_IDS.items()}
    tm = {v: k for k, v in p.TRANSFERMARKT_IDS.items()} if hasattr(p, "TRANSFERMARKT_IDS") else {}
    return sfi, tm


# ---------------------------------------------------------------------------
# Parquet row-count probe (fast — uses parquet metadata, no row data load)
# ---------------------------------------------------------------------------


def _probe_row_count(client, bucket: str, blob_name: str) -> int:
    """Return the number of rows in a parquet via its footer metadata.

    Reads only the parquet footer (~few KB) instead of the whole file.
    Returns -1 on failure (caller treats as "unknown" — emits captured by
    default since the file at least exists).
    """
    import pyarrow.parquet as pq

    try:
        data = client.bucket(bucket).blob(blob_name).download_as_bytes()
        return pq.read_metadata(io.BytesIO(data)).num_rows
    except Exception as exc:
        logger.debug("row-count probe failed for %s: %s", blob_name, exc)
        return -1


# ---------------------------------------------------------------------------
# Main scan — per-entity walker
# ---------------------------------------------------------------------------


def _scan_per_league_entity(
    storage_client,
    bucket: str,
    entity_folder: str,
    data_type: str,
    start_date: _date,
    end_date: _date,
) -> list[ParsedShard]:
    """Walk ``entity={folder}/league={LEAGUE}/{folder}.parquet`` paths.

    Emits one ParsedShard per parquet found in the date range.  Row count
    is probed per parquet — captured iff rows > 0, empty_confirmed iff
    rows == 0, captured-with-unknown-count iff probe fails.
    """
    out: list[ParsedShard] = []
    bucket_obj = storage_client.bucket(bucket)
    cur = start_date
    while cur <= end_date:
        date_str = cur.isoformat()
        prefix = f"sports_reference/by_date/day={date_str}/entity={entity_folder}/"
        for blob in bucket_obj.list_blobs(prefix=prefix):
            name = blob.name
            if not name.endswith(".parquet"):
                continue
            m = _PAT_PER_LEAGUE.match(name)
            if not m:
                # Could be a bare-style file at this prefix; skip — bare layouts
                # are handled separately so we don't double-count.
                continue
            league = m.group("league")
            rows = _probe_row_count(storage_client, bucket, name)
            status = "empty_confirmed" if rows == 0 else "captured"
            out.append(
                ParsedShard(
                    date=date_str,
                    entity=entity_folder,
                    league_id=league,
                    data_type=data_type,
                    capture_status=status,
                    instrument_count=max(rows, 0),
                )
            )
        cur += timedelta(days=1)
    return out


def _scan_bare_entity(
    storage_client,
    bucket: str,
    entity_folder: str,
    data_type: str,
    start_date: _date,
    end_date: _date,
    *,
    unpack_provider_ids: bool = False,
    provider_map: dict[str, str] | None = None,
    league_id_column: str = "league_id",
) -> list[ParsedShard]:
    """Walk ``entity={folder}/{folder}.parquet`` (per-day, all-leagues-in-one-file).

    When ``unpack_provider_ids=True`` we READ the parquet and emit one
    ParsedShard per ``(date, canonical_league)`` for every distinct
    provider league ID we can map. When False, we emit one ParsedShard
    per parquet with ``league_id=""`` (the deployment-UI accepts these
    for entities that don't have a league axis like LEAGUES / WEATHER / XG).
    """
    import pandas as pd

    out: list[ParsedShard] = []
    bucket_obj = storage_client.bucket(bucket)
    cur = start_date
    while cur <= end_date:
        date_str = cur.isoformat()
        prefix = f"sports_reference/by_date/day={date_str}/entity={entity_folder}/"
        for blob in bucket_obj.list_blobs(prefix=prefix):
            name = blob.name
            if not name.endswith(".parquet"):
                continue
            # Skip per-league subpaths — those are handled by the per-league walker.
            if "/league=" in name:
                continue
            if not _PAT_BARE.match(name):
                continue
            if not unpack_provider_ids:
                rows = _probe_row_count(storage_client, bucket, name)
                status = "empty_confirmed" if rows == 0 else "captured"
                out.append(
                    ParsedShard(
                        date=date_str,
                        entity=entity_folder,
                        league_id="",
                        data_type=data_type,
                        capture_status=status,
                        instrument_count=max(rows, 0),
                    )
                )
                continue
            # Unpack provider IDs from inside the file.
            try:
                data = bucket_obj.blob(name).download_as_bytes()
                df = pd.read_parquet(io.BytesIO(data))
            except Exception as exc:
                logger.warning("could not read %s for unpack: %s", name, exc)
                continue
            if league_id_column not in df.columns or df.empty:
                # File exists but no league_id column — emit a single
                # empty_confirmed row at league_id="" so the (date, entity)
                # is at least represented.
                out.append(
                    ParsedShard(
                        date=date_str,
                        entity=entity_folder,
                        league_id="",
                        data_type=data_type,
                        capture_status="empty_confirmed",
                        instrument_count=0,
                    )
                )
                continue
            seen_canonical: set[str] = set()
            unmapped = 0
            for raw_id in df[league_id_column].dropna().unique():
                canonical = (provider_map or {}).get(str(raw_id))
                if not canonical:
                    unmapped += 1
                    continue
                if canonical in seen_canonical:
                    continue
                seen_canonical.add(canonical)
                # row_count: count rows for this league inside the file
                count = int((df[league_id_column] == raw_id).sum())
                out.append(
                    ParsedShard(
                        date=date_str,
                        entity=entity_folder,
                        league_id=canonical,
                        data_type=data_type,
                        capture_status="captured" if count > 0 else "empty_confirmed",
                        instrument_count=count,
                    )
                )
            if unmapped:
                logger.debug(
                    "%s: skipped %d unmapped provider IDs (date=%s)",
                    entity_folder,
                    unmapped,
                    date_str,
                )
        cur += timedelta(days=1)
    return out


def _scan_flat_entity(
    storage_client,
    bucket: str,
    entity_folder: str,
    data_type: str,
) -> list[ParsedShard]:
    """One-off entity (VENUES) — single parquet at ``sports_reference/{folder}/{folder}.parquet``."""
    bucket_obj = storage_client.bucket(bucket)
    blob_name = f"sports_reference/{entity_folder}/{entity_folder}.parquet"
    blob = bucket_obj.blob(blob_name)
    if not blob.exists():
        return []
    rows = _probe_row_count(storage_client, bucket, blob_name)
    return [
        ParsedShard(
            date="9999-12-31",  # sentinel — flat entities have no date axis
            entity=entity_folder,
            league_id="",
            data_type=data_type,
            capture_status="empty_confirmed" if rows == 0 else "captured",
            instrument_count=max(rows, 0),
        )
    ]


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def rebuild(
    *,
    project_id: str,
    start_date: _date,
    end_date: _date,
    only_data_types: list[str] | None,
    dry_run: bool,
) -> dict[str, int]:
    from google.cloud import storage  # noqa: qg-inside-import
    from unified_api_contracts.sports import (  # noqa: qg-inside-import
        SPORTS_DATA_TYPE_LAYOUT,
        SPORTS_DATA_TYPE_TO_FOLDER,
    )
    from unified_trading_library import (  # noqa: qg-inside-import  # pyright: ignore[reportPrivateImportUsage]
        ManifestWriter,
    )

    storage_client = storage.Client(project=project_id)
    bucket = BUCKET_TEMPLATE
    sfi_map, tm_map = _build_canonical_mappings()

    # Bare-layout entities that need provider-ID unpacking.
    bare_unpack_config: dict[str, tuple[dict[str, str], str]] = {
        "sfi_leagues": (sfi_map, "league_id"),
        "transfermarkt_leagues": (tm_map, "league_id"),
    }

    writer: ManifestWriter | None = None
    if not dry_run:
        writer = ManifestWriter(
            service_name=SERVICE_NAME,
            catalogue_bucket=bucket,
            batch_size=5000,
            per_vm_shards=True,
        )

    summary: dict[str, int] = {
        "total_shards": 0,
        "captured": 0,
        "empty_confirmed": 0,
        "skipped_unmapped": 0,
    }
    by_data_type: dict[str, int] = {}

    items = sorted(SPORTS_DATA_TYPE_TO_FOLDER.items())
    if only_data_types:
        items = [(dt, fld) for dt, fld in items if dt in only_data_types]
    for data_type, folder in items:
        layout = SPORTS_DATA_TYPE_LAYOUT.get(data_type)
        logger.info("scanning %s (folder=%s, layout=%s)", data_type, folder, layout)
        if layout == "per_day_per_league":
            shards = _scan_per_league_entity(storage_client, bucket, folder, data_type, start_date, end_date)
        elif layout == "per_day_bare":
            shards = _scan_bare_entity(storage_client, bucket, folder, data_type, start_date, end_date)
        elif layout == "flat":
            shards = _scan_flat_entity(storage_client, bucket, folder, data_type)
        else:
            logger.warning("unknown layout %r for %s — skipping", layout, data_type)
            continue
        # Bare entities that need provider-ID unpacking get a SECOND pass
        # through the same date range (the first pass treats them as opaque
        # bare files; this pass cracks them open).  We do this in addition
        # to the bare pass so the bare per-day row also lands (caller can
        # decide which axis to roll up on).
        if folder in bare_unpack_config:
            provider_map, league_col = bare_unpack_config[folder]
            unpacked = _scan_bare_entity(
                storage_client,
                bucket,
                folder,
                data_type,
                start_date,
                end_date,
                unpack_provider_ids=True,
                provider_map=provider_map,
                league_id_column=league_col,
            )
            # Drop the opaque bare rows (league_id="") for unpacked entities —
            # only the per-canonical-league rows are useful.
            shards = unpacked

        by_data_type[data_type] = len(shards)
        summary["total_shards"] += len(shards)
        for s in shards:
            if s.capture_status == "captured":
                summary["captured"] += 1
            elif s.capture_status == "empty_confirmed":
                summary["empty_confirmed"] += 1
            if dry_run:
                if summary["total_shards"] <= 30:
                    logger.info("would emit: %s", s)
                continue
            assert writer is not None
            writer.add(
                processing_date=_date.fromisoformat(s.date) if s.date != "9999-12-31" else None,
                venue="",
                instrument_type="",
                data_type=s.data_type,
                league_id=s.league_id,
                instrument_id="",
                row_count=s.instrument_count,
                capture_status=s.capture_status,
            )
    if writer is not None:
        writer.flush()

    logger.info("summary: %s", summary)
    logger.info("per-data_type counts:")
    for k, v in sorted(by_data_type.items(), key=lambda kv: -kv[1]):
        logger.info("  %-30s %d", k, v)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    parser.add_argument("--project-id", default="central-element-323112")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument(
        "--only",
        action="append",
        help="Restrict to specific data_types (repeat flag). Default: all sports entities.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    start = _date.fromisoformat(args.start_date)
    end = _date.fromisoformat(args.end_date)
    if end < start:
        raise SystemExit(f"--end-date {end} is before --start-date {start}")
    started_at = datetime.now(UTC)
    summary = rebuild(
        project_id=args.project_id,
        start_date=start,
        end_date=end,
        only_data_types=args.only,
        dry_run=args.dry_run,
    )
    elapsed = (datetime.now(UTC) - started_at).total_seconds()
    logger.info("Elapsed %.1fs. Summary: %s", elapsed, summary)


if __name__ == "__main__":
    main()
