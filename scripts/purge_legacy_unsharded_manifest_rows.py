#!/usr/bin/env python3
"""One-time purge of legacy ``league_id=""`` manifest rows when per-league shards exist.

SSOT
----

* Plan: ``unified-trading-pm/plans/active/sports_manifest_shard_migration_cleanup_2026_04_21.plan.md``
  (Phase 3).
* UTL primitive: ``unified_trading_library.manifest_migrations.LegacyRowPurger``.

Problem
-------

Historically ``rescan_sports_fixtures_canonical.py`` and the orchestrator
itself emitted a date-level aggregate manifest row for many SPORTS entities
(WEATHER, XG, FIXTURES, etc.) — one row per ``(date, data_type)`` with
``league_id=""``. Once Phase-2 switched the orchestrator to per-league-only
emission and Phase-1 of this plan extended the rescan to emit per-league
shards for WEATHER + XG, the legacy unsharded rows are pure residue: the
deployment-api aggregator (``_sports_honest_coverage``) groups manifest rows
by ``league_id`` and silently discards the empty-league-id bucket.

This script removes, for each ``(date, data_type)`` combination, the
``league_id=""`` row when at least one sharded ``league_id != ""`` row
already exists for the same ``(date, data_type)`` + service. If no sharded
row exists, the legacy row is preserved — purging it would delete the last
trace of an attempt. In that case, the right follow-up is to re-run the
rescan for that entity (``--entity-type FIXTURES|WEATHER|XG ...``) and then
re-run this purge.

Usage
-----

::

    # Dry run — print how many rows would be removed per entity
    python scripts/purge_legacy_unsharded_manifest_rows.py --dry-run

    # Apply (default: all entities with per-league shards in the plan)
    python scripts/purge_legacy_unsharded_manifest_rows.py

    # Scoped to specific entities
    python scripts/purge_legacy_unsharded_manifest_rows.py --entity-type WEATHER XG

The write is a single atomic read-modify-write against
``_index/availability_index.parquet`` via
``LegacyRowPurger.apply()``.
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import LegacyRowPurger, get_storage_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "instruments-store-sports-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"

# Entity data_types that use per-league sharding in Phase-5 of the
# sports-data-source-coverage-matrix + Phases 1-2 of
# sports_manifest_shard_migration_cleanup_2026_04_21.plan.md.
#
# DEFAULT covers the entities whose per-league emission shipped at or before
# this plan (FIXTURES + WEATHER + XG). Additional entities (FIXTURE_STATS,
# FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS, INJURIES, STANDINGS) can be
# purged once their per-league rescan shards are emitted — pass them
# explicitly via ``--entity-type`` after the rescan completes.
_DEFAULT_ENTITY_TYPES: list[str] = ["FIXTURES", "WEATHER", "XG"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the row count that would be removed without writing",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=BUCKET_NAME,
        help="GCS bucket holding the availability index",
    )
    parser.add_argument(
        "--index-blob",
        type=str,
        default=INDEX_BLOB,
        help="Path of the canonical index parquet inside the bucket",
    )
    parser.add_argument(
        "--entity-type",
        nargs="+",
        type=str,
        default=_DEFAULT_ENTITY_TYPES,
        help=(f"One or more entity data_types to purge legacy rows for. Default: {' '.join(_DEFAULT_ENTITY_TYPES)}"),
    )

    args = parser.parse_args()

    dry_run: bool = bool(args.dry_run)
    bucket_name: str = str(args.bucket)
    index_blob: str = str(args.index_blob)
    entity_types: list[str] = [str(e).upper() for e in args.entity_type]  # pyright: ignore[reportAny]

    storage = get_storage_client()
    logger.info(
        "Purge target: gs://%s/%s entities=%s mode=%s",
        bucket_name,
        index_blob,
        ",".join(entity_types),
        "DRY-RUN" if dry_run else "APPLY",
    )

    purger = LegacyRowPurger(
        bucket_name,
        service_name="instruments-service",
        storage=storage,
        index_blob=index_blob,
    )

    if dry_run:
        rows = purger.dry_run(entity_types)
        logger.info("Would remove %d legacy unsharded rows:", len(rows))
        for r in rows[:50]:
            logger.info(
                "  drop: date=%s data_type=%s league_id=%r capture_status=%s",
                r.date,
                r.data_type,
                r.league_id,
                r.capture_status,
            )
        if len(rows) > 50:
            logger.info("  ... and %d more rows", len(rows) - 50)
        return

    removed = purger.apply(entity_types)
    logger.info("Purge complete — removed %d legacy unsharded rows", removed)


if __name__ == "__main__":
    main()
