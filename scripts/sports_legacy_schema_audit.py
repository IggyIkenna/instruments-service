#!/usr/bin/env python3
"""Phase 0 audit: classify every sports_reference fixtures.parquet as LEGACY or NEW schema.

Fast path: enumerates ``day=YYYY-MM-DD`` partitions via delimiter listing, then
for each day lists the IMMEDIATE entity sub-prefixes (also delimiter listing,
not recursive). Legacy days have only one ``entity=fixtures/`` partition under
the day; new days additionally have ``entity=fixture_stats/`` (the split-out
match-stats parquet that the new-schema writer added). This signal is cheap
to derive — one delimiter listing per day — and matches the schema-split
distinction we actually care about for Phase 3.

Output: ``instruments-service/scripts/sports_legacy_schema_audit.json`` keyed
by ``day=YYYY-MM-DD`` with the schema variant + the entity inventory. Phase 3
VM-shards consume this to know which days need the legacy-to-new mapper vs
pass-through copy.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``.

Usage::

    cd instruments-service
    .venv/bin/python scripts/sports_legacy_schema_audit.py

Run-time ~3-5 min depending on network. Idempotent — re-runs overwrite.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
PREFIX = "sports_reference/by_date/"

OUT_PATH = Path(__file__).parent / "sports_legacy_schema_audit.json"


@dataclass(frozen=True)
class DayAudit:
    day: str
    schema: str  # "NEW" | "LEGACY" | "MISSING"
    entities: tuple[str, ...] = field(default_factory=tuple)


def _list_day_partitions(client: storage.Client) -> list[str]:
    """Enumerate ``day=YYYY-MM-DD`` partitions under the bucket prefix.

    Uses delimiter listing (``ls -d`` style) — one paginated scan over
    ~3,600 dir-style sub-prefixes.
    """
    days: list[str] = []
    iterator = client.list_blobs(BUCKET, prefix=PREFIX, delimiter="/")
    for _page in iterator.pages:
        for sub_prefix in iterator.prefixes:
            tail = sub_prefix[len(PREFIX) :].rstrip("/")
            if tail.startswith("day="):
                days.append(tail[len("day=") :])
        iterator.prefixes = set()
    return sorted(set(days))


def _classify(client: storage.Client, day: str) -> DayAudit:
    """List entities under one day partition; classify by entity-set signature."""
    day_prefix = f"{PREFIX}day={day}/"
    iterator = client.list_blobs(BUCKET, prefix=day_prefix, delimiter="/")
    entities: list[str] = []
    for _page in iterator.pages:
        for sub_prefix in iterator.prefixes:
            tail = sub_prefix[len(day_prefix) :].rstrip("/")
            if tail.startswith("entity="):
                entities.append(tail[len("entity=") :])
        iterator.prefixes = set()
    entity_set = frozenset(entities)

    if "fixtures" not in entity_set:
        return DayAudit(day=day, schema="MISSING", entities=tuple(sorted(entity_set)))

    # Legacy days have only the pre-2024 entity set: fixtures + injuries +
    # leagues + standings + teams + understat_xg. They lack the post-2024
    # split-out fixture_stats / fixture_lineups / fixture_events / weather
    # / footystats_* / player_stats partitions.
    is_legacy = "fixture_stats" not in entity_set
    return DayAudit(
        day=day,
        schema="LEGACY" if is_legacy else "NEW",
        entities=tuple(sorted(entity_set)),
    )


def main() -> int:
    client = storage.Client()
    logger.info("enumerating day partitions under gs://%s/%s …", BUCKET, PREFIX)
    t0 = time.monotonic()
    days = _list_day_partitions(client)
    logger.info("found %d day partitions in %.1fs", len(days), time.monotonic() - t0)
    if not days:
        logger.error("no day partitions found — bailing")
        return 1

    audits: dict[str, DayAudit] = {}
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=64) as pool:
        futures = {pool.submit(_classify, client, d): d for d in days}
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            audits[day] = fut.result()
            if i % 500 == 0:
                logger.info("classified %d/%d in %.1fs", i, len(days), time.monotonic() - t0)
    logger.info("classified %d in %.1fs total", len(audits), time.monotonic() - t0)

    summary: dict[str, int] = {}
    for a in audits.values():
        summary[a.schema] = summary.get(a.schema, 0) + 1
    logger.info("summary: %s", json.dumps(summary, sort_keys=True))

    out = {
        "_meta": {
            "bucket": BUCKET,
            "prefix": PREFIX,
            "method": "entity-set signature (legacy = no entity=fixture_stats/)",
            "total_days": len(audits),
            "summary": summary,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "days": {day: asdict(audits[day]) for day in sorted(audits)},
    }
    OUT_PATH.write_text(json.dumps(out, indent=2, sort_keys=False))
    logger.info("wrote %s (%d bytes)", OUT_PATH, OUT_PATH.stat().st_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
