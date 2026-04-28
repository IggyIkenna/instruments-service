#!/usr/bin/env python3
"""Phase 0 audit: classify every sports_reference fixtures.parquet as LEGACY or NEW schema.

Two-pass design:

* **Pass 1 (entity-set signature, fast)** enumerates ``day=YYYY-MM-DD``
  partitions via delimiter listing, then for each day lists the IMMEDIATE
  entity sub-prefixes. Days with ``entity=fixture_stats/`` are conclusively
  NEW; days without are LEGACY-candidates. ~2 min for 3,627 days.
* **Pass 2 (parquet schema probe, slower)** runs only on LEGACY-candidates.
  Reads the parquet footer (metadata only — no row data) to distinguish
  the actual schema:
    - has ``af_league_id`` column AND no ``league`` struct → ``ORPHAN_NEW``
      (fixtures parquet is new flat schema; just no fixture_stats partition
      yet — the live orchestrator post-Phase-0.5 produces this)
    - has ``league`` struct AND no ``af_league_id`` → ``LEGACY`` (true legacy
      nested-struct schema; needs the mapper in Phase 3)
    - other → ``MALFORMED`` (rare; investigate manually)

Output: ``instruments-service/scripts/sports_legacy_schema_audit.json`` keyed
by ``day=YYYY-MM-DD`` with the resolved schema variant. Phase 3 VM-shards
consume this to know which days need the legacy-to-new mapper vs
pass-through copy.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``.

Usage::

    cd instruments-service
    .venv/bin/python scripts/sports_legacy_schema_audit.py

Run-time ~3-7 min depending on network + how many LEGACY-candidates trigger
Pass 2. Idempotent — re-runs overwrite.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.types as pat
from google.api_core.exceptions import NotFound
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
PREFIX = "sports_reference/by_date/"

OUT_PATH = Path(__file__).parent / "sports_legacy_schema_audit.json"


@dataclass(frozen=True)
class DayAudit:
    day: str
    schema: str  # "NEW" | "LEGACY" | "ORPHAN_NEW" | "MISSING" | "MALFORMED"
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
    """Pass 1: classify by entity-set signature; Pass 2 promotes LEGACY_CANDIDATE."""
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

    # Days WITH ``entity=fixture_stats`` are conclusively NEW (the post-2024
    # split-out match-stats writer ran). Days WITHOUT need Pass 2 to
    # disambiguate ORPHAN_NEW (post-Phase-0.5 live writer; flat schema; no
    # stats yet) from LEGACY (pre-Phase-0.5 writer; nested struct schema).
    if "fixture_stats" in entity_set:
        return DayAudit(day=day, schema="NEW", entities=tuple(sorted(entity_set)))
    return DayAudit(day=day, schema="LEGACY_CANDIDATE", entities=tuple(sorted(entity_set)))


def _resolve_legacy_candidate(client: storage.Client, day: str) -> str:
    """Pass 2: probe parquet header. Returns LEGACY / ORPHAN_NEW / MALFORMED.

    Uses ``Blob.download_as_bytes()`` (better pooling than pyarrow's
    ``gs://`` URI which hangs at scale) then parses metadata via
    ``pq.read_metadata`` on a local BytesIO. Most fixtures parquets are
    < 100KB so this is cheap.
    """
    import io as _io

    blob_name = f"{PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    try:
        blob = client.bucket(BUCKET).blob(blob_name)
        raw = blob.download_as_bytes()
    except (NotFound, FileNotFoundError):
        return "MALFORMED"
    except (OSError, RuntimeError, ValueError):
        return "MALFORMED"

    try:
        meta = pq.read_metadata(_io.BytesIO(raw))
        schema = meta.schema.to_arrow_schema()
    except (OSError, RuntimeError, ValueError):
        return "MALFORMED"
    cols = frozenset(schema.names)
    has_af = "af_league_id" in cols
    has_legacy_struct = "league" in cols and any(f.name == "league" and pat.is_struct(f.type) for f in schema)
    if has_af and not has_legacy_struct:
        return "ORPHAN_NEW"
    if has_legacy_struct and not has_af:
        return "LEGACY"
    return "MALFORMED"


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
                logger.info("Pass 1 (entity-set): %d/%d in %.1fs", i, len(days), time.monotonic() - t0)
    logger.info("Pass 1 done: %d in %.1fs total", len(audits), time.monotonic() - t0)

    # Pass 2: probe parquet header for any LEGACY_CANDIDATE day to distinguish
    # ORPHAN_NEW (post-Phase-0.5 flat schema) from true LEGACY (nested struct).
    candidates = [d for d, a in audits.items() if a.schema == "LEGACY_CANDIDATE"]
    if candidates:
        logger.info("Pass 2 (parquet probe) on %d LEGACY candidates", len(candidates))
        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=32) as pool:
            futures2 = {pool.submit(_resolve_legacy_candidate, client, d): d for d in candidates}
            for i, fut in enumerate(as_completed(futures2), 1):
                day = futures2[fut]
                resolved = fut.result()
                audits[day] = DayAudit(day=day, schema=resolved, entities=audits[day].entities)
                if i % 100 == 0:
                    logger.info("Pass 2: %d/%d in %.1fs", i, len(candidates), time.monotonic() - t0)
        logger.info("Pass 2 done: %d in %.1fs", len(candidates), time.monotonic() - t0)

    summary: dict[str, int] = {}
    for a in audits.values():
        summary[a.schema] = summary.get(a.schema, 0) + 1
    logger.info("summary: %s", json.dumps(summary, sort_keys=True))

    out = {
        "_meta": {
            "bucket": BUCKET,
            "prefix": PREFIX,
            "method": "Pass 1: entity-set signature; Pass 2: parquet header probe on LEGACY candidates",
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
