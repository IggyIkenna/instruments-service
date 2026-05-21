"""Live-mode trigger handlers for instruments-service.

Each module in this package implements one named trigger from the closed
per-asset-group taxonomy declared in
``plans/epics/instruments_master.md`` Phase A.6 (UAC SSOT
landing follow-up). Triggers are dispatched by name from the
``InstrumentsHandler`` when ``--mode live --trigger <name>`` is invoked.

Conventions every trigger handler follows:

* Same UAC sports GCS path SSOT as batch (``candidate_parquet_paths`` /
  ``_write_fixtures_per_league``) — live ≠ different paths.
* Same ``ManifestWriter.record_captured`` row_key shape — the manifest
  cleanly supersedes prior batch / ``expected_unattempted`` rows by
  ``row_key``.
* ``available_at`` stamped per-row, write-time, equal to the
  live-pipeline-arrival timestamp per UAC
  ``AVAILABILITY_AT_SEMANTICS`` — fixtures stamp
  ``announced_at`` per CLAUDE.md "fixtures → announced_at".
* Idempotent upsert — re-running the same trigger on the same day +
  same league produces no duplicate rows on disk (sink overwrites the
  per-(day, league) partition) and no duplicate manifest rows (writer
  CAS dedupes by ``row_key``).

References:

* CLAUDE.md "Live = batch — same data, same fields, same timing
  semantics, different sources OK".
* ``plans/epics/instruments_master.md`` Phase B.1
  (sports daily fixture re-poll).
"""

from __future__ import annotations

from instruments_service.triggers.sports_fixtures_daily_repoll import (
    SPORTS_FIXTURES_DAILY_REPOLL_TRIGGER,
    run_sports_fixtures_daily_repoll,
)

__all__ = [
    "SPORTS_FIXTURES_DAILY_REPOLL_TRIGGER",
    "run_sports_fixtures_daily_repoll",
]
