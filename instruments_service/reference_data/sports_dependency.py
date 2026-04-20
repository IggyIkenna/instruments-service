"""Sports adapter dependency pre-flight check.

api-football is the canonical source of fixture IDs + league definitions for
the sports reference-data pipeline. All other sports adapters
(footystats, SFI, Understat, transfermarkt, open_meteo) depend on
api-football having been fetched first for the target date — they read its
``sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet``
output to look up canonical fixture IDs.

Historically this dependency was silent: downstream adapters would read an
empty / missing fixtures file and produce zero rows with no clear signal to
the operator. This module makes the dependency explicit by raising
``DependencyError`` from UTL at the factory / orchestrator pre-flight level
with an actionable CLI remediation message.

The check is intentionally placed OUTSIDE the per-venue shard loop — the
whole point is to fail loud before any shard starts processing. Shard-level
failure isolation (see ``codex/04-architecture/shard-level-failure-isolation.md``)
applies to in-loop adapter calls, not to pre-flight dependency gates.

SSOT: ``unified-trading-pm/codex/02-data/sports-adapter-dependency-order.md``
"""

from __future__ import annotations

import logging

from unified_trading_library import DependencyError, get_bucket_name, get_storage_client, get_write_bucket_name

from instruments_service.config import get_config

logger = logging.getLogger(__name__)


# Every non-api-football sports venue depends on api-football's canonical
# fixtures being present. Lowercase factory keys are mapped here.
_API_FOOTBALL_DEPENDENT_VENUES: frozenset[str] = frozenset(
    {
        "footystats",
        "understat",
        "transfermarkt",
        "soccer_football_info",
        "soccerfootball_info",
        "open_meteo",
        "betfair",
    }
)

_FIXTURES_PATH_TEMPLATE: str = "sports_reference/by_date/day={date}/entity=api_football/api_football.parquet"
_CANONICAL_FIXTURES_PATH_TEMPLATE: str = "sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"


def _resolve_sports_bucket() -> str:
    """Resolve the sports-reference bucket honouring ``IS_TEST_RUN``.

    Mirrors ``instruments_service.engine.orchestrator._get_instruments_bucket``
    for the SPORTS category so the pre-flight check reads from the same bucket
    the orchestrator writes to. TEST-mode uses the canonical ``-test-`` in
    middle (inserted between category and project_id) — SSOT:
    ``codex/02-data/per-category-bucket-layouts.md``. Delegates to UTL
    ``get_write_bucket_name``.
    """
    cfg = get_config()
    project = cfg.gcp_project_id or "test-project"
    try:
        return get_write_bucket_name("instruments", "SPORTS", project)
    except (ImportError, AttributeError):
        prefix = cfg.instruments_bucket_prefix
        prod_bucket = f"{prefix}-sports-{project}"
        if not cfg.is_test_run:
            return prod_bucket
        return prod_bucket.replace(f"-{project}", f"-test-{project}", 1)


def _blob_exists(bucket: str, path: str) -> bool:
    """Return True if the given GCS path exists.

    Returns False on any client / transport error — the caller surfaces a
    clear ``DependencyError`` rather than leaking the underlying exception.
    The mock storage backend (``CLOUD_MOCK_MODE=true``) returns a client that
    already gracefully handles missing blobs.
    """
    try:
        client = get_storage_client()
        blob = client.bucket(bucket).blob(path)
        return bool(blob.exists())
    except Exception as exc:
        logger.warning(
            "sports dep-check: storage probe failed bucket=%s path=%s: %s",
            bucket,
            path,
            exc,
        )
        return False


def _build_remediation_message(date: str, bucket: str, path: str) -> str:
    """Build the actionable error message shown to operators."""
    return (
        f"api-football reference data missing for date {date} in {bucket}\n"
        f"(expected gs://{bucket}/{path}).\n"
        f"Run this first:\n"
        f"  python -m instruments_service --operation instruments --mode batch \\\n"
        f"    --category SPORTS --sports-provider API_FOOTBALL \\\n"
        f"    --start-date {date} --end-date {date}"
    )


def check_api_football_dependency(date: str, bucket: str | None = None) -> None:
    """Raise ``DependencyError`` if api-football data is missing for the date.

    The fixtures parquet at
    ``sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet``
    is the downstream-observable artefact that enrichment adapters read to
    join on canonical fixture IDs. The per-entity parquet
    ``entity=api_football/api_football.parquet`` is accepted as an equivalent
    marker (some fetch paths land the raw api-football payload at that
    location before canonicalisation).

    Args:
        date: Target shard date (YYYY-MM-DD).
        bucket: Sports-reference bucket name. If None, resolved from config
            honouring ``IS_TEST_RUN``.

    Raises:
        DependencyError: If neither the canonical fixtures nor the raw
            api-football entity parquet exists for the date.
    """
    resolved_bucket = bucket or _resolve_sports_bucket()

    canonical_path = _CANONICAL_FIXTURES_PATH_TEMPLATE.format(date=date)
    raw_path = _FIXTURES_PATH_TEMPLATE.format(date=date)

    if _blob_exists(resolved_bucket, canonical_path):
        logger.debug(
            "sports dep-check OK: canonical fixtures present at gs://%s/%s",
            resolved_bucket,
            canonical_path,
        )
        return
    if _blob_exists(resolved_bucket, raw_path):
        logger.debug(
            "sports dep-check OK: raw api-football parquet present at gs://%s/%s",
            resolved_bucket,
            raw_path,
        )
        return

    message = _build_remediation_message(date, resolved_bucket, canonical_path)
    raise DependencyError(message)


def venue_requires_api_football(venue: str) -> bool:
    """Return True if the given sports venue depends on api-football reference data."""
    return venue.lower() in _API_FOOTBALL_DEPENDENT_VENUES
