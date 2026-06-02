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

Also provides :func:`check_sports_manifest_v9_columns` — a gated regression
guard that verifies the upstream sports manifest ``_index`` carries the
canonical v9 schema columns (``asset_group``, ``source``, ``pipeline_mode``,
``available_at``). This guard is safe to land pre-migration because it is
only enforced when ``SPORTS_V9_ENFORCED=true`` (via
:class:`~instruments_service.config.InstrumentsServiceConfig`) OR when the
manifest's own ``schema_version`` column reads 9. Pre-cutover (v8 data, flag
off) it logs a one-line WARNING and passes.

SSOT: ``unified-trading-pm/codex/02-data/sports-adapter-dependency-order.md``
       ``unified-trading-pm/plans/active/sports_manifest_canonicalisation_2026_06_01.md`` §P2
"""

from __future__ import annotations

import logging
import os
from typing import cast

import pandas as pd
from unified_trading_library import DependencyError, get_storage_client, resolve_bucket_name

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
    """Resolve the sports instruments-store bucket honouring ``IS_TEST_RUN``.

    Mirrors ``instruments_service.engine.orchestrator._get_instruments_bucket``
    exactly so the pre-flight check reads from the same env-tiered bucket the
    orchestrator WRITES to. Delegates to UTL ``resolve_bucket_name`` backed by
    ``deployment-service/configs/cloud-providers.yaml`` — the canonical SSOT.

    - DEPLOYMENT_ENV=prod  → ``instruments-store-sports-prd-{pid}``
    - DEPLOYMENT_ENV=dev   → ``instruments-store-sports-dev-{pid}``
    - IS_TEST_RUN=true     → DEPLOYMENT_ENV=test → ``instruments-store-sports-test-{pid}``
    """
    cfg = get_config()
    if cfg.is_test_run:
        prev = os.environ.get("DEPLOYMENT_ENV")
        os.environ["DEPLOYMENT_ENV"] = "test"
        try:
            return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
        finally:
            if prev is None:
                os.environ.pop("DEPLOYMENT_ENV", None)
            else:
                os.environ["DEPLOYMENT_ENV"] = prev
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


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
        f"api-football reference data missing for date {date} in {bucket}\n"  # noqa: gs-uri — multi-line f-string concat; gs:// is on line 101
        f"(expected gs://{bucket}/{path}).\n"
        f"Run this first:\n"
        f"  python -m instruments_service --operation instruments --mode batch \\\n"
        f"    --asset-group SPORTS --sports-provider API_FOOTBALL \\\n"
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


# ---------------------------------------------------------------------------
# v9 schema-column regression guard (Task 2 — sports_manifest_canonicalisation
# §P2, 2026-06-02).  Gated so it is safe to land pre-migration.
# ---------------------------------------------------------------------------

_V9_REQUIRED_COLUMNS: frozenset[str] = frozenset({"asset_group", "source", "pipeline_mode", "available_at"})


def check_sports_manifest_v9_columns(manifest_df: pd.DataFrame) -> None:
    """Assert that the upstream sports manifest ``_index`` carries v9 columns.

    This is a **gated regression guard** — safe to land before the
    single-walk canonicalisation walk completes.  Enforcement logic:

    1. If ``SPORTS_V9_ENFORCED=true`` (via ``InstrumentsServiceConfig``),
       OR the manifest's ``schema_version`` column is present and equals 9,
       RAISE ``DependencyError`` when any of
       (``asset_group``, ``source``, ``pipeline_mode``, ``available_at``)
       are missing from the DataFrame columns.

    2. Otherwise (default — pre-migration, v8 data, flag off):
       LOG a one-line WARNING and PASS.  This keeps the write-path healthy
       until the walk promotes all rows to v9.

    Args:
        manifest_df: The sports ``_index`` DataFrame read at preflight.
            Must be a pandas DataFrame (empty is fine — column set is what
            matters, not row count).

    Raises:
        DependencyError: Only when enforcement is active (see above) and
            one or more required v9 columns are absent.
    """
    present_cols = set(manifest_df.columns)
    missing = _V9_REQUIRED_COLUMNS - present_cols

    if not missing:
        # All v9 columns present — always pass.
        return

    cfg = get_config()

    # Determine whether to enforce.  Two activation paths:
    # (a) explicit env flag  SPORTS_V9_ENFORCED=true
    # (b) the manifest itself already claims schema_version=9 but is missing cols
    #     (should never happen after a correct walk — defensive guard).
    schema_version_is_9 = False
    if "schema_version" in present_cols and not manifest_df.empty:
        try:
            sv_raw: object = cast(object, manifest_df["schema_version"].dropna().iloc[0])
            schema_version_is_9 = int(str(sv_raw)) == 9
        except (IndexError, TypeError, ValueError):
            pass

    enforce = cfg.sports_v9_enforced or schema_version_is_9

    if enforce:
        raise DependencyError(
            f"Sports manifest _index is missing v9 schema columns: {sorted(missing)}. "
            "Run the sports canonicalisation walk "
            "(sports_manifest_canonicalisation_2026_06_01.md §E1-E8) before enabling SPORTS_V9_ENFORCED."
        )

    logger.warning(
        "sports v9-column preflight: upstream manifest missing %s "
        "— enforcement off (SPORTS_V9_ENFORCED=false, schema_version≠9); "
        "set SPORTS_V9_ENFORCED=true post-canonicalisation walk to enforce.",
        sorted(missing),
    )
