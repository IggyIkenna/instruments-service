"""Regression test: sports enrichment-provider pseudo-venues never reach the URDI fetch.

Root cause (sports_stats_delayed_live_capture_still_dead_post_fix_2026_07_29.md item 2):
``get_venues_for_asset_groups(["SPORTS"])`` deliberately includes the enrichment-only
pseudo-venues (FOOTYSTATS/UNDERSTAT/TRANSFERMARKT/SOCCER_FOOTBALL_INFO/OPEN_METEO) in
``active_venues`` so stage 7 enrichment can check membership — but ``_fetch_urdi_records``
(stage 2) forwarded the full ``active_venues`` list straight to the generic
``fetch_instruments_for_all_venues`` URDI call for any non-enrichment-only,
non-per-fixture sports entity dispatch, producing the live-production "No URDI adapter
for N venue(s)" warning/error on every such run.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from instruments_service.engine.orchestrator.process_fetch import _fetch_urdi_records
from instruments_service.engine.orchestrator.process_preflight import _ENRICHMENT_PROVIDERS

_SPORTS_ACTIVE_VENUES = [
    "API_FOOTBALL",
    "FOOTYSTATS",
    "UNDERSTAT",
    "TRANSFERMARKT",
    "SOCCER_FOOTBALL_INFO",
    "OPEN_METEO",
]


@pytest.mark.asyncio
async def test_enrichment_provider_pseudo_venues_excluded_from_urdi_fetch() -> None:
    """A core-entity sports dispatch must not pass enrichment pseudo-venues to URDI."""
    mock_fetch = AsyncMock(return_value=type("_R", (), {"records": [], "retryable_venues": [], "failed_venues": []})())

    with (
        patch("instruments_service.engine.orchestrator._DEFI_VENUES", []),
        patch("instruments_service.engine.orchestrator.SolanaCacheSession"),
        patch("instruments_service.engine.orchestrator.fetch_instruments_for_all_venues", mock_fetch),
        patch("instruments_service.engine.orchestrator._get_manifest_high_watermarks", return_value={}),
    ):
        await _fetch_urdi_records(
            active_venues=_SPORTS_ACTIVE_VENUES,
            api_keys=None,
            date="2026-07-29",
            mode="live",
            source=None,
            skip_urdi=False,
        )

    assert mock_fetch.await_count == 1
    fetched_venues = mock_fetch.await_args.args[0]
    assert "API_FOOTBALL" in fetched_venues
    for enrichment_venue in _ENRICHMENT_PROVIDERS:
        assert enrichment_venue not in fetched_venues
