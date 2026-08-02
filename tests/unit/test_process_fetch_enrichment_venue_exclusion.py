"""Regression test for sports_stats_delayed_live_capture_still_dead_post_fix_2026_07_29.

``active_venues`` for a sports run carries the 5 enrichment-provider pseudo-venues
(FOOTYSTATS/UNDERSTAT/TRANSFERMARKT/SOCCER_FOOTBALL_INFO/OPEN_METEO) purely so
downstream stage-7 enrichment / stage-4 zero-record handling can do membership
checks against it. They are never real URDI-fetchable venues (NO_ADAPTER_YET
sentinels by design) — passing them into ``_fetch_urdi_records``'s actual fetch
call produced a spurious "No URDI adapter"/"URDI fetch ... failed" log pair on
every non-enrichment-only, non-per-fixture sports dispatch. This asserts they
never reach ``fetch_instruments_for_all_venues``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.engine.orchestrator.process_fetch import _fetch_urdi_records
from instruments_service.engine.urdi_reference_provider import VenueFetchResult


@pytest.mark.asyncio
async def test_enrichment_provider_venues_excluded_from_urdi_fetch() -> None:
    active_venues = [
        "API_FOOTBALL",
        "FOOTYSTATS",
        "UNDERSTAT",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "OPEN_METEO",
    ]

    mock_fetch = AsyncMock(return_value=VenueFetchResult())
    with (
        patch("instruments_service.engine.orchestrator.fetch_instruments_for_all_venues", mock_fetch),
        patch("instruments_service.engine.orchestrator._DEFI_VENUES", []),
        patch("instruments_service.engine.orchestrator.SolanaCacheSession", MagicMock()),
    ):
        await _fetch_urdi_records(
            active_venues=active_venues,
            api_keys=None,
            date="2026-07-29",
            mode="batch",
            skip_urdi=False,
        )

    mock_fetch.assert_awaited_once()
    fetched_venues = mock_fetch.await_args.args[0]
    assert fetched_venues == ["API_FOOTBALL"]
    for enrichment_venue in (
        "FOOTYSTATS",
        "UNDERSTAT",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "OPEN_METEO",
    ):
        assert enrichment_venue not in fetched_venues


@pytest.mark.asyncio
async def test_skip_urdi_still_empties_venue_lists() -> None:
    """Unaffected control case: skip_urdi=True must still no-op the fetch entirely."""
    mock_fetch = AsyncMock(return_value=VenueFetchResult())
    with patch("instruments_service.engine.orchestrator.fetch_instruments_for_all_venues", mock_fetch):
        await _fetch_urdi_records(
            active_venues=["API_FOOTBALL", "UNDERSTAT"],
            api_keys=None,
            date="2026-07-29",
            mode="batch",
            skip_urdi=True,
        )

    mock_fetch.assert_not_awaited()
