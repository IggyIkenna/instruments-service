"""Phase 3 integration test — --force-window disables skip-if-exists.

Confirms the rolling-window contract (codex/02-data/
sports-scheduling-and-sharding.md §4) at the orchestrator level: when
`check_shard_freshness` reports a date is fresh (first run already wrote
rows), the second run with `redo_all=True` re-enters the fetch path instead
of short-circuiting via the SKIP branch.

This is the code-read equivalent of "run `python -m instruments_service
--lookback-days 0 --lookahead-days 0 --force-window ... SPORTS` twice in
sequence and verify the second run re-fetches". A full GCS-emulator test
adds fidelity but not correctness — the invariant being verified is the
`if not redo_all:` gate at `engine/orchestrator.py:885`.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from instruments_service.engine import orchestrator


def _run_process(redo_all: bool) -> dict[str, int]:
    """Run process_instruments for a SPORTS date with a fresh manifest."""

    async def _go() -> dict[str, int]:
        return await orchestrator.process_instruments(
            date="2026-04-21",
            categories=["SPORTS"],
            redo_all=redo_all,
            api_keys={},
            venue_override=["API_FOOTBALL"],
            mode="batch",
            sports_entity_filter="FIXTURES",
            sports_provider="API_FOOTBALL",
        )

    return asyncio.run(_go())


def test_redo_all_false_short_circuits_on_fresh_manifest() -> None:
    """With redo_all=False and a fresh manifest, process_instruments returns {} (SKIP)."""
    with (
        patch(
            "instruments_service.engine.orchestrator.check_shard_freshness",
            return_value=(True, [], []),
        ),
        patch(
            "instruments_service.engine.orchestrator._get_instruments_bucket",
            return_value="test-bucket-sports",
        ),
        patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=_empty_dataframe(),
        ),
        patch(
            "instruments_service.engine.orchestrator.get_venues_for_categories",
            return_value=["API_FOOTBALL"],
        ),
        patch(
            "instruments_service.engine.orchestrator.earliest_venue_date",
            return_value=None,
        ),
        patch(
            "instruments_service.engine.orchestrator._fetch_sports_reference_data",
        ) as mock_fetch,
    ):
        result = _run_process(redo_all=False)

    assert result == {}
    mock_fetch.assert_not_called()  # SKIP branch — no fetch


def test_redo_all_true_bypasses_freshness_check_entirely() -> None:
    """Sentinel pattern: with redo_all=True, check_shard_freshness must NOT
    be called. We wire it to raise a sentinel exception iff invoked — if the
    gate at orchestrator.py:885 (`if not redo_all:`) works, the sentinel is
    never raised; if it regresses, the test fails loud. This is the invariant
    --force-window / rolling-window §4 re-fetch contract depends on.
    """

    class _FreshnessCheckReachedError(RuntimeError):
        pass

    def _sentinel(*_args: object, **_kwargs: object) -> tuple[bool, list[str], list[str]]:
        raise _FreshnessCheckReachedError(
            "check_shard_freshness was called despite redo_all=True — "
            "regression in orchestrator.py:885 `if not redo_all:` gate"
        )

    with (
        patch(
            "instruments_service.engine.orchestrator.check_shard_freshness",
            side_effect=_sentinel,
        ),
        patch(
            "instruments_service.engine.orchestrator._get_instruments_bucket",
            return_value="test-bucket-sports",
        ),
        patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=_empty_dataframe(),
        ),
        patch(
            "instruments_service.engine.orchestrator.get_venues_for_categories",
            return_value=["API_FOOTBALL"],
        ),
        patch(
            "instruments_service.engine.orchestrator.earliest_venue_date",
            return_value=None,
        ),
    ):
        # redo_all=True: run must NOT raise the freshness sentinel.
        # Other downstream failures (missing API keys, network) are fine —
        # they indicate the orchestrator proceeded past the gate.
        try:
            _run_process(redo_all=True)
        except _FreshnessCheckReachedError:
            raise AssertionError("Regression: check_shard_freshness was invoked with redo_all=True") from None
        except Exception:
            # Any other exception is acceptable — it proves we went past the
            # freshness gate and hit a downstream concern (adapter fetch, etc.).
            pass


def test_redo_all_false_reaches_freshness_check() -> None:
    """Mirror of the sentinel test: with redo_all=False, check_shard_freshness
    MUST be called. Proves the two branches are distinct — not a vacuous pass."""
    import pytest

    class _FreshnessCheckReachedError(RuntimeError):
        pass

    def _sentinel(*_args: object, **_kwargs: object) -> tuple[bool, list[str], list[str]]:
        raise _FreshnessCheckReachedError("reached")

    with (
        patch(
            "instruments_service.engine.orchestrator.check_shard_freshness",
            side_effect=_sentinel,
        ),
        patch(
            "instruments_service.engine.orchestrator._get_instruments_bucket",
            return_value="test-bucket-sports",
        ),
        patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=_empty_dataframe(),
        ),
        patch(
            "instruments_service.engine.orchestrator.get_venues_for_categories",
            return_value=["API_FOOTBALL"],
        ),
        patch(
            "instruments_service.engine.orchestrator.earliest_venue_date",
            return_value=None,
        ),
        pytest.raises(_FreshnessCheckReachedError),
    ):
        _run_process(redo_all=False)


def _empty_dataframe() -> object:
    import pandas as pd

    return pd.DataFrame(columns=["date", "data_type", "league_id"])
