"""Unit tests for live mode (imports, single-cycle with mocks).

Ensures live mode can be imported and run without hitting real APIs.
Quality gates include these tests - catch import/config regressions.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler


def test_live_mode_handler_imports_cleanly() -> None:
    """Verify LiveModeHandler can be imported (catches import chain errors)."""
    assert LiveModeHandler is not None


@pytest.mark.asyncio
async def test_live_mode_single_cycle_with_mocked_service() -> None:
    """Run single-cycle with mocked InstrumentsService - no real API calls."""
    config = {"project_id": "test-project"}
    handler = LiveModeHandler(config)

    # Mock generate_instruments_for_date to return success
    mock_df = MagicMock()
    mock_df.__len__ = lambda _: 5
    mock_result = {
        "success": True,
        "instruments_by_category": {
            "CEFI": mock_df,
        },
    }

    with (
        patch.object(
            handler.instruments_service,
            "generate_instruments_for_date",
            new_callable=AsyncMock,
            return_value=mock_result,
        ),
        patch("instruments_service.cli.handlers.live_mode_handler.setup_events"),
        patch("instruments_service.cli.handlers.live_mode_handler.log_event"),
        patch("instruments_service.cli.handlers.live_mode_handler.publish_coordination_event"),
        patch("instruments_service.cli.handlers.live_mode_handler.get_config") as mock_config,
    ):
        mock_config.return_value.get_bucket_for_category.return_value = "test-bucket"

        # Call _run_live_mode directly (async) - handler.run() wraps in sync entry point
        # which conflicts with pytest-asyncio's event loop
        result = await handler._run_live_mode(
            interval=15,
            categories=["CEFI"],
            venues=None,
            single_cycle=True,
        )

    assert result["status"] == "success"
    assert result.get("cycles") == 1
