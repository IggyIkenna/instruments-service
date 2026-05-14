"""Verify ServiceBootstrap is correctly wired in instruments-service CLI.

Tests exercise main_service_cli() with all infra mocked out, asserting that
ServiceBootstrap is constructed with the correct service_name and that run() is
called exactly once.  Failure propagation is also verified.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _run_cli(*, run_side_effect: Exception | None = None) -> MagicMock:
    """Invoke main_service_cli() with all external calls mocked.

    Returns the ServiceBootstrap class mock so callers can assert on it.
    """
    mock_bootstrap_cls = MagicMock()
    mock_instance = MagicMock()
    mock_bootstrap_cls.return_value = mock_instance
    if run_side_effect is not None:
        mock_instance.run.side_effect = run_side_effect
    else:
        mock_instance.run.return_value = None

    with (
        patch("instruments_service.cli.main.get_config", return_value=MagicMock()),
        patch(
            "instruments_service.cli.main.ServiceBootstrap",
            mock_bootstrap_cls,
        ),
    ):
        from instruments_service.cli.main import main_service_cli

        if run_side_effect is not None:
            with pytest.raises(type(run_side_effect)):
                main_service_cli()
        else:
            main_service_cli()

    return mock_bootstrap_cls


def test_service_bootstrap_constructed_with_correct_service_name() -> None:
    mock_cls = _run_cli()
    mock_cls.assert_called_once()
    assert mock_cls.call_args.kwargs.get("service_name") == "instruments-service"


def test_service_bootstrap_run_called_once() -> None:
    mock_cls = _run_cli()
    mock_cls.return_value.run.assert_called_once()


def test_service_bootstrap_run_failure_propagates() -> None:
    _run_cli(run_side_effect=RuntimeError("simulated failure"))
