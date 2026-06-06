"""Bucket-resolution coverage for sports_dependency._resolve_sports_bucket.

Parallels `engine.orchestrator._get_instruments_bucket` tests but for the
pre-flight dependency check. Exercises: resolve_bucket_name happy path and
IS_TEST_RUN env-override logic (matching the orchestrator pattern exactly).
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from unified_trading_library import DependencyError

from instruments_service.reference_data import sports_dependency


def _blob(name: str) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    return blob


def test_dep_check_finds_canonical_per_league_pipeline_mode_fixtures() -> None:
    """Regression: per-league fixtures under pipeline_mode=batch_api_football/ satisfy the check.

    The IS writer + v9 migration write FIXTURES per-league at
    ``…/day={D}/pipeline_mode=batch_api_football/entity=fixtures/league={L}/fixtures.parquet``.
    A bare-blob exact probe at ``entity=fixtures/fixtures.parquet`` missed this layout → the
    dependency check would spuriously raise post-migration. The prefix probe must find it.
    """
    date = "2026-04-14"
    canonical_prefix = f"sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures/"
    per_league_obj = canonical_prefix + "league=EPL/fixtures.parquet"

    client = MagicMock()
    client.list_blobs.side_effect = lambda bucket, prefix: [_blob(per_league_obj)] if prefix == canonical_prefix else []

    with patch(
        "instruments_service.reference_data.sports_dependency.get_storage_client",
        return_value=client,
    ):
        # Must NOT raise — canonical per-league pipeline_mode fixtures present.
        sports_dependency.check_api_football_dependency(date, bucket="instruments-store-sports-prd-x")


def test_dep_check_finds_legacy_per_league_fixtures() -> None:
    """Pre-migration per-league fixtures (no pipeline_mode=) also satisfy the check."""
    date = "2026-04-14"
    legacy_prefix = f"sports_reference/by_date/day={date}/entity=fixtures/"
    per_league_obj = legacy_prefix + "league=EPL/fixtures.parquet"

    client = MagicMock()
    client.list_blobs.side_effect = lambda bucket, prefix: [_blob(per_league_obj)] if prefix == legacy_prefix else []

    with patch(
        "instruments_service.reference_data.sports_dependency.get_storage_client",
        return_value=client,
    ):
        sports_dependency.check_api_football_dependency(date, bucket="instruments-store-sports-prd-x")


def test_dep_check_raises_when_no_fixtures_anywhere() -> None:
    """No fixtures under any prefix or exact blob → DependencyError (unchanged loud-fail)."""
    date = "2026-04-14"
    client = MagicMock()
    client.list_blobs.return_value = []
    client.bucket.return_value.blob.return_value.exists.return_value = False

    with (
        patch(
            "instruments_service.reference_data.sports_dependency.get_storage_client",
            return_value=client,
        ),
        pytest.raises(DependencyError),
    ):
        sports_dependency.check_api_football_dependency(date, bucket="instruments-store-sports-prd-x")


def test_resolve_sports_bucket_uses_resolve_bucket_name() -> None:
    """_resolve_sports_bucket delegates to resolve_bucket_name with canonical args."""
    with patch(
        "instruments_service.reference_data.sports_dependency.resolve_bucket_name",
        return_value="instruments-store-sports-prd-test-project",
    ) as mock_rbn:
        out = sports_dependency._resolve_sports_bucket()
    assert "sports" in out.lower()
    mock_rbn.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="sports")


def test_resolve_sports_bucket_prod_no_env_override() -> None:
    """Non-test-run path calls resolve_bucket_name without altering DEPLOYMENT_ENV."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.is_test_run = False

    captured_env: list[str | None] = []

    def spy_resolve(**kwargs: object) -> str:
        captured_env.append(os.environ.get("DEPLOYMENT_ENV"))
        return "instruments-store-sports-prd-my-project"

    with (
        patch(
            "instruments_service.reference_data.sports_dependency.resolve_bucket_name",
            side_effect=spy_resolve,
        ),
        patch(
            "instruments_service.reference_data.sports_dependency.get_config",
            return_value=fake_cfg,
        ),
    ):
        original_env = os.environ.get("DEPLOYMENT_ENV")
        out = sports_dependency._resolve_sports_bucket()

    assert "sports" in out.lower()
    # DEPLOYMENT_ENV must not have been forced to "test"
    assert captured_env[0] != "test" or original_env == "test"
    # env is not mutated post-call
    assert os.environ.get("DEPLOYMENT_ENV") == original_env


def test_resolve_sports_bucket_test_run_sets_env() -> None:
    """IS_TEST_RUN=True temporarily sets DEPLOYMENT_ENV=test for resolve_bucket_name."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.is_test_run = True

    captured_env: list[str | None] = []

    def spy_resolve(**kwargs: object) -> str:
        captured_env.append(os.environ.get("DEPLOYMENT_ENV"))
        return "instruments-store-sports-test-my-project"

    with (
        patch(
            "instruments_service.reference_data.sports_dependency.resolve_bucket_name",
            side_effect=spy_resolve,
        ),
        patch(
            "instruments_service.reference_data.sports_dependency.get_config",
            return_value=fake_cfg,
        ),
    ):
        # Ensure DEPLOYMENT_ENV is not "test" before the call so we can detect the override.
        env_before = os.environ.pop("DEPLOYMENT_ENV", None)
        try:
            out = sports_dependency._resolve_sports_bucket()
        finally:
            if env_before is not None:
                os.environ["DEPLOYMENT_ENV"] = env_before

    assert "sports" in out.lower()
    # resolve_bucket_name was called while DEPLOYMENT_ENV == "test"
    assert captured_env == ["test"]
    # env is restored after the call
    assert os.environ.get("DEPLOYMENT_ENV") == env_before


def test_resolve_sports_bucket_restores_env_on_exception() -> None:
    """DEPLOYMENT_ENV is restored even if resolve_bucket_name raises."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.is_test_run = True

    env_before = os.environ.pop("DEPLOYMENT_ENV", None)
    try:
        with (
            patch(
                "instruments_service.reference_data.sports_dependency.resolve_bucket_name",
                side_effect=RuntimeError("boom"),
            ),
            patch(
                "instruments_service.reference_data.sports_dependency.get_config",
                return_value=fake_cfg,
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            sports_dependency._resolve_sports_bucket()
    finally:
        if env_before is not None:
            os.environ["DEPLOYMENT_ENV"] = env_before

    # env must be restored (None == absent, matching env_before)
    assert os.environ.get("DEPLOYMENT_ENV") == env_before
