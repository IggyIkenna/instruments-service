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


def test_dep_check_finds_canonical_fixtures_schedule_post_cutover() -> None:
    """Regression: the 2026-07-14+ writer cutover has NO legacy dual-write — post-cutover
    dates carry ONLY entity=fixtures_schedule/entity=fixtures_outcomes, zero entity=fixtures
    objects. Before this fix, the pre-flight gate only probed entity=fixtures and would
    spuriously raise DependencyError for every date on/after the cutover, blocking all
    api-football-dependent venue adapters (footystats/understat/transfermarkt/
    soccer_football_info/open_meteo/betfair). The split fixtures_schedule prefix must
    satisfy the check on its own.
    """
    date = "2026-07-14"
    canonical_schedule_prefix = (
        f"sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures_schedule/"
    )
    per_league_obj = canonical_schedule_prefix + "league=EPL/fixtures_schedule.parquet"

    client = MagicMock()
    client.list_blobs.side_effect = lambda bucket, prefix: (
        [_blob(per_league_obj)] if prefix == canonical_schedule_prefix else []
    )

    with patch(
        "instruments_service.reference_data.sports_dependency.get_storage_client",
        return_value=client,
    ):
        # Must NOT raise — fixtures_schedule alone is an equivalent "api-football ran" marker.
        sports_dependency.check_api_football_dependency(date, bucket="instruments-store-sports-prd-x")


def test_dep_check_finds_legacy_fixtures_schedule_prefix() -> None:
    """Defensive: a bare (no pipeline_mode= segment) fixtures_schedule prefix also satisfies
    the check, mirroring the legacy-vs-canonical fallback already used for entity=fixtures."""
    date = "2026-07-14"
    legacy_schedule_prefix = f"sports_reference/by_date/day={date}/entity=fixtures_schedule/"
    per_league_obj = legacy_schedule_prefix + "league=EPL/fixtures_schedule.parquet"

    client = MagicMock()
    client.list_blobs.side_effect = lambda bucket, prefix: (
        [_blob(per_league_obj)] if prefix == legacy_schedule_prefix else []
    )

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
    """_resolve_sports_bucket delegates to resolve_bucket_name with canonical args.

    Non-test-run (default config) → ``deployment_env=None`` (resolve from the process
    env, no override). The call now carries the explicit ``deployment_env`` kwarg
    (the os.environ-mutation mechanism was replaced).
    """
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.is_test_run = False
    with (
        patch(
            "instruments_service.reference_data.sports_dependency.resolve_bucket_name",
            return_value="instruments-store-sports-prd-test-project",
        ) as mock_rbn,
        patch(
            "instruments_service.reference_data.sports_dependency.get_config",
            return_value=fake_cfg,
        ),
    ):
        out = sports_dependency._resolve_sports_bucket()
    assert "sports" in out.lower()
    mock_rbn.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env=None)


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


def test_resolve_sports_bucket_test_run_passes_deployment_env_test() -> None:
    """IS_TEST_RUN=True resolves the test tier via the explicit ``deployment_env="test"``
    kwarg — NOT by mutating the process ``DEPLOYMENT_ENV`` (the banned config-env write)."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.is_test_run = True

    captured: list[object] = []
    env_during: list[str | None] = []

    def spy_resolve(**kwargs: object) -> str:
        captured.append(kwargs.get("deployment_env"))
        env_during.append(os.environ.get("DEPLOYMENT_ENV"))
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
        env_before = os.environ.pop("DEPLOYMENT_ENV", None)
        try:
            out = sports_dependency._resolve_sports_bucket()
        finally:
            if env_before is not None:
                os.environ["DEPLOYMENT_ENV"] = env_before

    assert "sports" in out.lower()
    # The tier is forced via the kwarg, NOT via the process env.
    assert captured == ["test"]
    # The process env was NOT mutated to "test" during the call.
    assert env_during == [None]
    assert os.environ.get("DEPLOYMENT_ENV") == env_before


def test_resolve_sports_bucket_does_not_mutate_env_on_exception() -> None:
    """The process DEPLOYMENT_ENV is never mutated, even if resolve_bucket_name raises
    (the os.environ-mutation try/finally was removed — the tier is a pure kwarg)."""
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

    # env must be unchanged (None == absent, matching env_before)
    assert os.environ.get("DEPLOYMENT_ENV") == env_before
