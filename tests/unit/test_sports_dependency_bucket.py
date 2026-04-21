"""Bucket-resolution coverage for sports_dependency._resolve_sports_bucket.

Parallels `engine.orchestrator._get_instruments_bucket` tests but for the
pre-flight dependency check. Exercises: UTL happy path, ImportError fallback,
prod vs test-run branches.
"""

from __future__ import annotations

from unittest.mock import patch

from instruments_service.reference_data import sports_dependency


def test_resolve_sports_bucket_delegates_to_utl() -> None:
    with patch(
        "instruments_service.reference_data.sports_dependency.get_write_bucket_name",
        return_value="instruments-store-sports-test-project",
    ) as mock_gb:
        out = sports_dependency._resolve_sports_bucket()
    assert "sports" in out.lower()
    assert mock_gb.call_args.args[:2] == ("instruments", "SPORTS")


def test_resolve_sports_bucket_fallback_prod_branch() -> None:
    """ImportError/AttributeError from UTL falls back to prefix-based computation."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.gcp_project_id = "my-project"
    fake_cfg.is_test_run = False
    with (
        patch(
            "instruments_service.reference_data.sports_dependency.get_write_bucket_name",
            side_effect=AttributeError,
        ),
        patch(
            "instruments_service.reference_data.sports_dependency.get_config",
            return_value=fake_cfg,
        ),
    ):
        out = sports_dependency._resolve_sports_bucket()
    assert out.endswith("-my-project")
    assert "-test-" not in out  # prod branch


def test_resolve_sports_bucket_fallback_test_branch() -> None:
    """IS_TEST_RUN=true routes to the `-test-{pid}` sibling bucket via fallback."""
    import instruments_service.config.service_config as sc

    fake_cfg = sc.InstrumentsServiceConfig()
    fake_cfg.gcp_project_id = "my-project"
    fake_cfg.is_test_run = True
    with (
        patch(
            "instruments_service.reference_data.sports_dependency.get_write_bucket_name",
            side_effect=ImportError,
        ),
        patch(
            "instruments_service.reference_data.sports_dependency.get_config",
            return_value=fake_cfg,
        ),
    ):
        out = sports_dependency._resolve_sports_bucket()
    assert "-test-my-project" in out
