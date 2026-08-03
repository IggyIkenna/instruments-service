"""Unit tests for ``--operation=reprocess-shards`` (``_run_reprocess_shards``).

The generic replacement for the 13 near-identical one-off "load manifest ->
filter by predicate -> flip status -> write back" scripts audited in
``manifest_reprocessing_generic_utility_2026_07_07.md``. This CLI subcommand
wires ``unified_trading_library.select_shards_for_reprocess`` /
``reprocess_shards`` into instruments-service per script-homes.md's
production-verb rule.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
import pytest

from instruments_service.cli.main import _run_reprocess_shards


def _manifest_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "asset_group": "cefi",
                "venue": "ASTER",
                "capture_status": "attempted_failed",
                "error_reason": "TARDIS_HTTP_404",
                "date": "2025-01-01",
            },
            {
                "asset_group": "cefi",
                "venue": "ASTER",
                "capture_status": "attempted_failed",
                "error_reason": "TARDIS_HTTP_500",
                "date": "2025-01-02",
            },
            {
                "asset_group": "cefi",
                "venue": "BINANCE",
                "capture_status": "captured",
                "error_reason": "",
                "date": "2025-01-01",
            },
        ]
    )


def test_rejects_unknown_capture_status(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        _run_reprocess_shards(["--bucket", "some-bucket", "--capture-status", "not_a_real_status"])
    assert exc_info.value.code == 1
    err = json.loads(capsys.readouterr().err)
    assert "not a valid CaptureStatus" in err["error"]


def test_requires_bucket_or_asset_group(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        _run_reprocess_shards([])
    assert exc_info.value.code == 1
    err = json.loads(capsys.readouterr().err)
    assert "requires --bucket or --asset-group" in err["error"]


def test_dry_run_default_matches_and_reports_no_mutation(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("unified_trading_library.read_availability_index", return_value=_manifest_df()):
        _run_reprocess_shards(
            [
                "--bucket",
                "some-bucket",
                "--venue",
                "ASTER",
                "--error-reason-contains",
                "404",
            ]
        )
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True
    assert out["matched"] == 1
    assert out["flipped"] == 0
    assert out["bucket"] == "some-bucket"
    assert out["venue"] == "ASTER"
    assert out["error_reason_contains"] == "404"


def test_error_reason_contains_is_case_insensitive(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("unified_trading_library.read_availability_index", return_value=_manifest_df()):
        _run_reprocess_shards(["--bucket", "some-bucket", "--error-reason-contains", "http_404"])
    out = json.loads(capsys.readouterr().out)
    assert out["matched"] == 1


def test_apply_without_shard_isolation_env_aborts(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MANIFEST_PER_VM_SHARDS", raising=False)
    monkeypatch.delenv("VM_NAME", raising=False)
    with (
        patch("unified_trading_library.read_availability_index", return_value=_manifest_df()),
        pytest.raises(SystemExit) as exc_info,
    ):
        _run_reprocess_shards(["--bucket", "some-bucket", "--venue", "ASTER", "--apply"])
    assert exc_info.value.code == 1
    err = json.loads(capsys.readouterr().err)
    assert "MANIFEST_PER_VM_SHARDS" in err["error"]


def test_bucket_resolved_from_asset_group_when_no_explicit_bucket(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch("instruments_service.cli.main.get_write_bucket_name", return_value="resolved-bucket") as mock_get_bucket,
        patch("unified_trading_library.read_availability_index", return_value=_manifest_df()),
    ):
        _run_reprocess_shards(["--asset-group", "CEFI"])
    mock_get_bucket.assert_called_once_with("instruments", "cefi")
    out = json.loads(capsys.readouterr().out)
    assert out["bucket"] == "resolved-bucket"
