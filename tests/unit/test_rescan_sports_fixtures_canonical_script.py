"""Unit tests for scripts/rescan_sports_fixtures_canonical.py helpers (coverage + regression)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def rescan_module():  # type: ignore[no-untyped-def]
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "scripts" / "rescan_sports_fixtures_canonical.py"
    spec = importlib.util.spec_from_file_location("rescan_sports_fixtures_canonical", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_chunked_date_ranges_delegates_to_utl(rescan_module) -> None:
    assert rescan_module.chunked_date_ranges("2024-01-01", "2024-01-03", 2) == [
        ("2024-01-01", "2024-01-02"),
        ("2024-01-03", "2024-01-03"),
    ]


def test_drop_instruments_fixtures_per_league(rescan_module) -> None:
    drop = rescan_module._drop_instruments_fixtures_per_league
    assert drop(pd.Series({"data_type": "FIXTURES", "service_name": "instruments-service", "league_id": "EPL"}))
    assert not drop(pd.Series({"data_type": "FIXTURES", "service_name": "instruments-service", "league_id": ""}))
    assert not drop(pd.Series({"data_type": "OTHER", "service_name": "instruments-service", "league_id": "EPL"}))


def test_parse_date_from_blob_path(rescan_module) -> None:
    assert (
        rescan_module._parse_date("sports_reference/by_date/day=2024-01-02/entity=fixtures/x.parquet") == "2024-01-02"
    )
    assert rescan_module._parse_date("bad/path") is None
