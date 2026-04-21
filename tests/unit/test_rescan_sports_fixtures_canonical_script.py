"""Unit tests for scripts/rescan_sports_fixtures_canonical.py helpers (coverage + regression)."""

from __future__ import annotations

import importlib.util
import sys
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
    # Register in sys.modules BEFORE exec so @dataclass can resolve forward-refs
    # via ``cls.__module__`` (Python 3.13 dataclasses.py line 757 expects this).
    sys.modules[spec.name] = mod
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


def test_entity_handlers_registered(rescan_module) -> None:
    """FIXTURES + WEATHER + XG entity handlers are registered for --entity-type dispatch."""
    handlers = rescan_module._ENTITY_HANDLERS
    assert set(handlers.keys()) >= {"FIXTURES", "WEATHER", "XG"}
    assert handlers["FIXTURES"].blob_filename == "fixtures.parquet"
    assert handlers["WEATHER"].blob_filename == "weather.parquet"
    assert handlers["XG"].blob_filename == "understat_xg.parquet"


def test_build_drop_filter_scopes_by_data_type(rescan_module) -> None:
    weather_filter = rescan_module._build_drop_filter("WEATHER")
    fixtures_filter = rescan_module._build_drop_filter("FIXTURES")

    weather_row = pd.Series({"data_type": "WEATHER", "service_name": "instruments-service", "league_id": "EPL"})
    fixtures_row = pd.Series({"data_type": "FIXTURES", "service_name": "instruments-service", "league_id": "EPL"})

    # WEATHER filter drops only WEATHER rows
    assert weather_filter(weather_row)
    assert not weather_filter(fixtures_row)

    # FIXTURES filter drops only FIXTURES rows
    assert fixtures_filter(fixtures_row)
    assert not fixtures_filter(weather_row)

    # Empty league_id is never dropped (legacy unsharded row) — purger's job.
    empty_league = pd.Series({"data_type": "WEATHER", "service_name": "instruments-service", "league_id": ""})
    assert not weather_filter(empty_league)


def test_scan_xg_blob_groups_by_league_column(rescan_module, tmp_path, monkeypatch) -> None:
    """XG scanner groups by the 'league' column and emits per-league captured rows."""
    df = pd.DataFrame(
        {
            "league": ["EPL", "EPL", "LA_LIGA", "LA_LIGA", "LA_LIGA"],
            "home": ["Arsenal", "Chelsea", "Barca", "Real", "Atletico"],
        }
    )
    buf = tmp_path / "xg.parquet"
    df.to_parquet(buf)

    class _FakeStorage:
        def download_bytes(self, bucket: str, blob_path: str) -> bytes:
            return buf.read_bytes()

    blob_path = "sports_reference/by_date/day=2024-09-15/entity=understat_xg/understat_xg.parquet"
    entries = rescan_module._scan_xg_blob(_FakeStorage(), "bucket", blob_path)

    # Two leagues, each with ``captured`` status and correct counts
    entries_by_league = {e["league_id"]: e for e in entries}
    assert entries_by_league["EPL"]["data_type"] == "XG"
    assert entries_by_league["EPL"]["instrument_count"] == 2
    assert entries_by_league["EPL"]["capture_status"] == "captured"
    assert entries_by_league["LA_LIGA"]["instrument_count"] == 3
