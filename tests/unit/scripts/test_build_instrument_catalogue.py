"""Unit tests — build_instrument_catalogue.py roll-up + monotonic guard, and the
audit_instrument_definition_completeness.py provisional completeness summary.

Tests cover (no GCS — pure functions + module-by-path load, mirroring the v2 enumerator tests):
  - build_catalogue_dataframe lifecycle math: first/last day windows, available_to=None when
    present on the latest snapshot day, delisted instrument gets available_to stamped, metadata
    follows the most-recent snapshot, instrument_key/instrument_id id-column fallback, empty input.
  - evaluate_monotonic_guard accept (first run / growth / equal) + reject (shrink) + override.
  - The catalogue output is consumable by enumerate_expected_universe._catalog_from_dataframe
    (no schema drift).
  - summarise_completeness: status tabulation, attempted_failed gap surfacing, provisional verdict.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 1 + Phase 0 P0 tests).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rollup() -> ModuleType:
    return _load_script_module("build_instrument_catalogue.py", "_build_instrument_catalogue_test_module")


@pytest.fixture(scope="module")
def audit() -> ModuleType:
    return _load_script_module(
        "audit_instrument_definition_completeness.py",
        "_audit_instrument_definition_completeness_test_module",
    )


def _snapshot(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# build_catalogue_dataframe — lifecycle math
# ---------------------------------------------------------------------------


def test_rollup_first_last_day_window(rollup: ModuleType) -> None:
    """available_from = first day present; available_to = last day when not on latest day."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        (d2, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        # AAA absent on d3 → delisted; BBB present on the latest day → still active.
        (d3, _snapshot([{"instrument_key": "BBB", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}

    assert by_id["AAA"]["available_from"] == "2024-01-01"
    assert by_id["AAA"]["available_to"] == "2024-01-02"  # delisted before the latest day
    assert by_id["BBB"]["available_from"] == "2024-01-03"
    assert by_id["BBB"]["available_to"] is None  # present on the latest day → still active


def test_rollup_active_on_latest_day_has_null_available_to(rollup: ModuleType) -> None:
    """An instrument present on the latest snapshot day has available_to=None."""
    d1, d2 = date(2024, 5, 1), date(2024, 5, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "X", "venue": "V"}])),
        (d2, _snapshot([{"instrument_key": "X", "venue": "V"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["available_from"] == "2024-05-01"
    assert row["available_to"] is None


def test_rollup_metadata_follows_most_recent_snapshot(rollup: ModuleType) -> None:
    """Metadata (venue/type/chain) is taken from the instrument's most-recent definition."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d2, _snapshot([{"instrument_key": "P", "venue": "NEW_V", "instrument_type": "PERP", "chain": "ARBITRUM"}])),
        (d1, _snapshot([{"instrument_key": "P", "venue": "OLD_V", "instrument_type": "PERP", "chain": "ETHEREUM"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["venue"] == "NEW_V"
    assert row["chain"] == "ARBITRUM"


def test_rollup_supports_instrument_id_column(rollup: ModuleType) -> None:
    """The id column falls back to instrument_id when instrument_key is absent."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe([(d1, _snapshot([{"instrument_id": "ZZZ", "venue": "V"}]))])
    assert df.to_dict("records")[0]["instrument_id"] == "ZZZ"


def test_rollup_skips_blank_ids_and_empty_frames(rollup: ModuleType) -> None:
    """Rows with no usable id are skipped; empty frames contribute only their day to the axis."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "A", "venue": "V"}, {"instrument_key": "", "venue": "V"}])),
        (d2, pd.DataFrame()),  # empty frame — still advances the latest-day axis
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    assert list(df["instrument_id"]) == ["A"]
    # A last seen on d1 but latest day is d2 (empty) → A is delisted.
    assert df.to_dict("records")[0]["available_to"] == "2024-01-01"


def test_rollup_empty_input_returns_catalog_columns(rollup: ModuleType) -> None:
    df = rollup.build_catalogue_dataframe([])
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)
    assert df.empty


def test_rollup_output_consumable_by_enumerator(rollup: ModuleType) -> None:
    """The rolled-up catalogue feeds enumerate_expected_universe._catalog_from_dataframe (no drift)."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_for_catalogue_rollup_test")
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    df = rollup.build_catalogue_dataframe(
        [
            (d1, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
            (d2, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
        ]
    )
    entries = enumerator._catalog_from_dataframe(df)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.instrument_id == "BTC-USDT"
    assert entry.venue == "BINANCE"
    assert entry.available_from == "2024-01-01"
    assert entry.available_to is None  # active on the latest day


# ---------------------------------------------------------------------------
# evaluate_monotonic_guard
# ---------------------------------------------------------------------------


def test_guard_accepts_first_run(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(10, None, allow_shrink=False)
    assert decision.accept
    assert decision.reason == "no_prior_catalogue"


def test_guard_accepts_growth_and_equal(rollup: ModuleType) -> None:
    assert rollup.evaluate_monotonic_guard(11, 10, allow_shrink=False).accept
    assert rollup.evaluate_monotonic_guard(10, 10, allow_shrink=False).accept


def test_guard_rejects_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=False)
    assert not decision.accept
    assert decision.reason == "shrink_blocked"


def test_guard_override_allows_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=True)
    assert decision.accept
    assert decision.reason == "shrink_overridden"


# ---------------------------------------------------------------------------
# summarise_completeness (audit tool)
# ---------------------------------------------------------------------------


def test_completeness_complete_when_no_failed_cells(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-01", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "empty_confirmed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.attempted_failed == 0
    assert report.status_counts["captured"] == 1
    assert report.status_counts["empty_confirmed"] == 1


def test_completeness_surfaces_attempted_failed_gaps(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-02", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
            {"date": "2024-01-03", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert not report.is_complete
    assert report.attempted_failed == 2
    assert report.failed_by_venue["OKX"] == 2
    assert ("OKX", "2024-01-02", "INSTRUMENTS") in report.gap_sample


def test_completeness_blank_status_coerced_to_captured(audit: ModuleType) -> None:
    """Legacy blank/NaN capture_status mirrors the manifest read path (→ captured), not a gap."""
    index_df = pd.DataFrame([{"date": "2024-01-01", "venue": "V", "data_type": "INSTRUMENTS", "capture_status": None}])
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.status_counts["captured"] == 1


def test_completeness_empty_index(audit: ModuleType) -> None:
    report = audit.summarise_completeness(pd.DataFrame(), "cefi")
    assert report.total_rows == 0
    assert report.is_complete  # vacuously — no failed cells (provisional)
