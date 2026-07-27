"""Verify ``writers.py::_write_venue`` inherits the shared UTL instrument_type casing canon.

``tradfi_casing_100pct_redrift_2026_07_27.md`` root-caused this repo's universe enumerator
(``engine/orchestrator/writers.py``'s ``_write_venue``) as one of the dominant sources of
lowercase tradfi ``instrument_type`` manifest rows (~39,500 measured live) — it stamps the raw
adapter-df token (e.g. ``future``/``equity``/``etf``) straight into ``manifest.record_captured()``
with no local casing logic of its own. The operator-ruled fix (``BLK-f3950c25``) centralizes the
casing canon at the shared UTL ``ManifestWriter`` write seam
(``unified_trading_library.canonical.canonicalize_manifest_instrument_type``) so every dependent
service — this one included — inherits it for free with no code change here. This test proves
that inheritance actually holds for THIS repo's real call shape (the exact kwargs
``_write_venue`` passes: ``asset_group="tradfi"``, ``data_type="instruments"``, a lowercase
``instrument_type`` token, ``pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE``) — a live
re-read of the buffered record after the real ``record_captured()`` call, not a re-test of UTL's
own canon-mapping logic (already covered by unified-trading-library's own
``test_manifest_instrument_type_casing_canon.py``).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pandas as pd
import pytest
import unified_trading_library.events as _events_module
from unified_api_contracts import PipelineMode
from unified_trading_library import ManifestWriter, MockEventSink, setup_events


@pytest.fixture
def mock_sink() -> Iterator[MockEventSink]:
    previous_writer = _events_module._writer
    previous_mode = _events_module._mode
    previous_service = _events_module._service_name
    sink = MockEventSink()
    setup_events(service_name="instruments-service", mode="batch", sink=sink)
    yield sink
    _events_module._writer = previous_writer
    _events_module._mode = previous_mode
    _events_module._service_name = previous_service


@pytest.fixture
def writer(mock_sink: MockEventSink) -> Iterator[ManifestWriter]:
    w = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket="test-bucket",
        sink=object(),
        batch_size=1000,
        strict_validation=False,
    )
    yield w


def _instruments_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["ES", "NQ"],
            "available_at": pd.to_datetime(["2026-07-27T00:00:00Z"] * 2, utc=True),
        }
    )


@pytest.mark.parametrize(
    ("raw_itype", "expected_upper"),
    [
        ("future", "FUTURE"),
        ("equity", "EQUITY"),
        ("etf", "ETF"),
        ("continuous_future", "FUTURE"),
    ],
)
def test_write_venue_record_captured_call_shape_uppercases_tradfi_itype(
    writer: ManifestWriter, raw_itype: str, expected_upper: str
) -> None:
    """Mirrors ``writers.py::_write_venue``'s real ``manifest.record_captured(...)`` call shape
    for a tradfi venue's per-instrument-type split — proves this repo's own call site inherits
    the UTL casing canon (no local fix needed here)."""
    df = _instruments_df()
    writer.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": "2026-07-27", "venue": "CME", "instrument_type": raw_itype},
        df=df,
        row_count=len(df),
        asset_group="tradfi",
        instrument_type=raw_itype,
        data_type="instruments",
        venue="CME",
        chain="",
        pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
        service_emission_state=None,
        attempted_at=datetime.now(UTC),
        validate=False,
    )
    assert writer._records[-1].instrument_type == expected_upper


def test_write_venue_record_captured_leaves_bundle_grain_lowercase(writer: ManifestWriter) -> None:
    """``futures_chain``/``options_chain`` stay lowercase — the permanent bundle-grain exclusion,
    unaffected by the casing canon."""
    df = _instruments_df()
    writer.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": "2026-07-27", "venue": "CME", "instrument_type": "futures_chain"},
        df=df,
        row_count=len(df),
        asset_group="tradfi",
        instrument_type="futures_chain",
        data_type="instruments",
        venue="CME",
        chain="",
        pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
        service_emission_state=None,
        attempted_at=datetime.now(UTC),
        validate=False,
    )
    assert writer._records[-1].instrument_type == "futures_chain"
