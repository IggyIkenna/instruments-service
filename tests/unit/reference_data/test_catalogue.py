"""Tests for instruments_service.reference_data.catalogue.CatalogueBuilder.

The builder delegates discovery to ``fetch_instruments_for_all_venues`` —
tests patch that single entry point with fake per-category records and
assert canonical ``instrument_key`` population, ``available_from/to`` logic,
and write-partition resolution through a mocked data sink.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.internal.reference.instrument import (
    InstrumentType,
    OptionType,
)

from instruments_service.engine.urdi_reference_provider import VenueFetchResult
from instruments_service.reference_data.catalogue import CatalogueBuilder

# ---------------------------------------------------------------------------
# Fixtures: fake records per category
# ---------------------------------------------------------------------------


def _cefi_records() -> list[InstrumentRecord]:
    return [
        InstrumentRecord(
            instrument_key="",
            venue="BINANCE-FUTURES",
            instrument_type=InstrumentType.PERPETUAL,
            raw_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
        ),
        InstrumentRecord(
            instrument_key="",
            venue="BINANCE-FUTURES",
            instrument_type=InstrumentType.PERPETUAL,
            raw_symbol="ETHUSDT",
            base_asset="ETH",
            quote_asset="USDT",
        ),
        InstrumentRecord(
            instrument_key="",
            venue="BINANCE-SPOT",
            instrument_type=InstrumentType.SPOT_PAIR,
            raw_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
        ),
    ]


def _tradfi_records() -> list[InstrumentRecord]:
    # 1 future + 1 option (representing an options_chain member)
    expiry = datetime(2026, 6, 20, tzinfo=UTC)
    return [
        InstrumentRecord(
            instrument_key="",
            venue="CME",
            instrument_type=InstrumentType.FUTURE,
            raw_symbol="ES",
            base_asset="ES",
            quote_asset="USD",
            expiry=expiry,
            available_from_datetime=datetime(2025, 3, 20, tzinfo=UTC),
        ),
        InstrumentRecord(
            instrument_key="",
            venue="CME",
            instrument_type=InstrumentType.OPTION,
            raw_symbol="ES",
            base_asset="ES",
            quote_asset="USD",
            expiry=expiry,
            strike=Decimal("5000"),
            option_type=OptionType.CALL,
            available_from_datetime=datetime(2025, 3, 20, tzinfo=UTC),
        ),
    ]


def _defi_records() -> list[InstrumentRecord]:
    listing = datetime(2022, 3, 1, tzinfo=UTC)
    return [
        InstrumentRecord(
            instrument_key="",
            venue="AAVEV3-ETHEREUM",
            instrument_type=InstrumentType.LENDING,
            raw_symbol="USDC",
            base_asset="USDC",
            quote_asset="USDC",
            available_from_datetime=listing,
        ),
        InstrumentRecord(
            instrument_key="",
            venue="UNISWAPV3-ETHEREUM",
            instrument_type=InstrumentType.POOL,
            raw_symbol="USDC-WETH-500",
            base_asset="USDC",
            quote_asset="WETH",
            available_from_datetime=listing,
        ),
    ]


def _fetch_stub_factory(mapping: dict[str, list[InstrumentRecord]]) -> AsyncMock:
    async def _fetch(
        venues: list[str],
        instrument_type: str | None = None,
        api_keys: dict[str, str] | None = None,
        date: str | None = None,
        mode: str = "batch",
    ) -> VenueFetchResult:
        del instrument_type, api_keys, date, mode
        # Use the first venue's first segment to decide the category bucket.
        if not venues:
            return VenueFetchResult(records=[])
        first = venues[0].upper()
        if "BINANCE" in first or "BYBIT" in first or "OKX" in first or "COINBASE" in first or "DERIBIT" in first:
            return VenueFetchResult(records=list(mapping.get("CEFI", [])))
        if first in {"CME", "NASDAQ", "NYSE", "CBOE", "ICE", "FX"}:
            return VenueFetchResult(records=list(mapping.get("TRADFI", [])))
        return VenueFetchResult(records=list(mapping.get("DEFI", [])))

    return AsyncMock(side_effect=_fetch)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_cefi_populates_canonical_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _fetch_stub_factory({"CEFI": _cefi_records()})
    monkeypatch.setattr(
        "instruments_service.reference_data.catalogue.catalogue_builder.fetch_instruments_for_all_venues",
        stub,
    )
    builder = CatalogueBuilder()
    records = builder.build_cefi()

    assert len(records) == 3
    assert all(rec.instrument_key for rec in records)
    assert records[0].instrument_key == "BINANCE-FUTURES:PERPETUAL:BTCUSDT"
    assert records[1].instrument_key == "BINANCE-FUTURES:PERPETUAL:ETHUSDT"
    assert records[2].instrument_key == "BINANCE-SPOT:SPOT_PAIR:BTCUSDT"


def test_build_tradfi_sets_available_from_and_to(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _fetch_stub_factory({"TRADFI": _tradfi_records()})
    monkeypatch.setattr(
        "instruments_service.reference_data.catalogue.catalogue_builder.fetch_instruments_for_all_venues",
        stub,
    )
    builder = CatalogueBuilder()
    records = builder.build_tradfi()

    assert len(records) == 2
    future, option = records
    assert future.instrument_key == "CME:FUTURE:ES-20260620"
    # available_to_datetime defaults to expiry for dated derivatives
    assert future.available_to_datetime == future.expiry
    assert future.available_from_datetime == datetime(2025, 3, 20, tzinfo=UTC)
    assert option.instrument_key == "CME:OPTION:ES-20260620-5000-C"
    assert option.available_to_datetime == option.expiry


def test_build_defi_preserves_venue_chain_split(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _fetch_stub_factory({"DEFI": _defi_records()})
    monkeypatch.setattr(
        "instruments_service.reference_data.catalogue.catalogue_builder.fetch_instruments_for_all_venues",
        stub,
    )
    builder = CatalogueBuilder()
    records = builder.build_defi()

    assert len(records) == 2
    keys = {rec.instrument_key for rec in records}
    assert "AAVEV3-ETHEREUM:LENDING:USDC" in keys
    assert "UNISWAPV3-ETHEREUM:POOL:USDC-WETH-500" in keys


def test_build_all_concatenates(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _fetch_stub_factory(
        {
            "CEFI": _cefi_records(),
            "TRADFI": _tradfi_records(),
            "DEFI": _defi_records(),
        }
    )
    monkeypatch.setattr(
        "instruments_service.reference_data.catalogue.catalogue_builder.fetch_instruments_for_all_venues",
        stub,
    )
    builder = CatalogueBuilder()
    all_records = builder.build_all()

    # 3 (CEFI) + 2 (TRADFI) + 2 (DEFI)
    assert len(all_records) == 7


def test_write_to_gcs_uses_resolved_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    bucket_seen: dict[str, str] = {}
    partition_seen: dict[str, object] = {}

    fake_sink = MagicMock()
    fake_sink.write.return_value = (
        "gs://instruments-store-cefi-test-project/reference_data/instruments/category=cefi/all.parquet"
    )

    def _fake_get_data_sink(**kwargs: object) -> MagicMock:
        bucket_seen["bucket"] = str(kwargs.get("bucket", ""))
        return fake_sink

    monkeypatch.setattr(
        "instruments_service.reference_data.catalogue.catalogue_builder.get_data_sink",
        _fake_get_data_sink,
    )

    builder = CatalogueBuilder(
        bucket_resolver=lambda category: f"instruments-store-{category.lower()}-test",
    )
    records = _cefi_records()
    # Populate canonical ids so the write represents a realistic catalogue.
    from instruments_service.reference_data.catalogue.catalogue_builder import _ensure_canonical_id

    canonical = [_ensure_canonical_id(r) for r in records]
    uri = builder.write_to_gcs(canonical, "CEFI")

    assert bucket_seen["bucket"] == "instruments-store-cefi-test"
    fake_sink.write.assert_called_once()
    kwargs = fake_sink.write.call_args.kwargs
    partition_seen.update(kwargs.get("partition") or {})
    assert kwargs.get("filename") == "all.parquet"
    assert kwargs.get("format") == "parquet"
    assert partition_seen["category"] == "cefi"
    assert "written_at" in partition_seen
    assert uri.startswith("gs://instruments-store-cefi-test-project")
