"""Unit tests — Spark adapter populates DeFi metadata fields on InstrumentRecord.

Verifies that the dedicated Spark reference-data adapter (Messari standardized
lending schema) builds InstrumentRecord entries with all the DeFi metadata
fields consumed by downstream components — pool_address, atoken_address,
debt_token_address, base_asset_contract_address, base_asset_decimals,
base_asset_symbol_onchain — using the same convention as aave_v3.py.

Spark differs from Aave V3 in two material ways:

* Field shape — Spark exposes ``markets {{ inputToken outputToken _vToken }}``
  not ``reserves {{ underlyingAsset aToken }}``.
* Risk-param scaling — Spark integers are percentages (``"83"`` = 83%)
  not Aave-V3's RAY-scaled basis points.

Tests are credential-free: they exercise the pure mapping helper
``_build_market_records`` directly with fixture dicts that mirror live
subgraph responses captured 2026-04-29.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.spark import (
    SparkReferenceDataAdapter,
)


def _mock_aiohttp_session_post(json_data: object) -> MagicMock:
    """Create a mock aiohttp session whose POST returns json_data."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_data)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


def _mock_aiohttp_session_error(exc: Exception) -> MagicMock:
    """Create a mock aiohttp session whose POST raises exc."""
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(side_effect=exc)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


# --- Fixtures (real on-chain shape, captured 2026-04-29) --------------------


def _wsteth_market_fixture() -> dict[str, object]:
    """A representative wstETH market on Spark Ethereum.

    Borrowable AND collateralisable (the canonical case).
    """
    return {
        "id": "0x12b54025c112aa61face2cdb7118740875a566e9",
        "name": "Spark wstETH",
        "isActive": True,
        "canBorrowFrom": True,
        "canUseAsCollateral": True,
        "maximumLTV": "83",
        "liquidationThreshold": "84",
        "liquidationPenalty": "7",
        "reserveFactor": "0.3",
        "createdTimestamp": "1678192187",
        "inputToken": {
            "id": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
            "symbol": "wstETH",
            "name": "Wrapped liquid staked Ether 2.0",
            "decimals": 18,
        },
        "outputToken": {
            "id": "0x12b54025c112aa61face2cdb7118740875a566e9",
            "symbol": "spwstETH",
            "name": "Spark wstETH",
            "decimals": 18,
        },
        "_vToken": {
            "id": "0xd5c3e3b566a42a6110513ac7670c1a86d76e13e6",
            "symbol": "variableDebtwstETH",
        },
    }


def _usdc_market_fixture() -> dict[str, object]:
    """A USDC market on Spark Ethereum — borrowable but not collateral."""
    return {
        "id": "0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815",
        "name": "Spark USDC",
        "isActive": True,
        "canBorrowFrom": True,
        "canUseAsCollateral": False,
        "maximumLTV": "0",
        "liquidationThreshold": "0",
        "liquidationPenalty": "0",
        "reserveFactor": "0.01",
        "createdTimestamp": "1678192187",
        "inputToken": {
            "id": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "symbol": "USDC",
            "name": "USD Coin",
            "decimals": 6,
        },
        "outputToken": {
            "id": "0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815",
            "symbol": "spUSDC",
            "name": "Spark USDC",
            "decimals": 6,
        },
        "_vToken": {
            "id": "0x7b70d04099cb9cfb1db7b6820badafb4c5c70a67",
            "symbol": "variableDebtUSDC",
        },
    }


def _sdai_market_fixture() -> dict[str, object]:
    """An sDAI-style market with borrowing disabled — supply-only path."""
    return {
        "id": "0x4a3f2a3d27e89b1e1f2e1f3e1f4e1f5e1f6e1f7e",
        "name": "Spark sDAI",
        "isActive": True,
        "canBorrowFrom": False,
        "canUseAsCollateral": True,
        "maximumLTV": "74",
        "liquidationThreshold": "76",
        "liquidationPenalty": "5",
        "reserveFactor": "0.1",
        "createdTimestamp": "1700000000",
        "inputToken": {
            "id": "0x83f20f44975d03b1b09e64809b757c47f942beea",
            "symbol": "sDAI",
            "name": "Savings DAI",
            "decimals": 18,
        },
        "outputToken": {
            "id": "0x4a3f2a3d27e89b1e1f2e1f3e1f4e1f5e1f6e1f7e",
            "symbol": "spsDAI",
            "name": "Spark sDAI",
            "decimals": 18,
        },
        "_vToken": {
            "id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "symbol": "variableDebtsDAI",
        },
    }


# --- Helpers ----------------------------------------------------------------


_TEST_AVAILABLE_SINCE = datetime(2023, 5, 9, tzinfo=UTC)


def _adapter() -> SparkReferenceDataAdapter:
    return SparkReferenceDataAdapter(chain="ETHEREUM")


def _records_for(market: dict[str, object]) -> list[InstrumentRecord]:
    return _adapter()._build_market_records(market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)


# --- Tests ------------------------------------------------------------------


class TestSparkMetadataPopulation:
    def test_borrowable_market_yields_two_records(self) -> None:
        records = _records_for(_wsteth_market_fixture())
        assert len(records) == 2
        keys = {rec.instrument_key for rec in records}
        assert "SPARK-ETHEREUM:A_TOKEN:AWSTETH" in keys
        assert "SPARK-ETHEREUM:DEBT_TOKEN:DEBTWSTETH" in keys

    def test_supply_only_market_yields_single_record(self) -> None:
        records = _records_for(_sdai_market_fixture())
        assert len(records) == 1
        assert records[0].instrument_key == "SPARK-ETHEREUM:A_TOKEN:ASDAI"

    def test_a_token_record_populates_all_metadata_fields(self) -> None:
        records = _records_for(_wsteth_market_fixture())
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)

        assert isinstance(a_record, InstrumentRecord)
        assert a_record.instrument_type == InstrumentType.LENDING
        assert a_record.status == InstrumentStatus.ACTIVE
        assert a_record.venue == "SPARK-ETHEREUM"
        assert a_record.base_asset == "WSTETH"
        assert a_record.underlying == "WSTETH"
        assert a_record.available_from_datetime == _TEST_AVAILABLE_SINCE

        # DeFi metadata — pool / underlying / aToken / debt-token plumbing.
        assert a_record.pool_address == "0x12b54025c112aa61face2cdb7118740875a566e9"
        assert a_record.base_asset_contract_address == "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"
        assert a_record.base_asset_decimals == 18
        assert a_record.base_asset_symbol_onchain == "wstETH"
        assert a_record.atoken_address == "0x12b54025c112aa61face2cdb7118740875a566e9"
        assert a_record.debt_token_address == "0xd5c3e3b566a42a6110513ac7670c1a86d76e13e6"

    def test_debt_token_record_shares_metadata(self) -> None:
        records = _records_for(_wsteth_market_fixture())
        debt_record = next(r for r in records if "DEBT_TOKEN" in r.instrument_key)

        assert debt_record.instrument_type == InstrumentType.LENDING
        assert debt_record.base_asset == "WSTETH"
        assert debt_record.atoken_address == "0x12b54025c112aa61face2cdb7118740875a566e9"
        assert debt_record.debt_token_address == "0xd5c3e3b566a42a6110513ac7670c1a86d76e13e6"

    def test_usdc_decimals_six(self) -> None:
        records = _records_for(_usdc_market_fixture())
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)
        assert a_record.base_asset_decimals == 6
        assert a_record.base_asset_symbol_onchain == "USDC"
        assert a_record.instrument_key == "SPARK-ETHEREUM:A_TOKEN:AUSDC"

    def test_instrument_key_format(self) -> None:
        records = _records_for(_usdc_market_fixture())
        keys = sorted(r.instrument_key for r in records)
        assert keys == [
            "SPARK-ETHEREUM:A_TOKEN:AUSDC",
            "SPARK-ETHEREUM:DEBT_TOKEN:DEBTUSDC",
        ]

    def test_missing_input_token_yields_empty(self) -> None:
        bad_market = _wsteth_market_fixture()
        del bad_market["inputToken"]
        records = _adapter()._build_market_records(bad_market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)
        assert records == []

    def test_missing_symbol_or_underlying_yields_empty(self) -> None:
        # Empty symbol → skip
        bad_market = _wsteth_market_fixture()
        bad_market["inputToken"] = {
            "id": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
            "symbol": "",
            "decimals": 18,
        }
        records = _adapter()._build_market_records(bad_market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)
        assert records == []

    def test_string_decimals_parsed_as_int(self) -> None:
        # Spark sometimes returns decimals as string; ensure parse is robust.
        market = _wsteth_market_fixture()
        token = market["inputToken"]
        assert isinstance(token, dict)
        token["decimals"] = "18"
        records = _adapter()._build_market_records(market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)
        assert a_record.base_asset_decimals == 18

    def test_invalid_decimals_yields_none(self) -> None:
        market = _wsteth_market_fixture()
        token = market["inputToken"]
        assert isinstance(token, dict)
        token["decimals"] = "not-a-number"
        records = _adapter()._build_market_records(market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)
        assert a_record.base_asset_decimals is None

    def test_missing_v_token_yields_none_debt_address(self) -> None:
        market = _usdc_market_fixture()
        del market["_vToken"]
        records = _adapter()._build_market_records(market, "SPARK-ETHEREUM", _TEST_AVAILABLE_SINCE)
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)
        assert a_record.debt_token_address is None
        # atoken_address still set from outputToken
        assert a_record.atoken_address == "0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815"

    def test_default_available_since_falls_back_to_floor(self) -> None:
        # Pass available_since=None; adapter must populate from
        # get_protocol_floor_date("spark", "ETHEREUM") = 2023-05-09.
        market = _wsteth_market_fixture()
        records = _adapter()._build_market_records(market, "SPARK-ETHEREUM", None)
        a_record = next(r for r in records if "A_TOKEN" in r.instrument_key)
        assert a_record.available_from_datetime is not None
        assert a_record.available_from_datetime.year == 2023


class TestSparkAdapterIdentity:
    def test_venue_property(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        assert adapter.venue == "spark"

    def test_chain_uppercased(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ethereum")
        assert adapter._chain == "ETHEREUM"


class TestSparkApiUrlResolution:
    """Cover ``_resolve_api_url`` branches."""

    def test_missing_api_key_returns_none(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")  # api_key=None
        assert adapter._resolve_api_url() is None

    def test_missing_subgraph_id_returns_none(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        with patch(
            "instruments_service.reference_data.adapters.defi.spark.get_subgraph_id",
            return_value=None,
        ):
            assert adapter._resolve_api_url() is None

    def test_url_built_from_key_and_subgraph(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="abc123", chain="ETHEREUM")
        with patch(
            "instruments_service.reference_data.adapters.defi.spark.get_subgraph_id",
            return_value="sg-id",
        ):
            url = adapter._resolve_api_url()
        assert url == "https://gateway.thegraph.com/api/abc123/subgraphs/id/sg-id"


class TestSparkResolveBlockNum:
    """Cover ``_resolve_block_num`` branches."""

    async def test_no_date_returns_none(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")  # date=None
        assert await adapter._resolve_block_num() is None

    async def test_date_set_calls_date_to_block(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM", date="2024-01-15")
        with patch(
            "instruments_service.reference_data.adapters.defi.spark.date_to_block",
            new=AsyncMock(return_value=12345),
        ) as mock_d2b:
            block = await adapter._resolve_block_num()
        assert block == 12345
        mock_d2b.assert_awaited_once_with("2024-01-15", chain="ETHEREUM")

    async def test_date_set_but_unresolvable_returns_none(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM", date="1999-01-01")
        with patch(
            "instruments_service.reference_data.adapters.defi.spark.date_to_block",
            new=AsyncMock(return_value=None),
        ):
            assert await adapter._resolve_block_num() is None


class TestSparkLogFetchError:
    """Cover ``_log_fetch_error`` (event emission + classification)."""

    def test_log_fetch_error_emits_classified_event(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        with patch("instruments_service.reference_data.adapters.defi.spark.log_event") as mock_log_event:
            adapter._log_fetch_error(aiohttp.ClientError("boom"))
        # Single call with ADAPTER_FETCH_FAILED + venue/error_code keys.
        assert mock_log_event.call_count == 1
        args, kwargs = mock_log_event.call_args
        assert args[0] == "ADAPTER_FETCH_FAILED"
        details = kwargs.get("details") or (args[1] if len(args) > 1 else {})
        assert details["venue"] == "spark"
        assert details["endpoint"] == "thegraph_markets"
        assert "error_code" in details
        assert "action" in details
        assert "retry_safe" in details


class TestSparkGetInstrumentsAsync:
    """Cover the ``get_instruments`` async path end-to-end."""

    async def test_wrong_instrument_type_returns_empty(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    async def test_no_api_key_returns_empty(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")  # no api_key
        result = await adapter.get_instruments()
        assert result == []

    async def test_get_instruments_success_yields_records(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        markets_payload = {"data": {"markets": [_wsteth_market_fixture(), _sdai_market_fixture()]}}
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post(markets_payload),
            ),
        ):
            results = await adapter.get_instruments()
        # wstETH → 2 records (borrowable), sDAI → 1 record (supply-only) = 3 total
        assert len(results) == 3
        keys = {r.instrument_key for r in results}
        assert "SPARK-ETHEREUM:A_TOKEN:AWSTETH" in keys
        assert "SPARK-ETHEREUM:DEBT_TOKEN:DEBTWSTETH" in keys
        assert "SPARK-ETHEREUM:A_TOKEN:ASDAI" in keys

    async def test_get_instruments_uses_creation_ts_map(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        markets_payload = {"data": {"markets": [_wsteth_market_fixture()]}}
        custom_ts = datetime(2024, 8, 1, tzinfo=UTC)
        wsteth_output = "0x12b54025c112aa61face2cdb7118740875a566e9"
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={wsteth_output: custom_ts}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post(markets_payload),
            ),
        ):
            results = await adapter.get_instruments()
        a_record = next(r for r in results if "A_TOKEN" in r.instrument_key)
        assert a_record.available_from_datetime == custom_ts

    async def test_get_instruments_missing_data_field_returns_empty(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        # Subgraph "errors" response — no `data` key.
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post({"errors": [{"message": "indexing"}]}),
            ),
        ):
            results = await adapter.get_instruments()
        assert results == []

    async def test_get_instruments_non_dict_response_returns_empty(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        # Defensive: subgraph returned a list / string.
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post(["unexpected"]),
            ),
        ):
            results = await adapter.get_instruments()
        assert results == []

    async def test_get_instruments_client_error_raises_connection_error(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("boom")),
            ),
            patch("instruments_service.reference_data.adapters.defi.spark.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    async def test_get_instruments_with_historical_block_clause(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM", date="2024-06-01")
        markets_payload = {"data": {"markets": []}}
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.date_to_block",
                new=AsyncMock(return_value=20_000_000),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post(markets_payload),
            ),
        ):
            results = await adapter.get_instruments()
        assert results == []

    async def test_get_instruments_market_without_output_token_skipped_for_ts_lookup(
        self,
    ) -> None:
        # Market without an outputToken still produces records via
        # _build_market_records (which derives metadata from inputToken)
        # but contributes no address to the creation-timestamp lookup.
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        market = _wsteth_market_fixture()
        del market["outputToken"]
        markets_payload = {"data": {"markets": [market]}}
        mock_batch = AsyncMock(return_value={})
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=mock_batch,
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_post(markets_payload),
            ),
        ):
            await adapter.get_instruments()
        # No outputToken addresses → batch_resolve was not invoked at all.
        mock_batch.assert_not_awaited()


class TestSparkGetInstrumentLookup:
    """Cover the ``get_instrument`` symbol-lookup helper."""

    async def test_get_instrument_returns_match(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        records = _records_for(_wsteth_market_fixture())
        # raw_symbol is the underlying ERC-20 address — match on that path.
        target_raw = records[0].raw_symbol
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=records)):
            found = await adapter.get_instrument(target_raw)
        assert found is not None
        assert found.raw_symbol == target_raw

    async def test_get_instrument_returns_none_when_empty(self) -> None:
        adapter = SparkReferenceDataAdapter(api_key="k", chain="ETHEREUM")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            found = await adapter.get_instrument("NOPE-XYZ")
        assert found is None


class TestSparkUnsupportedSurfaces:
    """Cover the four NotImplementedError stubs (options/expiry/funding/ohlcv)."""

    async def test_options_chain_not_implemented(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("WSTETH")

    async def test_expiry_calendar_not_implemented(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("WSTETH")

    async def test_funding_rate_not_implemented(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("WSTETH")

    async def test_ohlcv_not_implemented(self) -> None:
        adapter = SparkReferenceDataAdapter(chain="ETHEREUM")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("WSTETH")
