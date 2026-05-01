"""Tests for orchestrator.process_instruments — the main processing pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import (
    _write_venue,
    process_instruments,
)
from instruments_service.engine.urdi_reference_provider import VenueFetchResult


def _make_record(
    instrument_key: str = "TEST:SPOT:BTCUSDT",
    venue: str = "BINANCE-SPOT",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue=venue,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=Decimal("0.01"),
        available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
    )


class TestProcessInstruments:
    @pytest.mark.asyncio
    async def test_no_active_venues_returns_empty(self) -> None:
        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
                return_value=["TOTALLY_NEW_VENUE"],
            ),
            patch(
                "instruments_service.engine.orchestrator.is_venue_available",
                return_value=False,
            ),
        ):
            result = await process_instruments("2026-03-22", ["CEFI"])
        assert result == {}

    @pytest.mark.asyncio
    async def test_zero_records_raises_runtime_error(self) -> None:
        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
                return_value=["BINANCE-SPOT"],
            ),
            patch(
                "instruments_service.engine.orchestrator.is_venue_available",
                return_value=True,
            ),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult()),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            pytest.raises(RuntimeError, match="zero records"),
        ):
            await process_instruments("2026-03-22", ["CEFI"])

    @pytest.mark.asyncio
    async def test_successful_processing(self) -> None:
        records = [_make_record(), _make_record(instrument_key="TEST:SPOT:ETHUSDT", base_asset="ETH")]

        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
                return_value=["BINANCE-SPOT"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=records)),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.DomainValidationService") as mock_dvs,
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            mock_dvs.return_value.validate_for_domain = MagicMock()
            result = await process_instruments("2026-03-22", ["CEFI"])

        assert isinstance(result, dict)
        assert sum(result.values()) == 2

    @pytest.mark.asyncio
    async def test_datetime_input_converted_to_string(self) -> None:
        """process_instruments accepts datetime objects and converts to string."""
        records = [_make_record()]
        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
                return_value=["BINANCE-SPOT"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=records)),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.DomainValidationService") as mock_dvs,
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            mock_dvs.return_value.validate_for_domain = MagicMock()
            result = await process_instruments(datetime(2026, 3, 22, tzinfo=UTC), ["CEFI"])
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_venue_override_bypasses_category_lookup(self) -> None:
        records = [_make_record()]
        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False

        with (
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=records)),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.DomainValidationService") as mock_dvs,
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            mock_dvs.return_value.validate_for_domain = MagicMock()
            result = await process_instruments(
                "2026-03-22",
                ["CEFI"],
                venue_override=["BINANCE-SPOT"],
            )
        assert isinstance(result, dict)


class TestWriteVenue:
    def test_write_success(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"instrument_key": ["A"], "venue": ["BINANCE-SPOT"]})
        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        counts: dict[str, int] = {}

        with patch("instruments_service.engine.orchestrator._write_catalogue_record"):
            _write_venue("BINANCE-SPOT", df, "2026-03-22", "test-bucket", mock_sink, counts, mock_sampler)

        assert counts["BINANCE-SPOT"] == 1
        mock_sink.write.assert_called_once()

    def test_write_failure_logged_not_raised(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"instrument_key": ["A"], "venue": ["BINANCE-SPOT"]})
        mock_sink = MagicMock()
        mock_sink.write.side_effect = OSError("disk full")
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        counts: dict[str, int] = {}

        with patch("instruments_service.engine.orchestrator.log_event"):
            _write_venue("BINANCE-SPOT", df, "2026-03-22", "test-bucket", mock_sink, counts, mock_sampler)

        assert "BINANCE-SPOT" not in counts  # not written due to error

    def test_write_with_sampling(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"instrument_key": ["A"], "venue": ["BINANCE-SPOT"]})
        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = True
        counts: dict[str, int] = {}

        with patch("instruments_service.engine.orchestrator._write_catalogue_record"):
            _write_venue("BINANCE-SPOT", df, "2026-03-22", "test-bucket", mock_sink, counts, mock_sampler)

        mock_sampler.generate_csv_sample.assert_called_once()
