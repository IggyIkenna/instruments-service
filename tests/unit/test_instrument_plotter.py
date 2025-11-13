"""
Unit tests for InstrumentPlotter.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from instruments_service.app.visualization.instrument_plotter import InstrumentPlotter


class TestInstrumentPlotter:
    """Tests for InstrumentPlotter."""

    @pytest.fixture
    def plotter(self):
        """Create InstrumentPlotter fixture."""
        return InstrumentPlotter()

    @pytest.fixture
    def sample_instruments(self):
        """Create sample instruments DataFrame."""
        return pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE:SPOT_PAIR:BTC-USDT",
                    "BINANCE:SPOT_PAIR:ETH-USDT",
                    "DERIBIT:PERPETUAL:BTC-USD",
                ],
                "venue": ["BINANCE", "BINANCE", "DERIBIT"],
                "instrument_type": ["SPOT_PAIR", "SPOT_PAIR", "PERPETUAL"],
                "available_from_datetime": [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, tzinfo=timezone.utc),
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                ],
                "data_types": [
                    "trades,book_snapshot_5",
                    "trades,book_snapshot_5",
                    "trades,derivative_ticker",
                ],
            }
        )

    def test_init(self, plotter):
        """Test InstrumentPlotter initialization."""
        assert plotter.figure is None

    def test_plot_instrument_availability(self, plotter, sample_instruments):
        """Test plotting instrument availability."""
        fig = plotter.plot_instrument_availability(sample_instruments)
        assert fig is not None
        assert len(fig.data) > 0

    def test_plot_venue_distribution(self, plotter, sample_instruments):
        """Test plotting venue distribution."""
        fig = plotter.plot_venue_distribution(sample_instruments)
        assert fig is not None
        assert len(fig.data) > 0

    def test_plot_venue_distribution_missing_column(self, plotter):
        """Test plotting venue distribution with missing column."""
        df = pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        with pytest.raises(ValueError, match="must contain 'venue' column"):
            plotter.plot_venue_distribution(df)

    def test_plot_instrument_type_distribution(self, plotter, sample_instruments):
        """Test plotting instrument type distribution."""
        fig = plotter.plot_instrument_type_distribution(sample_instruments)
        assert fig is not None
        assert len(fig.data) > 0

    def test_plot_instrument_type_distribution_missing_column(self, plotter):
        """Test plotting instrument type distribution with missing column."""
        df = pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        with pytest.raises(ValueError, match="must contain 'instrument_type' column"):
            plotter.plot_instrument_type_distribution(df)

    def test_plot_data_type_availability(self, plotter, sample_instruments):
        """Test plotting data type availability."""
        fig = plotter.plot_data_type_availability(sample_instruments)
        assert fig is not None
        assert len(fig.data) > 0

    def test_plot_data_type_availability_missing_column(self, plotter):
        """Test plotting data type availability with missing column."""
        df = pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        with pytest.raises(ValueError, match="must contain 'data_types' column"):
            plotter.plot_data_type_availability(df)

    def test_plot_instrument_availability_custom_title(self, plotter, sample_instruments):
        """Test plotting with custom title."""
        fig = plotter.plot_instrument_availability(
            sample_instruments, title="Custom Title", height=800
        )
        assert fig is not None
        assert fig.layout.title.text == "Custom Title"
        assert fig.layout.height == 800


