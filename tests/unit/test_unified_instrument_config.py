"""
Unit tests for UnifiedInstrumentConfig (Databento/TradFi config).

Task 1.4.4: DatabentoCategoryAdapter - instruments-service uses UnifiedInstrumentConfig
for TradFi/Databento symbol and dataset configuration.
"""

import pytest

from instruments_service.config.venue_config import UnifiedInstrumentConfig


class TestUnifiedInstrumentConfig:
    """Test UnifiedInstrumentConfig Databento/TradFi config."""

    @pytest.fixture
    def config(self):
        """Create UnifiedInstrumentConfig fixture."""
        return UnifiedInstrumentConfig()

    def test_get_symbols_for_dataset(self, config):
        """Test get_symbols_for_dataset returns symbols for a dataset."""
        symbols = config.get_symbols_for_dataset("GLBX.MDP3")
        assert isinstance(symbols, list)
        # GLBX.MDP3 is CME futures dataset
        if symbols:
            assert all(isinstance(s, str) for s in symbols)

    def test_get_symbols_for_venue(self, config):
        """Test get_symbols_for_venue returns symbols for a venue."""
        symbols = config.get_symbols_for_venue("CME")
        assert isinstance(symbols, list)
        if symbols:
            assert all(isinstance(s, str) for s in symbols)

    def test_get_symbols_by_type(self, config):
        """Test get_symbols_by_type returns symbols by instrument type."""
        symbols = config.get_symbols_by_type("FUTURE")
        assert isinstance(symbols, list)
        if symbols:
            assert all(isinstance(s, str) for s in symbols)

    def test_get_dataset_and_stype_existing_symbol(self, config):
        """Test get_dataset_and_stype for existing symbol."""
        all_insts = config.get_all_instruments()
        if all_insts:
            first = all_insts[0]
            result = config.get_dataset_and_stype(first.symbol)
            assert result is not None
            dataset, stype = result
            assert isinstance(dataset, str)
            assert isinstance(stype, str)

    def test_get_dataset_and_stype_unknown_symbol(self, config):
        """Test get_dataset_and_stype for unknown symbol returns None."""
        result = config.get_dataset_and_stype("UNKNOWN.SYMBOL.XYZ")
        assert result is None

    def test_get_instrument_by_symbol(self, config):
        """Test get_instrument returns instrument for symbol."""
        all_insts = config.get_all_instruments()
        if all_insts:
            first = all_insts[0]
            inst = config.get_instrument(first.symbol)
            assert inst is not None
            assert inst.symbol == first.symbol
            assert inst.dataset == first.dataset

    def test_get_human_readable_name(self, config):
        """Test get_human_readable_name converts exchange code."""
        # Exchange codes like ES, CL map to human names
        name = config.get_human_readable_name("ES")
        assert isinstance(name, str)
        # Unknown code returns as-is
        unknown = config.get_human_readable_name("XX")
        assert unknown == "XX"
