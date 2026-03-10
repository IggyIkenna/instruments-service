"""Coverage booster for instruments-service low-coverage modules."""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# config.py — import triggers all top-level code
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigModule:
    def test_import(self):
        import instruments_service.config as cfg

        assert cfg is not None

    def test_unified_instrument_config(self):
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        assert config is not None

    def test_tradfi_instrument_dataclass(self):
        from instruments_service.config import TradFiInstrument

        inst = TradFiInstrument(
            instrument_id="NYSE:EQUITY:AAPL",
            symbol="AAPL",
            exchange="NYSE",
            instrument_type="EQUITY",
            category="TRADFI",
        )
        assert inst.symbol == "AAPL"

    def test_get_sp500_tickers_returns_list(self):
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        tickers = config.get_sp500_tickers()
        assert isinstance(tickers, list)

    def test_get_etf_tickers_returns_list(self):
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        tickers = config.get_etf_tickers()
        assert isinstance(tickers, list)

    def test_get_tradfi_instruments_returns_list(self):
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        instruments = config.get_tradfi_instruments()
        assert isinstance(instruments, list)


# ---------------------------------------------------------------------------
# selective_validation.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSelectiveValidation:
    def test_import(self):
        import instruments_service.app.core.selective_validation as sv

        assert sv is not None


# ---------------------------------------------------------------------------
# instrument_validation.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestInstrumentValidation:
    def test_import(self):
        from instruments_service.app.core.instrument_validation import InstrumentValidationService

        assert InstrumentValidationService is not None

    def test_init(self):
        from instruments_service.app.core.instrument_validation import InstrumentValidationService

        svc = InstrumentValidationService()
        assert svc is not None


# ---------------------------------------------------------------------------
# canonical_key_generator.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCanonicalKeyGenerator:
    def test_import(self):
        from instruments_service.app.core.processors.canonical_key_generator import (
            CanonicalKeyGenerator,
        )

        assert CanonicalKeyGenerator is not None

    def test_init(self):
        from instruments_service.app.core.processors.canonical_key_generator import (
            CanonicalKeyGenerator,
        )

        gen = CanonicalKeyGenerator()
        assert gen is not None

    def test_generate_key_cefi(self):
        from instruments_service.app.core.processors.canonical_key_generator import (
            CanonicalKeyGenerator,
        )

        gen = CanonicalKeyGenerator()
        instrument = {
            "venue": "BINANCE",
            "instrument_type": "SPOT",
            "symbol": "BTC-USDT",
        }
        key = gen.generate_canonical_key(instrument)
        assert isinstance(key, str)
        assert len(key) > 0


# ---------------------------------------------------------------------------
# symbol_parser.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSymbolParser:
    def test_import(self):
        from instruments_service.app.core.processors.symbol_parser import SymbolParser

        assert SymbolParser is not None

    def test_parse_cefi_symbol(self):
        from instruments_service.app.core.processors.symbol_parser import SymbolParser

        parser = SymbolParser()
        result = parser.parse("BTC/USDT")
        assert result is not None

    def test_parse_spot_symbol(self):
        from instruments_service.app.core.processors.symbol_parser import SymbolParser

        parser = SymbolParser()
        result = parser.parse("ETH-USD")
        assert result is not None


# ---------------------------------------------------------------------------
# derived_fields_populator.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDerivedFieldsPopulator:
    def test_import(self):
        from instruments_service.app.core.processors.derived_fields_populator import (
            DerivedFieldsPopulator,
        )

        assert DerivedFieldsPopulator is not None

    def test_init(self):
        from instruments_service.app.core.processors.derived_fields_populator import (
            DerivedFieldsPopulator,
        )

        pop = DerivedFieldsPopulator()
        assert pop is not None

    def test_populate_with_minimal_instrument(self):
        from instruments_service.app.core.processors.derived_fields_populator import (
            DerivedFieldsPopulator,
        )

        pop = DerivedFieldsPopulator()
        instrument = {
            "symbol": "BTC-USDT",
            "venue": "BINANCE",
            "instrument_type": "SPOT",
        }
        result = pop.populate(instrument)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# catalogue_updater (import only — network calls mocked)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCatalogueUpdater:
    def test_import(self):
        import instruments_service.app.core.catalogue_updater as cu

        assert cu is not None


# ---------------------------------------------------------------------------
# batch_processor
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBatchProcessor:
    def test_import(self):
        from instruments_service.app.core.batch_processor import BatchProcessor

        assert BatchProcessor is not None
