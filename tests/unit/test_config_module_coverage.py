"""
Unit tests for instruments_service/config.py

Covers:
- DATABENTO_VALID_PARENT_SYMBOLS and DATABENTO_VALID_OPTIONS_SYMBOLS dicts
- EXCHANGE_CODE_TO_NAME dict
- _load_sp500_tickers() function (cache + contents)
- _load_tradfi_instruments() function (cache + contents)
- TradFiInstrument dataclass
- InstrumentDefinition alias
- UnifiedInstrumentConfig class methods
"""

from instruments_service.config import (
    DATABENTO_VALID_OPTIONS_SYMBOLS,
    DATABENTO_VALID_PARENT_SYMBOLS,
    EXCHANGE_CODE_TO_NAME,
    InstrumentDefinition,
    TradFiInstrument,
    UnifiedInstrumentConfig,
    _get_data_dir,
    _load_sp500_tickers,
    _load_tradfi_instruments,
)


class TestDatabentoDicts:
    def test_parent_symbols_non_empty(self) -> None:
        assert len(DATABENTO_VALID_PARENT_SYMBOLS) > 10

    def test_es_futures_in_parent_symbols(self) -> None:
        assert "ES" in DATABENTO_VALID_PARENT_SYMBOLS
        sym, dataset = DATABENTO_VALID_PARENT_SYMBOLS["ES"]
        assert sym == "ES.FUT"
        assert dataset == "GLBX.MDP3"

    def test_sp500_alias_in_parent_symbols(self) -> None:
        assert "SP500" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["SP500"] == DATABENTO_VALID_PARENT_SYMBOLS["ES"]

    def test_ice_futures_in_parent_symbols(self) -> None:
        assert "BRENT" in DATABENTO_VALID_PARENT_SYMBOLS
        _, dataset = DATABENTO_VALID_PARENT_SYMBOLS["BRENT"]
        assert dataset == "IFEU.IMPACT"

    def test_options_symbols_non_empty(self) -> None:
        assert len(DATABENTO_VALID_OPTIONS_SYMBOLS) > 0

    def test_es_options_in_options_symbols(self) -> None:
        assert "ES" in DATABENTO_VALID_OPTIONS_SYMBOLS
        sym, _dataset = DATABENTO_VALID_OPTIONS_SYMBOLS["ES"]
        assert sym == "ES.OPT"

    def test_exchange_code_to_name_non_empty(self) -> None:
        assert len(EXCHANGE_CODE_TO_NAME) > 10

    def test_exchange_code_mapping(self) -> None:
        assert EXCHANGE_CODE_TO_NAME["ES"] == "SP500"
        assert EXCHANGE_CODE_TO_NAME["GC"] == "GOLD"
        assert EXCHANGE_CODE_TO_NAME["CL"] == "CRUDE"


class TestGetDataDir:
    def test_returns_path(self) -> None:
        from pathlib import Path

        result = _get_data_dir()
        assert isinstance(result, Path)
        assert result.name == "data"


class TestLoadSp500Tickers:
    def test_loads_sp500_tickers(self) -> None:
        sp500, _nasdaq = _load_sp500_tickers()
        assert isinstance(sp500, list)
        assert len(sp500) > 100  # S&P 500 has ~500 tickers

    def test_loads_nasdaq_tickers(self) -> None:
        _sp500, nasdaq = _load_sp500_tickers()
        assert isinstance(nasdaq, list)
        assert len(nasdaq) > 50

    def test_no_duplicates_in_sp500(self) -> None:
        sp500, _ = _load_sp500_tickers()
        # ETFs added without duplicates
        assert len(sp500) == len(set(sp500))

    def test_cache_returns_same_list(self) -> None:
        result1, _ = _load_sp500_tickers()
        result2, _ = _load_sp500_tickers()
        assert result1 is result2  # Same cached object


class TestLoadTradfiInstruments:
    def test_loads_instruments(self) -> None:
        instruments, _exchange_map = _load_tradfi_instruments()
        assert isinstance(instruments, list)
        assert len(instruments) > 0

    def test_loads_exchange_map(self) -> None:
        _instruments, exchange_map = _load_tradfi_instruments()
        assert isinstance(exchange_map, dict)
        assert len(exchange_map) > 0

    def test_cache_consistency(self) -> None:
        r1, m1 = _load_tradfi_instruments()
        r2, m2 = _load_tradfi_instruments()
        assert r1 is r2
        assert m1 is m2


class TestTradFiInstrument:
    def test_basic_construction(self) -> None:
        inst = TradFiInstrument(
            symbol="ES.FUT",
            venue="CME",
            instrument_type="FUTURE",
            dataset="GLBX.MDP3",
            stype_in="parent",
        )
        assert inst.symbol == "ES.FUT"
        assert inst.venue == "CME"
        assert inst.quote_asset == "USD"  # default
        assert inst.base_asset is None  # default

    def test_with_optional_fields(self) -> None:
        inst = TradFiInstrument(
            symbol="GC.FUT",
            venue="CME",
            instrument_type="FUTURE",
            dataset="GLBX.MDP3",
            stype_in="parent",
            base_asset="GOLD",
            exchange_code="GC",
            underlying="XAU",
        )
        assert inst.base_asset == "GOLD"
        assert inst.exchange_code == "GC"
        assert inst.underlying == "XAU"

    def test_instrument_definition_alias(self) -> None:
        # InstrumentDefinition is a backward compat alias for TradFiInstrument
        inst = InstrumentDefinition(
            symbol="SPY",
            venue="NASDAQ",
            instrument_type="ETF",
            dataset="DBEQ.BASIC",
            stype_in="raw_symbol",
        )
        assert isinstance(inst, TradFiInstrument)


class TestUnifiedInstrumentConfig:
    def setup_method(self) -> None:
        self.config = UnifiedInstrumentConfig()

    def test_instruments_property_non_empty(self) -> None:
        instruments = self.config.instruments
        assert len(instruments) > 0

    def test_exchange_code_to_name_property(self) -> None:
        mapping = self.config.exchange_code_to_name
        assert isinstance(mapping, dict)
        assert len(mapping) > 0

    def test_get_symbols_for_venue_cme(self) -> None:
        symbols = self.config.get_symbols_for_venue("CME")
        assert len(symbols) > 0
        # ES.FUT should be in CME
        assert "ES.FUT" in symbols

    def test_get_symbols_for_venue_case_insensitive(self) -> None:
        symbols_upper = self.config.get_symbols_for_venue("CME")
        symbols_lower = self.config.get_symbols_for_venue("cme")
        assert symbols_upper == symbols_lower

    def test_get_symbols_for_dataset(self) -> None:
        symbols = self.config.get_symbols_for_dataset("GLBX.MDP3")
        assert len(symbols) > 0

    def test_get_symbols_by_type_future(self) -> None:
        futures = self.config.get_symbols_by_type("FUTURE")
        assert len(futures) > 0
        assert "ES.FUT" in futures

    def test_get_symbols_by_type_equity(self) -> None:
        equities = self.config.get_symbols_by_type("EQUITY")
        assert len(equities) > 0

    def test_get_symbols_by_type_etf(self) -> None:
        etfs = self.config.get_symbols_by_type("ETF")
        assert len(etfs) > 0
        assert "SPY" in etfs

    def test_get_dataset_and_stype(self) -> None:
        result = self.config.get_dataset_and_stype("ES.FUT")
        assert result is not None
        dataset, stype = result
        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_dataset_and_stype_missing(self) -> None:
        result = self.config.get_dataset_and_stype("NONEXISTENT.SYMBOL")
        assert result is None

    def test_get_instrument_by_symbol(self) -> None:
        inst = self.config.get_instrument("ES.FUT")
        assert inst is not None
        assert inst.symbol == "ES.FUT"

    def test_get_instrument_with_venue_filter(self) -> None:
        inst = self.config.get_instrument("ES.FUT", venue="CME")
        assert inst is not None
        inst_wrong = self.config.get_instrument("ES.FUT", venue="NASDAQ")
        assert inst_wrong is None

    def test_get_instrument_missing(self) -> None:
        inst = self.config.get_instrument("NONEXISTENT.SYMBOL")
        assert inst is None

    def test_get_human_readable_name_direct(self) -> None:
        name = self.config.get_human_readable_name("ES")
        assert name == "SP500"

    def test_get_human_readable_name_micro(self) -> None:
        # MES is micro ES - should map to SP500 via base_code "ES"
        name = self.config.get_human_readable_name("MES")
        assert name == "SP500"

    def test_get_human_readable_name_unknown(self) -> None:
        name = self.config.get_human_readable_name("UNKNOWN_CODE")
        assert name == "UNKNOWN_CODE"  # Returns original if not found

    def test_get_all_instruments_includes_sp500(self) -> None:
        all_insts = self.config.get_all_instruments()
        assert len(all_insts) > 500  # SP500 equities + base instruments

    def test_known_etfs_set(self) -> None:
        assert "SPY" in UnifiedInstrumentConfig.KNOWN_ETFS
        assert "QQQ" in UnifiedInstrumentConfig.KNOWN_ETFS
        assert "IBIT" in UnifiedInstrumentConfig.KNOWN_ETFS  # Bitcoin ETF

    def test_space_to_dot_symbols(self) -> None:
        assert "BRK B" in UnifiedInstrumentConfig.SPACE_TO_DOT_SYMBOLS
        assert UnifiedInstrumentConfig.SPACE_TO_DOT_SYMBOLS["BRK B"] == "BRK.B"

    def test_sp500_equities_use_correct_dataset(self) -> None:
        all_insts = self.config.get_all_instruments()
        equities = [i for i in all_insts if i.instrument_type == "EQUITY"]
        assert all(i.dataset == "DBEQ.BASIC" for i in equities)
        assert all(i.stype_in == "raw_symbol" for i in equities)

    def test_etf_classification(self) -> None:
        all_insts = self.config.get_all_instruments()
        spy_insts = [i for i in all_insts if i.symbol == "SPY"]
        assert len(spy_insts) > 0
        assert spy_insts[0].instrument_type == "ETF"

    def test_brk_b_uses_dot_format(self) -> None:
        all_insts = self.config.get_all_instruments()
        symbols = [i.symbol for i in all_insts]
        # BRK B should be converted to BRK.B for Databento API compatibility
        assert "BRK.B" in symbols
        assert "BRK B" not in symbols
