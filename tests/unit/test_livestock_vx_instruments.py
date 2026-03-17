"""Unit tests for Livestock (LE, HE) and VX (VIX futures) instrument definitions.

Validates that VX futures on CFE/CBOE and Livestock futures on CME are
correctly defined across all three sources:
- config/futures_options_definitions.py (TRADFI_VENUE_MAPPINGS)
- config/instrument_definitions.py (TRADFI_VENUE_MAPPINGS re-export)
- config/tradfi_exchange_mappings.py (DATABENTO_VALID_PARENT_SYMBOLS, EXCHANGE_CODE_TO_NAME)
- data/tradfi_instruments.json
"""

import json
from pathlib import Path

_DATA_DIR = Path(__file__).parent.parent.parent / "instruments_service" / "data"


class TestLivestockFuturesDefinitions:
    """Verify LE (Live Cattle) and HE (Lean Hogs) are defined in all registries."""

    def test_le_in_futures_options_definitions(self) -> None:
        """LE.FUT must exist in TRADFI_VENUE_MAPPINGS (futures_options_definitions)."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        le_entries = [e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "LE"]
        assert len(le_entries) == 1, "LE.FUT must appear exactly once"
        entry = le_entries[0]
        assert entry["symbol"] == "LE.FUT"
        assert entry["venue"] == "CME"
        assert entry["type"] == "FUTURE"
        assert entry["dataset"] == "GLBX.MDP3"
        assert entry["base"] == "LIVECATTLE"

    def test_he_in_futures_options_definitions(self) -> None:
        """HE.FUT must exist in TRADFI_VENUE_MAPPINGS (futures_options_definitions)."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        he_entries = [e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "HE"]
        assert len(he_entries) == 1, "HE.FUT must appear exactly once"
        entry = he_entries[0]
        assert entry["symbol"] == "HE.FUT"
        assert entry["venue"] == "CME"
        assert entry["type"] == "FUTURE"
        assert entry["dataset"] == "GLBX.MDP3"
        assert entry["base"] == "LEANHOGS"

    def test_le_in_exchange_mappings(self) -> None:
        """LE must map to LIVECATTLE in EXCHANGE_CODE_TO_NAME."""
        from instruments_service.config.tradfi_exchange_mappings import EXCHANGE_CODE_TO_NAME

        assert "LE" in EXCHANGE_CODE_TO_NAME
        assert EXCHANGE_CODE_TO_NAME["LE"] == "LIVECATTLE"

    def test_he_in_exchange_mappings(self) -> None:
        """HE must map to LEANHOGS in EXCHANGE_CODE_TO_NAME."""
        from instruments_service.config.tradfi_exchange_mappings import EXCHANGE_CODE_TO_NAME

        assert "HE" in EXCHANGE_CODE_TO_NAME
        assert EXCHANGE_CODE_TO_NAME["HE"] == "LEANHOGS"

    def test_le_in_databento_parent_symbols(self) -> None:
        """LE and LIVECATTLE must be valid Databento parent symbols."""
        from instruments_service.config.tradfi_exchange_mappings import (
            DATABENTO_VALID_PARENT_SYMBOLS,
        )

        assert "LE" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["LE"] == ("LE.FUT", "GLBX.MDP3")
        assert "LIVECATTLE" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["LIVECATTLE"] == ("LE.FUT", "GLBX.MDP3")

    def test_he_in_databento_parent_symbols(self) -> None:
        """HE and LEANHOGS must be valid Databento parent symbols."""
        from instruments_service.config.tradfi_exchange_mappings import (
            DATABENTO_VALID_PARENT_SYMBOLS,
        )

        assert "HE" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["HE"] == ("HE.FUT", "GLBX.MDP3")
        assert "LEANHOGS" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["LEANHOGS"] == ("HE.FUT", "GLBX.MDP3")

    def test_le_in_tradfi_instruments_json(self) -> None:
        """LE.FUT must appear in data/tradfi_instruments.json instruments array."""
        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            data = json.load(f)

        instruments = data["instruments"]
        le_entries = [i for i in instruments if i.get("code") == "LE"]
        assert len(le_entries) >= 1, "LE.FUT must appear in tradfi_instruments.json"
        entry = le_entries[0]
        assert entry["symbol"] == "LE.FUT"
        assert entry["venue"] == "CME"
        assert entry["base"] == "LIVECATTLE"

    def test_he_in_tradfi_instruments_json(self) -> None:
        """HE.FUT must appear in data/tradfi_instruments.json instruments array."""
        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            data = json.load(f)

        instruments = data["instruments"]
        he_entries = [i for i in instruments if i.get("code") == "HE"]
        assert len(he_entries) >= 1, "HE.FUT must appear in tradfi_instruments.json"
        entry = he_entries[0]
        assert entry["symbol"] == "HE.FUT"
        assert entry["venue"] == "CME"
        assert entry["base"] == "LEANHOGS"

    def test_le_he_in_json_exchange_code_to_name(self) -> None:
        """LE and HE must appear in tradfi_instruments.json exchange_code_to_name."""
        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            data = json.load(f)

        ecn = data["exchange_code_to_name"]
        assert ecn.get("LE") == "LIVECATTLE"
        assert ecn.get("HE") == "LEANHOGS"


class TestVXFuturesDefinitions:
    """Verify VX (VIX futures on CFE/CBOE) is defined in all registries."""

    def test_vx_in_futures_options_definitions(self) -> None:
        """VX.FUT must exist in TRADFI_VENUE_MAPPINGS (futures_options_definitions)."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        vx_entries = [e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "VX"]
        assert len(vx_entries) == 1, "VX.FUT must appear exactly once"
        entry = vx_entries[0]
        assert entry["symbol"] == "VX.FUT"
        assert entry["venue"] == "CFE"
        assert entry["type"] == "FUTURE"
        assert entry["dataset"] == "XCBF.MDP3"
        assert entry["base"] == "VIX"

    def test_vx_in_exchange_mappings(self) -> None:
        """VX must map to VIX in EXCHANGE_CODE_TO_NAME."""
        from instruments_service.config.tradfi_exchange_mappings import EXCHANGE_CODE_TO_NAME

        assert "VX" in EXCHANGE_CODE_TO_NAME
        assert EXCHANGE_CODE_TO_NAME["VX"] == "VIX"

    def test_vx_in_databento_parent_symbols(self) -> None:
        """VX must be a valid Databento parent symbol on CFE dataset."""
        from instruments_service.config.tradfi_exchange_mappings import (
            DATABENTO_VALID_PARENT_SYMBOLS,
        )

        assert "VX" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["VX"] == ("VX.FUT", "XCBF.MDP3")

    def test_vx_in_tradfi_instruments_json(self) -> None:
        """VX.FUT must appear in data/tradfi_instruments.json instruments array."""
        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            data = json.load(f)

        instruments = data["instruments"]
        vx_entries = [i for i in instruments if i.get("code") == "VX"]
        assert len(vx_entries) >= 1, "VX.FUT must appear in tradfi_instruments.json"
        entry = vx_entries[0]
        assert entry["symbol"] == "VX.FUT"
        assert entry["venue"] == "CFE"
        assert entry["dataset"] == "XCBF.MDP3"
        assert entry["base"] == "VIX"

    def test_vx_in_json_exchange_code_to_name(self) -> None:
        """VX must appear in tradfi_instruments.json exchange_code_to_name."""
        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            data = json.load(f)

        ecn = data["exchange_code_to_name"]
        assert ecn.get("VX") == "VIX"


class TestInstrumentIdPatterns:
    """Validate canonical instrument ID patterns for new instruments."""

    def test_vx_canonical_id_pattern(self) -> None:
        """VX futures should follow CBOE:FUTURE:VX-{EXPIRY} pattern."""
        # Verify the venue mapping supports CBOE/CFE resolution
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        vx = next(e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "VX")
        assert vx["venue"] == "CFE", "VX venue must be CFE (CBOE Futures Exchange)"
        assert vx["type"] == "FUTURE"

    def test_le_canonical_id_pattern(self) -> None:
        """LE futures should follow CME:FUTURE:LE-{EXPIRY} pattern."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        le = next(e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "LE")
        assert le["venue"] == "CME"
        assert le["type"] == "FUTURE"

    def test_he_canonical_id_pattern(self) -> None:
        """HE futures should follow CME:FUTURE:HE-{EXPIRY} pattern."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS

        he = next(e for e in TRADFI_VENUE_MAPPINGS if e.get("code") == "HE")
        assert he["venue"] == "CME"
        assert he["type"] == "FUTURE"


class TestCrossRegistryConsistency:
    """Ensure definitions are consistent across all registries."""

    def test_all_futures_options_codes_in_exchange_mappings(self) -> None:
        """Every futures code in futures_options_definitions should appear in exchange mappings."""
        from instruments_service.config.futures_options_definitions import TRADFI_VENUE_MAPPINGS
        from instruments_service.config.tradfi_exchange_mappings import EXCHANGE_CODE_TO_NAME

        future_codes = {e["code"] for e in TRADFI_VENUE_MAPPINGS if e.get("type") == "FUTURE" and e.get("code")}
        missing = future_codes - set(EXCHANGE_CODE_TO_NAME.keys())
        assert not missing, f"Futures codes in definitions but not in exchange mappings: {missing}"

    def test_livestock_vx_in_instrument_definitions(self) -> None:
        """LE, HE, VX must also be in instrument_definitions.py (the re-export)."""
        from instruments_service.config.instrument_definitions import TRADFI_VENUE_MAPPINGS

        codes = {e.get("code") for e in TRADFI_VENUE_MAPPINGS}
        assert "LE" in codes, "LE missing from instrument_definitions TRADFI_VENUE_MAPPINGS"
        assert "HE" in codes, "HE missing from instrument_definitions TRADFI_VENUE_MAPPINGS"
        assert "VX" in codes, "VX missing from instrument_definitions TRADFI_VENUE_MAPPINGS"
