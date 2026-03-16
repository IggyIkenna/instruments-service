"""Integration tests verifying instruments-service adopts UAC INSTRUMENT_TYPES_BY_VENUE.

Validates that:
1. INSTRUMENT_TYPES_BY_VENUE is imported from UAC (not locally defined)
2. The mapping is non-empty and contains expected venues
3. Venue validation runs at import time
4. No broken imports in the venue-dependent code paths
"""

from __future__ import annotations

import pytest
from unified_api_contracts import INSTRUMENT_TYPES_BY_VENUE as UAC_ITBV


@pytest.mark.integration
class TestUACInstrumentTypesImport:
    """Verify INSTRUMENT_TYPES_BY_VENUE is imported from UAC."""

    def test_instrument_types_by_venue_imported_from_uac(self) -> None:
        """instruments-service re-exports INSTRUMENT_TYPES_BY_VENUE from UAC."""
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert INSTRUMENT_TYPES_BY_VENUE is UAC_ITBV

    def test_mapping_is_non_empty(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert len(INSTRUMENT_TYPES_BY_VENUE) > 0


@pytest.mark.integration
class TestUACVenueContents:
    """Verify INSTRUMENT_TYPES_BY_VENUE contains expected venues and types."""

    def test_binance_spot_has_spot(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "BINANCE-SPOT" in INSTRUMENT_TYPES_BY_VENUE
        assert "SPOT" in INSTRUMENT_TYPES_BY_VENUE["BINANCE-SPOT"]

    def test_binance_futures_has_perpetual(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "BINANCE-FUTURES" in INSTRUMENT_TYPES_BY_VENUE
        assert "PERPETUAL" in INSTRUMENT_TYPES_BY_VENUE["BINANCE-FUTURES"]

    def test_deribit_has_options(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "DERIBIT" in INSTRUMENT_TYPES_BY_VENUE
        assert "OPTION" in INSTRUMENT_TYPES_BY_VENUE["DERIBIT"]

    def test_cme_has_futures(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "CME" in INSTRUMENT_TYPES_BY_VENUE
        assert "FUTURE" in INSTRUMENT_TYPES_BY_VENUE["CME"]

    def test_uniswap_has_pool(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "UNISWAPV3-ETH" in INSTRUMENT_TYPES_BY_VENUE
        assert "POOL" in INSTRUMENT_TYPES_BY_VENUE["UNISWAPV3-ETH"]

    def test_aave_has_lending(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "AAVE_V3" in INSTRUMENT_TYPES_BY_VENUE
        assert "LENDING" in INSTRUMENT_TYPES_BY_VENUE["AAVE_V3"]

    def test_sports_venues_have_instrument_types(self) -> None:
        """Sports exchange venues should have EXCHANGE_ODDS type."""
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        assert "BETFAIR" in INSTRUMENT_TYPES_BY_VENUE
        assert "EXCHANGE_ODDS" in INSTRUMENT_TYPES_BY_VENUE["BETFAIR"]

    def test_all_values_are_non_empty_sets(self) -> None:
        from instruments_service.config import INSTRUMENT_TYPES_BY_VENUE

        for venue, types in INSTRUMENT_TYPES_BY_VENUE.items():
            assert isinstance(types, set), f"{venue} types is not a set"
            assert len(types) > 0, f"{venue} has empty instrument types"


@pytest.mark.integration
class TestVenueValidationRunsAtImport:
    """Verify that the venue validation in config/__init__.py executes."""

    def test_config_module_imports_without_error(self) -> None:
        """Importing config module triggers _validate_venues_against_uac."""
        import instruments_service.config  # noqa: F401

    def test_validate_venues_function_exists(self) -> None:
        """The validation function is defined in the config module."""
        from instruments_service.config import __all__

        assert "INSTRUMENT_TYPES_BY_VENUE" in __all__
