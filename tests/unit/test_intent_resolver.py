"""Comprehensive tests for intent_resolver — StrategyInstrumentIntent resolution and validation.

All tests are credential-free with no external dependencies.
"""

from __future__ import annotations

import pytest
from unified_api_contracts.internal.domain.strategy_service import (
    ResolvedInstruments,
    StrategyInstrumentIntent,
)

from instruments_service.reference_data.intent_resolver import (
    resolve_instruments,
    validate_resolved_instruments,
)


def _make_intent(
    protocol: str = "AAVE_V3",
    chain: str = "ETHEREUM",
    base_currencies: list[str] | None = None,
    instrument_types: list[str] | None = None,
    venue_filter: list[str] | None = None,
) -> StrategyInstrumentIntent:
    """Create a test intent.  Default protocol is ``AAVE_V3``
    so that ``protocol.upper() in "AAVE_V3-ETHEREUM"`` is True.
    """
    return StrategyInstrumentIntent(
        protocol=protocol,
        chain=chain,
        base_currencies=base_currencies if base_currencies is not None else ["WETH", "USDC"],
        instrument_types=instrument_types if instrument_types is not None else [],
        venue_filter=venue_filter if venue_filter is not None else [],
    )


def _make_instrument(
    instrument_id: str = "AAVE_V3-ETHEREUM:A_TOKEN:WETH",
    venue: str = "AAVE_V3-ETHEREUM",
    instrument_type: str = "A_TOKEN",
    base_currency: str = "WETH",
) -> dict[str, str]:
    return {
        "instrument_id": instrument_id,
        "venue": venue,
        "instrument_type": instrument_type,
        "base_currency": base_currency,
    }


# ---------------------------------------------------------------------------
# resolve_instruments
# ---------------------------------------------------------------------------


class TestResolveInstruments:
    def test_basic_resolution(self) -> None:
        intent = _make_intent()
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
            _make_instrument(instrument_id="id2", base_currency="USDC"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 2
        assert result.date == "2024-06-01"
        assert result.missing_currencies == []

    def test_missing_currency(self) -> None:
        intent = _make_intent(base_currencies=["WETH", "WBTC"])
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 1
        assert "WBTC" in result.missing_currencies

    def test_protocol_filter(self) -> None:
        # Protocol filter: intent.protocol.upper() must be a substring of venue.upper()
        # "AAVE_V3" in "AAVE_V3-ETHEREUM" = True, "AAVE_V3" not in "MORPHO-ETHEREUM"
        intent = _make_intent(protocol="AAVE_V3")
        available = [
            _make_instrument(instrument_id="id1", venue="AAVE_V3-ETHEREUM", base_currency="WETH"),
            _make_instrument(instrument_id="id2", venue="MORPHO-ETHEREUM", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert "id1" in result.instrument_ids
        assert "id2" not in result.instrument_ids

    def test_chain_filter(self) -> None:
        intent = _make_intent(chain="ETHEREUM")
        available = [
            _make_instrument(instrument_id="id1", venue="AAVE_V3-ETHEREUM", base_currency="WETH"),
            _make_instrument(instrument_id="id2", venue="AAVE_V3-ARBITRUM", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert "id1" in result.instrument_ids
        assert "id2" not in result.instrument_ids

    def test_instrument_type_filter(self) -> None:
        intent = _make_intent(instrument_types=["A_TOKEN"])
        available = [
            _make_instrument(instrument_id="id1", instrument_type="A_TOKEN", base_currency="WETH"),
            _make_instrument(instrument_id="id2", instrument_type="DEBT_TOKEN", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert "id1" in result.instrument_ids
        assert "id2" not in result.instrument_ids

    def test_venue_filter(self) -> None:
        intent = _make_intent(venue_filter=["AAVE_V3-ETHEREUM"])
        available = [
            _make_instrument(instrument_id="id1", venue="AAVE_V3-ETHEREUM", base_currency="WETH"),
            _make_instrument(instrument_id="id2", venue="AAVE_V3-ARBITRUM", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert "id1" in result.instrument_ids

    def test_case_insensitive_currency_matching(self) -> None:
        intent = _make_intent(base_currencies=["weth"])
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 1

    def test_empty_available_instruments(self) -> None:
        intent = _make_intent()
        result = resolve_instruments(intent, [], "2024-06-01")
        assert len(result.instrument_ids) == 0
        assert set(result.missing_currencies) == {"WETH", "USDC"}

    def test_empty_base_currencies(self) -> None:
        intent = _make_intent(base_currencies=[])
        available = [_make_instrument()]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 0

    def test_no_protocol_filter(self) -> None:
        intent = _make_intent(protocol="")
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 1

    def test_no_chain_filter(self) -> None:
        intent = _make_intent(chain="")
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 1

    def test_multiple_instruments_per_currency(self) -> None:
        intent = _make_intent(base_currencies=["WETH"])
        available = [
            _make_instrument(instrument_id="id1", base_currency="WETH"),
            _make_instrument(instrument_id="id2", base_currency="WETH"),
        ]
        result = resolve_instruments(intent, available, "2024-06-01")
        assert len(result.instrument_ids) == 2


# ---------------------------------------------------------------------------
# validate_resolved_instruments
# ---------------------------------------------------------------------------


class TestValidateResolvedInstruments:
    def _make_resolved(
        self,
        instrument_ids: list[str] | None = None,
        missing: list[str] | None = None,
        protocol: str = "AAVE_V3",
        chain: str = "ETHEREUM",
    ) -> ResolvedInstruments:
        intent = _make_intent(protocol=protocol, chain=chain)
        return ResolvedInstruments(
            intent=intent,
            instrument_ids=instrument_ids if instrument_ids is not None else ["id1"],
            date="2024-06-01",
            missing_currencies=missing if missing is not None else [],
        )

    def test_all_resolved_returns_empty_warnings(self) -> None:
        resolved = self._make_resolved(instrument_ids=["id1", "id2"])
        warnings = validate_resolved_instruments(resolved)
        assert warnings == []

    def test_missing_required_currency_raises(self) -> None:
        resolved = self._make_resolved(missing=["WBTC"])
        with pytest.raises(ValueError, match="Instrument resolution failed"):
            validate_resolved_instruments(resolved, required_currencies=["WBTC"])

    def test_missing_required_no_raise(self) -> None:
        resolved = self._make_resolved(missing=["WBTC"])
        warnings = validate_resolved_instruments(resolved, required_currencies=["WBTC"], raise_on_missing=False)
        assert len(warnings) == 1
        assert "WBTC" in warnings[0]

    def test_min_instruments_not_met_raises(self) -> None:
        resolved = self._make_resolved(instrument_ids=[])
        with pytest.raises(ValueError, match="Instrument resolution failed"):
            validate_resolved_instruments(resolved, min_instruments=1)

    def test_min_instruments_not_met_no_raise(self) -> None:
        resolved = self._make_resolved(instrument_ids=[])
        warnings = validate_resolved_instruments(resolved, min_instruments=1, raise_on_missing=False)
        assert any("Only 0 instruments" in w for w in warnings)

    def test_min_instruments_met(self) -> None:
        resolved = self._make_resolved(instrument_ids=["id1", "id2"])
        warnings = validate_resolved_instruments(resolved, min_instruments=2)
        assert warnings == []

    def test_default_required_currencies_from_intent(self) -> None:
        """When required_currencies is None, uses all base_currencies from intent."""
        resolved = self._make_resolved(missing=["WETH"])
        with pytest.raises(ValueError, match="Instrument resolution failed"):
            validate_resolved_instruments(resolved)

    def test_multiple_missing_currencies(self) -> None:
        resolved = self._make_resolved(missing=["WETH", "USDC"])
        with pytest.raises(ValueError):
            validate_resolved_instruments(resolved)

    def test_no_missing_no_min_returns_empty(self) -> None:
        resolved = self._make_resolved(instrument_ids=["a"], missing=[])
        warnings = validate_resolved_instruments(resolved, min_instruments=0)
        assert warnings == []
