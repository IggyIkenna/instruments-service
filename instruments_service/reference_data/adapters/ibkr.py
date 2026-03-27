# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportArgumentType=false, reportUnknownMemberType=false, reportOperatorIssue=false, reportCallIssue=false, reportUnusedVariable=false, reportReturnType=false, reportAttributeAccessIssue=false, reportUnnecessaryCast=false, reportAssignmentType=false, reportMissingParameterType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false, reportUnnecessaryComparison=false, reportUnknownLambdaType=false, reportMissingTypeArgument=false, reportOptionalMemberAccess=false, reportConstantRedefinition=false, reportDeprecated=false, reportGeneralTypeIssues=false, reportIncompatibleMethodOverride=false, reportIndexIssue=false, reportOptionalIterable=false, reportOptionalOperand=false, reportOptionalSubscript=false, reportPossiblyUnboundVariable=false, reportPrivateUsage=false, reportTypeCommentUsage=false, reportUnnecessaryIsInstance=false, reportUnusedFunction=false, reportMissingImports=false
"""IBKR reference data adapter — ib_insync implementation.

Uses the inject-IB pattern: the caller creates one connected ib_insync.IB instance
pointing at ibkr-gateway-infra and injects it into this adapter. The adapter never
calls IB().connect() internally — connection lifecycle belongs to the caller.

Auth pattern:
  TWS / IB Gateway socket connection. No API key — auth is the socket handshake.
  Credentials stored in Secret Manager as "ibkr-account-credentials" are used by
  the ibkr-gateway-infra process, not by this adapter directly.

VCR status: NOT_APPLICABLE
  IB Gateway uses a proprietary binary socket protocol (EClient/EWrapper via
  ib_insync), not HTTP. Use MagicMock(spec=IB) for unit tests per the canonical
  pattern documented in unified-trading-codex/02-data/vcr-cassette-ownership.md.

Canonical test usage (MagicMock inject-IB pattern)::

    mock_ib = MagicMock(spec=IB)
    mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[...])
    adapter = IBKRReferenceDataAdapter(ib=mock_ib)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import ib_insync  # fails loud if not installed — no fallback (rule: no try/except ImportError)
from unified_api_contracts import (
    IBKRContractDetails,
    IBKRCorporateAction,
)
from unified_api_contracts.internal import (
    AssetClass,
    InstrumentRecord,
    InstrumentStatus,
    InstrumentType,
)

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalCorporateAction,
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Map IBKR secType → canonical InstrumentType
_SEC_TYPE_MAP: dict[str, InstrumentType] = {
    "STK": InstrumentType.SPOT,
    "FUT": InstrumentType.FUTURES,
    "OPT": InstrumentType.OPTION,
    "CASH": InstrumentType.SPOT,
    "CFD": InstrumentType.PERP,
    "BOND": InstrumentType.SPOT,
    "FOP": InstrumentType.OPTION,
    "WAR": InstrumentType.OPTION,
    "IOPT": InstrumentType.OPTION,
}

# Map IBKR secType → canonical AssetClass
_SEC_TYPE_ASSET_CLASS_MAP: dict[str, AssetClass] = {
    "STK": AssetClass.EQUITY,
    "FUT": AssetClass.COMMODITY,
    "OPT": AssetClass.EQUITY,
    "CASH": AssetClass.FX,
    "CFD": AssetClass.EQUITY,
    "BOND": AssetClass.FIXED_INCOME,
    "FOP": AssetClass.COMMODITY,
}

# IBKR corporate action type code → canonical string
_CA_TYPE_MAP: dict[str, str] = {
    "Dividends": "dividend",
    "Splits": "stock_split",
    "Mergers": "merger",
    "BonusRights": "rights_issue",
    "StockDividend": "stock_dividend",
    "SpinOffs": "spinoff",
    "TenderOffers": "tender_offer",
}

# Default equity instrument filter (SMART routing, USD, STK)
_DEFAULT_EQUITY_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "NVDA",
    "META",
    "NFLX",
    "BRK B",
    "JPM",
    "JNJ",
    "UNH",
    "XOM",
    "V",
    "PG",
    "MA",
    "HD",
    "CVX",
    "ABBV",
    "MRK",
    "PEP",
    "KO",
    "AVGO",
    "COST",
    "DIS",
    "WMT",
    "CSCO",
    "TMO",
    "ABT",
    "MCD",
    "ADBE",
    "DHR",
    "VZ",
    "ACN",
    "NKE",
    "LIN",
    "TXN",
    "NEE",
    "WFC",
    "CRM",
    "PM",
    "BMY",
    "ORCL",
    "QCOM",
    "RTX",
]


class IBKRReferenceDataAdapter(BaseReferenceDataAdapter):
    """IBKR reference data adapter using ib_insync inject-IB pattern.

    Accepts an injected ib_insync.IB object. The caller owns the connection
    lifecycle — this adapter never calls IB().connect() or IB().disconnect()
    internally.

    Primary use cases:
      - fetch_instruments(): equity instrument records via reqContractDetails
      - fetch_corporate_actions(): dividends + splits via IBKRCorporateAction

    Other methods (options chains, expiry calendars, OHLCV) are stubs —
    TradFi reference data for these is sourced from Databento for batch;
    this adapter only handles the TWS live path for contract details and
    corporate actions.

    Canonical production usage (ib_insync.IB + UnifiedCloudConfig)::

        ib = IB()
        ib.connect(cfg.ibkr_gateway_host, cfg.ibkr_gateway_port, clientId=1)
        adapter = IBKRReferenceDataAdapter(ib=ib)
        instruments = await adapter.get_instruments(instrument_type="equity")
        ib.disconnect()

    Canonical test usage (MagicMock inject-IB pattern)::

        mock_ib = MagicMock(spec=IB)
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[...])
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instruments()
    """

    def __init__(
        self,
        ib: object | None = None,
        project_id: str | None = None,
        api_key: str | None = None,
    ) -> None:
        """Initialise the IBKR reference data adapter.

        Args:
            ib: A connected ib_insync.IB instance (or MagicMock(spec=IB) in
                tests). The caller owns the connection lifecycle. Pass None
                only when running tests that do not invoke any IB methods.
            project_id: Deprecated. Retained for BaseReferenceDataAdapter compat.
            api_key: Not used for IBKR (auth is socket-based). Retained for
                BaseReferenceDataAdapter compat.
        """
        super().__init__(project_id=project_id, api_key=api_key)
        self._ib: object | None = ib

    @property
    def venue(self) -> str:
        return "ibkr"

    def _get_ib(self) -> object:
        """Return the injected IB connection object.

        Raises:
            RuntimeError: If no IB connection was injected (ib=None).
        """
        if self._ib is None:
            raise RuntimeError(
                "IBKRReferenceDataAdapter requires an injected IB connection. "
                "Pass a connected ib_insync.IB instance: "
                "IBKRReferenceDataAdapter(ib=ib_instance). "
                "In tests, pass MagicMock(spec=IB)."
            )
        return self._ib

    def _make_contract(self, symbol: str, sec_type: str = "STK") -> object:
        """Create an ib_insync Stock/Future/Option contract object.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            sec_type: IBKR security type ("STK", "FUT", "OPT").
        """
        if sec_type == "STK":
            return cast(object, ib_insync).Stock(symbol, "SMART", "USD")
        if sec_type == "FUT":
            return cast(object, ib_insync).Future(symbol, currency="USD")
        if sec_type in ("OPT", "FOP"):
            return cast(object, ib_insync).Option(symbol, currency="USD")
        # Generic contract fallback
        contract = cast(object, ib_insync).Contract()
        cast(object, contract).symbol = symbol
        cast(object, contract).secType = sec_type
        cast(object, contract).currency = "USD"
        return contract

    def _extract_raw_from_details(self, details: object) -> dict[str, object]:
        """Extract raw field dict from ib_insync ContractDetails object."""
        contract = getattr(details, "contract", details)
        return {
            "conid": getattr(contract, "conId", None),
            "symbol": getattr(contract, "symbol", None),
            "secType": getattr(contract, "secType", None),
            "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
            "strike": getattr(contract, "strike", None),
            "right": getattr(contract, "right", None),
            "multiplier": getattr(contract, "multiplier", None),
            "exchange": getattr(contract, "exchange", None),
            "currency": getattr(contract, "currency", None),
            "localSymbol": getattr(contract, "localSymbol", None),
            "tradingClass": getattr(contract, "tradingClass", None),
            "minTick": getattr(details, "minTick", None),
            "longName": getattr(details, "longName", None),
            "industry": getattr(details, "industry", None),
            "category": getattr(details, "category", None),
            "subcategory": getattr(details, "subcategory", None),
            "timeZoneId": getattr(details, "timeZoneId", None),
            "underConid": getattr(details, "underConId", None),
        }

    def _build_instrument_from_uac(self, uac: IBKRContractDetails) -> InstrumentRecord | None:
        """Build InstrumentRecord from validated IBKRContractDetails schema."""
        symbol = uac.symbol
        if not symbol:
            return None
        sec_type = uac.secType or "STK"
        instrument_type = _SEC_TYPE_MAP.get(sec_type, InstrumentType.SPOT)
        asset_class = _SEC_TYPE_ASSET_CLASS_MAP.get(sec_type, AssetClass.EQUITY)
        currency = uac.currency or "USD"
        tick_size = Decimal(str(uac.minTick)) if uac.minTick else Decimal("0.01")
        return InstrumentRecord(
            instrument_key=f"{symbol}:{sec_type}:{currency}",
            symbol=symbol,
            venue=self.venue,
            asset_class=asset_class,
            instrument_type=instrument_type,
            base=symbol,
            quote=currency,
            tick_size=tick_size,
            lot_size=Decimal("1"),
            contract_size=Decimal(str(uac.multiplier)) if uac.multiplier else Decimal("1"),
            status=InstrumentStatus.ACTIVE,
        )

    def _contract_details_to_instrument(
        self,
        details: object,
    ) -> InstrumentRecord | None:
        """Normalise an ib_insync ContractDetails object to InstrumentRecord.

        Parses through the UAC IBKRContractDetails schema at the parse boundary.

        Args:
            details: ib_insync ContractDetails object (or dict in tests).

        Returns:
            InstrumentRecord, or None if the symbol is missing.
        """
        raw: dict[str, object] = details if isinstance(details, dict) else self._extract_raw_from_details(details)
        uac = IBKRContractDetails.model_validate(raw)
        return self._build_instrument_from_uac(uac)

    def _resolve_sec_type_and_symbols(
        self,
        instrument_type: str | None,
    ) -> tuple[str, list[str]] | None:
        """Resolve IBKR secType and symbol list from canonical instrument_type.

        Returns:
            (sec_type, symbols) tuple, or None if instrument_type is unsupported.
        """
        if instrument_type in (None, "equity", "spot"):
            return "STK", list(_DEFAULT_EQUITY_SYMBOLS)
        if instrument_type == "future":
            return "FUT", list(_DEFAULT_EQUITY_SYMBOLS[:10])
        if instrument_type == "option":
            return "OPT", list(_DEFAULT_EQUITY_SYMBOLS[:5])
        return None

    async def _fetch_details_for_symbol(
        self,
        ib: object,
        symbol: str,
        sec_type: str,
    ) -> list[object]:
        """Fetch contract details for a single symbol. Returns [] on error."""
        contract = self._make_contract(symbol, sec_type)
        try:
            details_list = await cast(
                object,
                cast(object, ib).reqContractDetailsAsync(contract),
            )
            return cast(list[object], details_list) if details_list else []
        except Exception as exc:
            logger.warning(
                "IBKR reqContractDetails failed for symbol=%s: %s — skipping",
                symbol,
                exc,
            )
            return []

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch equity instruments via ib_insync reqContractDetails.

        Queries IBKR for contract details on a canonical set of equity symbols
        using reqContractDetailsAsync. Returns empty list with a warning if the
        IB connection is unavailable (graceful degradation).

        Args:
            instrument_type: Filter by type ("equity"/"spot"/"future"/"option").
                None returns equity (STK) instruments. Only "equity"/"spot" and
                "future"/"option" are supported via this adapter.

        Returns:
            list of InstrumentRecord, one per successfully resolved contract.
            Returns [] if IB connection is not available.
        """
        if self._ib is None:
            logger.warning(
                "IBKRReferenceDataAdapter.get_instruments called without an injected IB "
                "connection — returning empty list. Pass ib=<connected IB instance>."
            )
            return []

        resolved = self._resolve_sec_type_and_symbols(instrument_type)
        if resolved is None:
            logger.warning(
                "IBKRReferenceDataAdapter.get_instruments: unsupported instrument_type=%r "
                "— returning empty list. Supported: equity, spot, future, option.",
                instrument_type,
            )
            return []

        sec_type, symbols = resolved
        ib = self._get_ib()
        results = await self._fetch_all_symbols(ib, symbols, sec_type)
        logger.info(
            "IBKRReferenceDataAdapter.get_instruments: fetched %d instruments (instrument_type=%r, sec_type=%s)",
            len(results),
            instrument_type,
            sec_type,
        )
        return results

    async def _fetch_all_symbols(
        self,
        ib: object,
        symbols: list[str],
        sec_type: str,
    ) -> list[InstrumentRecord]:
        """Fetch and convert contract details for all symbols in the list."""
        results: list[InstrumentRecord] = []
        for symbol in symbols:
            details_list = await self._fetch_details_for_symbol(ib, symbol, sec_type)
            for details in details_list:
                instrument = self._contract_details_to_instrument(details)
                if instrument is not None:
                    results.append(instrument)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by symbol via ib_insync reqContractDetails.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").

        Returns:
            InstrumentRecord if found, None otherwise.
        """
        if self._ib is None:
            logger.warning(
                "IBKRReferenceDataAdapter.get_instrument called without injected IB — "
                "returning None. Pass ib=<connected IB instance>."
            )
            return None

        ib = self._get_ib()
        details_list = await self._fetch_details_for_symbol(ib, symbol, "STK")
        if not details_list:
            return None

        return self._contract_details_to_instrument(details_list[0])

    def _log_isin(self, symbol: str, first_details: object) -> None:
        """Log any ISIN identifiers from secIdList (TagValue pairs)."""
        sec_id_list = getattr(first_details, "secIdList", []) or []
        for tag_value in sec_id_list:
            tag = getattr(tag_value, "tag", None)
            value = getattr(tag_value, "val", None)
            if tag == "ISIN" and value:
                logger.debug("IBKR: symbol=%s ISIN=%s", symbol, value)

    def _build_ca_from_notes(
        self,
        symbol: str,
        notes: str,
        from_dt: datetime,
    ) -> CanonicalCorporateAction:
        """Build a CanonicalCorporateAction from ContractDetails.notes string."""
        ca_raw: dict[str, object] = {
            "symbol": symbol,
            "type": "Dividends" if "Dividend" in notes else "Splits",
            "description": notes[:200],
            "reportDate": from_dt.strftime("%Y-%m-%d"),
        }
        uac_ca = IBKRCorporateAction.model_validate(ca_raw)
        return CanonicalCorporateAction(
            venue=self.venue,
            symbol=uac_ca.symbol,
            action_type=_CA_TYPE_MAP.get(uac_ca.type or "", "other"),
            effective_date=from_dt,
            ratio=None,
            cash_amount=uac_ca.amount,
            currency=uac_ca.currency or "USD",
            description=uac_ca.description or "",
        )

    def _parse_ca_from_details(
        self,
        symbol: str,
        first_details: object,
        from_dt: datetime,
    ) -> list[CanonicalCorporateAction]:
        """Parse corporate actions from a single ContractDetails object.

        Extracts dividend/split hints from the details.notes field and logs
        any ISIN identifiers from the secIdList.

        Args:
            symbol: Ticker symbol being queried.
            first_details: ib_insync ContractDetails object (or dict in tests).
            from_dt: Reference date for effective_date on parsed actions.

        Returns:
            list of CanonicalCorporateAction (may be empty).
        """
        self._log_isin(symbol, first_details)
        notes = getattr(first_details, "notes", None) or ""
        if "Dividend" not in notes and "Split" not in notes:
            return []
        return [self._build_ca_from_notes(symbol, notes, from_dt)]

    async def get_corporate_actions(
        self,
        symbol: str,
        from_dt: datetime,
        to_dt: datetime | None = None,
    ) -> list[CanonicalCorporateAction]:
        """Fetch corporate actions for a symbol via ib_insync.

        Uses reqContractDetailsAsync to obtain contract details, then parses
        corporate action hints from the ContractDetails.notes field.
        Returns [] with a warning when IB is unavailable (graceful degradation).

        Args:
            symbol: Ticker symbol (e.g. "AAPL", "MSFT").
            from_dt: Start date for corporate action history (UTC).
            to_dt: End date (defaults to now if None).

        Returns:
            list of CanonicalCorporateAction. Returns [] if IB unavailable.
        """
        if to_dt is None:
            to_dt = datetime.now(UTC)

        if self._ib is None:
            logger.warning(
                "IBKRReferenceDataAdapter.get_corporate_actions called without injected "
                "IB — returning empty list. Pass ib=<connected IB instance>."
            )
            return []

        ib = self._get_ib()
        details_list = await self._fetch_details_for_symbol(ib, symbol, "STK")

        if not details_list:
            logger.debug("IBKR: no contract details for corporate actions symbol=%s", symbol)
            return []

        first_details = details_list[0]
        actions = self._parse_ca_from_details(symbol, first_details, from_dt)
        logger.info(
            "IBKRReferenceDataAdapter.get_corporate_actions: symbol=%s, range=%s\u2192%s, actions=%d",
            symbol,
            from_dt.date(),
            to_dt.date(),
            len(actions),
        )
        return actions

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError(
            "IBKR get_options_chain: use Databento adapter for TradFi options chains "
            "in batch mode. For live options data, implement reqSecDefOptParams via "
            "the injected IB object."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError(
            "IBKR get_expiry_calendar: use Databento adapter for TradFi expiry calendars in batch mode."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError(f"get_funding_rate not applicable for {self.venue} (TradFi equity venue)")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError(
            f"get_ohlcv not implemented for {self.venue} — use Databento adapter for TradFi OHLCV data"
        )

    def _build_stub_instrument(self, symbol: str) -> InstrumentRecord:
        """Build a placeholder instrument for testing purposes."""
        return InstrumentRecord(
            instrument_key=symbol,
            symbol=symbol,
            venue=self.venue,
            asset_class=AssetClass.EQUITY,
            instrument_type=InstrumentType.SPOT,
            base=symbol,
            quote="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            contract_size=Decimal("1"),
            status=InstrumentStatus.ACTIVE,
        )
