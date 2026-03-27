"""CCXT generic reference data adapter — wraps ccxt for multi-venue support."""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import ccxt.async_support as ccxta
from unified_api_contracts.internal import InstrumentRecord

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)


class CCXTReferenceDataAdapter(BaseReferenceDataAdapter):
    """Generic CCXT-backed reference data adapter.

    Delegates to ccxt async exchange instances for venues not yet given dedicated adapters.
    Requires ccxt to be installed: uv pip install ccxt
    """

    def __init__(
        self,
        venue: str,
        project_id: str | None = None,
        api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._venue = venue

    @property
    def venue(self) -> str:
        return self._venue

    def _get_exchange(self) -> ccxta.Exchange:
        """Instantiate a ccxt async exchange by venue name."""
        exchange_class = getattr(ccxta, self._venue, None)
        if exchange_class is None:
            raise RuntimeError(
                f"ccxt does not support exchange {self._venue!r}. "
                "Check ccxt.exchanges for the list of supported venues."
            )
        return exchange_class()  # type: ignore[no-any-return]

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active markets via ccxt load_markets and map to InstrumentRecord list."""
        exchange = self._get_exchange()
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        try:
            markets: dict[str, object] = await exchange.load_markets()
        finally:
            await exchange.close()
        for symbol, market_raw in markets.items():
            record = self._parse_ccxt_market(symbol, market_raw, now, instrument_type)
            if record is not None:
                results.append(record)
        return results

    @staticmethod
    def _map_ccxt_type(ccxt_type: str) -> str:
        """Map ccxt market type string to URDI instrument_type."""
        if ccxt_type == "spot":
            return "spot"
        if ccxt_type in ("swap", "perpetual"):
            return "perp"
        if ccxt_type == "future":
            return "future"
        if ccxt_type == "option":
            return "option"
        return ccxt_type

    @staticmethod
    def _extract_market_sizes(
        market: dict[str, object],
    ) -> tuple[object, object, object, object]:
        """Extract tick_raw, lot_raw, min_raw, contract_size_raw from a ccxt market dict."""
        precision_raw: object = market.get("precision")
        precision: dict[str, object] = precision_raw if isinstance(precision_raw, dict) else {}
        limits_raw: object = market.get("limits")
        limits: dict[str, object] = limits_raw if isinstance(limits_raw, dict) else {}
        amount_raw: object = limits.get("amount")
        amount_limits: dict[str, object] = amount_raw if isinstance(amount_raw, dict) else {}
        return (
            precision.get("price"),
            precision.get("amount"),
            amount_limits.get("min"),
            market.get("contractSize"),
        )

    def _parse_ccxt_market(
        self,
        symbol: str,
        market_raw: object,
        now: datetime,
        instrument_type: str | None,
    ) -> InstrumentRecord | None:
        """Map a single ccxt market dict to an InstrumentRecord."""
        if not isinstance(market_raw, dict):
            return None
        market: dict[str, object] = market_raw
        if not market.get("active", True):
            return None
        mapped_type = self._map_ccxt_type(str(market.get("type") or "spot"))
        if instrument_type is not None and mapped_type != instrument_type:
            return None
        base = str(market.get("base") or "")
        quote = str(market.get("quote") or "")
        settle = market.get("settle")
        expiry = self._parse_ccxt_expiry(market.get("expiryDatetime"))
        tick_raw, lot_raw, min_raw, contract_size_raw = self._extract_market_sizes(market)
        return InstrumentRecord(
            instrument_key=symbol,
            venue=self.venue,
            symbol=symbol,
            raw_symbol=str(market.get("id") or symbol),
            instrument_type=mapped_type,
            base_asset=base,
            quote_asset=quote,
            tick_size=Decimal(str(tick_raw)) if tick_raw is not None else Decimal("0.01"),
            lot_size=Decimal(str(lot_raw)) if lot_raw is not None else Decimal("0.001"),
            min_order_size=Decimal(str(min_raw)) if min_raw is not None else Decimal("0.001"),
            contract_size=Decimal(str(contract_size_raw)) if contract_size_raw else Decimal("1"),
            settlement_asset=str(settle) if settle else None,
            expiry=expiry,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
        )

    @staticmethod
    def _parse_ccxt_expiry(expiry_ts: object) -> datetime | None:
        """Parse ccxt expiryDatetime string to UTC datetime."""
        if not expiry_ts:
            return None
        try:
            return datetime.fromisoformat(str(expiry_ts).replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by symbol via ccxt load_markets."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Build options chain from ccxt markets filtered by underlying and expiry."""
        instruments = await self.get_instruments(instrument_type="option")
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            if inst.base_asset != underlying.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            if inst.option_type and inst.option_type.lower().startswith("c"):
                calls.append(inst)
            else:
                puts.append(inst)
        now = datetime.now(UTC)
        target_expiry = expiry or (calls[0].expiry if calls else now)
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=target_expiry or now,
            strikes=sorted(strikes),
            calls=calls,
            puts=puts,
            fetched_at=now,
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        """Build expiry calendar from ccxt markets for the given underlying."""
        instruments = await self.get_instruments(instrument_type=instrument_type)
        expiry_set: set[datetime] = set()
        for inst in instruments:
            if inst.base_asset == underlying.upper() and inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch current funding rate via ccxt fetch_funding_rate (unified method).

        The ccxt FundingRate structure includes fundingRate and nextFundingDatetime keys.
        """
        exchange = self._get_exchange()
        now = datetime.now(UTC)
        try:
            await exchange.load_markets()
            rate_data: dict[str, object] = await exchange.fetch_funding_rate(symbol)
        finally:
            await exchange.close()

        rate_raw = rate_data.get("fundingRate", 0) or 0
        mark_price_raw = rate_data.get("markPrice")
        next_dt_raw = rate_data.get("nextFundingDatetime")
        next_funding_time: datetime
        if next_dt_raw:
            try:
                next_funding_time = datetime.fromisoformat(str(next_dt_raw).replace("Z", "+00:00")).astimezone(UTC)
            except ValueError:
                next_funding_time = now
        else:
            next_funding_time = now

        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=Decimal(str(mark_price_raw)) if mark_price_raw is not None else None,
            updated_at=now,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars via ccxt fetch_ohlcv (unified method).

        Each bar from ccxt is [timestamp_ms, open, high, low, close, volume].
        """
        exchange = self._get_exchange()
        try:
            await exchange.load_markets()
            raw_bars: list[list[object]] = await exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
        finally:
            await exchange.close()

        results: list[OHLCVRef] = []
        for bar in raw_bars:
            if len(bar) < 6:
                continue
            ts_ms = int(str(bar[0]))
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
            results.append(
                OHLCVRef(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(bar[1])),
                    high=Decimal(str(bar[2])),
                    low=Decimal(str(bar[3])),
                    close=Decimal(str(bar[4])),
                    volume=Decimal(str(bar[5])),
                    interval=interval,
                )
            )
        return results
