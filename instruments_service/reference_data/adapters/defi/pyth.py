"""Pyth Network oracle price adapter — Solana on-chain price feed discovery.

Discovers Pyth price feed accounts for major Solana asset pairs. Returns oracle
price feeds as InstrumentRecord with instrument_type=SPOT, capturing confidence
intervals and publish_time from the Hermes batch endpoint.

Pyth is UNBANNED for Solana on-chain price feeds (CLAUDE.md 2026-05-06).
Solana-only; other chains use Chainlink.

Data source: Pyth Hermes REST (https://hermes.pyth.network/v2/) — batch prices.
             Pyth on-chain live (https://pythnet.rpcpool.com/) — for live pipeline.
Program ID: rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ (Pythnet receiver)
Reference: https://docs.pyth.network/

Pyth price feed IDs for major Solana pairs (SSOT — never hardcode elsewhere):
  SOL/USD:     H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG
  JITOSOL/USD: 7yyaeuJ1GGtVBLT2z2xub5ZWYKaNhF28mj1RdV4VDFVk
  mSOL/USD:    E4v1BBgoso9s64TQvmyownAVJbhbEPGyzA3qn4n46qj9
  bSOL/USD:    AFrYBhb5wKQtxRS9UA9YRS4V3oFXrfW1Kq3hZedLgGiP
  JUP/USD:     7dbob1psH1iZBS7qPsm3Kwbf5DzSXK8Jyg31CTgTnxH5
  RAY/USD:     AnLf8tVYCM816gmBjiy8n53eXKKEDydT5piYjjQDPgTB
  BONK/USD:    8ihFLu5FimgTQ1Unh4dVyEHUGodJ738bWMDule9otdKX
  WIF/USD:     6ABgrEZkHDexkBEBhXAHmCFmcj1mNLyyhGEe3VC2DfcK
  JTO/USD:     nJnMsAf9Es6SKY9YaB88weinqoNVrav3wGD7SVoiPaC
  USDC/USD:    Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.registry import get_solana_protocol_url
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_HERMES_API_URL = get_solana_protocol_url("pyth", "api_url") or "https://hermes.pyth.network/v2"
_PYTHNET_URL = get_solana_protocol_url("pyth", "pythnet_url") or "https://pythnet.rpcpool.com"
_DEFAULT_CHAIN = "SOLANA"
_PYTH_DEPLOY_DATE = get_protocol_floor_date("pyth")

# Pyth price feed IDs for major Solana pairs (SSOT — sourced from https://pyth.network/price-feeds)
# Format: symbol -> (feed_id, description)
PYTH_PRICE_FEEDS: dict[str, tuple[str, str]] = {
    "SOL/USD": (
        "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG",
        "Pyth SOL/USD price feed",
    ),
    "JITOSOL/USD": (
        "7yyaeuJ1GGtVBLT2z2xub5ZWYKaNhF28mj1RdV4VDFVk",
        "Pyth JitoSOL/USD price feed",
    ),
    "MSOL/USD": (
        "E4v1BBgoso9s64TQvmyownAVJbhbEPGyzA3qn4n46qj9",
        "Pyth mSOL/USD price feed",
    ),
    "BSOL/USD": (
        "AFrYBhb5wKQtxRS9UA9YRS4V3oFXrfW1Kq3hZedLgGiP",
        "Pyth bSOL/USD price feed",
    ),
    "JUP/USD": (
        "7dbob1psH1iZBS7qPsm3Kwbf5DzSXK8Jyg31CTgTnxH5",
        "Pyth JUP/USD price feed",
    ),
    "RAY/USD": (
        "AnLf8tVYCM816gmBjiy8n53eXKKEDydT5piYjjQDPgTB",
        "Pyth RAY/USD price feed",
    ),
    "BONK/USD": (
        "8ihFLu5FimgTQ1Unh4dVyEHUGodJ738bWMDule9otdKX",
        "Pyth BONK/USD price feed",
    ),
    "WIF/USD": (
        "6ABgrEZkHDexkBEBhXAHmCFmcj1mNLyyhGEe3VC2DfcK",
        "Pyth WIF/USD price feed",
    ),
    "JTO/USD": (
        "nJnMsAf9Es6SKY9YaB88weinqoNVrav3wGD7SVoiPaC",
        "Pyth JTO/USD price feed",
    ),
    "USDC/USD": (
        "Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD",
        "Pyth USDC/USD price feed",
    ),
}


def _classify_pyth_error(exc: Exception, status: int | None = None) -> str:
    """Map a Pyth HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class PythOracleReferenceDataAdapter(BaseReferenceDataAdapter):
    """Pyth Network oracle reference data: price feed discovery via Hermes REST API.

    Uses the Pyth Hermes batch endpoint (https://hermes.pyth.network/v2/updates/price/latest)
    to discover price feeds for major Solana assets. Each feed is represented
    as a SPOT instrument with Pyth as the venue.

    This adapter is used by the instruments-service to expose oracle price
    metadata. The actual oracle prices for the arbitrage_price_dispersion
    strategy are fetched by the market-tick-data-service (MTDS) at runtime.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()

    @property
    def venue(self) -> str:
        return f"PYTH-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Pyth."""
        error_code = _classify_pyth_error(exc)
        classification = classify_venue_error("pyth", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Pyth %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            endpoint,
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": endpoint,
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Pyth oracle price feed instruments.

        All Pyth feeds are represented as SPOT instruments (oracle price references).
        """
        if instrument_type not in (None, InstrumentType.SPOT, "spot"):
            logger.info("Pyth oracle only supports SPOT instruments; requested %s", instrument_type)
            return []

        results: list[InstrumentRecord] = []
        for symbol, (feed_id, description) in PYTH_PRICE_FEEDS.items():
            record = self._build_feed_record(symbol, feed_id, description)
            if record:
                results.append(record)

        logger.info("Pyth: built %d oracle price feed instruments on %s", len(results), self._chain)
        return results

    async def fetch_latest_prices(
        self,
        feed_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        """Fetch latest prices from Pyth Hermes for given feed IDs.

        Args:
            feed_ids: List of Pyth price feed IDs. Defaults to all PYTH_PRICE_FEEDS.

        Returns:
            List of price update dicts from Hermes API.
        """
        if feed_ids is None:
            feed_ids = [feed_id for feed_id, _ in PYTH_PRICE_FEEDS.values()]

        # Build Hermes batch query
        ids_params = "&".join(f"ids[]={fid}" for fid in feed_ids)
        url = f"{_HERMES_API_URL}/updates/price/latest?{ids_params}&encoding=hex&parsed=true"

        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "updates/price/latest")
                raise ConnectionError(str(exc)) from exc
            logger.error("Pyth Hermes price fetch failed after retries: %s", exc)
            raise

        raw: dict[str, object] = data if isinstance(data, dict) else {}
        parsed = raw.get("parsed")
        if isinstance(parsed, list):
            return parsed
        return []

    def _build_feed_record(
        self,
        symbol: str,
        feed_id: str,
        description: str,
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord for a Pyth oracle price feed.

        The feed_id is stored as raw_symbol for traceability back to
        the on-chain account. The symbol is stored as the human-readable
        base_asset/quote_asset pair.
        """
        if not symbol or not feed_id:
            return None

        parts = symbol.split("/")
        if len(parts) != 2:
            return None

        base_asset = parts[0].strip().upper()
        quote_asset = parts[1].strip().upper()

        instrument_key = f"{self.venue}:SPOT:{base_asset}-{quote_asset}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            raw_symbol=feed_id,  # Pyth feed ID as canonical identifier
            instrument_type=InstrumentType.SPOT,
            base_asset=base_asset,
            quote_asset=quote_asset,
            settle_asset=quote_asset,
            margin_type=MarginType.LINEAR,
            tick_size=Decimal("0.000001"),  # Pyth publishes sub-cent precision
            min_size=Decimal("0"),  # Oracle prices have no minimum trade size
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_PYTH_DEPLOY_DATE,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        sym_upper = symbol.upper()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == sym_upper:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Pyth does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Pyth oracle feeds have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Pyth funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Pyth OHLCV not supported via reference data")
