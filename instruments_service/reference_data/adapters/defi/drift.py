"""Drift reference data adapter -- instrument discovery via SDK TypeScript constants.

Discovers Drift perpetual and spot markets on Solana.
Markets are returned as InstrumentRecord with instrument_type=PERPETUAL or SPOT_PAIR.
The `instrument_key`'s middle TYPE segment matches (fixed 2026-07-09 — it
previously said the shorthand `PERP`/`SPOT`, neither of which is a real
`InstrumentType` enum member (`PERPETUAL`/`SPOT_PAIR` are); same class of fix
as the on-chain-perp PERP-vs-PERPETUAL canonicalization — see
`canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md`).

Data source: Drift SDK TypeScript constants on GitHub (MainnetPerpMarkets / MainnetSpotMarkets).
The old Data API (https://data.api.drift.trade) is dead (404/CloudFront 403 as of 2026-06).
Reference: https://docs.drift.trade/
"""

import logging
import re
from datetime import date, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import AssetGroup, build_canonical_instrument_id, classify_venue_error
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

_SDK_PERP_MARKETS_URL = get_solana_protocol_url("drift", "sdk_perp_markets_url") or (
    "https://raw.githubusercontent.com/drift-labs/protocol-v2/master/sdk/src/constants/perpMarkets.ts"
)
_SDK_SPOT_MARKETS_URL = get_solana_protocol_url("drift", "sdk_spot_markets_url") or (
    "https://raw.githubusercontent.com/drift-labs/protocol-v2/master/sdk/src/constants/spotMarkets.ts"
)
_DEFAULT_CHAIN = "SOLANA"
_DRIFT_DEPLOY_DATE = get_protocol_floor_date("drift")

# Archive metadata (is_mtds_contract_audit_2026_05_20 Phase 2).
# MTDS handlers derive S3 fetch URLs from these fields; hardcoding in MTDS is banned.
_DRIFT_PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
_DRIFT_S3_ARCHIVE_URL_TEMPLATE = (
    f"https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com/program/{_DRIFT_PROGRAM_ID}"
    "/market/{market}/{record_type}/{year}/{day}"
)
_DRIFT_ARCHIVE_RECORD_TYPES: dict[str, str] = {
    "trades": "tradeRecords",
    "funding_rate": "fundingRateRecords",
    "liquidations": "liquidationRecords",
    "orders": "orderRecords",
}
# Drift V1 S3 archive stopped writing 2025-01-08 (direct S3 probe 2026-05-20).
# Dates after this → EXPECTED_PAST_SOURCE_COVERAGE_END.
_DRIFT_ARCHIVE_COVERAGE_START: dict[str, date] = {
    "trades": date(2020, 9, 14),
    "funding_rate": date(2021, 5, 1),
    "liquidations": date(2020, 9, 14),
    "orders": date(2020, 9, 14),
}
_DRIFT_ARCHIVE_COVERAGE_END: dict[str, date] = {
    "trades": date(2025, 1, 8),
    "funding_rate": date(2025, 1, 8),
    "liquidations": date(2025, 1, 8),
    "orders": date(2025, 1, 8),
}


def _classify_drift_error(exc: Exception, status: int | None = None) -> str:
    """Map a Drift HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class DriftReferenceDataAdapter(BaseReferenceDataAdapter):
    """Drift reference data: perp and spot market discovery via Data API.

    Uses the public Data API (https://data.api.drift.trade/stats/markets)
    which requires no auth and returns all 137 markets (74 perp + 63 spot).
    Each Drift perp market produces one instrument with instrument_type=PERPETUAL.
    Drift settles in USDC.
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
        """Return the venue identifier."""
        return f"DRIFT-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Drift."""
        error_code = _classify_drift_error(exc)
        classification = classify_venue_error("drift", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Drift %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch active Drift perp and spot markets as instruments."""
        markets = await self._fetch_all_markets()
        results: list[InstrumentRecord] = []

        for market in markets:
            market_type = str(market.get("marketType", ""))
            status = str(market.get("status", ""))
            if status != "active":
                continue

            if market_type == "perp" and instrument_type in (None, InstrumentType.PERPETUAL):
                record = self._build_perp_record(market)
                if record:
                    results.append(record)
            elif market_type == "spot" and instrument_type in (None, InstrumentType.SPOT_PAIR):
                record = self._build_spot_record(market)
                if record:
                    results.append(record)

        logger.info("Drift: fetched %d instruments on %s", len(results), self._chain)
        return results

    async def _fetch_ts_content(
        self,
        session: aiohttp.ClientSession,
        url: str,
        label: str,
    ) -> str:
        """Fetch raw TypeScript file content as text from a GitHub raw URL."""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                return await resp.text()
        except aiohttp.ClientError as exc:
            self._log_fetch_error(exc, label)
            raise ConnectionError(str(exc)) from exc

    @staticmethod
    def _parse_ts_markets(content: str, market_type: str) -> list[dict[str, object]]:
        """Parse Drift SDK TypeScript market constants into normalised dicts.

        Returns dicts with keys: symbol, baseAsset, marketType, status.
        Filters DELISTED markets (status != 'active').
        """
        section_name = "MainnetPerpMarkets" if market_type == "perp" else "MainnetSpotMarkets"
        section_pat = re.compile(
            rf"{re.escape(section_name)}\s*(?::\s*\w[\w<>\[\], ]*?)?\s*=\s*\[",
        )
        m = section_pat.search(content)
        if not m:
            logger.warning("Drift SDK: could not locate %s in TS content", section_name)
            return []
        # Bracket-depth walk to extract the array section
        start = m.end()
        depth = 1
        pos = start
        while pos < len(content) and depth > 0:
            ch = content[pos]
            if ch in ("[", "{"):
                depth += 1
            elif ch in ("]", "}"):
                depth -= 1
            pos += 1
        section = content[start : pos - 1]
        results: list[dict[str, object]] = []
        obj_pat = re.compile(r"\{([^{}]*)\}", re.DOTALL)
        sym_pat = re.compile(r"""symbol\s*:\s*['"]([^'"]+)['"]""")
        base_pat = re.compile(r"""baseAssetSymbol\s*:\s*['"]([^'"]+)['"]""")
        delisted_pat = re.compile(r"DELISTED", re.IGNORECASE)
        for obj_m in obj_pat.finditer(section):
            obj_text = obj_m.group(1)
            sym_m = sym_pat.search(obj_text)
            if not sym_m:
                continue
            symbol = sym_m.group(1)
            if market_type == "perp":
                base_m = base_pat.search(obj_text)
                base_asset = base_m.group(1) if base_m else symbol.split("-")[0]
            else:
                base_asset = symbol.upper()
            status = "delisted" if delisted_pat.search(obj_text) else "active"
            results.append(
                {
                    "symbol": symbol,
                    "baseAsset": base_asset,
                    "marketType": market_type,
                    "status": status,
                }
            )
        return results

    async def _fetch_all_markets(self) -> list[dict[str, object]]:
        """Fetch all markets from Drift SDK TypeScript constants on GitHub."""
        async with self._make_session() as session:
            perp_content = await self._fetch_ts_content(session, _SDK_PERP_MARKETS_URL, "sdk/perpMarkets.ts")
            spot_content = await self._fetch_ts_content(session, _SDK_SPOT_MARKETS_URL, "sdk/spotMarkets.ts")
        perp_markets = self._parse_ts_markets(perp_content, "perp")
        spot_markets = self._parse_ts_markets(spot_content, "spot")
        return [*perp_markets, *spot_markets]

    def _build_perp_record(
        self,
        market: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Drift perp market."""
        symbol = str(market.get("symbol", ""))
        if not symbol:
            return None

        # Prefer SDK-provided baseAsset; fall back to splitting the symbol string
        raw_base = market.get("baseAsset") or market.get("baseAssetSymbol", "")
        base_asset = (
            str(raw_base).upper() if raw_base else (symbol.split("-")[0].upper() if "-" in symbol else symbol.upper())
        )

        venue_tag = self.venue
        # Key/field mismatch fix (2026-07-09, C4) + builder retrofit — TYPE
        # segment changed PERP -> PERPETUAL to match instrument_type below.
        instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI, venue_tag, InstrumentType.PERPETUAL, symbol, passthrough=True
        )

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=symbol,
            instrument_type=InstrumentType.PERPETUAL,
            base_asset=base_asset,
            quote_asset="USDC",
            settle_asset="USDC",
            margin_type=MarginType.LINEAR,
            tick_size=Decimal("0.0001"),
            min_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_DRIFT_DEPLOY_DATE,
            timezone="UTC",
            listed_at=_DRIFT_DEPLOY_DATE.date() if _DRIFT_DEPLOY_DATE else None,
            source_archive_url_template=_DRIFT_S3_ARCHIVE_URL_TEMPLATE,
            source_record_types=_DRIFT_ARCHIVE_RECORD_TYPES,
            source_coverage_start=_DRIFT_ARCHIVE_COVERAGE_START,
            source_coverage_end=_DRIFT_ARCHIVE_COVERAGE_END,
        )

    def _build_spot_record(
        self,
        market: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Drift spot market."""
        symbol = str(market.get("symbol", ""))
        base_asset = str(market.get("baseAsset", symbol)).upper()
        if not base_asset:
            return None

        venue_tag = self.venue
        # Key/field mismatch fix (2026-07-09, C4) + builder retrofit — TYPE
        # segment changed SPOT -> SPOT_PAIR to match instrument_type below.
        instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI, venue_tag, InstrumentType.SPOT_PAIR, base_asset, passthrough=True
        )

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=symbol or base_asset,
            instrument_type=InstrumentType.SPOT_PAIR,
            base_asset=base_asset,
            quote_asset="USDC",
            tick_size=Decimal("0.0001"),
            min_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_DRIFT_DEPLOY_DATE,
            timezone="UTC",
            listed_at=_DRIFT_DEPLOY_DATE.date() if _DRIFT_DEPLOY_DATE else None,
            source_archive_url_template=_DRIFT_S3_ARCHIVE_URL_TEMPLATE,
            source_record_types=_DRIFT_ARCHIVE_RECORD_TYPES,
            source_coverage_start=_DRIFT_ARCHIVE_COVERAGE_START,
            source_coverage_end=_DRIFT_ARCHIVE_COVERAGE_END,
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Drift does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Drift markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Drift funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Drift OHLCV not supported via reference data")
