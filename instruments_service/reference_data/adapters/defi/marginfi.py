"""MarginFi reference data adapter — instrument discovery via the MarginFi public bank cache.

Discovers MarginFi lending-market Banks on Solana. Each Bank emits a
supply-side ``A_TOKEN`` instrument (collateral deposited into the bank) and a
borrow-side ``DEBT_TOKEN`` instrument (asset borrowed against the pooled
liquidity) — the same A_TOKEN/DEBT_TOKEN split ``aave_v3.py`` / ``morpho.py``
use per reserve/market
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md). Every Bank in
MarginFi's isolated cross-collateral pool design is both depositable and
borrowable, so both legs are always emitted.

Data source: MarginFi has no dedicated public REST API for bank discovery —
bank state lives entirely on-chain as Anchor program accounts under the
marginfi-v2 program (``MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA``), and
the officially-documented access path is the TypeScript/Rust SDK reading
those accounts directly (https://docs.marginfi.com/mfi-v2). MarginFi's own
web app (app.marginfi.com) avoids a full on-chain program-account scan for
its bank list by reading a lightweight public JSON cache it publishes to
Google Cloud Storage — the same cache is used here as the real, live,
documented-by-usage HTTP data source (no API key, no RPC rate limits):
    - https://storage.googleapis.com/mrgn-public/mrgn-bank-metadata-cache.json
    - https://storage.googleapis.com/mrgn-public/mrgn-token-metadata-cache.json

Reference: https://docs.marginfi.com/ ; https://github.com/mrgnlabs/marginfi-v2
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import AssetGroup, build_canonical_instrument_id, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._solana_utils import batch_resolve_creation_timestamps, get_protocol_floor_date

logger = logging.getLogger(__name__)

_BANK_METADATA_URL = "https://storage.googleapis.com/mrgn-public/mrgn-bank-metadata-cache.json"
_TOKEN_METADATA_URL = "https://storage.googleapis.com/mrgn-public/mrgn-token-metadata-cache.json"
_DEFAULT_CHAIN = "SOLANA"
_MARGINFI_DEPLOY_DATE = get_protocol_floor_date("marginfi")

# MarginFi v2 program address (mainnet) — matches
# unified_api_contracts.registry.capability_declarations._defi_chain_data
# .SOLANA_DEFI_PROTOCOLS["marginfi"]["program_id"].
_MARGINFI_PROGRAM_ID = "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"


def _classify_marginfi_error(exc: Exception, status: int | None = None) -> str:
    """Map a MarginFi HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


def _sanitize_symbol(symbol: str) -> str:
    """Strip whitespace from a raw bank token symbol for safe instrument_key embedding."""
    return symbol.replace(" ", "")


class MarginfiReferenceDataAdapter(BaseReferenceDataAdapter):
    """MarginFi reference data: lending-market Bank discovery from the public bank cache.

    Fetches MarginFi's public bank + token metadata caches and emits one
    A_TOKEN + one DEBT_TOKEN instrument per Bank (supply/borrow split).
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
        return f"MARGINFI-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for MarginFi."""
        error_code = _classify_marginfi_error(exc)
        classification = classify_venue_error("marginfi", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "MarginFi %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            endpoint,
            exc,
            error_code,
            action,
            retry_safe,
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch MarginFi Banks (from the public bank/token metadata caches) as instruments."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        banks, token_decimals = await self._fetch_bank_and_token_caches()
        if not banks:
            return []

        addresses = [str(b.get("bankAddress", "")) for b in banks if b.get("bankAddress")]
        ts_map = await batch_resolve_creation_timestamps(addresses) if addresses else {}

        venue_tag = self.venue
        results: list[InstrumentRecord] = []
        for bank in banks:
            available_since = ts_map.get(str(bank.get("bankAddress", "")), _MARGINFI_DEPLOY_DATE)
            results.extend(self._build_bank_records(bank, token_decimals, venue_tag, available_since))

        logger.info(
            "MarginFi: fetched %d supply/debt instruments from %d banks",
            len(results),
            len(banks),
        )
        return results

    async def _fetch_bank_and_token_caches(
        self,
    ) -> tuple[list[dict[str, object]], dict[str, int]]:
        """Fetch + join the public bank-metadata and token-metadata caches.

        Returns (banks, mint_address -> decimals).
        """
        try:
            async with self._make_session() as session:
                bank_data = await self._get_with_retry(session, _BANK_METADATA_URL)
                token_data = await self._get_with_retry(session, _TOKEN_METADATA_URL)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "bank_or_token_metadata_cache")
                raise ConnectionError(str(exc)) from exc
            logger.error("MarginFi metadata cache request failed after retries: %s", exc)
            raise

        banks: list[dict[str, object]] = bank_data if isinstance(bank_data, list) else []
        tokens: list[dict[str, object]] = token_data if isinstance(token_data, list) else []

        token_decimals: dict[str, int] = {}
        for tok in tokens:
            addr = str(tok.get("address", ""))
            decimals = tok.get("decimals")
            if addr and isinstance(decimals, int):
                token_decimals[addr] = decimals

        return banks, token_decimals

    @staticmethod
    def _build_bank_records(
        bank: dict[str, object],
        token_decimals: dict[str, int],
        venue_tag: str,
        available_since: datetime,
    ) -> list[InstrumentRecord]:
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one MarginFi Bank."""
        bank_address = str(bank.get("bankAddress", ""))
        mint = str(bank.get("tokenAddress", ""))
        raw_symbol_field = str(bank.get("tokenSymbol", ""))
        if not bank_address or not mint or not raw_symbol_field:
            return []

        # base_asset_decimals is REQUIRED (non-null) for DeFi on-chain
        # instrument types (workspace rule: null decimals is a silent price-
        # normalisation bug, not a "best-effort" field). If the mint has no
        # match in the token-metadata cache, skip this bank honestly rather
        # than fabricate a guessed decimals value (never a fake placeholder).
        decimals_int = token_decimals.get(mint)
        if decimals_int is None:
            logger.warning(
                "MarginFi: skipping bank %s (%s) — mint %s has no token-metadata-cache decimals entry",
                bank_address,
                raw_symbol_field,
                mint,
            )
            return []

        symbol = _sanitize_symbol(raw_symbol_field)

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": bank_address,
            "base_asset": raw_symbol_field,
            "quote_asset": "",
            "tick_size": Decimal("0.000001"),
            "min_size": Decimal("0.000001"),
            "contract_size": Decimal("1"),
            "expiry": None,
            "strike": None,
            "option_type": None,
            "status": InstrumentStatus.ACTIVE,
            "underlying": raw_symbol_field,
            "available_from_datetime": available_since,
            # DeFi metadata
            "pool_address": bank_address,
            "base_asset_contract_address": mint,
            "base_asset_decimals": decimals_int,
            "base_asset_symbol_onchain": raw_symbol_field,
        }

        a_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "marginfi",
            InstrumentType.A_TOKEN,
            f"A{symbol}",
            chain="SOLANA",
            passthrough=True,
        )
        debt_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "marginfi",
            InstrumentType.DEBT_TOKEN,
            f"DEBT{symbol}",
            chain="SOLANA",
            passthrough=True,
        )

        return [
            InstrumentRecord(
                instrument_key=a_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=a_token_instrument_key,
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=debt_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=debt_token_instrument_key,
                instrument_type=InstrumentType.DEBT_TOKEN,
                **base_kwargs,
            ),
        ]

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key.endswith(f":{symbol}"):
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("MarginFi does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("MarginFi lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("MarginFi lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("MarginFi OHLCV not supported via reference data")
