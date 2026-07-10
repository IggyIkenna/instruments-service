"""Solblaze reference data adapter — instrument discovery for the bSOL LST.

Discovers the Solblaze liquid staking token (bSOL) on Solana.
Token is returned as InstrumentRecord with instrument_type="LST" (fixed
2026-07-09 — key/field mismatch, same class as PERP-vs-PERPETUAL; see
`lido.py`'s module docstring for the full rationale).

Pure static-registry adapter: get_instruments returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.

References:
- https://solblaze.org/
- bSOL token mint: bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1
- Solblaze stake pool program: stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov84HgPwr
- Launch date: 2022-02-17 (conservative mainnet-launch floor from chain records).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts import AssetGroup, build_canonical_instrument_id
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "SOLANA"

# Solblaze mainnet launch (2022-02-17) — first bSOL mint from chain records.
# PROTOCOL_LAUNCH_DATES does not yet have a SOLBLAZE entry; date sourced from
# on-chain genesis of the stake pool program.
_SOLBLAZE_DEPLOY_DATE = datetime(2022, 2, 17, tzinfo=UTC)

# bSOL token mint on Solana mainnet (9 decimals — standard SPL token).
_BSOL_MINT = "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"
_BSOL_DECIMALS = 9

# MTDS handlers derive the exchange-rate fetch URL from this field; hardcoding is banned.
# Fixed 2026-07-10 (mtds_is_full_adapter_smoketest_findings_2026_07_07.md P2/P3):
# /api/v1/exchange_rate 404s (confirmed live 2026-07-10); /api/v1/stats is the real
# working endpoint and is the semantically-correct replacement — its
# stats.conversion.bsol_to_sol field is a genuine exchange rate, unlike /api/v1/apy
# (also live/working, but APY-only, no exchange-rate field).
_SOLBLAZE_EXCHANGE_RATE_URL_TEMPLATE = "https://stake.solblaze.org/api/v1/stats"

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "BSOL",
        "mint_address": _BSOL_MINT,
        "underlying": "SOL",
    },
]


class SolblazeReferenceDataAdapter(BaseReferenceDataAdapter):
    """Solblaze reference data: bSOL LST token discovery.

    Solblaze launched on Solana mainnet 2022-02-17. bSOL is a yield-bearing
    liquid staking token; its exchange rate vs SOL appreciates as Solana
    staking rewards accrue across the Solblaze stake pool.
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
        return "solblaze"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Solblaze LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SOLBLAZE-{self._chain}"

        for token in _LST_TOKENS:
            symbol = token["symbol"]
            mint = token["mint_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    # Routed through the shared canonical builder (2026-07-09 retrofit,
                    # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 1) — DRY,
                    # no output change.
                    instrument_key=build_canonical_instrument_id(
                        AssetGroup.DEFI, venue_tag, InstrumentType.LST, symbol, passthrough=True
                    ),
                    venue=venue_tag,
                    raw_symbol=mint,
                    instrument_type=InstrumentType.LST,
                    base_asset=underlying,
                    quote_asset="",
                    tick_size=Decimal("0.000000001"),
                    min_size=Decimal("0.000000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=underlying,
                    available_from_datetime=_SOLBLAZE_DEPLOY_DATE,
                    base_asset_contract_address=mint,
                    base_asset_decimals=_BSOL_DECIMALS,
                    source_archive_url_template=_SOLBLAZE_EXCHANGE_RATE_URL_TEMPLATE,
                )
            )

        logger.info("Solblaze: fetched %d LST instruments on %s", len(results), self._chain)
        return results

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
        raise NotImplementedError("Solblaze does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Solblaze bSOL has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Solblaze bSOL has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Solblaze OHLCV not supported via reference data")
