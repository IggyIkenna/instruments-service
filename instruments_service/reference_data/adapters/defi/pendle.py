"""Pendle Finance reference data adapter — instrument discovery for PT/YT/SY tokens.

Pendle is a yield-tokenization protocol that splits a yield-bearing asset into
three SPL/ERC-20 tokens per market:

* SY (Standardized Yield) — wraps the underlying yield-bearing asset; no
  expiry, redeemable 1:1 for the underlying.
* PT (Principal Token) — represents the principal at maturity. Trades at a
  discount; redeems 1:1 for the underlying at expiry. Implies a fixed yield
  for buyers held-to-maturity.
* YT (Yield Token) — represents the variable yield streamed between issuance
  and maturity. Decays toward zero at expiry.

Each Pendle market is identified by its (underlying, maturity) tuple and has
its own AMM contract that quotes PT<->SY swaps. PT/YT/SY tokens for one market
are sibling contracts.

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated
snapshot of CURRENTLY-ACTIVE markets (maturity > today as of 2026-05-12) on
Ethereum + Arbitrum. No network access. Tests are credential-free and offline.

Encoding choice — instrument_type
---------------------------------
The closest UAC ``InstrumentType`` value for Pendle PT/YT/SY is ``YIELD_BEARING``
(no PT/YT-specific enum exists). The role (PT/YT/SY) is encoded in the
``instrument_key`` segment, e.g. ``PENDLE-ETHEREUM:PT:PT-stETH-25JUN2026``.
PT/YT carry an ``expiry`` datetime; SY carries ``expiry=None`` (no maturity).

References
----------
* https://pendle.finance/
* https://app.pendle.finance/trade/markets
* https://docs.pendle.finance/
* Active-markets source: Pendle V2 official REST API
  ``https://api-v2.pendle.finance/core/v1/{chainId}/markets/active`` (queried
  2026-05-12).
* Launch dates from ``unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES``:
  ``("ETHEREUM", "PENDLE")  == 2021-06-15`` (Pendle V1 mainnet),
  ``("ARBITRUM", "PENDLE")  == 2024-01-26`` (Pendle V2 Arbitrum expansion).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Pendle V1 Ethereum mainnet GA (2021-06-15) — mirrors
# PROTOCOL_LAUNCH_DATES[("ETHEREUM", "PENDLE")].
_PENDLE_ETH_DEPLOY_DATE = datetime(2021, 6, 15, tzinfo=UTC)
# Pendle V2 Arbitrum GA (2024-01-26) — mirrors
# PROTOCOL_LAUNCH_DATES[("ARBITRUM", "PENDLE")].
_PENDLE_ARB_DEPLOY_DATE = datetime(2024, 1, 26, tzinfo=UTC)

# Curated snapshot of Pendle V2 markets currently active as of 2026-05-12
# (maturity strictly > today). Each entry yields THREE InstrumentRecord rows
# (PT, YT, SY) sharing the same market AMM but with distinct token addresses.
#
# Address provenance: Pendle V2 official REST API (api-v2.pendle.finance) —
# `https://api-v2.pendle.finance/core/v1/{chainId}/markets/active` queried
# 2026-05-12. Cross-verified against https://app.pendle.finance/trade/markets.
# The API returns addresses prefixed with the EIP-155 chain id (e.g. "1-0x..."
# or "42161-0x...") — only the bare hex address is stored here.
_PENDLE_MARKETS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "wstETH-25JUN2026",
            "underlying": "wstETH",
            "pt_address": "0x9ce6478ef45bb1baac69efd8a3ea0ed110a43042",
            "yt_address": "0x12cc7b6ee36b1a33ebc33dc41c39d383b3b33896",
            "sy_address": "0xcbc72d92b2dc8187414f6734718563898740c0bc",
            "market_address": "0xcfd848b9f6fef552204014ac67901223ad6bf679",
            "underlying_token_address": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "weETH-25JUN2026",
            "underlying": "weETH",
            "pt_address": "0x8e8b8d3b2dca78cb04b9914b7ec1ad72f671f96d",
            "yt_address": "0xf25586be8b7bbcdbf1585ffe02d50c6d1f2bc51b",
            "sy_address": "0xac0047886a985071476a1186be89222659970d65",
            "market_address": "0x95a9504f246cd65bf91c2c550a53f7f415227ed1",
            "underlying_token_address": "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "weETHs-25JUN2026",
            "underlying": "weETHs",
            "pt_address": "0x6cc3237ae1bbeba960c35b47b0424d34b6404037",
            "yt_address": "0xe25310adc40bd9ed6ea0eefe22bdff208d8e207d",
            "sy_address": "0x012badcc6e824c2ea32bd5367ebda3be3402c9c5",
            "market_address": "0xc7d566f5cd575fdad0e982bd238d9abcf29807ea",
            "underlying_token_address": "0x917cee801a67f933f2e6b33fc0cd1ed2d5909d88",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "sUSDe-13AUG2026",
            "underlying": "sUSDe",
            "pt_address": "0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb",
            "yt_address": "0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2",
            "sy_address": "0xbf98480425a29197e5d99d003017f63a1e595d02",
            "market_address": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
            "underlying_token_address": "0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            "maturity_iso": "2026-08-13T00:00:00Z",
        },
        {
            "symbol": "USDe-13AUG2026",
            "underlying": "USDe",
            "pt_address": "0x3ec43f4158d51df0928979c09eaf33cad287065c",
            "yt_address": "0x8becb3beca07029ffddd2135082468cb8af19ca9",
            "sy_address": "0xf0bacd9c3d94fc924dbcaaf644208c4e3f4d3bb4",
            "market_address": "0x43c97094da0e894d3af2fda6f507d59a29888251",
            "underlying_token_address": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
            "maturity_iso": "2026-08-13T00:00:00Z",
        },
    ],
    "ARBITRUM": [
        {
            "symbol": "wstETH-25JUN2026",
            "underlying": "wstETH",
            "pt_address": "0x71fbf40651e9d4278a74586afc99f307f369ce9a",
            "yt_address": "0x25bda1edd6af17c61399aa0eb84b93daa3069764",
            "sy_address": "0x80c12d5b6cc494632bf11b03f09436c8b61cc5df",
            "market_address": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            "underlying_token_address": "0x5979d7b546e38e414f7e9822514be443a4800529",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "weETH-25JUN2026",
            "underlying": "weETH",
            "pt_address": "0xab7f3837e6e721abbc826927b655180af6a04388",
            "yt_address": "0xff9826c358a822d00187b487c349bc5e7f30788a",
            "sy_address": "0xa6c895eb332e91c5b3d00b7baeeaae478cc502da",
            "market_address": "0x46d62a8dede1bf2d0de04f2ed863245cbba5e538",
            "underlying_token_address": "0x35751007a407ca6feffe80b3cb397736d2cf4dbe",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "rETH-25JUN2026",
            "underlying": "rETH",
            "pt_address": "0x3362c1265a0522f321253708c9fb176f2274fa8d",
            "yt_address": "0xe03c236ca6f07755ad631a42dd5d60e293b1bf71",
            "sy_address": "0xc0cf4b266be5b3229c49590b59e67a09c15b22f4",
            "market_address": "0xd7eac6c9109d8c3977ebce4e7dbf0a5a9532e240",
            "underlying_token_address": "0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "uniETH-25JUN2026",
            "underlying": "uniETH",
            "pt_address": "0xd8d5fbbaad1e80aa0352b2029a594caeff6cf1ec",
            "yt_address": "0x0fb95ec4086b3007c894ad78703bc634150a1d50",
            "sy_address": "0x5b56f4bf94807b547eb5e7eba5cd8c033af83aed",
            "market_address": "0x7b8aec3d1986b4becbf1c651ff6a953cc0117b05",
            "underlying_token_address": "0x3d15fd46ce9e551498328b1c83071d9509e2c3a0",
            "maturity_iso": "2026-06-25T00:00:00Z",
        },
        {
            "symbol": "USDai-15OCT2026",
            "underlying": "USDai",
            "pt_address": "0xc9d24ad0bb25f34098e226a8c5192dea7bacccae",
            "yt_address": "0xaf67341456151ab8c270e0962966092181c2eb80",
            "sy_address": "0x5edcbc20cac67adc2e724d4348ff85132b085b82",
            "market_address": "0xa8a0dea40174cfc30fea9e3a77f182ab33f46e25",
            "underlying_token_address": "0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef",
            "maturity_iso": "2026-10-15T00:00:00Z",
        },
    ],
}

# Roles emitted per market — order is stable for deterministic test assertions.
_PENDLE_ROLES: tuple[str, ...] = ("PT", "YT", "SY")

# Stable-asset decimals for Pendle markets seen here; default 18 is standard for
# ERC-20 wrappers. Keep the table small — Pendle PT/YT/SY all inherit the
# underlying's decimals which is 18 for every market in the curated snapshot.
_PENDLE_TOKEN_DECIMALS_DEFAULT: int = 18


def _get_deploy_date(chain: str) -> datetime:
    if chain == "ARBITRUM":
        return _PENDLE_ARB_DEPLOY_DATE
    return _PENDLE_ETH_DEPLOY_DATE


def _parse_maturity(maturity_iso: str) -> datetime:
    """Parse ``YYYY-MM-DDTHH:MM:SSZ`` into a UTC datetime."""
    # ``fromisoformat`` accepts trailing 'Z' from Python 3.11 onward.
    return datetime.fromisoformat(maturity_iso.replace("Z", "+00:00"))


def _role_token_address(role: str, market: dict[str, str]) -> str:
    if role == "PT":
        return market["pt_address"]
    if role == "YT":
        return market["yt_address"]
    if role == "SY":
        return market["sy_address"]
    raise ValueError(f"Unknown Pendle role: {role!r}")


class PendleReferenceDataAdapter(BaseReferenceDataAdapter):
    """Pendle Finance reference data: PT/YT/SY token discovery.

    Pendle V1 launched on Ethereum mainnet 2021-06-15; the protocol moved to V2
    architecture in 2023, with Arbitrum support added 2024-01-26. Each Pendle
    market splits a yield-bearing asset into PT (principal, fixed yield), YT
    (variable yield, decays to zero at maturity), and SY (standardized
    yield-wrapper, no expiry). This adapter emits each PT/YT/SY token of every
    currently-active market as a separate ``InstrumentRecord``.
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
        return "pendle"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Pendle PT/YT/SY tokens for active markets on this chain.

        Each curated market emits THREE records (PT, YT, SY). PT/YT carry the
        market's maturity datetime in ``expiry``; SY carries ``expiry=None``.
        """
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"PENDLE-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        markets = _PENDLE_MARKETS_BY_CHAIN.get(self._chain, [])

        for market in markets:
            symbol = market["symbol"]
            underlying = market["underlying"]
            maturity = _parse_maturity(market["maturity_iso"])

            for role in _PENDLE_ROLES:
                token_address = _role_token_address(role, market)
                # SY tokens are perpetual yield-wrappers — they have no
                # maturity. PT/YT redeem/decay at the market's expiry.
                expiry: datetime | None = None if role == "SY" else maturity

                results.append(
                    InstrumentRecord(
                        instrument_key=f"{venue_tag}:{role}:{role}-{symbol}",
                        venue=venue_tag,
                        raw_symbol=token_address,
                        instrument_type=InstrumentType.YIELD_BEARING,
                        base_asset=underlying,
                        quote_asset="",
                        tick_size=Decimal("0.000001"),
                        min_size=Decimal("0.000001"),
                        contract_size=Decimal("1"),
                        expiry=expiry,
                        strike=None,
                        option_type=None,
                        status=InstrumentStatus.ACTIVE,
                        underlying=underlying,
                        available_from_datetime=deploy_date,
                        base_asset_contract_address=token_address,
                        base_asset_decimals=_PENDLE_TOKEN_DECIMALS_DEFAULT,
                    )
                )

        logger.info(
            "Pendle: fetched %d PT/YT/SY instruments across %d markets on %s",
            len(results),
            len(markets),
            self._chain,
        )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
        raise NotImplementedError("Pendle does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Pendle expiry calendar is not exposed via reference data")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Pendle markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Pendle OHLCV not supported via reference data")
