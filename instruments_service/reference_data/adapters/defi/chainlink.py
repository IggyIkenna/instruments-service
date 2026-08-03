"""Chainlink oracle price adapter — multi-chain EVM price-feed discovery.

Enumerates Chainlink Data Feed aggregator contracts for major assets across the
five EVM chains MTDS captures (ETHEREUM / ARBITRUM / BASE / OPTIMISM / POLYGON).
Each feed is a two-token quoted pair (e.g. ``ETH/USD``, ``WBTC/BTC``,
``stETH/ETH``) returned as an ``InstrumentRecord`` with
``instrument_type=SPOT_PAIR`` — mirroring the Pyth oracle adapter (``pyth.py``),
which is the shape this adapter is modelled on. The ``instrument_key`` is built
through ``build_canonical_instrument_id`` so the middle TYPE segment is the real
``InstrumentType.SPOT_PAIR`` enum value (``CHAINLINK-ETHEREUM:SPOT_PAIR:ETH-USD``),
not the pre-fix ``:SPOT:`` shorthand literal.

Enumeration is STATIC (the curated per-chain aggregator registry below) — no
network call is made at reference-data discovery time, exactly like the Pyth
adapter enumerates its curated feed-id map. The actual oracle prices are fetched
by the market-tick-data-service (MTDS) ``oracle_prices`` handler at runtime via
the Alchemy RPC ``latestRoundData()`` selector against each aggregator address.

Feed aggregator addresses SSOT (never hardcode elsewhere): the per-chain
``_CHAINLINK_FEEDS_BY_CHAIN`` map below mirrors MTDS's
``cli/handlers/_oracle_prices_constants.py`` so the IS-enumerated feed universe
== the set MTDS fetches (canonical IS-enumerate → MTDS-fetch coupling).

Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts import AssetGroup, build_canonical_instrument_id
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.registry import get_chain_genesis_date

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Chains MTDS queries in oracle_prices — Ethereum first, then L2s. Mirrors
# MTDS ``_oracle_prices_constants._ORACLE_CHAINS``.
CHAINLINK_CHAINS: tuple[str, ...] = ("ETHEREUM", "ARBITRUM", "BASE", "OPTIMISM", "POLYGON")

# ── Per-chain Chainlink aggregator registry (SSOT) ───────────────────────────
# feed_name -> (aggregator_address, base_asset, quote_asset). Mirrors MTDS
# ``_CHAINLINK_FEEDS_BY_CHAIN`` (addresses DERIVED from docs.chain.link) so the
# enumerated universe matches the set MTDS's oracle_prices handler fetches.
_CHAINLINK_FEEDS_BY_CHAIN: dict[str, dict[str, tuple[str, str, str]]] = {
    "ETHEREUM": {
        "ETH/USD": ("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419", "ETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "stETH/USD": ("0xCfE54B5cD566aB89272946F602D76Ea879CAb4a8", "stETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "stETH/ETH": ("0x86392dC19c0b719886221c78AB11eb8Cf5c52812", "stETH", "ETH"),  # DERIVED 2026-07-20 chain.link
        "cbETH/ETH": ("0xF017fcB346A1885194689bA23Eff2fE6fA5C483b", "cbETH", "ETH"),  # DERIVED 2026-07-20 chain.link
        "rETH/ETH": ("0x536218f9E9Eb48863970252233c8F271f554C2d0", "rETH", "ETH"),  # DERIVED 2026-07-20 chain.link
        # weETH/ETH + ezETH/ETH: VERIFIED live (2026-07-21, wf_f629fbb4-7da eth_call
        # probe) — real RefPrice aggregators, added per lst_rate_honest_coverage plan
        # Phase 1 (codex: lst-exchange-rate-surfaces.md surface #3). Mirrors MTDS
        # ``_oracle_prices_constants.py`` dict entry — the invariant test asserts
        # the two stay in sync. rsETH/wstETH deliberately NOT added: rsETH's only
        # on-chain feed is an Exchange-Rate feed (not a price aggregator); wstETH
        # only has a Calculated-USD feed (operator decision pending) — see the plan.
        "weETH/ETH": ("0x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22", "weETH", "ETH"),  # DERIVED 2026-07-21 eth_call
        "ezETH/ETH": ("0x636A000262F6aA9e1F094ABF0aD8f645C44f641C", "ezETH", "ETH"),  # DERIVED 2026-07-21 eth_call
        "BTC/USD": ("0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c", "BTC", "USD"),  # DERIVED 2026-07-20 chain.link
        "WBTC/BTC": ("0xfdFD9C85aD200c506Cf9e21F1FD8dd01932FBB23", "WBTC", "BTC"),  # DERIVED 2026-07-20 chain.link
        "USDC/USD": ("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6", "USDC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDT/USD": ("0x3E7d1eAB13ad0104d2750B8863b489D65364e32D", "USDT", "USD"),  # DERIVED 2026-07-20 chain.link
        "DAI/USD": ("0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9", "DAI", "USD"),  # DERIVED 2026-07-20 chain.link
        "AAVE/USD": ("0x547a514d5e3769680Ce22B2361c10Ea13619e8a9", "AAVE", "USD"),  # DERIVED 2026-07-20 chain.link
        "COMP/USD": ("0xdbd020CAeF83eFd542f4De03e3cF0C28A4428bd5", "COMP", "USD"),  # DERIVED 2026-07-20 chain.link
        "LINK/USD": ("0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c", "LINK", "USD"),  # DERIVED 2026-07-20 chain.link
        "UNI/USD": ("0x553303d460EE0afB37EdFf9bE42922D8FF63220e", "UNI", "USD"),  # DERIVED 2026-07-20 chain.link
        "CRV/USD": ("0xCd627aA160A6fA45Eb793D19Ef54f5062F20f33f", "CRV", "USD"),  # DERIVED 2026-07-20 chain.link
        "MKR/USD": ("0xec1D1B3b0443256cc3860e24a46F108e699484Aa", "MKR", "USD"),  # DERIVED 2026-07-20 chain.link
        "SNX/USD": ("0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699", "SNX", "USD"),  # DERIVED 2026-07-20 chain.link
        "BAL/USD": ("0xdF2917806E30300537aEB49A7663062F4d1F2b5F", "BAL", "USD"),  # DERIVED 2026-07-20 chain.link
        "LDO/ETH": ("0x4e844125952D32AcdF339BE976c98E22F6F318dB", "LDO", "ETH"),  # DERIVED 2026-07-20 chain.link
        "SOL/USD": ("0x4ffC43a60e009B551865A93d232E33Fce9f01507", "SOL", "USD"),  # DERIVED 2026-07-20 chain.link
        "MATIC/USD": ("0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676", "MATIC", "USD"),  # DERIVED 2026-07-20 chain.link
        "AVAX/USD": ("0xFF3EEb22B5E3dE6e705b44749C2559d704923FD7", "AVAX", "USD"),  # DERIVED 2026-07-20 chain.link
        "BNB/USD": ("0x14E613Ac691a42f21B17961bA18c6e35CC2Da893", "BNB", "USD"),  # DERIVED 2026-07-20 chain.link
    },
    "ARBITRUM": {
        "ETH/USD": ("0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612", "ETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "BTC/USD": ("0x6ce185860a4963106506C203335A2910413708e9", "BTC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDC/USD": ("0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3", "USDC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDT/USD": ("0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7", "USDT", "USD"),  # DERIVED 2026-07-20 chain.link
        "LINK/USD": ("0x86E53CF1B870786351Da77A57575e79CB55812Cb", "LINK", "USD"),  # DERIVED 2026-07-20 chain.link
        "ARB/USD": ("0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6", "ARB", "USD"),  # DERIVED 2026-07-20 chain.link
    },
    "BASE": {
        "ETH/USD": ("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70", "ETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "BTC/USD": ("0xCCADC697c55bbB68dc5bCdf8d3CBe83CdD4E071E", "BTC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDC/USD": ("0x7e860098F58bBFC8648a4311b374B1D669a2bc6B", "USDC", "USD"),  # DERIVED 2026-07-20 chain.link
        "cbETH/ETH": ("0x868a501e68F3D1E89CfC0D22F6b22E8dabce5F04", "cbETH", "ETH"),  # DERIVED 2026-07-20 chain.link
    },
    "OPTIMISM": {
        "ETH/USD": ("0x13e3Ee699D1909E989722E753853AE30b17e08c5", "ETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "BTC/USD": ("0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593", "BTC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDC/USD": ("0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3", "USDC", "USD"),  # DERIVED 2026-07-20 chain.link
        "LINK/USD": ("0xCc232dcFAAE6354cE191Bd574108c1aD03f86229", "LINK", "USD"),  # DERIVED 2026-07-20 chain.link
        "OP/USD": ("0x0D276FC14719f9292D5C1eA2198673d1f4269246", "OP", "USD"),  # DERIVED 2026-07-20 chain.link
    },
    "POLYGON": {
        "ETH/USD": ("0xF9680D99D6C9589e2a93a78A04A279e509205945", "ETH", "USD"),  # DERIVED 2026-07-20 chain.link
        "BTC/USD": ("0xc907E116054Ad103354f2D350FD2514433D57F6f", "BTC", "USD"),  # DERIVED 2026-07-20 chain.link
        "MATIC/USD": ("0xAB594600376Ec9fD91F8e8dC44E4fb146AD26832", "MATIC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDC/USD": ("0xfE4A8cc5b5B2366C1B58Bea3858e81843583ee2e", "USDC", "USD"),  # DERIVED 2026-07-20 chain.link
        "USDT/USD": ("0x0A6513e40db6EB1b165753AD52E80663aeA50545", "USDT", "USD"),  # DERIVED 2026-07-20 chain.link
        "LINK/USD": ("0xd9FFdb71EbE7496cC440152d43986Aae0AB76665", "LINK", "USD"),  # DERIVED 2026-07-20 chain.link
        "AAVE/USD": ("0x72484B12719E23115761D5DA1646945632979bB6", "AAVE", "USD"),  # DERIVED 2026-07-20 chain.link
    },
}


class ChainlinkOracleReferenceDataAdapter(BaseReferenceDataAdapter):
    """Chainlink oracle reference data: static per-chain price-feed enumeration.

    Enumerates the curated ``_CHAINLINK_FEEDS_BY_CHAIN`` aggregator registry for
    a single EVM chain (parsed from the canonical venue ``CHAINLINK-<CHAIN>``).
    Each feed becomes a ``SPOT_PAIR`` instrument with the Chainlink aggregator
    address stored as ``raw_symbol`` for traceability back to the on-chain
    contract. The actual oracle prices are fetched by MTDS at runtime — this
    adapter only exposes the reference-data universe.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()
        genesis = get_chain_genesis_date(self._chain)
        if genesis is None:
            raise ValueError(
                f"Chainlink adapter: no chain genesis date for {self._chain!r} — "
                f"register it in unified_api_contracts.registry.chain_env.CHAIN_GENESIS_DATES."
            )
        # Conservative availability floor: a feed cannot pre-date its chain's
        # genesis. Consistent with MTDS's oracle_prices handler, which gates
        # pre-coverage Chainlink days via get_chain_genesis_date.
        self._available_from = datetime.fromisoformat(genesis).replace(tzinfo=UTC)

    @property
    def venue(self) -> str:
        """Return the venue identifier (``CHAINLINK-<CHAIN>``)."""
        return f"CHAINLINK-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Chainlink oracle price-feed instruments for this chain.

        All Chainlink feeds are two-token quoted pairs (SPOT_PAIR). Returns an
        empty list for chains not in the curated registry.
        """
        if instrument_type not in (None, InstrumentType.SPOT_PAIR, "spot"):
            logger.info("Chainlink oracle only supports SPOT instruments; requested %s", instrument_type)
            return []

        feeds = _CHAINLINK_FEEDS_BY_CHAIN.get(self._chain, {})
        results: list[InstrumentRecord] = []
        for feed_name, (address, base_asset, quote_asset) in feeds.items():
            record = self._build_feed_record(feed_name, address, base_asset, quote_asset)
            if record:
                results.append(record)

        logger.info("Chainlink: built %d oracle price feed instruments on %s", len(results), self._chain)
        return results

    def _build_feed_record(
        self,
        feed_name: str,
        address: str,
        base_asset: str,
        quote_asset: str,
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord for a Chainlink aggregator feed.

        The aggregator address is stored as raw_symbol for traceability back to
        the on-chain contract. base/quote are the human-readable pair.
        """
        if not feed_name or not address:
            return None

        base = base_asset.strip().upper()
        quote = quote_asset.strip().upper()

        # Route through the shared canonical builder so the emitted TYPE segment
        # is the real InstrumentType enum value (SPOT_PAIR). A Chainlink feed is
        # a two-token quoted pair, so it passes the DeFi SPOT_PAIR two-token
        # validator. venue already carries the composed VENUE-CHAIN token →
        # passthrough=True keeps the id.
        instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI, self.venue, InstrumentType.SPOT_PAIR, f"{base}-{quote}", passthrough=True
        )

        return InstrumentRecord(
            instrument_key=instrument_key,
            # DeFi symbols are already human-readable — canonical_instrument_id
            # mirrors instrument_key (no raw-code-to-name translation gap).
            canonical_instrument_id=instrument_key,
            venue=self.venue,
            raw_symbol=address,  # Chainlink aggregator address as canonical identifier
            instrument_type=InstrumentType.SPOT_PAIR,
            base_asset=base,
            quote_asset=quote,
            settle_asset=quote,
            margin_type=MarginType.LINEAR,
            tick_size=Decimal("0.00000001"),  # Chainlink USD feeds publish 8-decimal precision
            min_size=Decimal("0"),  # Oracle prices have no minimum trade size
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=self._available_from,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by aggregator address or base asset."""
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Chainlink does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Chainlink oracle feeds have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Chainlink funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv; not supported for this venue."""
        raise NotImplementedError("Chainlink OHLCV not supported via reference data")
