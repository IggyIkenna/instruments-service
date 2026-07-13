"""Adapter factory for unified-reference-data-interface."""

import contextlib
import logging
from datetime import date as date_type

from unified_api_contracts.registry import (
    NO_ADAPTER_YET,
    VENUE_PREFIX_TO_PROTOCOL,
    VENUE_TO_ADAPTER_KEY,
    CapabilityResolutionError,
    UnsupportedOperationError,
    bootstrap_capabilities,
    validate_operation,
)

from .adapters.cefi.aster import AsterReferenceDataAdapter
from .adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter
from .adapters.cefi.coinbase_cde import CoinbaseCdeReferenceDataAdapter
from .adapters.cefi.deribit_combo_adapter import DeribitComboReferenceDataAdapter
from .adapters.cefi.deribit_options_adapter import DeribitOptionsReferenceDataAdapter
from .adapters.cefi.extended import ExtendedReferenceDataAdapter
from .adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter
from .adapters.cefi.kalshi_perp import KalshiPerpReferenceDataAdapter
from .adapters.cefi.lighter import LighterReferenceDataAdapter
from .adapters.cefi.pacifica import PacificaReferenceDataAdapter
from .adapters.cefi.polymarket_perp import PolymarketPerpReferenceDataAdapter
from .adapters.cefi.tardis import TardisReferenceDataAdapter
from .adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from .adapters.defi.balancer import BalancerReferenceDataAdapter
from .adapters.defi.beefy import BeefyReferenceDataAdapter
from .adapters.defi.benqi import BenqiReferenceDataAdapter
from .adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from .adapters.defi.convex import ConvexReferenceDataAdapter
from .adapters.defi.curve import CurveReferenceDataAdapter
from .adapters.defi.drift import DriftReferenceDataAdapter
from .adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter
from .adapters.defi.ethena import EthenaReferenceDataAdapter
from .adapters.defi.etherfi import EtherFiReferenceDataAdapter
from .adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter
from .adapters.defi.euler_v2 import EulerV2ReferenceDataAdapter
from .adapters.defi.flash_trade import FlashTradeReferenceDataAdapter
from .adapters.defi.fluid import FluidReferenceDataAdapter
from .adapters.defi.idle import IdleReferenceDataAdapter
from .adapters.defi.jito import JitoReferenceDataAdapter
from .adapters.defi.jito_restaking import JitoRestakingReferenceDataAdapter
from .adapters.defi.kamino import KaminoReferenceDataAdapter
from .adapters.defi.karak import KarakReferenceDataAdapter
from .adapters.defi.kelpdao import KelpDaoReferenceDataAdapter
from .adapters.defi.lido import LidoReferenceDataAdapter
from .adapters.defi.mango import MangoReferenceDataAdapter
from .adapters.defi.marginfi import MarginfiReferenceDataAdapter
from .adapters.defi.marinade import MarinadeReferenceDataAdapter
from .adapters.defi.morpho import MorphoReferenceDataAdapter
from .adapters.defi.orca import OrcaReferenceDataAdapter
from .adapters.defi.pendle import PendleReferenceDataAdapter
from .adapters.defi.puffer import PufferReferenceDataAdapter
from .adapters.defi.radiant import RadiantReferenceDataAdapter
from .adapters.defi.raydium import RaydiumReferenceDataAdapter
from .adapters.defi.renzo import RenzoReferenceDataAdapter
from .adapters.defi.rocket_pool import RocketPoolReferenceDataAdapter
from .adapters.defi.sanctum import SanctumReferenceDataAdapter
from .adapters.defi.solana_native_staking import SolanaNativeStakingAdapter
from .adapters.defi.solblaze import SolblazeReferenceDataAdapter
from .adapters.defi.solend import SolendReferenceDataAdapter
from .adapters.defi.spark import SparkReferenceDataAdapter
from .adapters.defi.symbiotic import SymbioticReferenceDataAdapter
from .adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from .adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter
from .adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter
from .adapters.defi.venus import VenusReferenceDataAdapter
from .adapters.defi.yearn import YearnReferenceDataAdapter
from .adapters.defi.zeta import ZetaReferenceDataAdapter
from .adapters.prediction.kalshi import KalshiReferenceDataAdapter
from .adapters.prediction.polymarket import PolymarketReferenceDataAdapter
from .adapters.sports.adapters.api_football_reference import ApiFootballReferenceDataAdapter
from .adapters.sports.adapters.betfair import BetfairReferenceDataAdapter
from .adapters.tradfi.databento import DatabentoReferenceDataAdapter
from .adapters.tradfi.ibkr import IBKRReferenceDataAdapter
from .adapters.tradfi.massive import MassiveReferenceDataAdapter
from .adapters.tradfi.tradfi_live import TradFiLiveReferenceDataAdapter
from .base_adapter import BaseReferenceDataAdapter

_logger = logging.getLogger(__name__)

# Bootstrap capability registry once at module load (idempotent)
with contextlib.suppress(Exception):
    bootstrap_capabilities()

# Venue → adapter-KEY routing is UAC-owned data (registry venue_adapter_keys —
# codex/04-architecture/instrument-universe-registry-consolidation.md). This
# module keeps only the key → adapter-CLASS table (_ADAPTERS below) plus
# instantiation logic; it declares NO venue truth of its own.

# Maps canonical CeFi venues → CCXT exchange IDs for live mode.
# In live mode, these venues use CCXT (real-time public endpoints) instead of Tardis
# (historical-only). No API key needed — instrument definitions are public.
_CANONICAL_VENUE_TO_CCXT_EXCHANGE: dict[str, str] = {
    "BINANCE-SPOT": "binance",
    "BINANCE-FUTURES": "binanceusdm",
    "BYBIT": "bybit",
    "BYBIT-SPOT": "bybit",
    "BYBIT-FUTURES": "bybit",
    "OKX": "okx",
    "OKX-SPOT": "okx",
    "OKX-SWAP": "okx",
    "OKX-FUTURES": "okx",
    "DERIBIT": "deribit",
    "COINBASE-SPOT": "coinbase",
    "UPBIT": "upbit",
    # Tier-3 CeFi — Kraken spot (public /0/public/AssetPairs, no auth needed) and
    # futures (Kraken Derivatives, formerly Cryptofacilities) via krakenfutures.
    # Credentials are NOT required for live instrument discovery — public endpoints.
    # kraken-api-key / kraken-api-secret are for trading only (execution-service).
    "KRAKEN-SPOT": "kraken",
    "KRAKEN-FUTURES": "krakenfutures",
}

_ADAPTERS: dict[str, type[BaseReferenceDataAdapter]] = {
    "aave_v3": AaveV3ReferenceDataAdapter,
    "api_football": ApiFootballReferenceDataAdapter,
    "aster": AsterReferenceDataAdapter,
    "coinbase_cde": CoinbaseCdeReferenceDataAdapter,
    "deribit_combo": DeribitComboReferenceDataAdapter,
    "deribit_options": DeribitOptionsReferenceDataAdapter,
    "balancer": BalancerReferenceDataAdapter,
    "beefy": BeefyReferenceDataAdapter,
    "benqi": BenqiReferenceDataAdapter,
    "betfair": BetfairReferenceDataAdapter,
    "compound_v3": CompoundV3ReferenceDataAdapter,
    "convex": ConvexReferenceDataAdapter,
    "curve": CurveReferenceDataAdapter,
    "databento": DatabentoReferenceDataAdapter,
    "drift": DriftReferenceDataAdapter,
    "extended": ExtendedReferenceDataAdapter,
    "eigenlayer": EigenLayerReferenceDataAdapter,
    "flash_trade": FlashTradeReferenceDataAdapter,
    "ethena": EthenaReferenceDataAdapter,
    "ethfi_governance": EthFiGovernanceReferenceDataAdapter,
    "etherfi": EtherFiReferenceDataAdapter,
    "euler_v2": EulerV2ReferenceDataAdapter,
    "fluid": FluidReferenceDataAdapter,
    "hyperliquid": HyperliquidReferenceDataAdapter,
    "idle": IdleReferenceDataAdapter,
    "jito": JitoReferenceDataAdapter,
    "jito_restaking": JitoRestakingReferenceDataAdapter,
    "kelpdao": KelpDaoReferenceDataAdapter,
    "kamino": KaminoReferenceDataAdapter,
    "karak": KarakReferenceDataAdapter,
    "mango": MangoReferenceDataAdapter,
    "marginfi": MarginfiReferenceDataAdapter,
    "marinade": MarinadeReferenceDataAdapter,
    "massive": MassiveReferenceDataAdapter,
    "ibkr": IBKRReferenceDataAdapter,
    "kalshi": KalshiReferenceDataAdapter,
    "kalshi_perp": KalshiPerpReferenceDataAdapter,
    "lighter": LighterReferenceDataAdapter,
    "lido": LidoReferenceDataAdapter,
    "morpho": MorphoReferenceDataAdapter,
    "orca": OrcaReferenceDataAdapter,
    "pacifica": PacificaReferenceDataAdapter,
    "pendle": PendleReferenceDataAdapter,
    "polymarket": PolymarketReferenceDataAdapter,
    "polymarket_perp": PolymarketPerpReferenceDataAdapter,
    "radiant": RadiantReferenceDataAdapter,
    "puffer": PufferReferenceDataAdapter,
    "raydium": RaydiumReferenceDataAdapter,
    "renzo": RenzoReferenceDataAdapter,
    "rocket_pool": RocketPoolReferenceDataAdapter,
    "sanctum": SanctumReferenceDataAdapter,
    "solana_native": SolanaNativeStakingAdapter,
    "solblaze": SolblazeReferenceDataAdapter,
    "solend": SolendReferenceDataAdapter,
    "spark": SparkReferenceDataAdapter,
    "symbiotic": SymbioticReferenceDataAdapter,
    "tardis": TardisReferenceDataAdapter,
    "uniswap_v2": UniswapV2ReferenceDataAdapter,
    "uniswap_v3": UniswapV3ReferenceDataAdapter,
    "uniswap_v4": UniswapV4ReferenceDataAdapter,
    "venus": VenusReferenceDataAdapter,
    "yearn": YearnReferenceDataAdapter,
    "zeta": ZetaReferenceDataAdapter,
}


# Maps URDI adapter key → UAC data source name (for API key lookup via DATA_SOURCE_TO_SECRET).
# Used by services to know which credential each URDI adapter needs.
# UTL's validate_api_keys_for_venues() returns {data_source: api_key}.
ADAPTER_DATA_SOURCES: dict[str, str] = {
    "hyperliquid": "hyperliquid",
    "aster": "aster",
    # Coinbase Derivatives Exchange (CDE) — public REST endpoint (Advanced Trade
    # products?product_type=FUTURE), no API key needed for instrument discovery.
    "coinbase_cde": "",
    "tardis": "tardis",
    "databento": "databento",
    "massive": "massive",
    "ibkr": "ibkr",
    "polymarket": "polymarket",
    "kalshi": "kalshi",
    # Crypto-perp CLOBs — public REST endpoints for contract enumeration, no API key needed.
    # Trading/funding/depth requires auth (separate MTDS data pipeline).
    "kalshi_perp": "",
    "polymarket_perp": "",
    "uniswap_v2": "thegraph",
    "uniswap_v3": "thegraph",
    "uniswap_v4": "thegraph",
    "aave_v3": "thegraph",
    "compound_v3": "thegraph",
    "morpho": "thegraph",
    "fluid": "thegraph",
    "lido": "thegraph",
    "etherfi": "thegraph",
    "ethena": "thegraph",
    "balancer": "balancer_api_v3",
    "curve": "rpc",
    # New protocols — all use The Graph subgraphs
    "pancakeswap_v3": "thegraph",
    "sushiswap_v3": "thegraph",
    "sushiswap": "thegraph",
    "aerodrome_v3": "thegraph",
    "camelot_v3": "thegraph",
    "velodrome_v2": "thegraph",
    "trader_joe_v2": "thegraph",
    "gmx": "thegraph",
    "spark": "thegraph",
    "api_football": "api_football",
    "betfair": "betfair",
    # Deribit combo: public REST endpoint, no API key needed
    "deribit_combo": "",
    # Deribit options: public REST endpoint (get_instruments + ticker), no API key needed
    "deribit_options": "",
    # EigenLayer / EtherFi governance — on-chain, no API key needed
    "eigenlayer": "",
    "ethfi_governance": "",
    # Solana adapters use public REST APIs (no API key needed)
    "drift": "",
    "kamino": "",
    "raydium": "",
    "orca": "",
    "marinade": "",
    "jito": "",
    # Solana lending adapters (2026-07-09) — public REST/JSON APIs, no API key needed.
    "solend": "",
    "marginfi": "",
    # Solana perp DEX adapters (Plan B 2026-05-13) — public REST APIs, no API key needed
    "mango": "",
    "zeta": "",
    "flash_trade": "",
    "pacifica": "",
    # Layer-2 perp DEX adapters — public REST APIs, no API key needed
    "lighter": "",
    "extended": "",
    # Phase-4 lending protocols. Curated registries — no live data source
    # for instrument discovery (deploy dates resolved via direct RPC).
    "euler_v2": "",
    "radiant": "",
    "venus": "",
    "benqi": "",
    # LST / LRT protocols — curated single-token registries (deploy dates hardcoded).
    "rocket_pool": "",
    "renzo": "",
    "kelpdao": "",
    "puffer": "",
    "sanctum": "",
    "solblaze": "",
    "solana_native": "",  # static registry; MTDS fetches rates via solana_rpc + helius_rpc
    # Restaking vault protocols — curated static vault registries.
    "symbiotic": "",
    "karak": "",
    # Vault / yield-aggregator protocols — curated static vault registries.
    "convex": "",
    "idle": "",
    "yearn": "",
    "beefy": "",
    # Pendle yield tokenization (PT/YT/SY) — curated active-markets snapshot.
    "pendle": "",
    # Jito Restaking (Solana NCN-vault primitive) — curated VRT registry.
    "jito_restaking": "",
    # solayer + picasso + cambrian removed 2026-06-02 (operator decision; no usable/decodable
    # DeFi data source). SSOT: plans/active/issues/issue_docs_remediation_sweep_2026_06_02.md.
}


# Adapter pool: reuse adapter instances across calls.
# Key: (adapter_key, api_key, venue, date?) → adapter instance.
# Date is included for date-aware adapters like Databento (target_date baked at init).
_adapter_pool: dict[tuple[str, str | None, str, str | None], BaseReferenceDataAdapter] = {}


def clear_adapter_pool() -> None:
    """Clear the adapter pool + all adapter caches. Call on credential rotation."""
    for adapter in _adapter_pool.values():
        adapter.clear_cache()
    _adapter_pool.clear()


#: TradFi reference adapter keys sharing the target_date + venue_filter contract
#: (both emit the canonical InstrumentRecord; Massive is Polygon.io-API compatible
#: — the sanctioned alt to Databento whose re-runs are billing-blocked 2026-06).
_DATE_AWARE_TRADFI_ADAPTER_KEYS: frozenset[str] = frozenset({"databento", "massive"})


def _resolve_uac_adapter_key(canonical_venue: str) -> str:
    """UAC venue → adapter key. Raises loudly for unknown or sentinel venues."""
    adapter_key = VENUE_TO_ADAPTER_KEY.get(canonical_venue)
    if adapter_key is None:
        supported = sorted(v for v, k in VENUE_TO_ADAPTER_KEY.items() if k != NO_ADAPTER_YET)
        raise ValueError(
            f"No URDI adapter for canonical venue {canonical_venue!r} — venue is UNKNOWN to UAC. "
            f"Register it in unified_api_contracts/registry/venue_adapter_keys.py "
            f"(real key or NO_ADAPTER_YET sentinel). Supported: {supported}"
        )
    if adapter_key == NO_ADAPTER_YET:
        raise ValueError(
            f"No URDI adapter for canonical venue {canonical_venue!r} — UAC declares it "
            f"adapterless (NO_ADAPTER_YET sentinel: MTDS-owned venue, source-as-venue artifact, "
            f"or expand-only bare form). See unified_api_contracts/registry/venue_adapter_keys.py."
        )
    return adapter_key


def _resolve_source_aware_adapter_key(adapter_key: str, source: str | None) -> str:
    """Source-aware routing: ``source="massive"`` re-points a TradFi venue that
    defaults to Databento → the Massive adapter (Databento re-runs billing-blocked
    2026-06; Massive refills ``by_date/``). Same canonical InstrumentRecord output."""
    if source == "massive" and adapter_key == "databento":
        return "massive"
    return adapter_key


def _build_date_aware_tradfi_adapter(
    adapter_key: str,
    *,
    project_id: str | None,
    api_key: str | None,
    date: str | None,
    canonical_venue: str,
) -> BaseReferenceDataAdapter:
    """Build a Databento/Massive adapter — date + venue filter so each venue only
    fetches its own instruments (target_date baked in at init)."""
    target = date_type.fromisoformat(date) if date else None
    if adapter_key == "massive":
        return MassiveReferenceDataAdapter(
            project_id=project_id,
            api_key=api_key,
            target_date=target,
            venue_filter=canonical_venue,
        )
    return DatabentoReferenceDataAdapter(
        project_id=project_id,
        api_key=api_key,
        target_date=target,
        venue_filter=canonical_venue,
    )


def _resolve_tardis_exchanges_for_venue(
    canonical_venue: str,
    venue_instrument_type_to_tardis: dict[tuple[str, str], str],
    tardis_to_venue: dict[str, str],
    direct_or_suffixed_exchange: str | None,
) -> list[str]:
    """Resolve ALL Tardis exchanges backing a canonical venue for IS enumeration.

    Itype-aware FIRST: some venues (e.g. OKX) span MULTIPLE real Tardis
    exchanges depending on instrument_type (okex/okex-swap/okex-futures/
    okex-options) — the venue-only lookup (``direct_or_suffixed_exchange``,
    from ``VenueMapping.get_tardis_exchange_for_venue``) can return at most
    ONE exchange, silently dropping every other instrument type's universe.
    Falls back to the venue-only lookup (then a direct lowercase-conversion
    candidate) for venues with a single 1:1 exchange. Mirrors MTDS's
    itype-aware ``_resolve_tardis_exchange`` fix — see
    cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md.
    """
    itype_exchanges = sorted(
        {exch for (venue, _itype), exch in venue_instrument_type_to_tardis.items() if venue == canonical_venue}
    )
    if itype_exchanges:
        return itype_exchanges
    tardis_exchange = direct_or_suffixed_exchange
    if tardis_exchange is None:
        # Fallback: try direct lowercase conversion (BYBIT → bybit, DERIBIT → deribit)
        # Only valid if the result exists as a key in tardis_to_venue
        candidate = canonical_venue.lower()
        if candidate in tardis_to_venue:
            tardis_exchange = candidate
    return [tardis_exchange] if tardis_exchange else []


def get_adapter_for_canonical_venue(
    canonical_venue: str,
    api_key: str | None = None,
    project_id: str | None = None,
    date: str | None = None,
    extra_api_keys: dict[str, str] | None = None,
    mode: str = "batch",
    source: str | None = None,
) -> BaseReferenceDataAdapter:
    """Create a reference data adapter for a UAC canonical venue name.

    This is the preferred entry point for services that work with UAC canonical
    venue names (e.g. "UNISWAP_V3-ETHEREUM", "BINANCE-SPOT"). Translates via
    UAC ``VENUE_TO_ADAPTER_KEY`` (venue→key is UAC-owned data; this factory owns
    only key→class) and delegates to create_reference_data_adapter().

    For CeFi venues in live mode, routes to CCXT (real-time public endpoints)
    instead of Tardis (historical-only). Batch mode always uses Tardis for
    the full historical instrument universe.

    Args:
        canonical_venue: UAC canonical venue name (e.g. "UNISWAP_V3-ETHEREUM").
        api_key: Injected API key from Secret Manager (via UTL validate_api_keys_for_venues).
        project_id: Deprecated.
        date: ISO date string for date-aware adapters.
        extra_api_keys: Additional API keys for multi-key adapters.
        mode: "batch" (default) or "live". Live mode uses CCXT for CeFi venues.

    Raises:
        ValueError: If no adapter exists for this canonical venue name (venue
            unknown to UAC, or declared adapterless via NO_ADAPTER_YET).
    """
    adapter_key = _resolve_uac_adapter_key(canonical_venue)
    adapter_key = _resolve_source_aware_adapter_key(adapter_key, source)

    # Live mode: route CeFi Tardis venues to CCXT (real-time public endpoints).
    # Tardis is historical-only and can't provide live instrument definitions.
    ccxt_exchange_id = _CANONICAL_VENUE_TO_CCXT_EXCHANGE.get(canonical_venue)
    if mode == "live" and adapter_key == "tardis" and ccxt_exchange_id:  # noqa: L2-mode-seam — adapter source-routing (different source per mode is the one allowed L2 seam)
        pool_key = ("ccxt", None, canonical_venue, None)
        if pool_key in _adapter_pool:
            return _adapter_pool[pool_key]
        _logger.info(
            "Live mode: %s → CCXT (%s) instead of Tardis",
            canonical_venue,
            ccxt_exchange_id,
        )
        adapter: BaseReferenceDataAdapter = CCXTReferenceDataAdapter(
            venue=ccxt_exchange_id,
            canonical_venue=canonical_venue,
        )
        _adapter_pool[pool_key] = adapter
        return adapter

    # Live mode: route TradFi Databento venues to GCS-first adapter.
    # Reads the most recent GCS snapshot, filters expired instruments,
    # falls back to Databento (T-3 days) if no GCS data.
    if mode == "live" and adapter_key == "databento":  # noqa: L2-mode-seam — adapter source-routing (different source per mode is the one allowed L2 seam)
        pool_key = ("tradfi_live", api_key, canonical_venue, None)
        if pool_key in _adapter_pool:
            return _adapter_pool[pool_key]
        _logger.info(
            "Live mode: %s → GCS-first TradFi adapter (Databento fallback)",
            canonical_venue,
        )
        adapter = TradFiLiveReferenceDataAdapter(
            venue_filter=canonical_venue,
            api_key=api_key,
            project_id=project_id,
        )
        _adapter_pool[pool_key] = adapter
        return adapter

    # Check pool — reuse existing adapter if same key + credentials + venue + date
    # Include canonical_venue in pool key so AAVE_V3-ARBITRUM != AAVE_V3-ETHEREUM
    # Include date for Databento (target_date baked into adapter at init time)
    pool_date = date if adapter_key in ("databento", "massive") else None
    pool_key = (adapter_key, api_key, canonical_venue, pool_date)
    if pool_key in _adapter_pool:
        return _adapter_pool[pool_key]

    # DeFi adapters that accept chain parameter (EVM + Solana).
    # Membership here = factory parses the chain segment from "<VENUE>-<CHAIN>"
    # and passes it as `chain=` to the adapter ctor. Adapters NOT in this set
    # default to ETHEREUM regardless of canonical-venue suffix — fine for true
    # single-chain adapters; latent bug for multi-chain ones (see 2026-05-12
    # additions: renzo / karak / idle / yearn / beefy / pendle / jito_restaking).
    defi_graph_adapters = {
        "uniswap_v2",
        "uniswap_v3",
        "uniswap_v4",
        "aave_v3",
        "compound_v3",
        "morpho",
        "fluid",
        "balancer",
        "curve",
        "spark",
        # Phase-4 lending protocols (multi-chain via curated registries).
        "euler_v2",
        "radiant",
        "venus",
        "benqi",
        # Solana adapters
        "drift",
        "kamino",
        "raydium",
        "orca",
        "marinade",
        "jito",
        # Multi-chain LST/LRT/restaking adapters (2026-05-12 latent fix —
        # these were registered for multiple canonical venues earlier in the
        # session but were missing from this set, so non-Ethereum venues
        # silently used the ETHEREUM default chain).
        "renzo",
        "karak",
        "idle",
        "yearn",
        # Phase-2 deferred adapters shipped 2026-05-12.
        "beefy",
        "pendle",
        "jito_restaking",
    }

    # Some adapters need extra constructor parameters derived from the canonical venue name.
    adapter: BaseReferenceDataAdapter
    if adapter_key == "api_football" and date is not None:
        adapter = ApiFootballReferenceDataAdapter(api_key=api_key, project_id=project_id, date=date)
    elif adapter_key in defi_graph_adapters:
        # DeFi adapters: parse chain from venue name, pass chain + optional date
        parts = canonical_venue.split("-", 1)
        chain = parts[1] if len(parts) == 2 else "ETHEREUM"
        adapter_class = _ADAPTERS[adapter_key]
        # Resolve the actual protocol slug from the venue prefix (e.g., PANCAKESWAP_V3 → pancakeswap_v3)
        # This allows adapter reuse: UniV3 adapter can serve PancakeSwap, SushiSwap, etc.
        venue_prefix = parts[0] if len(parts) >= 1 else canonical_venue
        resolved_protocol = VENUE_PREFIX_TO_PROTOCOL.get(venue_prefix, adapter_key)
        # Only some adapters accept date (aave_v3, uniswap_v2/v3/v4)
        accepts_date = {"uniswap_v2", "uniswap_v3", "uniswap_v4", "aave_v3", "compound_v3", "spark"}
        # Pass protocol_slug for adapters that support it (UniV3, AaveV3)
        supports_protocol_slug = {"uniswap_v3", "aave_v3"}
        kwargs: dict[str, str | None] = {"project_id": project_id, "api_key": api_key, "chain": chain}
        if adapter_key in accepts_date:
            kwargs["date"] = date
        if adapter_key in supports_protocol_slug and resolved_protocol != adapter_key:
            kwargs["protocol_slug"] = resolved_protocol
        adapter = adapter_class(**kwargs)
    elif adapter_key == "tardis":
        # Tardis: pass ONLY the specific exchange(s) for this venue (not all
        # defaults) — see _resolve_tardis_exchanges_for_venue for the
        # itype-aware multi-exchange resolution (e.g. bare "OKX").
        from unified_api_contracts import VenueMapping as _VM_cls

        _vm = _VM_cls()
        tardis_exchanges = _resolve_tardis_exchanges_for_venue(
            canonical_venue,
            _vm.venue_instrument_type_to_tardis,
            _vm.tardis_to_venue,
            _vm.get_tardis_exchange_for_venue(canonical_venue),
        )
        if not tardis_exchanges:
            # FAIL LOUD — do not silently fetch all exchanges
            raise ValueError(
                f"No Tardis exchange mapping for canonical venue {canonical_venue!r}. "
                f"Add a mapping in VenueMapping.tardis_to_venue or "
                f"venue_instrument_type_to_tardis for this venue."
            )
        _logger.debug("Tardis: %s → exchanges=%s", canonical_venue, tardis_exchanges)
        # NO api_key — IS enumeration uses the free no-auth /v1/exchanges path (operator 2026-06-23).
        # canonical_venue_override tags every parsed instrument as canonical_venue
        # explicitly — required because the adapter's per-exchange venue
        # resolution (tardis_to_venue, a 1:1 reverse map) cannot represent
        # "these N exchanges' rows all belong to this ONE requested venue".
        adapter = TardisReferenceDataAdapter(
            project_id=project_id,
            exchanges=tardis_exchanges,
            canonical_venue_override=canonical_venue,
        )
    elif adapter_key in _DATE_AWARE_TRADFI_ADAPTER_KEYS:
        adapter = _build_date_aware_tradfi_adapter(
            adapter_key,
            project_id=project_id,
            api_key=api_key,
            date=date,
            canonical_venue=canonical_venue,
        )
    elif adapter_key == "polymarket" and extra_api_keys:
        # Polymarket: pass API-Football key for fixture cross-referencing
        af_key = extra_api_keys.get("api_football")
        adapter = PolymarketReferenceDataAdapter(
            project_id=project_id,
            api_key=api_key,
            api_football_api_key=af_key,
        )
    else:
        adapter = create_reference_data_adapter(adapter_key, project_id=project_id, api_key=api_key)

    _adapter_pool[pool_key] = adapter
    return adapter


def create_reference_data_adapter(
    venue: str,
    project_id: str | None = None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Create and return a reference data adapter for the given venue.

    Args:
        venue: Venue identifier (aster, hyperliquid, ibkr, databento, tardis,
               ccxt, betfair, polymarket, kalshi, api_football,
               or any DeFi protocol key).
        project_id: Deprecated. Retained for call-site compatibility but no
                    longer used for internal Secret Manager lookups.
        api_key: API key for the venue. The calling service MUST fetch this
                 from Secret Manager and pass it in. Adapters that require
                 authentication will raise ``ValueError`` if not provided.

    Raises:
        ValueError: If venue is not supported.
    """
    venue_lower = venue.lower()
    _run_refdata_preflight(venue_lower)
    adapter_class = _ADAPTERS.get(venue_lower)
    if adapter_class is None:
        supported = sorted(_ADAPTERS.keys())
        raise ValueError(f"Unsupported venue: {venue!r}. Supported: {supported}")
    return adapter_class(project_id=project_id, api_key=api_key)


def _run_refdata_preflight(venue: str) -> None:
    """Run informational capability preflight for a reference-data venue.

    URDI is read-only, so this is advisory — log a warning if the venue/env
    combination is flagged as unsupported but never block adapter creation.
    Unknown venues (not in the capability registry) are silently skipped.
    """
    try:
        validate_operation(venue, "get_instruments")
    except CapabilityResolutionError:
        _logger.debug(
            "refdata_preflight_skip: venue=%s not in capability registry",
            venue,
        )
    except UnsupportedOperationError as exc:
        _logger.warning(
            "refdata_preflight_warn: venue=%s flagged as unsupported — proceeding anyway (read-only): %s",
            venue,
            exc,
        )
