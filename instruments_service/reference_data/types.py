"""TypedDict definitions for exchange API responses.

All venue-specific TypedDicts have been migrated to UAC Pydantic models.
Import directly from UAC in adapters — do not use TypedDicts for external API shapes.

Migrated TypedDicts → UAC Pydantic models:

Binance (BinanceSymbolFilter, BinanceSymbol, BinanceOptionSymbol, BinanceExchangeInfo):
    unified_api_contracts.external.binance.market_schemas
        BinanceFuturesExchangeInfo, BinanceInstrumentInfo, BinanceOptionInstrumentInfo

Bybit (BybitPriceFilter, BybitLotSizeFilter, BybitSymbol, BybitResult, BybitResponse):
    unified_api_contracts.external.bybit.schemas
        BybitInstrumentInfo, BybitInstrumentsResult, BybitInstrumentsResponse

Coinbase (CoinbaseProduct, CoinbaseResponse):
    unified_api_contracts.external.coinbase.schemas
        CoinbaseProductInfo, CoinbaseProductsResponse

Deribit (DeribitInstrument, DeribitResult):
    unified_api_contracts.external.deribit.schemas
        DeribitInstrumentInfoFull, DeribitGetInstrumentsResponse, DeribitGetInstrumentResponse

Hyperliquid (HyperliquidAssetInfo, HyperliquidUniverse):
    unified_api_contracts.external.hyperliquid.schemas
        HyperliquidAssetInfo, HyperliquidMeta

OKX (OkxInstrument, OkxResponse):
    unified_api_contracts.external.okx.schemas
        OKXInstrumentInfo, OKXInstrumentsResponse

Previously migrated:
    DatabentoInstrument →
        unified_api_contracts.external.databento.schemas
        .DatabentoReferenceInstrument
    TardisInstrument →
        unified_api_contracts.external.tardis.schemas
        .TardisInstrumentDetail
    TardisExchange →
        unified_api_contracts.external.tardis.schemas
        .TardisExchangeDetail
"""
