"""CCXT TypedDict type definitions.

Extracted from ccxt_service.py for file size compliance.
"""

from typing import TypedDict


class CCXTPrecision(TypedDict, total=False):
    """CCXT market precision fields."""

    price: float | None
    amount: float | None
    base: float | None
    quote: float | None


class CCXTLimits(TypedDict, total=False):
    """CCXT market limits fields."""

    amount: dict[str, float | None] | None
    price: dict[str, float | None] | None
    cost: dict[str, float | None] | None


class CCXTMarket(TypedDict, total=False):
    """CCXT market structure with known fields."""

    id: str
    symbol: str
    base: str | None
    quote: str | None
    settle: str | None
    baseId: str | None
    quoteId: str | None
    settleId: str | None
    type: str | None
    spot: bool | None
    margin: bool | None
    future: bool | None
    option: bool | None
    swap: bool | None
    contract: bool | None
    linear: bool | None
    inverse: bool | None
    contractSize: float | None
    expiry: float | None
    expiryDatetime: str | None
    strike: float | None
    optionType: str | None
    precision: CCXTPrecision | None
    limits: CCXTLimits | None
    active: bool | None
    taker: float | None
    maker: float | None
    percentage: bool | None
    tierBased: bool | None
    info: dict[str, str | float | int | bool | None] | None


class CCXTMarketData(TypedDict):  # CORRECT-LOCAL
    """CCXT service market data container."""

    exchange: object  # CCXT exchange instance
    markets: dict[str, CCXTMarket]
    exchange_id: str


# Metadata and leverage limits from CCXT (str, float, int, None values)
class CCXTLeverageTier(TypedDict, total=False):
    """CCXT leverage tier structure."""

    tier: int | None
    currency: str | None
    minNotional: float | None
    maxNotional: float | None
    maxLeverage: float | None
    initialMargin: float | None
    maintenanceMargin: float | None
    info: dict[str, str | float | int | bool | None] | None


# Metadata and leverage limits from CCXT (str, float, int, None values)
CCXTMetadata = dict[str, str | float | int | None]
