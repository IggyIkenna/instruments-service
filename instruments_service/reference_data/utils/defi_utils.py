"""Shared utilities for DeFi reference-data adapters.

Extracted from uniswap_v2, uniswap_v3, uniswap_v4, aave_v3, curve, balancer, morpho
to eliminate copy-paste duplication across 7 adapters.
"""

from datetime import UTC, datetime

#: Tokens that act as quote/price denominators in DeFi pool pairs.
DEFI_QUOTE_TOKENS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "WETH", "ETH", "WBTC", "USDE"})


def classify_graph_error(exc: Exception, status: int | None = None) -> str:
    """Map a The Graph / DeFi subgraph HTTP or network error to a UAC error code."""
    if status == 429:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "503" in msg or "unavailable" in msg:
        return "503"
    return "UNKNOWN"


def order_base_quote(sym0: str, sym1: str) -> tuple[str, str]:
    """Order two token symbols into (base, quote) using quote-token preference.

    Returns ``(base, quote)`` where ``quote`` is the stablecoin / wrapped-ETH
    when one of the tokens is in ``DEFI_QUOTE_TOKENS``, otherwise preserves order.
    """
    s0, s1 = sym0.upper(), sym1.upper()
    if s1 in DEFI_QUOTE_TOKENS and s0 not in DEFI_QUOTE_TOKENS:
        return s0, s1
    if s0 in DEFI_QUOTE_TOKENS and s1 not in DEFI_QUOTE_TOKENS:
        return s1, s0
    return s0, s1


def parse_created_timestamp(ts_raw: object) -> datetime | None:
    """Parse a ``createdAtTimestamp`` field (Unix seconds as string/int) into UTC datetime.

    Returns ``None`` for missing, unparseable, or out-of-range values.
    """
    if ts_raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(str(ts_raw)), tz=UTC)
    except (ValueError, OSError):
        return None
