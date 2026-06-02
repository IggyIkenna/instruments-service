"""Shared utilities for DeFi reference-data adapters.

Extracted from uniswap_v2, uniswap_v3, uniswap_v4, aave_v3, curve, balancer, morpho
to eliminate copy-paste duplication across 7 adapters.
"""

from datetime import UTC, datetime
from typing import cast

#: Tokens that act as quote/price denominators in DeFi pool pairs.
DEFI_QUOTE_TOKENS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "WETH", "ETH", "WBTC", "USDE"})

# Standard EVM token decimals (ERC-20 standard is 18; stablecoins and BTC wrappers differ).
_EVM_TOKEN_DECIMALS: dict[str, int] = {
    "USDC": 6,
    "USDT": 6,
    "USDE": 6,
    "WBTC": 8,
    "ETH": 18,
    "WETH": 18,
    "WSTETH": 18,
    "STETH": 18,
    "RETH": 18,
    "DAI": 18,
    "FRAX": 18,
    "LUSD": 18,
    "CRVUSD": 18,
    "SUSD": 18,
    "TUSD": 18,
    "WBNB": 18,
    "BNB": 18,
    "AVAX": 18,
    "WAVAX": 18,
    "SAVAX": 18,
    "WETHE": 18,
    "BTCB": 18,
    "ARB": 18,
    "OP": 18,
    "MATIC": 18,
    "WMATIC": 18,
    "AAVE": 18,
    "UNI": 18,
    "LINK": 18,
    "CRV": 18,
    "LDO": 18,
    "SNX": 18,
    "MKR": 18,
    "COMP": 18,
    "YFI": 18,
    "CBETH": 18,
    "MSOL": 18,
}


def resolve_evm_token_decimals(symbol: str) -> int:
    """Return canonical ERC-20 decimals for a known token; default 18."""
    return _EVM_TOKEN_DECIMALS.get(symbol.upper(), 18)


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


def assert_subgraph_payload(data: object, *, venue: str, chain: str) -> dict[str, object]:
    """Validate a GraphQL subgraph JSON response; return its inner ``data`` dict, or RAISE.

    Raises ``ConnectionError`` (the IS discovery caller ``urdi_reference_provider`` classifies it as a
    retryable NETWORK failure → records the venue/day ``attempted_failed``) when the response signals a
    TRANSIENT FETCH FAILURE rather than a legitimate empty universe:

    * HTTP 200 with a top-level ``errors`` array (GraphQL error / rate-limit / indexing lag), OR
    * a missing / non-dict ``data`` payload.

    A present ``data`` dict (even with empty collections inside) is a legitimate response and is returned.

    DeFi-plan A8 / operator directive 2026-05-07: a soft ``200-with-errors`` must NOT be silently
    ``return []`` — that masks the failure as an empty instrument universe, so those instrument-days drop
    out of the expected coverage denominator entirely (false-complete coverage) instead of being honestly
    ``attempted_failed``.
    """
    if not isinstance(data, dict):
        raise ConnectionError(f"{venue}-{chain}: non-dict subgraph response — transient fetch failure")
    payload: dict[str, object] = cast("dict[str, object]", data)
    errors: object = payload.get("errors")
    if errors:
        raise ConnectionError(
            f"{venue}-{chain}: subgraph returned GraphQL errors — transient fetch failure: {str(errors)[:200]}"
        )
    inner: object = payload.get("data")
    if not isinstance(inner, dict):
        raise ConnectionError(f"{venue}-{chain}: subgraph response missing 'data' payload — transient fetch failure")
    return cast("dict[str, object]", inner)


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

    Returns ``None`` for missing, unparseable, zero, or out-of-range values.
    Epoch 0 (1970-01-01) is treated as "unknown" — no DeFi protocol existed before 2015.
    """
    if ts_raw is None:
        return None
    try:
        ts = int(str(ts_raw))
        if ts <= 0:
            return None
        return datetime.fromtimestamp(ts, tz=UTC)
    except (ValueError, OSError):
        return None
