"""Tardis symbol / expiry / quote parsing helpers.

Cohesion module of the ``adapters.cefi.tardis`` package (split from the former
monolithic ``adapters/cefi/tardis.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators and sibling functions resolve through ``_tardis`` — the
live package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.cefi.tardis.<name>")`` targets and
cross-module monkeypatching behave exactly as they did before the split.
"""

# Package-internal access: the tardis package is ONE logical namespace split
# across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import contextlib
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from unified_api_contracts.internal import InstrumentType, MarginType, OptionType

if TYPE_CHECKING:
    from unified_api_contracts import TardisInstrumentDetail

    from instruments_service.reference_data.adapters.cefi import tardis as _tardis
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.cefi.tardis._pkg_ref import tardis_namespace as _tardis

__all__ = [
    "_BITFINEX_BASE_ALIASES",
    "_BITFINEX_QUOTE_ALIASES",
    "_DERIBIT_MONTHS",
    "_QUOTE_CURRENCIES",
    "_QUOTE_CURRENCIES_SET",
    "_infer_derivative_quote",
    "_infer_margin_type",
    "_normalize_option_type",
    "_parse_ddmmmyy",
    "_parse_deribit_symbol_expiry",
    "_parse_expiry",
    "_parse_underscore_yymmdd_symbol_expiry",
    "_parse_yymmdd_symbol_expiry",
    "_passes_asset_filter",
    "_resolve_base_quote",
    "_resolve_bitfinex_spot",
    "_resolve_option_fields",
    "_split_kraken_symbol",
    "_split_symbol",
]


def _normalize_option_type(raw: str | None) -> OptionType | None:
    """Normalize option type to OptionType enum, or None if unrecognised."""
    if not raw:
        return None
    upper = raw.strip().upper()
    if upper in ("CALL", "C"):
        return OptionType.CALL
    if upper in ("PUT", "P"):
        return OptionType.PUT
    return None


# Quote currencies for symbol splitting (longest first for correct prefix matching)
# Extended from instruments-service/engine/processors/symbol_parser.py _ALL_QUOTE_SUFFIXES
_QUOTE_CURRENCIES: list[str] = [
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "USDE",
    "BRZ",
    "IDRT",
    "BIDR",
    "USD",
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "TRX",
    "ADA",
    "SOL",
    "LTC",
    "DOT",
    "LINK",
    "UNI",
    "AVAX",
    "DOGE",
    "SHIB",
    "PEPE",
    "FLOKI",
    "WIF",
    "BONK",
    "MEME",
    "KRW",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "TRY",
    "BRL",
    "RUB",
    "NGN",
    "ZAR",
    "DAI",
    "VAI",
    "GEL",
    "CZK",
    "MXN",
    "ARS",
    "COP",
    "CLP",
    "PEN",
    "VES",
    "UAH",
    "PLN",
    "RON",
    "CNY",
    "HKD",
    # 2026-05-03: extra fiats spotted on OKX-SPOT (Middle East, SE Asia, S Asia).
    # Without these, BTC-{AED,SAR,MYR,...} fell through to USD and collided into
    # the same instrument_key as BTC-USD.
    "AED",
    "SAR",
    "MYR",
    "SGD",
    "IDR",
    "PHP",
    "THB",
    "INR",
    "VND",
    "TWD",
]

# Set version for O(1) lookup in _split_symbol
_QUOTE_CURRENCIES_SET: frozenset[str] = frozenset(_QUOTE_CURRENCIES)


def _parse_expiry(s: str | None) -> datetime | None:
    """Parse ISO date/datetime string (with optional Z suffix) to UTC datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


_DERIBIT_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _parse_deribit_symbol_expiry(raw_id: str) -> datetime | None:
    """Parse expiry from Deribit symbol format: BTC-27MAR26-190000-C.

    The second segment is DDMMMYY (e.g. 27MAR26 = 2026-03-27).
    Also handles futures like BTC-27MAR26 (no strike/option segments).
    """
    parts = raw_id.split("-")
    if len(parts) < 2 or len(parts[1]) < 5:
        return None
    return _tardis._parse_ddmmmyy(parts[1])


def _parse_ddmmmyy(date_part: str) -> datetime | None:
    """Parse DDMMMYY string (e.g. '27MAR26') into a UTC datetime."""
    try:
        m = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", date_part.upper())
        if not m:
            return None
        day, month_str, year_str = int(m.group(1)), m.group(2), int(m.group(3))
        month = _DERIBIT_MONTHS.get(month_str)
        if not month:
            return None
        return datetime(2000 + year_str, month, day, tzinfo=UTC)
    except (ValueError, IndexError):
        return None


def _parse_yymmdd_symbol_expiry(raw_id: str) -> datetime | None:
    """Parse expiry from OKX-style symbol: BASE-QUOTE-YYMMDD (e.g. BTC-USD-260626).

    The last segment is a 6-digit YYMMDD date string.  Also handles the
    shorter BASE-YYMMDD variant used by some exchanges.
    """
    parts = raw_id.split("-")
    # Try last segment first (covers both BASE-QUOTE-YYMMDD and BASE-YYMMDD)
    for idx in (-1, -2):
        if abs(idx) > len(parts):
            continue
        seg = parts[idx]
        if len(seg) == 6 and seg.isdigit():
            try:
                yy, mm, dd = int(seg[:2]), int(seg[2:4]), int(seg[4:6])
                return datetime(2000 + yy, mm, dd, tzinfo=UTC)
            except ValueError:
                continue
    return None


def _parse_underscore_yymmdd_symbol_expiry(raw_id: str) -> datetime | None:
    """Parse expiry from Kraken-Futures-style symbol: FI_XBTUSD_240329.

    The last segment (underscore-separated) is a 6-digit YYMMDD date string.
    Perpetuals (e.g. ``PF_XBTUSD_PERP``) return None — non-digit suffix.
    """
    parts = raw_id.split("_")
    for idx in (-1, -2):
        if abs(idx) > len(parts):
            continue
        seg = parts[idx]
        if len(seg) == 6 and seg.isdigit():
            try:
                yy, mm, dd = int(seg[:2]), int(seg[2:4]), int(seg[4:6])
                return datetime(2000 + yy, mm, dd, tzinfo=UTC)
            except ValueError:
                continue
    return None


#: Bitfinex uses non-standard short BASE tickers on its concatenated spot symbols
#: (``ALGUSD`` = ALGO/USD). Map them back to the canonical ``CEFI_BASE_ASSET_UNIVERSE``
#: tickers so the base-membership leg of the MVP capture predicate passes (the
#: 25-base spot↔perp overlap incl. ALG/ATO; cefi_universe_capture_rule 2026-06-23).
_BITFINEX_BASE_ALIASES: dict[str, str] = {
    "ALG": "ALGO",
    "ATO": "ATOM",
    "DSH": "DASH",
    "IOT": "IOTA",
    "UDC": "USDC",
    "UST": "USDT",  # Bitfinex code for Tether USDT (NOT TerraUSD)
    "DOG": "DOGE",
    "MNA": "MANA",
    "AGI": "FET",  # legacy SingularityNET (now FET)
    "QSH": "QASH",
    "DAT": "DATA",
    "SNG": "SNGLS",
}

#: Bitfinex QUOTE pseudo-codes → canonical quote tickers.
_BITFINEX_QUOTE_ALIASES: dict[str, str] = {
    "UST": "USDT",  # Tether USDT
    "UDC": "USDC",
}


def _resolve_bitfinex_spot(raw_id: str) -> tuple[str, str]:
    """Resolve a Bitfinex spot ``raw_id`` to canonical ``(base, quote)``.

    Bitfinex spot symbols come in TWO shapes:
      * ``<BASE>:<QUOTE>`` (modern / >3-char tickers): ``AAVE:USD``, ``LINK:UST``,
        ``1INCH:UST``. The colon is the base/quote separator.
      * concatenated 3-char short tickers (legacy): ``ALGUSD`` (ALG/USD),
        ``ATOUST`` (ATO/UST), ``DSHBTC``. No separator, no metadata.

    Bitfinex's short base codes (``ALG``/``ATO``/``DSH``/``IOT``) and its quote
    pseudo-codes (``UST``=USDT, ``UDC``=USDC) are mapped back to canonical tickers
    (cefi_universe_capture_rule 2026-06-23). Returns ``("", "")`` when neither shape
    resolves (caller falls back).
    """
    upper = raw_id.upper()
    # Colon-delimited: BASE:QUOTE
    if ":" in upper:
        left, _, right = upper.partition(":")
        base = _BITFINEX_BASE_ALIASES.get(left, left)
        quote = _BITFINEX_QUOTE_ALIASES.get(right, right)
        return base, quote
    # Concatenated short ticker: <BASE><QUOTE>. Quote suffixes longest-first;
    # include Bitfinex pseudo-quotes UST/UDC before the canonical set.
    bfx_quotes = ("USDT", "USDC", "UST", "UDC", "USD", "BTC", "ETH", "EUR", "GBP", "JPY")
    for q in bfx_quotes:
        if upper.endswith(q) and len(upper) > len(q):
            raw_base = upper[: -len(q)]
            base = _BITFINEX_BASE_ALIASES.get(raw_base, raw_base)
            quote = _BITFINEX_QUOTE_ALIASES.get(q, q)
            return base, quote
    return "", ""


def _resolve_base_quote(item: TardisInstrumentDetail, raw_id: str, exchange: str) -> tuple[str, str]:
    """Parse base/quote from Tardis metadata or fall back to symbol splitting.

    For derivatives without an explicit quote suffix (e.g. BTC-PERPETUAL,
    BTC-27MAR26-190000-C), the settlement currency is inferred from the
    exchange context:
      - deribit: USD (inverse) or USDC (linear)
      - okex coin-margined (-USD_UM-): USD
      - binance-futures COIN-M: USD
      - all others: USD as safe default for derivatives
    """
    base = item.baseCurrency or ""
    quote = item.quoteCurrency or ""
    if base and quote:
        return base, quote

    upper_id = raw_id.upper()
    # Kraken spot (Tardis ``kraken``) uses ``<BASE>/<QUOTE>`` — AAVE/USD, AAVE/ETH,
    # 1INCH/USD, XBT/USD. Kraken Futures (Tardis ``cryptofacilities``) uses a typed
    # prefix + concatenated ``<BASE><QUOTE>`` — PF_AAVEUSD (linear perp), PI_XBTUSD
    # (inverse perp), FI_XBTUSD_240329 (inverse future). The generic ``/``-naive +
    # quote-suffix splitting mis-parses BOTH (``AAVE/`` keeps the slash; ``PF_AAVE``
    # keeps the prefix; ``XBTUSD`` greedily matches the TUSD quote → ``XB``), so they
    # are handled here. Kraken denominates BTC as ``XBT`` → map back to canonical BTC.
    if exchange in ("kraken", "cryptofacilities"):
        kraken_base, kraken_quote = _split_kraken_symbol(upper_id)
        if kraken_base:
            return kraken_base, kraken_quote
    # Bitfinex derivatives use ``<BASE>F0:<QUOTE>F0`` — ``F0`` is Bitfinex's
    # "perpetual" marker, not a currency suffix. Examples:
    #   BTCF0:USTF0  → base=BTC,  quote=USDT (USDT-margined linear perp)
    #   ETHF0:BTCF0  → base=ETH,  quote=BTC  (BTC-margined inverse perp)
    #   XAUTF0:BTCF0 → base=XAUT, quote=BTC
    # Strip the F0 marker on both sides; map the Bitfinex pseudo-codes
    # (``UST``→``USDT``) back to canonical currency tickers.
    if exchange == "bitfinex-derivatives" and ":" in upper_id:
        left, right = upper_id.split(":", 1)
        base = left[:-2] if left.endswith("F0") else left
        quote = right[:-2] if right.endswith("F0") else right
        # Bitfinex pseudo-tickers → canonical (base ALG→ALGO/ATO→ATOM/…; quote UST→USDT)
        return (
            _tardis._BITFINEX_BASE_ALIASES.get(base, base),
            _tardis._BITFINEX_QUOTE_ALIASES.get(quote, quote),
        )
    # Bitfinex SPOT (Tardis ``bitfinex``): BASE:QUOTE (modern) or concatenated
    # 3-char short tickers (ALGUSD/ATOUST). Non-standard base + quote codes are
    # mapped to canonical (cefi_universe_capture_rule 2026-06-23).
    if exchange == "bitfinex":
        bfx_base, bfx_quote = _tardis._resolve_bitfinex_spot(upper_id)
        if bfx_base:
            return bfx_base, bfx_quote
    if "-" in upper_id:
        parts = upper_id.split("-")
        # UPBIT uses QUOTE-BASE format: KRW-BTC, BTC-DOT, USDT-SOL
        if exchange == "upbit" and len(parts) >= 2:
            return parts[1], parts[0]
        # Use the full curated quote set (~50 currencies, fiat + crypto + stables)
        # so spot pairs like BTC-TRY / BTC-BRL / BTC-AUD / BTC-AED keep their
        # actual quote currency. Pre-2026-05-03 a hard-coded 7-currency subset
        # collapsed every "exotic" fiat to USD via the derivative-fallback below
        # → 4 distinct OKX-SPOT pairs all collided into BTC-USD.
        if len(parts) >= 2 and parts[1] in _tardis._QUOTE_CURRENCIES_SET:
            return parts[0], parts[1]
        # Derivatives without explicit quote suffix — infer settlement currency.
        # Deribit USDC-denominated instruments contain "USDC" in the symbol.
        derived_base = parts[0]
        derived_quote = _tardis._infer_derivative_quote(upper_id, exchange)
        # Handle underscore-separated base: BTC_USDC → base=BTC, quote=USDC
        # Deribit linear instruments use BASE_QUOTE format (BTC_USDC-PERPETUAL)
        if "_" in derived_base:
            sub_parts = derived_base.split("_", 1)
            if sub_parts[1] in ("USDC", "USDT", "USD", "BUSD"):
                derived_base = sub_parts[0]
                derived_quote = sub_parts[1]
        return derived_base, derived_quote

    # Concatenated: BTCUSDT, ETHUSDT → split by known quote suffix
    return _tardis._split_symbol(upper_id)


def _infer_derivative_quote(upper_id: str, exchange: str) -> str:
    """Infer the quote/settlement currency for a derivative without explicit quote suffix.

    Returns the most specific match based on symbol patterns and exchange conventions.
    """
    # USDC-denominated: symbol contains USDC (e.g. BTC_USDC-PERPETUAL on deribit)
    if "USDC" in upper_id:
        return "USDC"
    # USDT-denominated: symbol contains USDT (e.g. BTCUSDT_PERP on some exchanges)
    if "USDT" in upper_id:
        return "USDT"
    # OKX coin-margined: BTC-USD_UM-SWAP, BTC-USD_UM-260626
    if "USD_UM" in upper_id or "USD_CM" in upper_id:
        return "USD"
    # Everything else: crypto derivatives default to USD settlement
    # (deribit inverse, binance COIN-M, generic perpetuals/futures)
    return "USD"


def _infer_margin_type(
    instrument_type: InstrumentType,
    quote: str,
    raw_id: str,
    exchange: str,
) -> MarginType | None:
    """Infer margin type for derivative instruments.

    INVERSE: coin-margined (settled in base asset — USD quote but crypto settlement).
    LINEAR: USDT/USDC-margined or crypto-margined with stable quote.
    None: spot instruments.

    Coin-margined patterns:
      - quote == "USD" on binance-futures (COIN-M), deribit (inverse), OKX (USD_UM)
      - Deribit has both inverse (USD-settled) and linear (USDC-settled) — distinguish by quote
    """
    if instrument_type not in (InstrumentType.FUTURE, InstrumentType.PERPETUAL, InstrumentType.OPTION):
        return None
    upper_id = raw_id.upper()
    # OKX coin-margined: symbol contains USD_UM or USD_CM
    if "USD_UM" in upper_id or "USD_CM" in upper_id:
        return MarginType.INVERSE
    # Binance COIN-M futures: exchange is "binance-futures" (USDT-M, has coin-M perps
    # on the same endpoint via USD quote) OR "binance-delivery" (the dedicated COIN-M
    # delivery endpoint — always inverse). Quote is USD (not USDT/USDC) for both.
    if exchange in ("binance-futures", "binance-delivery") and quote.upper() == "USD":
        return MarginType.INVERSE
    # Deribit inverse: settled in BTC/ETH (USD quote but coin-margined)
    if exchange == "deribit" and quote.upper() == "USD":
        return MarginType.INVERSE
    # All other derivatives with stable quote (USDT, USDC) or default → linear
    return MarginType.LINEAR


def _passes_asset_filter(base: str, quote: str, instrument_type: str, venue: str | None = None) -> bool:
    """Check if the instrument passes the CeFi venue universe filter.

    FULL-UNIVERSE enumeration (operator 2026-06-23, mirrors the BUG #4 HL/ASTER
    whitelist drop in ``aster.py``/``hyperliquid.py``): SPOT/PERP/FUTURE on a
    canonical quote enumerate for EVERY base asset — the curated
    ``CEFI_BASE_ASSET_UNIVERSE`` majors whitelist is NO LONGER a gate for them
    (funding/price history for small/illiquid coins is valuable; the reference
    universe must equal the venues' real listed universe so the catalogue is
    ⊇ the captured present-set). The two surviving gates are venue-volume-safe,
    not coin-curation:
      1. accepted-quote gate — USDT/USDC/USD fleet-wide, PLUS the per-venue
         extensions from the UAC SSOT ``accepted_quotes_for_venue`` (KRW is
         accepted ONLY for UPBIT — the kimchi-premium venue, operator 2026-06-23).
         Drops exotic cross pairs (BASE/EUR, BASE/BTC) elsewhere; derivatives
         carry no quote and pass.
      2. OPTIONS underlyings stay restricted to ``CEFI_OPTIONS_UNDERLYINGS``
         (BTC/ETH) — a Deribit-options-per-coin explosion is a genuine
         data-volume constraint (DERIBIT already ~213k historical rows), and the
         operator universe SSOT documents options as BTC/ETH-only by design.

    Args:
        base: Base asset ticker.
        quote: Quote asset ticker ("" for derivatives).
        instrument_type: Canonical instrument type string.
        venue: Canonical venue (e.g. ``UPBIT``) for the per-venue accepted-quote
            extension. ``None`` → fleet-default quotes (USDT/USDC/USD).
    """
    base_upper = base.upper()
    quote_upper = quote.upper() if quote else ""
    accepted_quotes = _tardis.accepted_quotes_for_venue(venue)
    # Quote gate: USD + major stablecoins (+ KRW for UPBIT). No exotic cross pairs.
    # Derivatives (perps, futures, options) have no quote — allow them through.
    if quote_upper and quote_upper not in accepted_quotes:
        return False
    # Options: only BTC and ETH underlyings (per-coin option chains explode data).
    return not (instrument_type == "OPTION" and base_upper not in _tardis.CEFI_OPTIONS_UNDERLYINGS)


def _resolve_option_fields(
    item: TardisInstrumentDetail, instrument_type: str, raw_id: str
) -> tuple[Decimal | None, str | None]:
    """Extract strike price and option type from Tardis item metadata.

    Falls back to parsing Deribit-style option symbol names
    (e.g. BTC-27MAR26-190000-C).
    """
    strike_raw = item.strikePrice
    strike = Decimal(str(strike_raw)) if strike_raw is not None else None
    opt_type_raw = item.optionType
    opt_type = _tardis._normalize_option_type(opt_type_raw)

    if instrument_type.upper() == "OPTION" and (strike is None or opt_type is None):
        parts = raw_id.split("-")
        if len(parts) >= 4:
            if strike is None:
                with contextlib.suppress(Exception):
                    strike = Decimal(parts[-2])
            if opt_type is None and parts[-1] in ("C", "P"):
                opt_type = _tardis._normalize_option_type(parts[-1])

    return strike, opt_type


#: Kraken Futures (Tardis ``cryptofacilities``) instrument-type prefixes.
#: PF_ = linear (multi-collateral) perpetual; PI_ = inverse perpetual;
#: FI_ = inverse fixed-expiry future; FF_ = linear fixed-expiry future.
_KRAKEN_FUTURES_PREFIXES: tuple[str, ...] = ("PF_", "PI_", "FF_", "FI_")

#: Kraken denominates Bitcoin as ``XBT`` (ISO 4217-style); map back to the
#: canonical ``BTC`` ticker used by ``CEFI_BASE_ASSET_UNIVERSE``.
_KRAKEN_BASE_ALIASES: dict[str, str] = {"XBT": "BTC"}

#: Quote suffixes a Kraken Futures concatenated ``<BASE><QUOTE>`` body may carry,
#: longest-first so ``USD`` is not greedily matched before a stable variant.
_KRAKEN_FUTURES_QUOTES: tuple[str, ...] = ("USDT", "USDC", "USD")


def _split_kraken_symbol(upper_id: str) -> tuple[str, str]:
    """Split a Kraken spot (``AAVE/USD``) or Kraken-Futures (``PF_AAVEUSD``) id.

    Kraken spot ids are ``<BASE>/<QUOTE>`` (slash-separated). Kraken Futures ids
    (Tardis ``cryptofacilities``) carry a typed prefix (PF_/PI_/FF_/FI_) + a
    concatenated ``<BASE><QUOTE>`` body, optionally with a trailing ``_YYMMDD``
    expiry segment for fixed-expiry futures (``FI_XBTUSD_240329``). Returns
    ``(base, quote)`` with the Kraken ``XBT``→``BTC`` alias applied, or
    ``("", "")`` when the id matches neither shape (caller falls back).
    """
    # Spot: BASE/QUOTE
    if "/" in upper_id:
        base, _, quote = upper_id.partition("/")
        base = _KRAKEN_BASE_ALIASES.get(base, base)
        return base, quote
    # Futures: <PREFIX><BASE><QUOTE>[_<YYMMDD>]
    body = upper_id
    for prefix in _KRAKEN_FUTURES_PREFIXES:
        if body.startswith(prefix):
            body = body[len(prefix) :]
            break
    else:
        return "", ""
    # Drop a trailing expiry segment (fixed-expiry futures: AAVEUSD_240329).
    body = body.split("_", 1)[0]
    for quote in _KRAKEN_FUTURES_QUOTES:
        if body.endswith(quote) and len(body) > len(quote):
            base = body[: -len(quote)]
            base = _KRAKEN_BASE_ALIASES.get(base, base)
            return base, quote
    return "", ""


def _split_symbol(symbol: str) -> tuple[str, str]:
    """Split a concatenated symbol like BNBBTC into (BNB, BTC) using known quote currencies.

    Also handles underscore-separated symbols: BTC_USDC → (BTC, USDC) and the
    dated-future shape ``<BASE><QUOTE>_<YYMMDD|EXPIRY>`` (e.g. ``BTCUSDT_260626``,
    ``BTCBUSD_210129``) where the part AFTER ``_`` is a settlement/expiry tag, NOT
    a quote — the real ``<BASE><QUOTE>`` is the part BEFORE the underscore, which
    is then concatenated-suffix-matched (full-universe enumeration 2026-06-23: the
    binance-futures dated quarterlies previously fell through to an empty quote →
    raised InstrumentRecord's non-empty-quote guard → killed the whole venue).
    """
    # Underscore-separated: BTC_USDC, ETH_USDT, STETH_ETH (suffix IS a quote)
    if "_" in symbol:
        sub_parts = symbol.split("_", 1)
        if sub_parts[1] in _tardis._QUOTE_CURRENCIES_SET:
            return sub_parts[0], sub_parts[1]
        # Suffix is an expiry/settlement tag (e.g. 260626) — quote is embedded in the
        # left ``<BASE><QUOTE>`` body; concatenated-match that.
        left = sub_parts[0]
        for quote in _tardis._QUOTE_CURRENCIES:
            if left.endswith(quote) and len(left) > len(quote):
                return left[: -len(quote)], quote
    # Concatenated suffix match: BTCUSDT → (BTC, USDT)
    for quote in _tardis._QUOTE_CURRENCIES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""
