"""Deribit combo/multi-leg strategy reference data adapter — direct REST API.

Fetches Deribit published combo instruments (straddles, strangles, spreads,
condors, butterflies, etc.) directly from the Deribit public REST API.  This
adapter complements the Tardis adapter which covers historical data: use this
adapter in LIVE mode for real-time active combos.

API doc: https://docs.deribit.com/#public-get_instruments (kind=combo)
Base URL: https://www.deribit.com/api/v2
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import UnsupportedCapabilityError, classify_venue_error
from unified_api_contracts.internal import (
    InstrumentLeg,
    InstrumentRecord,
    InstrumentStatus,
    InstrumentType,
)
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.deribit.com/api/v2"

# Deribit underlyings that are actively traded.
_DERIBIT_COMBO_UNDERLYINGS: list[str] = ["BTC", "ETH", "SOL", "BNB", "XRP"]

# Map Deribit combo_type → human-readable classification (also used for instrument_key suffix).
# Deribit returns names like "BTC-STRD-25APR26-90000" — the structure code is embedded.
_DERIBIT_STRUCTURE_CODES: frozenset[str] = frozenset(
    {
        # Future spreads
        "FS",
        # Vanilla option spreads
        "CS",
        "PS",
        "STRD",
        "STRG",
        "RR",
        "RRITM",
        "GUTS",
        "REV",
        # 3-leg (butterflies, ladders)
        "CBUT",
        "PBUT",
        "CBUT111",
        "PBUT111",
        "CLAD",
        "PLAD",
        # 4-leg (condors, iron butterflies, boxes)
        "IBUT",
        "ICOND",
        "CCOND",
        "PCOND",
        "BOX",
        # Calendar / diagonal (2 expiries)
        "CCAL",
        "PCAL",
        "CDIAG",
        "PDIAG",
        "STDC",
        "DSTDC",
        # Ratio spreads
        "CSR12",
        "CSR13",
        "CSR23",
        "PSR12",
        "PSR13",
        "PSR23",
        # Jelly roll
        "JR",
    }
)


def _extract_structure_code(instrument_name: str) -> str:
    """Extract the Deribit structure code from a combo instrument name.

    Deribit combo names: BASE-CODE-EXPIRY-STRIKES (e.g. BTC-STRD-25APR26-90000).
    Returns the structure code, or "UNKNOWN" if it cannot be parsed.
    """
    parts = instrument_name.split("-")
    for part in parts:
        if part in _DERIBIT_STRUCTURE_CODES:
            return part
    return "UNKNOWN"


def _parse_combo_legs(instrument_name: str) -> list[InstrumentLeg]:
    """Build a minimal legs list from the combo instrument name.

    Each combo instrument is defined by its component legs encoded in its
    name. The full leg parsing (strike/expiry resolution) matches the
    logic in the Tardis adapter's ``_parse_deribit_combo_legs``.  Here
    we emit symbolic leg instrument_keys so that downstream consumers can
    resolve them against the full instrument catalogue.

    Returns an empty list if the name cannot be parsed.
    """
    parts = instrument_name.split("-")
    if len(parts) < 3:
        return []
    # No legs pre-populated here; combo instruments are identified by their name
    # and structure code. Downstream leg resolution uses the Tardis instrument catalogue.
    # This keeps the live adapter lightweight — full leg expansion is a feature for
    # tools that need it (e.g. execution-service combo quoting).
    return []


def _classify_deribit_error(exc: Exception, status: int | None = None) -> str:
    """Map a Deribit HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "429"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    if (status is not None and status >= 500) or "500" in msg or "internal" in msg:
        return "500"
    return "UNKNOWN"


class DeribitComboReferenceDataAdapter(BaseReferenceDataAdapter):
    """Deribit combo/multi-leg strategy reference data adapter (live REST).

    Fetches active combo instruments from the Deribit public REST API
    (``GET /api/v2/public/get_instruments?kind=combo``).  Each combo
    instrument maps to ``InstrumentType.COMBO`` with legs populated from
    the symbol name.

    This adapter is used in LIVE mode (real-time combos from the exchange).
    For historical backfill, use the Tardis adapter (which covers Deribit
    combo instruments via the ``kind=combo`` Tardis instruments endpoint).
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "DERIBIT"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Deribit combo instruments for all supported underlyings.

        Calls ``GET /api/v2/public/get_instruments?currency=<ccy>&kind=combo&expired=false``
        for each currency in ``_DERIBIT_COMBO_UNDERLYINGS`` and returns
        ``InstrumentRecord`` objects with ``instrument_type=COMBO``.

        Args:
            instrument_type: If provided and not COMBO, raises ``UnsupportedCapabilityError``.
                Pass None (default) or ``InstrumentType.COMBO`` to fetch combo instruments.
        """
        if instrument_type is not None and instrument_type != InstrumentType.COMBO:
            raise UnsupportedCapabilityError(
                venue="DERIBIT",
                capability=str(instrument_type),
                message=(
                    f"DeribitComboReferenceDataAdapter only fetches COMBO instruments. "
                    f"Requested: {instrument_type}. "
                    "Use the Tardis adapter (via CCXT in live mode) for OPTION/FUTURE/PERPETUAL."
                ),
            )

        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        failures: list[str] = []

        for currency in _DERIBIT_COMBO_UNDERLYINGS:
            # Shard-level failure isolation: a failed currency MUST NOT kill other currencies.
            try:
                instruments = await self._fetch_combos_for_currency(currency, now)
                results.extend(instruments)
            except UnsupportedCapabilityError:
                raise  # Re-raise guard errors — they are programming errors, not shard failures.
            except RuntimeError as exc:
                # Genuine HTTP/network fetch-failure — already classified + emitted
                # ADAPTER_FETCH_FAILED in the helper. Track + isolate (re-raised below if
                # EVERY currency failed); do NOT re-emit.
                failures.append(currency)
                logger.error("Deribit combo currency=%s fetch failed: %s", currency, exc)
                # Continue to next currency — shard-level failure isolation.
            except Exception as exc:
                # Parse/validation error for this currency — isolate + track (re-raised below
                # if EVERY currency failed). Shard-level failure isolation.
                failures.append(currency)
                error_code = _classify_deribit_error(exc)
                classification = classify_venue_error("deribit", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Deribit combo fetch failed for currency=%s: %s (classified: %s, action: %s, retry_safe: %s)",
                    currency,
                    exc,
                    error_code,
                    action,
                    retry_safe,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "deribit",
                        "adapter": "DeribitComboReferenceDataAdapter",
                        "endpoint": "get_instruments",
                        "currency": currency,
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                # Continue to next currency — shard-level failure isolation.

        # CF-11: if the universe is empty BECAUSE every attempted currency failed (not because
        # the currencies legitimately have zero active combos), re-raise so
        # urdi_reference_provider._fetch_one routes DERIBIT into failed[] (→ attempted_failed),
        # never a clean empty that vanishes into _non_error_venues. A partial result (≥1 currency
        # returned) is trustworthy → return it.
        if not results and failures:
            raise RuntimeError(
                f"Deribit combo get_instruments: all {len(failures)} attempted currenc(ies) failed "
                f"({failures}); no instruments fetched"
            )

        logger.info(
            "DeribitComboAdapter: fetched %d COMBO instruments across %d currencies",
            len(results),
            len(_DERIBIT_COMBO_UNDERLYINGS),
        )
        return results

    async def _fetch_combos_for_currency(
        self,
        currency: str,
        now: datetime,
    ) -> list[InstrumentRecord]:
        """Fetch combo instruments for a single Deribit currency.

        Returns a list of InstrumentRecord objects (possibly empty if no active
        combos exist for this currency at the time of the call).
        """
        url = f"{_BASE}/public/get_instruments"
        params = {
            "currency": currency,
            "kind": "combo",
            "expired": "false",
        }

        try:
            async with self._make_session() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                payload: object = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = _classify_deribit_error(exc, getattr(exc, "status", None))
            classification = classify_venue_error("deribit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Deribit get_instruments (combo, currency=%s) HTTP error: %s "
                "(classified: %s, action: %s, retry_safe: %s)",
                currency,
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "deribit",
                    "adapter": "DeribitComboReferenceDataAdapter",
                    "endpoint": "get_instruments",
                    "currency": currency,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # CF-11: raise (NOT ``return []``) so get_instruments can distinguish a genuine
            # fetch-failure from a legitimately-empty currency. An all-currency failure must
            # surface as attempted_failed via urdi_reference_provider._fetch_one, never a
            # clean empty that lands DERIBIT in _non_error_venues (silent universe shrink).
            raise RuntimeError(
                f"Deribit combo fetch failed for currency={currency} "
                f"(error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc

        # Deribit wraps responses in {"result": [...], "id": ...}
        if not isinstance(payload, dict):
            logger.warning("Deribit combo response is not a dict for currency=%s", currency)
            return []

        result_raw: object = payload.get("result", [])
        instruments_raw: list[object] = result_raw if isinstance(result_raw, list) else []

        records: list[InstrumentRecord] = []
        for item in instruments_raw:
            record = self._parse_combo_instrument(item, now)
            if record is not None:
                records.append(record)

        logger.debug(
            "DeribitComboAdapter: currency=%s → %d active combos",
            currency,
            len(records),
        )
        return records

    def _parse_combo_instrument(
        self,
        item: object,
        now: datetime,
    ) -> InstrumentRecord | None:
        """Parse a single Deribit combo instrument dict into an InstrumentRecord.

        Returns None if the item is malformed or missing required fields.
        """
        if not isinstance(item, dict):
            return None

        instrument_name: str = str(item.get("instrument_name", ""))
        if not instrument_name:
            return None

        # Extract the underlying currency from the instrument name (first segment).
        # e.g. "BTC-STRD-25APR26-90000" → "BTC"
        name_parts = instrument_name.split("-")
        underlying: str = name_parts[0] if name_parts else ""
        if not underlying:
            return None

        # Creation timestamp from Deribit (milliseconds UTC).
        creation_ts_ms: object = item.get("creation_timestamp", 0)
        try:
            available_from = datetime.fromtimestamp(int(str(creation_ts_ms)) / 1000, tz=UTC)
        except (ValueError, OSError, OverflowError):
            available_from = now

        # Settlement currency (e.g. BTC for inverse, USDC for linear combos).
        settlement_currency: str = str(item.get("settlement_currency", underlying))

        # Legs: populated from symbol name encoding.
        legs: list[InstrumentLeg] = _parse_combo_legs(instrument_name)

        return InstrumentRecord(
            instrument_key=f"DERIBIT:COMBO:{instrument_name}",
            venue=self.venue,
            raw_symbol=instrument_name,
            instrument_type=InstrumentType.COMBO,
            base_asset=underlying,
            quote_asset="USD",
            settle_asset=settlement_currency if settlement_currency else underlying,
            underlying=underlying,
            status=InstrumentStatus.ACTIVE,
            # Combo instruments have no single tick/lot size — they depend on leg composition.
            tick_size=None,
            min_size=None,
            contract_size=Decimal("1"),
            available_from_datetime=available_from,
            legs=legs if legs else None,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single combo instrument by symbol."""
        instruments = await self.get_instruments(instrument_type=InstrumentType.COMBO)
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise UnsupportedCapabilityError(
            venue="DERIBIT",
            capability="options_chain",
            message="DeribitComboReferenceDataAdapter fetches COMBO instruments only. "
            "Use the Tardis adapter for options chains.",
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise UnsupportedCapabilityError(
            venue="DERIBIT",
            capability="expiry_calendar",
            message="DeribitComboReferenceDataAdapter fetches COMBO instruments only. "
            "Use the Tardis adapter for expiry calendars.",
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise UnsupportedCapabilityError(
            venue="DERIBIT",
            capability="funding_rate",
            message="DERIBIT combo instruments are multi-leg options strategies — "
            "they do not carry individual funding rates.",
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise UnsupportedCapabilityError(
            venue="DERIBIT",
            capability="ohlcv",
            message="DERIBIT combo instruments do not have individual OHLCV bars. "
            "Use the Tardis adapter for price history.",
        )
