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
from unified_api_contracts.internal.reference.canonical_id_builder import build_leg
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


_OPTION_RIGHTS: frozenset[str] = frozenset({"C", "P"})


def _classify_deribit_leg_instrument_type(leg_name: str) -> InstrumentType | None:
    """Classify a Deribit combo leg's ``InstrumentType`` from its ``instrument_name`` shape.

    Deribit's ``public/get_combos`` leg objects are ``{amount, instrument_name}``
    only — no per-leg type field is returned — so the type must be inferred
    from the dash-delimited shape of ``instrument_name`` itself. Verified
    against real, live ``get_combos`` responses for BTC/ETH (2026-07-09, every
    currently-active combo for both currencies): every leg name is either 2
    dash-parts (``{BASE}-PERPETUAL`` or ``{BASE}-{DDMMMYY}``) or 4 dash-parts
    (``{BASE}-{DDMMMYY}-{STRIKE}-{C|P}``) — no 3-part or 5+-part shape was
    observed. Real examples seen: ``BTC-PERPETUAL``, ``BTC-10JUL26``,
    ``BTC-17JUL26-65000-C``.

    Returns ``None`` (caller drops the leg — same degrade-gracefully
    convention already used for a leg with a missing/empty ``instrument_name``
    or zero ``amount``) if the name doesn't match any known shape.
    """
    parts = leg_name.split("-")
    if len(parts) == 2:
        return InstrumentType.PERPETUAL if parts[1] == "PERPETUAL" else InstrumentType.FUTURE
    if len(parts) == 4 and parts[3] in _OPTION_RIGHTS:
        return InstrumentType.OPTION
    return None


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
        """Return the venue identifier.

        MUST be the registered venue id ``DERIBIT-COMBO`` (factory.py
        ``VENUE_TO_ADAPTER["DERIBIT-COMBO"]="deribit_combo"``), NOT the bare
        exchange ``DERIBIT`` — the URDI venue-tag filter
        (``_filter_records_to_venue``) keeps only records whose ``venue`` equals
        the BATCH canonical venue, so tagging combos ``DERIBIT`` dropped every
        fetched row as an "unknown venue" (the venue had 0 captured days since it
        was added 2026-05-23). The per-instrument ``instrument_key`` stays
        ``DERIBIT:COMBO:<name>`` (a separate identity field).
        """
        return "DERIBIT-COMBO"

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

        Uses the ``public/get_combos`` endpoint (NOT ``get_instruments?kind=combo``):
        Deribit retired the unified ``kind=combo`` value on ``get_instruments``
        (now HTTP 400 ``invalid value`` for ``param=kind``; it split into
        ``future_combo``/``option_combo``, neither of which carries the LEG
        structure). ``get_combos`` returns each combo with its STRUCTURED
        ``legs:[{amount, instrument_name}]`` — the canonical source for the
        ``InstrumentLeg`` list the validation registry requires for a COMBO (a
        leg-less COMBO is rejected "legs required for COMBO"). Verified live
        2026-06-18. Leg ``amount``: sign → BUY/SELL, ``abs`` → ratio;
        ``instrument_name`` → leg ``instrument_key``. A failure raises so the
        caller's shard-isolation routes it to attempted_failed (CF-11). SSOT:
        https://docs.deribit.com/#public-get_combos.

        SOURCE LIMITATION — DERIBIT-COMBO HISTORICAL DATA IS UNBACKFILLABLE:
        ``public/get_combos`` returns ONLY currently-live (active) combo
        instruments.  Deribit does NOT retain or expose the state of historical /
        expired combos — once a combo expires it disappears from the endpoint
        permanently.  Consequently:

        * Instrument-history cells for DERIBIT-COMBO prior to the live-capture
          window (i.e., dates before the adapter was deployed and running) are
          **structurally unbackfillable via REST** — this is a genuine upstream
          source limitation, NOT a pipeline gap or missing credential.
        * The manifest must represent those historical cells as
          ``empty_confirmed[EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE]`` (the
          source has no historical endpoint for this data, ever).  A
          ``SOURCE_RETURNED_ZERO`` would be incorrect (no fetch was made);
          ``attempted_failed`` is incorrect (no retry will succeed).
        * **DO NOT re-attempt a historical DERIBIT-COMBO backfill.**  The cefi
          8-venue backfill correctly could not fill it.  Any future audit that
          sees DERIBIT-COMBO historical cells as empty should classify them as
          expected typed-empty, not a fillable gap.
        * For live / forward capture the adapter works correctly: combos active
          today are returned and written as ``captured``.

        SSOT: ``codex/02-data/honest-absence-downstream-handling.md``
        § "DERIBIT-COMBO historical unavailability".
        """
        url = f"{_BASE}/public/get_combos"
        params = {"currency": currency}

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
                "Deribit get_combos (currency=%s) HTTP error: %s (classified: %s, action: %s, retry_safe: %s)",
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
                    "endpoint": "get_combos",
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
            logger.warning("Deribit get_combos response is not a dict for currency=%s", currency)
            return []

        result_raw: object = payload.get("result", [])
        combos_raw: list[object] = result_raw if isinstance(result_raw, list) else []

        records: list[InstrumentRecord] = []
        for item in combos_raw:
            record = self._parse_combo_instrument(item, currency, now)
            if record is not None:
                records.append(record)

        logger.debug(
            "DeribitComboAdapter: currency=%s → %d active combos",
            currency,
            len(records),
        )
        return records

    @staticmethod
    def _build_legs(raw_legs: object) -> list[InstrumentLeg]:
        """Map Deribit get_combos structured ``legs`` → list[InstrumentLeg].

        Each Deribit leg is ``{"amount": <signed int>, "instrument_name": <str>}``.
        ``amount`` sign → side (>0 BUY / <0 SELL); ``abs(amount)`` → ratio;
        ``instrument_name`` → the leg's ``instrument_key``, built via the
        shared UAC ``build_leg()`` (canonical ``VENUE:TYPE:SYMBOL`` grammar)
        instead of the prior ad hoc ``f"DERIBIT:{leg_name}"`` — which was
        missing the ``:TYPE:`` segment entirely (2 colon-parts instead of 3).
        The per-leg ``InstrumentType`` is classified from the raw
        ``instrument_name`` shape via ``_classify_deribit_leg_instrument_type``
        since ``get_combos`` doesn't return a per-leg type field. A leg whose
        name doesn't match any known shape is dropped (logged) rather than
        raised, matching the existing malformed-leg-skip convention below.
        """
        legs: list[InstrumentLeg] = []
        if not isinstance(raw_legs, list):
            return legs
        for raw in raw_legs:
            if not isinstance(raw, dict):
                continue
            leg_name = str(raw.get("instrument_name", ""))
            if not leg_name:
                continue
            try:
                amount = int(raw.get("amount", 0))
            except (TypeError, ValueError):
                amount = 0
            if amount == 0:
                continue
            leg_type = _classify_deribit_leg_instrument_type(leg_name)
            if leg_type is None:
                logger.warning(
                    "DeribitComboAdapter: cannot classify leg instrument_type for "
                    "instrument_name=%s (unexpected shape) — dropping leg",
                    leg_name,
                )
                continue
            legs.append(
                build_leg(
                    "DERIBIT",
                    leg_type,
                    leg_name,
                    side="BUY" if amount > 0 else "SELL",
                    ratio=abs(amount),
                    passthrough=True,
                )
            )
        return legs

    def _parse_combo_instrument(
        self,
        item: object,
        currency: str,
        now: datetime,
    ) -> InstrumentRecord | None:
        """Parse a single Deribit get_combos result dict into an InstrumentRecord.

        The get_combos item shape is ``{"id": <combo-name>, "legs": [...],
        "creation_timestamp": <ms>, "state": <str>, "instrument_id": <int>}``.
        Returns None if the item is malformed or missing required fields (incl. a
        combo with no parseable legs — the validation registry rejects leg-less
        COMBOs, so we drop it here rather than emit an invalid record).
        """
        if not isinstance(item, dict):
            return None

        combo_id: str = str(item.get("id", ""))
        if not combo_id:
            return None

        # Underlying currency from the combo id first segment (e.g. "BTC-CCAL-…" → "BTC"),
        # falling back to the queried currency.
        name_parts = combo_id.split("-")
        underlying: str = name_parts[0] if name_parts and name_parts[0] else currency

        # Creation timestamp from Deribit (milliseconds UTC).
        creation_ts_ms: object = item.get("creation_timestamp", 0)
        try:
            available_from = datetime.fromtimestamp(int(str(creation_ts_ms)) / 1000, tz=UTC)
        except (ValueError, OSError, OverflowError):
            available_from = now

        # Legs: from the get_combos STRUCTURED legs (canonical source). A combo
        # without parseable legs is invalid → drop (validation requires legs).
        legs: list[InstrumentLeg] = self._build_legs(item.get("legs"))
        if not legs:
            return None

        return InstrumentRecord(
            instrument_key=f"DERIBIT:COMBO:{combo_id}",
            venue=self.venue,
            raw_symbol=combo_id,
            instrument_type=InstrumentType.COMBO,
            base_asset=underlying,
            quote_asset="USD",
            settle_asset=underlying,
            underlying=underlying,
            status=InstrumentStatus.ACTIVE,
            # Combo instruments have no single tick/lot size — they depend on leg composition.
            tick_size=None,
            min_size=None,
            contract_size=Decimal("1"),
            available_from_datetime=available_from,
            legs=legs,
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
