"""
Instrument Synchronization — CEFI, TRADFI, and DEFI Venue Processing.

Extracted from instruments_service.py — contains the venue-specific instrument
generation logic for CeFi (Tardis), TradFi (Databento), and DeFi protocols.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from unified_api_contracts import VenueMapping
from unified_cloud_interface import get_secret_client
from unified_events_interface import VENUE_ZERO_INSTRUMENTS, log_event
from unified_internal_contracts import (
    EnhancedError,
    ErrorCategory,
    ErrorContext,
    ErrorRecoveryStrategy,
    ErrorSeverity,
    InstrumentRecord,
)
from unified_market_interface import TardisAdapter, get_adapter
from unified_reference_data_interface.adapters.databento import (
    DatabentoReferenceDataAdapter,  # noqa: deep-import — adapters not in URDI __init__.py; follow-up: promote to URDI top-level
)
from unified_reference_data_interface.adapters.tardis import (
    TardisReferenceDataAdapter,  # noqa: deep-import — adapters not in URDI __init__.py; follow-up: promote to URDI top-level
)

if TYPE_CHECKING:
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

from unified_api_contracts import DEFI_PROTOCOLS, DEFI_VENUE_TO_PROTOCOL

from instruments_service.config import (
    UnifiedInstrumentConfig,
)
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.special_instruments import (
    InstrumentDefDict,
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)

logger = logging.getLogger(__name__)

# Mapping from instruments-service exchange name → Databento dataset(s)
# URDI DatabentoReferenceDataAdapter works with datasets, not exchange names.
_EXCHANGE_TO_DATASETS: dict[str, list[str]] = {
    "CME": ["GLBX.MDP3"],
    "ICE": ["IFEU.IMPACT"],
    "NASDAQ": ["XNAS.ITCH"],
    "NYSE": ["XNYS.PILLAR"],
    "CBOE": ["OPRA.PILLAR"],
}


def _map_instrument_record_to_tradfi_definition(
    record: InstrumentRecord,
    exchange: str,
) -> InstrumentDefinition | None:
    """Map an URDI InstrumentRecord to an instruments-service InstrumentDefinition for TradFi.

    Returns None if the record cannot be mapped (e.g. missing required fields).

    Note: The URDI Databento adapter does not set available_since because the Databento
    reference API does not provide listing dates. We default to updated_at (the fetch
    timestamp) since the instruments are currently active.
    """
    available_from: str = ""
    if record.available_since:
        available_from = record.available_since.isoformat()
    elif record.updated_at:
        # Databento reference API only returns currently active instruments,
        # so updated_at (fetch time) is a valid lower bound.
        available_from = record.updated_at.isoformat()
    else:
        available_from = datetime.now(UTC).isoformat()

    # URDI Databento adapter generates keys as "DATASET:ID" (2 parts), but
    # InstrumentDefinition requires "VENUE:TYPE:SYMBOL" (3+ parts).
    # Construct the canonical format from exchange, instrument_type, and raw_symbol.
    raw_symbol = record.raw_symbol or record.symbol or ""
    inst_type_upper = str(record.instrument_type).upper()
    instrument_key = f"{exchange}:{inst_type_upper}:{raw_symbol.upper()}"

    return InstrumentDefinition(
        instrument_key=instrument_key,
        venue=exchange,
        instrument_type=str(record.instrument_type),
        symbol=record.symbol or record.raw_symbol or "",
        available_from_datetime=available_from,
        base_asset=record.base_asset or record.base or "",
        quote_asset=record.quote_asset or record.quote or "USD",
        expiry=record.expiry.isoformat() if record.expiry else None,
        option_type=record.option_type,
        strike=str(record.strike) if record.strike is not None else None,
        tick_size=str(record.tick_size) if record.tick_size else "",
        contract_size=float(record.contract_size) if record.contract_size else None,
        tardis_exchange="",
        databento_symbol=record.raw_symbol or "",
        exchange_raw_symbol=record.raw_symbol or "",
        data_provider="databento",
        market_category="tradfi",
    )


class InstrumentSyncMixin:
    """
    Mixin providing venue-specific instrument generation methods.

    Requires the host class to have:
        - self.venue_mapping: VenueMapping
        - self.processing_service: InstrumentProcessingService
    """

    # Attribute stubs — declared here so mixin methods can reference them;
    # concrete host classes must assign real values in __init__.
    venue_mapping: VenueMapping
    processing_service: InstrumentProcessingService

    @staticmethod
    def _fetch_venue_api_key(
        project_id: str | None,
        venue: str,
    ) -> str | None:
        """Fetch an API key from Secret Manager for the given venue.

        Convention: services fetch credentials and inject them into interface
        adapters. Returns None if project_id is not set (e.g. mock mode).
        """
        if project_id is None:
            return None
        secret_name = f"{venue.lower()}-api-key"
        client = get_secret_client(project_id=project_id)
        return client.get_secret(secret_name)

    def _filter_cefi_exchanges(self, cefi_exchanges: list[str], venues_filter: list[str]) -> list[str]:
        """Apply venue filtering to CEFI exchanges and return the filtered list."""
        if not venues_filter:
            logger.debug("No venue filter specified, processing exchanges: %s", cefi_exchanges)
            return cefi_exchanges

        tardis_venue_values: set[str] = set(self.venue_mapping.tardis_to_venue.values())
        cefi_venues: list[str] = [v for v in venues_filter if v in tardis_venue_values]

        if not cefi_venues:
            logger.info("No CEFI venues in filter, skipping CEFI processing")
            return []

        venue_to_exchanges: dict[str, list[str]] = self.venue_mapping.get_venue_to_tardis_exchanges()
        filtered_exchanges: list[str] = []
        for canonical_venue in cefi_venues:
            if canonical_venue in venue_to_exchanges:
                for raw_exchange in venue_to_exchanges[canonical_venue]:
                    if raw_exchange in cefi_exchanges and raw_exchange not in filtered_exchanges:
                        filtered_exchanges.append(raw_exchange)
                        logger.debug("  Mapped %s -> raw exchange %s", canonical_venue, raw_exchange)

        if filtered_exchanges:
            logger.info("Filtered CEFI exchanges by canonical venues %s: %s", cefi_venues, filtered_exchanges)
            return filtered_exchanges

        logger.warning("No matching CEFI exchanges found for canonical venues: %s", cefi_venues)
        return []

    def _check_venue_access(self, cefi_exchanges: list[str]) -> list[str]:
        """Pre-flight check: validate venue access and return accessible exchanges."""
        tardis_adapter_checked: TardisAdapter | None = None
        try:
            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            raw_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
            tardis_adapter_checked = cast(TardisAdapter, raw_adapter)
        except (ValueError, Exception) as e:
            logger.warning("Cannot check venue access: %s", e)

        if tardis_adapter_checked is None:
            return cefi_exchanges

        base_client = tardis_adapter_checked.base_client
        access_results: dict[str, tuple[bool, str]] = base_client.check_venues_access(cefi_exchanges)
        blocked: list[str] = [ex for ex, (ok, _) in access_results.items() if not ok]

        if not blocked:
            return cefi_exchanges

        for ex in blocked:
            _, error_msg = access_results[ex]
            logger.warning("Venue access blocked: %s - %s", ex, error_msg)

        accessible = [ex for ex in cefi_exchanges if ex not in blocked]
        if not accessible:
            logger.error("All %s CEFI venues blocked - skipping CEFI processing", len(cefi_exchanges))
            return []

        logger.warning(
            "%s/%s venues blocked, continuing with %s accessible venues",
            len(blocked),
            len(cefi_exchanges),
            len(accessible),
        )
        return accessible

    @staticmethod
    def _parse_raw_instruments(
        instruments_raw: list[dict[str, object]],
    ) -> dict[str, InstrumentDefinition]:
        """Parse raw instrument dicts into validated InstrumentDefinition objects."""
        result: dict[str, InstrumentDefinition] = {}
        for d in instruments_raw:
            try:
                inst = InstrumentDefinition.model_validate(d)
                key = cast(str, d.get("instrument_key") or inst.instrument_key)
                result[key] = inst
            except (ValueError, KeyError, TypeError, IndexError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
                logger.warning(
                    "Failed to create InstrumentDefinition for %s: %s",
                    d.get("instrument_key", "unknown"),
                    e,
                )
        return result

    async def _fetch_tardis_instruments(
        self, exchange: str, date: datetime, force: bool
    ) -> dict[str, InstrumentDefinition]:
        """Fetch instruments from URDI (normalized InstrumentRecord) for a Tardis exchange.

        Uses URDI TardisReferenceDataAdapter which returns normalized InstrumentRecord
        objects with canonical venue names, parsed base/quote, and proper instrument keys.
        URDI handles normalization using UAC VenueMapping — instruments-service just consumes.
        """
        try:
            logger.info("Processing CeFi exchange %s via URDI...", exchange)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            api_key = self._fetch_venue_api_key(project_id, "tardis")
            urdi_adapter = TardisReferenceDataAdapter(
                project_id=project_id,
                exchanges=[exchange],
                api_key=api_key,
            )
            instrument_records = await urdi_adapter.get_instruments()
            # Map InstrumentRecord (URDI output) → InstrumentDefinition (service model)
            # URDI handles normalization; instruments-service enriches with service-specific metadata
            target_date_only = date.date() if hasattr(date, "date") else date
            result: dict[str, InstrumentDefinition] = {}
            expired_count = 0
            for record in instrument_records:
                try:
                    # Filter expired instruments (options/futures/spreads past expiry date)
                    inst_type_lower = str(record.instrument_type).lower()
                    if record.expiry and inst_type_lower in ("future", "option", "spread"):
                        expiry_date = record.expiry.date() if hasattr(record.expiry, "date") else record.expiry
                        if target_date_only and expiry_date < target_date_only:
                            expired_count += 1
                            continue

                    available_from = record.available_since.isoformat() if record.available_since else ""
                    inst = InstrumentDefinition(
                        instrument_key=record.instrument_key,
                        venue=record.venue,
                        instrument_type=str(record.instrument_type),
                        symbol=record.symbol or record.raw_symbol or "",
                        available_from_datetime=available_from,
                        base_asset=record.base_asset or record.base or "",
                        quote_asset=record.quote_asset or record.quote or "",
                        expiry=record.expiry.isoformat() if record.expiry else None,
                        option_type=record.option_type,
                        strike=str(record.strike) if record.strike is not None else None,
                        tick_size=str(record.tick_size) if record.tick_size else "",
                        contract_size=float(record.contract_size) if record.contract_size else None,
                        tardis_exchange=exchange,
                        tardis_symbol=record.raw_symbol or "",
                        exchange_raw_symbol=record.raw_symbol or "",
                        data_provider="tardis",
                        market_category="cefi",
                    )
                    result[inst.instrument_key] = inst
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning("Failed to map InstrumentRecord %s: %s", record.instrument_key, e)
            if result or expired_count:
                logger.info(
                    "Processed %s instruments from %s via URDI (%s expired filtered out)",
                    len(result),
                    exchange,
                    expired_count,
                )
            return result
        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.exception("Failed to process %s: %s", exchange, e)
            return {}

    async def _process_onchain_clob_venues(
        self,
        venues_filter: list[str],
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI."""
        cefi_onchain_clob_venues: list[str] = self.venue_mapping.all_cefi_onchain_clob_venues
        cefi_clob_protocols: list[tuple[str, None]] = []

        if venues_filter:
            for venue in venues_filter:
                if venue.upper() in cefi_onchain_clob_venues:
                    cefi_clob_protocols.append((venue.lower(), None))
        else:
            for venue in cefi_onchain_clob_venues:
                cefi_clob_protocols.append((venue.lower(), None))

        if not cefi_clob_protocols:
            return {}

        all_instruments: dict[str, InstrumentDefinition] = {}
        protocol_names: list[str] = [p[0] for p in cefi_clob_protocols]
        logger.info("Processing %s on-chain CLOB venues (CEFI): %s", len(cefi_clob_protocols), protocol_names)

        for protocol, _chain in cefi_clob_protocols:
            try:
                clob_instruments = await self.processing_service.fetch_defi_instruments(
                    protocol=protocol,
                    target_date=date,
                )
                if clob_instruments:
                    all_instruments.update(cast(dict[str, InstrumentDefinition], clob_instruments))
                    logger.info(
                        "Processed %s instruments from %s (CEFI on-chain CLOB)", len(clob_instruments), protocol
                    )
            except (ValueError, KeyError, TypeError, IndexError, RuntimeError) as e:
                # Per-venue error isolation: log and continue with remaining venues.
                # One failed venue must not kill the entire date's processing.
                _err = EnhancedError(
                    message=f"On-chain CLOB venue {protocol} failed: {e}",
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.HIGH,
                    recovery_strategy=ErrorRecoveryStrategy.RETRY,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__, "venue": protocol}),
                )
                logger.error(
                    "[%s] %s — continuing with remaining venues",
                    _err.correlation_id,
                    _err.message,
                )
                log_event(
                    "VENUE_PROCESSING_FAILED",
                    details={
                        "venue": protocol,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "correlation_id": _err.correlation_id,
                        "category": "cefi_onchain_clob",
                    },
                )

        return all_instruments

    def _merge_exchange_results(
        self,
        results: list[dict[str, InstrumentDefinition]],
    ) -> dict[str, InstrumentDefinition]:
        """Merge results from parallel exchange fetches into a single dict."""
        merged: dict[str, InstrumentDefinition] = {}
        for result in results:
            if result:
                merged.update(result)
        return merged

    async def _process_cefi_exchanges(
        self,
        cefi_exchanges: list[str],
        venues_filter: list[str],
        date: datetime,
        force: bool,
    ) -> tuple[list[str], dict[str, InstrumentDefinition]]:
        """
        Process CeFi (Tardis) exchanges and on-chain CLOB venues.

        Returns:
            Tuple of (updated cefi_exchanges list, generated instruments dict)
        """
        cefi_exchanges = self._filter_cefi_exchanges(cefi_exchanges, venues_filter)

        if cefi_exchanges:
            cefi_exchanges = self._check_venue_access(cefi_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        if not cefi_exchanges:
            logger.info("Skipping CEFI processing - no exchanges to process")
        else:
            logger.info("Processing %s CeFi exchanges in parallel...", len(cefi_exchanges))
            results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
                *[self._fetch_tardis_instruments(ex, date, force) for ex in cefi_exchanges],
                return_exceptions=False,
            )
            all_instruments = self._merge_exchange_results(results)

        clob_instruments = await self._process_onchain_clob_venues(venues_filter, date)
        all_instruments.update(clob_instruments)

        return cefi_exchanges, all_instruments

    async def _process_tradfi_exchanges(
        self,
        venues_filter: list[str],
        tradfi_venues: list[str] | None,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process TradFi (Databento) exchanges."""
        databento_exchanges = self._resolve_databento_exchanges(venues_filter, tradfi_venues)
        if not databento_exchanges:
            logger.info("Skipping TRADFI processing - no exchanges to process")
            return {}

        logger.info("Processing %s TradFi venues...", len(databento_exchanges))
        results = await asyncio.gather(
            *[self._process_single_databento_exchange(ex, date) for ex in databento_exchanges],
            return_exceptions=False,
        )

        all_instruments: dict[str, InstrumentDefinition] = {}
        for result in results:
            if result:
                all_instruments.update(cast(dict[str, InstrumentDefinition], result))
        return all_instruments

    def _resolve_databento_exchanges(
        self,
        venues_filter: list[str],
        tradfi_venues: list[str] | None,
    ) -> list[str]:
        """Determine which Databento exchanges to process based on filters."""
        all_databento_exchanges: list[str] = self.venue_mapping.all_databento_venues

        if tradfi_venues:
            filtered = [v for v in tradfi_venues if v in all_databento_exchanges]
            if filtered:
                logger.info("Processing specified TradFi venues: %s", filtered)
                return filtered
            logger.warning("No valid TradFi venues in tradfi_venues=%s, skipping", tradfi_venues)
            return []

        if venues_filter:
            filtered = [v for v in venues_filter if v in all_databento_exchanges]
            if filtered:
                logger.info("Filtered TRADFI exchanges by venues %s: %s", filtered, filtered)
                return filtered
            logger.info("No TRADFI venues in filter, skipping TRADFI processing")
            return []

        default_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
        logger.info("No venue filter specified, processing default TRADFI exchanges: %s", default_exchanges)
        return default_exchanges

    async def _process_single_databento_exchange(
        self,
        exchange: str,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process a single Databento exchange."""
        try:
            if exchange == "CBOE":
                return self._process_cboe(date)
            elif exchange == "FX":
                return self._process_fx(date)
            elif exchange in ["NASDAQ", "NYSE"]:
                return await self._process_equity_exchange(exchange, date)
            else:
                return await self._process_generic_databento(exchange, date)
        except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.exception("Failed to process %s: %s", exchange, e)
            return {}

    @staticmethod
    def _process_cboe(date: datetime) -> dict[str, InstrumentDefinition]:
        """Process CBOE exchange (VIX instrument)."""
        vix_def_dict: InstrumentDefDict = create_vix_instrument_definition(date)
        if vix_def_dict:
            vix_def = InstrumentDefinition.model_validate(vix_def_dict)
            logger.info("Created VIX: %s", vix_def.instrument_key)
            return {vix_def.instrument_key: vix_def}
        return {}

    @staticmethod
    def _process_fx(date: datetime) -> dict[str, InstrumentDefinition]:
        """Process FX exchange (KRW/USD instrument)."""
        krwusd_def_dict: InstrumentDefDict = create_krwusd_instrument_definition(date)
        if krwusd_def_dict:
            krwusd_def = InstrumentDefinition.model_validate(krwusd_def_dict)
            logger.info("Created KRW/USD: %s", krwusd_def.instrument_key)
            return {krwusd_def.instrument_key: krwusd_def}
        return {}

    async def _process_equity_exchange(
        self,
        exchange: str,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process NASDAQ/NYSE exchanges including Bitcoin ETFs via URDI."""
        databento_config = UnifiedInstrumentConfig()
        instruments: dict[str, InstrumentDefinition] = {}
        symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

        # Bitcoin ETFs use static definitions (not from Databento API)
        instruments.update(self._create_bitcoin_etf_instruments(symbols, date))

        # Remaining equities/ETFs go through URDI Databento adapter
        other = await self._fetch_databento_instruments_via_urdi(exchange)
        if other:
            # Don't overwrite Bitcoin ETF static definitions
            btc_etf_keys = set(instruments.keys())
            for key, inst in other.items():
                if key not in btc_etf_keys:
                    instruments[key] = inst
            non_btc_count = len(other) - len(btc_etf_keys & set(other.keys()))
            if non_btc_count > 0:
                logger.info("Processed %s additional instruments from %s via URDI", non_btc_count, exchange)
        if instruments:
            logger.info("Processed %s total instruments from %s", len(instruments), exchange)
        return instruments

    @staticmethod
    def _create_bitcoin_etf_instruments(
        symbols: list[str],
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Create Bitcoin ETF instrument definitions if date is after launch."""
        bitcoin_etf_launch_date = datetime(2024, 1, 11, tzinfo=UTC)
        bitcoin_etf_tickers = ["IBIT", "FBTC", "ARKB"]
        instruments: dict[str, InstrumentDefinition] = {}

        if date < bitcoin_etf_launch_date:
            logger.debug("Skipping Bitcoin ETFs - date %s is before launch (2024-01-11)", date.strftime("%Y-%m-%d"))
            return instruments

        for ticker in bitcoin_etf_tickers:
            if ticker in symbols:
                etf_def_dict: InstrumentDefDict | None = create_bitcoin_etf_instrument_definition(
                    ticker,
                    date,
                    get_us_equity_trading_hours,
                )
                if etf_def_dict:
                    etf_def = InstrumentDefinition.model_validate(etf_def_dict)
                    instruments[etf_def.instrument_key] = etf_def
                    logger.info("Created Bitcoin ETF: %s", etf_def.instrument_key)
        return instruments

    async def _process_generic_databento(
        self,
        exchange: str,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process CME, ICE, or other generic Databento exchanges via URDI."""
        if exchange == "ICE":
            ice_us_launch = datetime(2018, 12, 23, tzinfo=UTC)
            if date < ice_us_launch:
                logger.info(
                    "Skipping ICE - date %s is before ICE US dataset launch (2018-12-23)",
                    date.strftime("%Y-%m-%d"),
                )
                return {}

        instruments = await self._fetch_databento_instruments_via_urdi(exchange)
        if instruments:
            logger.info("Processed %s instruments from %s via URDI", len(instruments), exchange)
        return instruments

    async def _fetch_databento_instruments_via_urdi(
        self,
        exchange: str,
    ) -> dict[str, InstrumentDefinition]:
        """Fetch instruments from URDI DatabentoReferenceDataAdapter for a TradFi exchange.

        Uses URDI DatabentoReferenceDataAdapter which returns normalized InstrumentRecord
        objects. URDI handles normalization — instruments-service maps to InstrumentDefinition.
        """
        try:
            datasets = _EXCHANGE_TO_DATASETS.get(exchange)
            if not datasets:
                logger.warning("No Databento dataset mapping for exchange %s", exchange)
                return {}

            logger.info("Processing TradFi exchange %s via URDI (datasets=%s)...", exchange, datasets)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            api_key = self._fetch_venue_api_key(project_id, "databento")
            urdi_adapter = DatabentoReferenceDataAdapter(
                project_id=project_id,
                datasets=datasets,
                api_key=api_key,
            )
            instrument_records = await urdi_adapter.get_instruments()

            target_date_only = datetime.now(UTC).date()  # Databento doesn't pass target_date
            result: dict[str, InstrumentDefinition] = {}
            expired_count = 0
            for record in instrument_records:
                try:
                    # Filter expired instruments
                    inst_type_lower = str(record.instrument_type).lower()
                    if record.expiry and inst_type_lower in ("future", "option", "spread", "spot"):
                        expiry_date = record.expiry.date() if hasattr(record.expiry, "date") else record.expiry
                        if expiry_date < target_date_only:
                            expired_count += 1
                            continue

                    inst = _map_instrument_record_to_tradfi_definition(record, exchange)
                    if inst is not None:
                        result[inst.instrument_key] = inst
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning("Failed to map InstrumentRecord %s: %s", record.instrument_key, e)

            if result or expired_count:
                logger.info(
                    "Processed %s instruments from %s via URDI (%s expired filtered out)",
                    len(result),
                    exchange,
                    expired_count,
                )
            return result
        except (ValueError, KeyError, TypeError, IndexError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.exception("Failed to process %s via URDI: %s", exchange, e)
            return {}

    async def _process_defi_protocols(
        self,
        venues_filter: list[str],
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process DeFi protocols."""
        defi_protocols = self._resolve_defi_protocols(venues_filter)
        if not defi_protocols:
            logger.info("Skipping DEFI processing - no protocols to process")
            return {}

        all_instruments: dict[str, InstrumentDefinition] = {}
        for protocol, chain in defi_protocols:
            instruments = await self._fetch_single_defi_protocol(protocol, chain, date)
            all_instruments.update(instruments)
        return all_instruments

    def _resolve_defi_protocols(
        self,
        venues_filter: list[str],
    ) -> list[tuple[str, str | None]]:
        """Determine which DeFi protocols to process based on venue filters."""
        if not venues_filter:
            logger.info("No venue filter specified, processing all DEFI protocols")
            return DEFI_PROTOCOLS

        defi_venues: list[str] = [v for v in venues_filter if v in self.venue_mapping.all_defi_venues]
        if not defi_venues:
            logger.info("No DEFI venues in filter, skipping DEFI processing")
            return []

        defi_protocols: list[tuple[str, str | None]] = []
        for venue in defi_venues:
            venue_key: str = venue.upper() if venue else ""
            if venue_key in DEFI_VENUE_TO_PROTOCOL:
                protocol, chain = DEFI_VENUE_TO_PROTOCOL[venue_key]
                if (protocol, chain) not in defi_protocols:
                    defi_protocols.append((protocol, chain))

        if defi_protocols:
            logger.info("Filtered DEFI protocols by venues %s: %s", defi_venues, [p[0] for p in defi_protocols])
        else:
            logger.warning("No matching DEFI protocols found for venues: %s", defi_venues)
        return defi_protocols

    async def _fetch_single_defi_protocol(
        self,
        protocol: str,
        chain: str | None,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Fetch instruments from a single DeFi protocol."""
        try:
            if chain:
                defi_instruments = await self.processing_service.fetch_defi_instruments(
                    protocol=protocol,
                    chain=chain,
                    target_date=date,
                )
            else:
                defi_instruments = await self.processing_service.fetch_defi_instruments(
                    protocol=protocol,
                    target_date=date,
                )
            if defi_instruments:
                result = cast(dict[str, InstrumentDefinition], defi_instruments)
                logger.info("Processed %s instruments from %s", len(result), protocol)
                return result
            log_event(
                VENUE_ZERO_INSTRUMENTS,
                details={
                    "venue": protocol,
                    "chain": chain or "unknown",
                    "reason": "adapter returned 0 instruments",
                },
            )
            return {}
        except (ValueError, KeyError, TypeError, IndexError, AttributeError, RuntimeError) as e:
            # Shard-level failure isolation: log and continue with remaining protocols.
            _err = EnhancedError(
                message=f"DeFi protocol {protocol} failed: {e}",
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.HIGH,
                recovery_strategy=ErrorRecoveryStrategy.RETRY,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__, "protocol": protocol}),
            )
            logger.error(
                "[%s] %s — continuing with remaining protocols",
                _err.correlation_id,
                _err.message,
            )
            log_event(
                "VENUE_PROCESSING_FAILED",
                details={
                    "venue": protocol,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "correlation_id": _err.correlation_id,
                    "category": "defi",
                },
            )
            return {}
