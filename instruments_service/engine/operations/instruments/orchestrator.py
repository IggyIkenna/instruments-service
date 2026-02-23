"""
Instruments Orchestration Module

Coordinates instrument generation workflow across market types (CeFi, TradFi, DeFi).
Handles venue filtering, parallel processing, and storage coordination.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date as date_type
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
from unified_events_interface import ErrorWarningCounter
from unified_market_interface import InstrumentDefinition, get_adapter
from unified_market_interface.models.venue_config import VenueMapping

from instruments_service.adapters import StorageAdapter
from instruments_service.config import (
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    UnifiedInstrumentConfig,
    instruments_config,
)
from instruments_service.engine.operations.instruments.batch_orchestrator import InstrumentBatchProcessor

if TYPE_CHECKING:
    pass
from instruments_service.engine.venues.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)

logger = logging.getLogger(__name__)


class InstrumentsOrchestrator:
    """
    Orchestrates instrument generation workflow.

    Responsibilities:
    - Coordinate processing across market types (CeFi, TradFi, DeFi)
    - Handle venue filtering and validation
    - Manage parallel processing of exchanges/venues
    - Coordinate storage operations
    - Track errors and warnings
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize the orchestrator.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - gcs_bucket: GCS bucket name (optional, auto-detected)
                - bigquery_dataset: BigQuery dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        # Initialize processing service - lazy import to avoid circular dependency
        from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

        processing_config: dict[str, str | bool] = {
            "project_id": str(cast(str | None, config.get("project_id")) or instruments_config.gcp_project_id),
            "enable_ccxt_integration": cast(bool, config.get("enable_ccxt_integration", True)),
            "enable_metadata_caching": cast(bool, config.get("enable_metadata_caching", True)),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = StorageAdapter()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping
        self.venue_mapping: VenueMapping = VenueMapping()

        logger.info("✅ InstrumentsOrchestrator initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: list[str] | str | None = None,
        instrument_ids: list[str] | str | None = None,
        tradfi_venues: list[str] | None = None,
        skip_storage: bool = False,
    ) -> dict[str, Any]:
        """
        Generate instruments for a specific date.

        Args:
            date: Target date for instrument generation
            exchanges: Optional list of exchanges to process (for CeFi mode)
            force: Force regeneration even if instruments exist
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include
            tradfi_venues: Optional list of specific TradFi venues to process (e.g., ["NASDAQ", "NYSE"])
            skip_storage: Skip storage (for live mode)

        Returns:
            Dictionary with generation results including error_count and warning_count
        """
        # Set up error/warning counter for this operation
        error_warning_counter = ErrorWarningCounter()
        root_logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)
        error_warning_counter.reset()

        # Initialize variables that may be used in finally block
        success = False
        instruments_df = None

        try:
            date_str = date.strftime("%Y-%m-%d")
            logger.info(f"📅 Generating instruments for {date_str} (CeFi={cefi}, TradFi={tradfi}, DeFi={defi})")

            # Normalize venues filter
            venues_filter = self._normalize_venues_filter(venues)

            # Extract venues from instrument_ids if provided
            if instrument_ids:
                venues_filter = self._extract_venues_from_instrument_ids(instrument_ids, venues_filter)

            # Validate venues against allowed venues for each market type
            if venues_filter:
                validation_result = self._validate_venues(venues_filter, cefi, tradfi, defi)
                if validation_result:
                    return validation_result

            all_instruments: dict[str, InstrumentDefinition] = {}

            # Process CEFI (Tardis) exchanges
            if cefi:
                cefi_instruments = await self._process_cefi(date, exchanges, force, venues_filter)
                all_instruments.update(cefi_instruments)

            # Process TRADFI (Databento) exchanges
            if tradfi:
                tradfi_instruments = await self._process_tradfi(date, venues_filter, tradfi_venues)
                all_instruments.update(tradfi_instruments)

            # Process DEFI protocols
            if defi:
                defi_instruments = await self._process_defi(date, venues_filter)
                all_instruments.update(defi_instruments)

            # If no mode flags specified, process all three modes
            if not cefi and not tradfi and not defi:
                logger.info("📋 No mode flags specified, processing all modes (CeFi, TradFi, DeFi)")
                return await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=True,
                    tradfi=True,
                    defi=True,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )

            # Apply instrument_id filtering if specified
            if instrument_ids:
                all_instruments = self._filter_by_instrument_ids(all_instruments, instrument_ids)

            # Convert to DataFrame
            instruments_df = self._convert_to_dataframe(all_instruments, skip_storage)

            # Handle case where no instruments generated
            if not all_instruments:
                return self._handle_no_instruments(date_str, tradfi, venues_filter, error_warning_counter, root_logger)

            # Ensure every requested TradFi venue has at least one row (holiday/closed placeholder)
            if tradfi and venues_filter:
                instruments_df = self._add_tradfi_placeholders(instruments_df, date, venues_filter)

            # Tag instruments with session_date_tag for close-date file (primary)
            if not instruments_df.empty and "regular_open_utc" in instruments_df.columns:
                instruments_df["session_date_tag"] = "close_date"

            # Store to cloud (primary close-date file) unless skip_storage (live mode writes to live/ path)
            if skip_storage:
                success = True
                logger.info(f"📤 Skipping batch storage (live mode) - {len(instruments_df)} instruments for live path")
            else:
                logger.info(f"📤 Storing {len(instruments_df)} instruments to cloud...")
                success = cast(
                    bool,
                    self.cloud_storage.store_instruments(
                        instruments_df=instruments_df, table_name="instruments", date=date
                    ),
                )

            # Handle UTC midnight spanning for CME and ICE (skip when skip_storage - live mode)
            if (
                success
                and not skip_storage
                and not instruments_df.empty
                and "regular_open_utc" in instruments_df.columns
            ):
                self._handle_utc_midnight_spanning(instruments_df, date)

            # Get error/warning counts before returning
            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count

            if success:
                logger.info(f"✅ Successfully stored instruments for {date_str}")
                result: dict[str, Any] = {
                    "status": "success",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "exchanges_processed": 0,
                    "venues": (
                        cast(list[str], instruments_df["venue"].unique().tolist())
                        if "venue" in instruments_df.columns
                        else []
                    ),
                    "error_count": error_count,
                    "warning_count": warning_count,
                }
                if skip_storage and not instruments_df.empty and "market_category" in instruments_df.columns:
                    result["instruments_by_category"] = {
                        str(cat): grp for cat, grp in instruments_df.groupby("market_category")
                    }
                return result
            else:
                logger.error(f"❌ Failed to store instruments for {date_str}")
                return {
                    "status": "error",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "message": "Storage failed",
                    "error_count": error_count,
                    "warning_count": warning_count,
                }

        except Exception as e:
            # Get error/warning counts before returning
            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count
            logger.error(f"❌ Exception during instrument generation: {e}", exc_info=True)
            return {
                "status": "error",
                "date": date_str if "date_str" in locals() else date.strftime("%Y-%m-%d"),
                "instruments_generated": 0,
                "message": f"Exception during processing: {str(e)}",
                "error_count": error_count,
                "warning_count": warning_count,
            }
        finally:
            # Ensure handler is removed even if exception occurs
            if error_warning_counter in root_logger.handlers:
                root_logger.removeHandler(error_warning_counter)

    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: list[str] | None = None,
        instrument_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generate instruments for a date range.

        Args:
            start_date: Start date
            end_date: End date
            exchanges: Optional list of exchanges to process
            force: Force regeneration
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include

        Returns:
            Dictionary with batch processing results
        """
        # Get date range from batch processor
        date_range = self.batch_processor.get_required_periods(start_date)

        # Filter to requested range
        date_range = [d for d in date_range if start_date <= d <= end_date]

        logger.info(f"📅 Processing {len(date_range)} dates from {start_date.date()} to {end_date.date()}")

        results: list[dict[str, Any]] = []
        total_generated: int = 0
        total_errors: int = 0

        for date in date_range:
            try:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=cefi,
                    tradfi=tradfi,
                    defi=defi,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )
                results.append(result)
                if result.get("status") == "success":
                    total_generated += cast(int, result.get("instruments_generated", 0))
                else:
                    total_errors += 1
            except Exception as e:
                logger.error(
                    f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}",
                    exc_info=True,
                )
                total_errors += 1
                results.append(
                    {
                        "status": "error",
                        "date": date.strftime("%Y-%m-%d"),
                        "error": str(e),
                    }
                )

        success_count: int = len([r for r in results if r.get("status") == "success"])
        success_rate = (success_count / len(date_range) * 100) if date_range else 0

        logger.info(f"📊 Batch processing complete: {success_count}/{len(date_range)} successful ({success_rate:.1f}%)")
        logger.info(f"   Total instruments generated: {total_generated}")
        logger.info(f"   Errors: {total_errors}")

        return {
            "status": "success" if total_errors == 0 else "partial",
            "dates_processed": len(date_range),
            "dates_successful": success_count,
            "dates_failed": total_errors,
            "success_rate_percent": success_rate,
            "total_instruments_generated": total_generated,
            "results": results,
        }

    def _normalize_venues_filter(self, venues: list[str] | str | None) -> list[str]:
        """Normalize venues filter to list of uppercase strings."""
        _venues_raw: list[str] | str = venues if venues is not None else []
        if isinstance(_venues_raw, str):
            _venues_raw = [_venues_raw]
        return [str(v).upper() for v in _venues_raw] if _venues_raw else []

    def _extract_venues_from_instrument_ids(
        self, instrument_ids: list[str] | str, venues_filter: list[str]
    ) -> list[str]:
        """Extract venues from instrument_ids and merge with venues filter."""
        instrument_ids_list: list[str] = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
        venues_from_instrument_ids: set[str] = set()

        for inst_id in instrument_ids_list:
            parts = str(inst_id).split(":")
            if len(parts) >= 1:
                venue_from_id: str = parts[0].upper()
                venues_from_instrument_ids.add(venue_from_id)
                logger.debug(f"  Extracted venue '{venue_from_id}' from instrument_id: {inst_id}")

        if venues_from_instrument_ids:
            logger.info(f"🔍 Extracted venues from instrument_ids: {sorted(venues_from_instrument_ids)}")

            if venues_filter:
                venues_filter = [v for v in venues_filter if v in venues_from_instrument_ids]
                if not venues_filter:
                    logger.warning(
                        f"⚠️ No matching venues between --venues and "
                        f"instrument_ids {venues_from_instrument_ids}. Processing will be skipped."
                    )
            else:
                venues_filter = list(venues_from_instrument_ids)
                logger.info(f"🔍 Using venues from instrument_ids as venue filter: {venues_filter}")

        return venues_filter

    def _validate_venues(self, venues_filter: list[str], cefi: bool, tradfi: bool, defi: bool) -> dict[str, Any] | None:
        """
        Validate venues against allowed venues for each market type.

        Returns:
            Error dict if validation fails, None if validation passes
        """
        allowed_cefi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_cefi_venues))
        allowed_tradfi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_databento_venues))
        allowed_defi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_defi_venues))

        market_types_being_processed: list[str] = []
        if cefi:
            market_types_being_processed.append("CEFI")
        if tradfi:
            market_types_being_processed.append("TRADFI")
        if defi:
            market_types_being_processed.append("DEFI")

        if not market_types_being_processed:
            market_types_being_processed = ["CEFI", "TRADFI", "DEFI"]

        allowed_venues_for_processing: set[str] = set()
        if "CEFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_cefi_venues)
        if "TRADFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_tradfi_venues)
        if "DEFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_defi_venues)

        invalid_venues: list[str] = []
        valid_venues_by_type: dict[str, list[str]] = {"CEFI": [], "TRADFI": [], "DEFI": []}

        for venue in venues_filter:
            venue_valid = False
            if venue in allowed_cefi_venues and "CEFI" in market_types_being_processed:
                valid_venues_by_type["CEFI"].append(venue)
                venue_valid = True
            if venue in allowed_tradfi_venues and "TRADFI" in market_types_being_processed:
                valid_venues_by_type["TRADFI"].append(venue)
                venue_valid = True
            if venue in allowed_defi_venues and "DEFI" in market_types_being_processed:
                valid_venues_by_type["DEFI"].append(venue)
                venue_valid = True

            if not venue_valid:
                invalid_venues.append(venue)

        if invalid_venues:
            error_msg = (
                f"❌ Invalid venues for market types {market_types_being_processed}: {invalid_venues}\n"
                f"   Allowed CEFI venues: {sorted(allowed_cefi_venues)}\n"
                f"   Allowed TRADFI venues: {sorted(allowed_tradfi_venues)}\n"
                f"   Allowed DEFI venues: {sorted(allowed_defi_venues)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        for market_type, valid_venues in valid_venues_by_type.items():
            if valid_venues:
                logger.info(f"✅ Valid {market_type} venues: {valid_venues}")

        if not any(valid_venues_by_type.values()):
            logger.warning(
                f"⚠️ No valid venues found after filtering. Skipping all processing. "
                f"Requested venues: {venues_filter if venues_filter else 'all'}, "
                f"Market types: {market_types_being_processed}"
            )
            error_warning_counter = ErrorWarningCounter()
            return {
                "status": "warning",
                "date": "",
                "instruments_generated": 0,
                "message": "No valid venues found after filtering",
                "error_count": error_warning_counter.error_count,
                "warning_count": error_warning_counter.warning_count,
            }

        return None

    async def _process_cefi(
        self, date: datetime, exchanges: list[str] | None, force: bool, venues_filter: list[str]
    ) -> dict[str, InstrumentDefinition]:
        """Process CeFi (Tardis) exchanges."""
        cefi_exchanges: list[str] = (
            cast(list[str], exchanges)
            if exchanges is not None
            else cast(list[str], self.venue_mapping.all_tardis_exchanges)
        )

        # Apply venue filtering for CEFI
        if venues_filter:
            tardis_venue_values: set[str] = set(cast(dict[str, str], self.venue_mapping.tardis_to_venue).values())
            cefi_venues: list[str] = [v for v in venues_filter if v in tardis_venue_values]

            if cefi_venues:
                venue_to_exchanges: dict[str, list[str]] = cast(
                    dict[str, list[str]],
                    self.venue_mapping.get_venue_to_tardis_exchanges(),
                )

                filtered_exchanges: list[str] = []
                for canonical_venue in cefi_venues:
                    if canonical_venue in venue_to_exchanges:
                        raw_exchanges_for_venue: list[str] = venue_to_exchanges[canonical_venue]
                        for raw_exchange in raw_exchanges_for_venue:
                            if raw_exchange in cefi_exchanges and raw_exchange not in filtered_exchanges:
                                filtered_exchanges.append(raw_exchange)
                                logger.debug(f"  Mapped {canonical_venue} -> raw exchange {raw_exchange}")

                if filtered_exchanges:
                    cefi_exchanges = filtered_exchanges
                    logger.info(f"🔍 Filtered CEFI exchanges by canonical venues {cefi_venues}: {cefi_exchanges}")
                else:
                    logger.warning(f"⚠️ No matching CEFI exchanges found for canonical venues: {cefi_venues}")
                    cefi_exchanges = []
            else:
                logger.info("🔍 No CEFI venues in filter, skipping CEFI processing")
                cefi_exchanges = []
        else:
            logger.debug(f"🔍 No venue filter specified, processing exchanges: {cefi_exchanges}")

        if cefi_exchanges:
            # Pre-flight check: Validate venue access
            cefi_exchanges = await self._check_venue_access(cefi_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        if not cefi_exchanges:
            logger.info("⏭️ Skipping CEFI processing - no exchanges to process")
        else:
            logger.info(f"🚀 Processing {len(cefi_exchanges)} CeFi exchanges in parallel...")

            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)

            async def process_single_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
                try:
                    logger.info(f"🔍 Processing CeFi exchange {exchange}...")
                    adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
                    instruments_raw = await adapter.fetch_instruments(
                        exchange=exchange,
                        target_date=date,
                        force_refresh=force,
                        normalize=True,
                    )
                    result: dict[str, InstrumentDefinition] = {}
                    for d in cast(list[dict[str, Any]], instruments_raw):
                        try:
                            inst = InstrumentDefinition(**d)
                            key = cast(str, d.get("instrument_key") or inst.instrument_key)
                            result[key] = inst
                        except Exception as e:
                            logger.warning(
                                f"Failed to create InstrumentDefinition for {d.get('instrument_key', 'unknown')}: {e}"
                            )
                    if result:
                        logger.info(f"✅ Processed {len(result)} instruments from {exchange}")
                    return result
                except Exception as e:
                    logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
                    return {}

            results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
                *[process_single_exchange(ex) for ex in cefi_exchanges],
                return_exceptions=False,
            )

            for result in results:
                if result:
                    all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        # Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI
        cefi_onchain_clob_venues: list[str] = cast(list[str], self.venue_mapping.all_cefi_onchain_clob_venues)
        cefi_clob_protocols: list[tuple[str, None]] = []

        if venues_filter:
            for venue in venues_filter:
                if venue.upper() in cefi_onchain_clob_venues:
                    protocol_name: str = venue.lower()
                    cefi_clob_protocols.append((protocol_name, None))
        else:
            for venue in cefi_onchain_clob_venues:
                cefi_clob_protocols.append((venue.lower(), None))

        if cefi_clob_protocols:
            protocol_names: list[str] = [p[0] for p in cefi_clob_protocols]
            logger.info(f"🚀 Processing {len(cefi_clob_protocols)} on-chain CLOB venues (CEFI): {protocol_names}")
            for protocol, chain in cefi_clob_protocols:
                try:
                    clob_instruments = self.processing_service.fetch_defi_instruments(
                        protocol=protocol,
                        target_date=date,
                    )
                    if clob_instruments:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], clob_instruments))
                        logger.info(
                            f"✅ Processed {len(clob_instruments)} instruments from {protocol} (CEFI on-chain CLOB)"
                        )
                except Exception as e:
                    logger.error(f"❌ Failed to process on-chain CLOB {protocol}: {e}", exc_info=True)

        return all_instruments

    async def _check_venue_access(self, cefi_exchanges: list[str]) -> list[str]:
        """Check venue access and filter out blocked venues."""
        adapter: object | None = None
        try:
            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            tardis_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
            if hasattr(tardis_adapter, "base_client"):
                base_client = getattr(tardis_adapter, "base_client")
                if hasattr(base_client, "check_venues_access"):
                    adapter = tardis_adapter
        except (ValueError, Exception) as e:
            logger.warning(f"⚠️ Cannot check venue access: {e}")

        if adapter is not None and hasattr(adapter, "base_client"):
            base_client = getattr(adapter, "base_client")
            if hasattr(base_client, "check_venues_access"):
                access_results: dict[str, tuple[bool, str]] = cast(
                    dict[str, tuple[bool, str]],
                    base_client.check_venues_access(cefi_exchanges),
                )
                blocked: list[str] = [ex for ex, (ok, _) in access_results.items() if not ok]
                if blocked:
                    for ex in blocked:
                        _, error_msg = access_results[ex]
                        logger.warning(f"⚠️ Venue access blocked: {ex} - {error_msg}")
                    accessible = [ex for ex in cefi_exchanges if ex not in blocked]
                    if not accessible:
                        logger.error(f"❌ All {len(cefi_exchanges)} CEFI venues blocked - skipping CEFI processing")
                        return []
                    else:
                        logger.warning(
                            f"⚠️ {len(blocked)}/{len(cefi_exchanges)} venues blocked, "
                            f"continuing with {len(accessible)} accessible venues"
                        )
                        return accessible

        return cefi_exchanges

    async def _process_tradfi(
        self, date: datetime, venues_filter: list[str], tradfi_venues: list[str] | None
    ) -> dict[str, InstrumentDefinition]:
        """Process TradFi (Databento) exchanges."""
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            databento_config = UnifiedInstrumentConfig()

            all_databento_exchanges: list[str] = cast(list[str], self.venue_mapping.all_databento_venues)

            databento_exchanges: list[str]
            if tradfi_venues:
                filtered_tradfi_venues = [v for v in tradfi_venues if v in all_databento_exchanges]
                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info(f"🔍 Processing specified TradFi venues: {databento_exchanges}")
                else:
                    logger.warning(
                        f"⚠️ No valid TradFi venues in tradfi_venues={tradfi_venues}, skipping TRADFI processing"
                    )
                    databento_exchanges = []
            elif venues_filter:
                filtered_tradfi_venues = [v for v in venues_filter if v in all_databento_exchanges]

                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info(
                        f"🔍 Filtered TRADFI exchanges by venues {filtered_tradfi_venues}: {databento_exchanges}"
                    )
                else:
                    logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                    databento_exchanges = []
            else:
                databento_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
                logger.info(f"🔍 No venue filter specified, processing default TRADFI exchanges: {databento_exchanges}")

            if not databento_exchanges:
                logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
            else:
                logger.info(f"🚀 Processing {len(databento_exchanges)} TradFi venues...")

                async def process_databento_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
                    try:
                        if exchange == "CBOE":
                            vix_def_dict: dict[str, Any] | None = create_vix_instrument_definition(date)
                            if vix_def_dict:
                                vix_def = InstrumentDefinition(**vix_def_dict)
                                logger.info(f"✅ Created VIX: {vix_def.instrument_key}")
                                return {vix_def.instrument_key: vix_def}
                            return {}
                        elif exchange == "FX":
                            krwusd_def_dict: dict[str, Any] | None = create_krwusd_instrument_definition(date)
                            if krwusd_def_dict:
                                krwusd_def = InstrumentDefinition(**krwusd_def_dict)
                                logger.info(f"✅ Created KRW/USD: {krwusd_def.instrument_key}")
                                return {krwusd_def.instrument_key: krwusd_def}
                            return {}
                        elif exchange in ["NASDAQ", "NYSE"]:
                            instruments: dict[str, InstrumentDefinition] = {}

                            symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                            bitcoin_etf_launch_date = datetime(2024, 1, 11, tzinfo=timezone.utc)
                            bitcoin_etf_tickers = ["IBIT", "FBTC", "ARKB"]

                            if date >= bitcoin_etf_launch_date:
                                for ticker in bitcoin_etf_tickers:
                                    if ticker in symbols:
                                        etf_def_dict: dict[str, Any] | None = create_bitcoin_etf_instrument_definition(
                                            ticker, date, get_us_equity_trading_hours
                                        )
                                        if etf_def_dict:
                                            etf_def = InstrumentDefinition(**etf_def_dict)
                                            instruments[etf_def.instrument_key] = etf_def
                                            logger.info(f"✅ Created Bitcoin ETF: {etf_def.instrument_key}")
                            else:
                                logger.debug(
                                    f"⏭️ Skipping Bitcoin ETFs - date {date.strftime('%Y-%m-%d')} "
                                    f"is before launch (2024-01-11)"
                                )

                            non_btc_etf_symbols = [s for s in symbols if s not in bitcoin_etf_tickers]
                            if non_btc_etf_symbols:
                                _raw_other: dict[str, InstrumentDefinition] = cast(
                                    dict[str, InstrumentDefinition],
                                    await self.processing_service.fetch_databento_instruments(
                                        exchange=exchange,
                                        symbols=non_btc_etf_symbols,
                                        target_date=date,
                                    )
                                    or {},
                                )
                                other_instruments = _raw_other
                                if other_instruments:
                                    instruments.update(other_instruments)
                                    logger.info(
                                        f"✅ Processed {len(other_instruments)} additional instruments from {exchange}"
                                    )

                            if instruments:
                                logger.info(f"✅ Processed {len(instruments)} total instruments from {exchange}")
                            return instruments
                        elif exchange == "ICE":
                            ice_us_launch = datetime(2018, 12, 23, tzinfo=timezone.utc)
                            if date < ice_us_launch:
                                logger.info(
                                    f"⏭️ Skipping ICE - date {date.strftime('%Y-%m-%d')} "
                                    f"is before ICE US dataset launch (2018-12-23)"
                                )
                                return {}

                            ice_symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                            if not ice_symbols:
                                logger.warning(f"⚠️ No symbols configured for {exchange}")
                                return {}

                            ice_instruments = cast(
                                dict[str, InstrumentDefinition],
                                await self.processing_service.fetch_databento_instruments(
                                    exchange=exchange,
                                    symbols=ice_symbols,
                                    target_date=date,
                                )
                                or {},
                            )

                            if ice_instruments:
                                logger.info(f"✅ Processed {len(ice_instruments)} ICE instruments")
                            return ice_instruments
                        else:
                            cme_symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                            if not cme_symbols:
                                logger.warning(f"⚠️ No symbols configured for {exchange}")
                                return {}

                            cme_instruments = cast(
                                dict[str, InstrumentDefinition],
                                await self.processing_service.fetch_databento_instruments(
                                    exchange=exchange,
                                    symbols=cme_symbols,
                                    target_date=date,
                                )
                                or {},
                            )

                            if cme_instruments:
                                logger.info(f"✅ Processed {len(cme_instruments)} instruments from {exchange}")
                            return cme_instruments
                    except Exception as e:
                        logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
                        return {}

                results = await asyncio.gather(
                    *[process_databento_exchange(ex) for ex in databento_exchanges],
                    return_exceptions=False,
                )

                for result in results:
                    if result:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        except Exception as e:
            logger.error(f"❌ Failed to initialize Databento processing: {e}", exc_info=True)

        return all_instruments

    async def _process_defi(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """Process DeFi protocols."""
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            if venues_filter:
                defi_venues: list[str] = [
                    v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_defi_venues)
                ]

                if defi_venues:
                    defi_protocols: list[tuple[str, str | None]] = []
                    for venue in defi_venues:
                        venue_key: str = venue.upper() if venue else ""
                        if venue_key in DEFI_VENUE_TO_PROTOCOL:
                            protocol, chain = DEFI_VENUE_TO_PROTOCOL[venue_key]
                            if (protocol, chain) not in defi_protocols:
                                defi_protocols.append((protocol, chain))
                    if defi_protocols:
                        defi_protocol_names: list[str] = [p[0] for p in defi_protocols]
                        logger.info(f"🔍 Filtered DEFI protocols by venues {defi_venues}: {defi_protocol_names}")
                    else:
                        logger.warning(f"⚠️ No matching DEFI protocols found for venues: {defi_venues}")
                        defi_protocols = []
                else:
                    logger.info("🔍 No DEFI venues in filter, skipping DEFI processing")
                    defi_protocols = []
            else:
                defi_protocols = DEFI_PROTOCOLS
                logger.info("🔍 No venue filter specified, processing all DEFI protocols")

            if not defi_protocols:
                logger.info("⏭️ Skipping DEFI processing - no protocols to process")
            else:
                for protocol, chain in defi_protocols:
                    try:
                        if chain:
                            defi_instruments = self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                chain=chain,
                                target_date=date,
                            )
                        else:
                            defi_instruments = self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                target_date=date,
                            )
                        if defi_instruments:
                            all_instruments.update(cast(dict[str, InstrumentDefinition], defi_instruments))
                            logger.info(f"✅ Processed {len(defi_instruments)} instruments from {protocol}")
                    except Exception as e:
                        logger.error(f"❌ Failed to process {protocol}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"❌ Failed to initialize DeFi processing: {e}", exc_info=True)

        return all_instruments

    def _filter_by_instrument_ids(
        self, all_instruments: dict[str, InstrumentDefinition], instrument_ids: list[str] | str
    ) -> dict[str, InstrumentDefinition]:
        """Filter instruments by instrument_ids."""
        instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
        instrument_ids_set: set[str] = set(str(inst_id).upper() for inst_id in instrument_ids_list)

        filtered_instruments: dict[str, InstrumentDefinition] = {}
        for inst_key, inst_obj in all_instruments.items():
            if inst_key.upper() in instrument_ids_set:
                filtered_instruments[inst_key] = inst_obj

        filtered_count = len(all_instruments) - len(filtered_instruments)
        if filtered_count > 0:
            logger.info(
                f"🔍 Filtered {filtered_count} instruments by instrument_ids, "
                f"{len(filtered_instruments)} matching instruments remaining"
            )

        if filtered_instruments:
            logger.info(f"✅ Matching instrument_ids: {list(filtered_instruments.keys())}")
        else:
            logger.warning(
                f"⚠️ No instruments matched the specified instrument_ids: {instrument_ids_list}. "
                f"Processed {len(all_instruments)} instruments but none matched."
            )

        return filtered_instruments

    def _convert_to_dataframe(
        self, all_instruments: dict[str, InstrumentDefinition], skip_storage: bool
    ) -> pd.DataFrame:
        """Convert instruments dict to DataFrame."""
        instruments_list: list[dict[str, Any]] = []
        for inst_key, inst_obj in all_instruments.items():
            _ = inst_key
            if hasattr(inst_obj, "model_dump"):
                instruments_list.append(cast(dict[str, Any], inst_obj.model_dump()))
            else:
                instruments_list.append(cast(dict[str, Any], inst_obj))

        instruments_df = pd.DataFrame(instruments_list)

        if skip_storage and not instruments_df.empty:
            from unified_cloud_services import determine_market_category

            def _row_to_market_category(row: pd.Series[Any]) -> str:
                raw = cast(dict[str, Any], row.to_dict())
                row_dict: dict[str, str | None] = {}
                for k, v in raw.items():
                    row_dict[str(k)] = str(v) if pd.notna(v) else None
                return determine_market_category(row_dict)

            if "market_category" not in instruments_df.columns:
                instruments_df["market_category"] = ""
            mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
            if mask.any():
                instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                    _row_to_market_category, axis=1
                )

        return instruments_df

    def _handle_no_instruments(
        self,
        date_str: str,
        tradfi: bool,
        venues_filter: list[str],
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, Any]:
        """Handle case where no instruments generated."""
        if tradfi and venues_filter:
            logger.warning(
                f"⚠️ No instruments generated from adapters for {date_str}. "
                f"This can happen on weekday holidays when Databento returns empty. "
                f"Placeholder logic will ensure files are written for requested TradFi venues."
            )
        else:
            logger.error(
                f"❌ No instruments generated for {date_str}. This is unexpected - check adapter/API connectivity."
            )
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count
            root_logger.removeHandler(error_warning_counter)
            return {
                "status": "error",
                "date": date_str,
                "instruments_generated": 0,
                "message": "No instruments generated - check adapter/API connectivity",
                "error_count": error_count,
                "warning_count": warning_count,
            }

        return {}

    def _add_tradfi_placeholders(
        self, instruments_df: pd.DataFrame, date: datetime, venues_filter: list[str]
    ) -> pd.DataFrame:
        """Add placeholders for missing TradFi venues."""
        requested_tradfi: list[str] = [
            v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_databento_venues)
        ]
        logger.debug(f"🔍 Placeholder check: Requested TradFi venues: {requested_tradfi}")

        if requested_tradfi:
            if isinstance(date, datetime):
                target_dt = date.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                target_dt = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
            if target_dt.tzinfo is None:
                target_dt = target_dt.replace(tzinfo=timezone.utc)
            date_iso = target_dt.isoformat().replace("+00:00", "Z")

            if "venue" not in instruments_df.columns or instruments_df.empty:
                logger.info("🔍 DataFrame empty or no venue column - adding placeholders for all requested venues")
                venues_present: set[str] = set()
            else:
                unique_venues = instruments_df["venue"].unique()
                venues_present = {str(v) for v in unique_venues}
                logger.debug(f"🔍 Venues present in DataFrame: {venues_present}")

            for venue_name in requested_tradfi:
                if venue_name not in venues_present:
                    placeholder: dict[str, Any] = {
                        "instrument_key": f"{venue_name}:MARKET_CLOSED:PLACEHOLDER",
                        "venue": venue_name,
                        "instrument_type": "MARKET_CLOSED",
                        "symbol": "PLACEHOLDER",
                        "available_from_datetime": date_iso,
                        "timestamp": target_dt,
                        "market_category": "TRADFI",
                        "data_provider": "databento",
                        "asset_class": "traditional",
                        "venue_type": "exchange",
                        "chain": "off-chain",
                        "databento_symbol": "PLACEHOLDER",
                        "tardis_exchange": "",
                        "is_trading_day": False,
                        "trading_hours_open": "holiday",
                        "trading_hours_close": "holiday",
                        "holiday_calendar": venue_name,
                        "trading_session": "regular",
                        "regular_open_utc": None,
                        "regular_close_utc": None,
                        "auction_open_utc": None,
                        "auction_close_utc": None,
                        "early_close_utc": None,
                    }
                    logger.info(
                        f"📅 Adding holiday/closed placeholder for {venue_name} on {date.strftime('%Y-%m-%d')} "
                        f"(no instruments from adapter so file would be missing)"
                    )
                    instruments_df = pd.concat([instruments_df, pd.DataFrame([placeholder])], ignore_index=True)
                else:
                    logger.debug(f"✅ {venue_name} already has instruments - no placeholder needed")

        return instruments_df

    def _handle_utc_midnight_spanning(self, instruments_df: pd.DataFrame, date: datetime) -> None:
        """Handle UTC midnight spanning for CME and ICE."""
        utc_spanning_instruments: list[pd.Series] = []
        file_date: date_type = date.date() if isinstance(date, datetime) else date

        for idx, row in instruments_df.iterrows():
            _ = idx
            if row.get("regular_open_utc") and row.get("regular_close_utc"):
                try:
                    open_dt = cast(pd.Timestamp, pd.to_datetime(row["regular_open_utc"]))
                    close_dt = cast(pd.Timestamp, pd.to_datetime(row["regular_close_utc"]))

                    if open_dt.date() != file_date and close_dt.date() == file_date:
                        utc_spanning_instruments.append(row)
                        logger.debug(
                            f"🌙 UTC midnight spanning: {row['instrument_key']} "
                            f"opens {open_dt.date()} closes {close_dt.date()}"
                        )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to parse session times for {row.get('instrument_key')}: {e}")

        if utc_spanning_instruments:
            spanning_df = pd.DataFrame(utc_spanning_instruments)
            spanning_df["session_date_tag"] = "open_date"

            open_date_val = cast(date_type, pd.to_datetime(spanning_df.iloc[0]["regular_open_utc"]).date())
            logger.info(
                f"🌙 Writing {len(spanning_df)} UTC-spanning instruments to open-date file: {open_date_val} "
                f"(session opens {open_date_val}, closes {file_date})"
            )

            open_date_dt = datetime.combine(open_date_val, datetime.min.time(), tzinfo=timezone.utc)
            span_success = cast(
                bool,
                self.cloud_storage.store_instruments(
                    instruments_df=spanning_df, table_name="instruments", date=open_date_dt
                ),
            )

            if span_success:
                logger.info(f"✅ Successfully wrote UTC-spanning instruments to {open_date_val}")
            else:
                logger.warning(f"⚠️ Failed to write UTC-spanning instruments to {open_date_val}")

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
    ) -> pd.DataFrame:
        """
        Query stored instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            base_asset: Optional base asset filter
            quote_asset: Optional quote asset filter

        Returns:
            DataFrame with instruments
        """
        result: pd.DataFrame = self.cloud_storage.query_instruments(venue=venue, instrument_type=instrument_type)
        if base_asset is not None and not result.empty and "base_asset" in result.columns:
            result = result[result["base_asset"] == base_asset]
        if quote_asset is not None and not result.empty and "quote_asset" in result.columns:
            result = result[result["quote_asset"] == quote_asset]
        return result

    def get_processing_stats(self) -> dict[str, Any]:
        """Get processing statistics."""
        return {
            "processing_service": self.processing_service.get_processing_stats(),
            "batch_processor": {
                "max_batch_size": self.batch_processor.max_batch_size,
                "lookback_days": self.batch_processor.lookback_days,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "processing_service"):
            self.processing_service.cleanup()
        logger.info("🧹 InstrumentsOrchestrator cleanup completed")
