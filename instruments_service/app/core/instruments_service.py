"""
Main Orchestration Service for Instruments Service

Coordinates instrument processing, storage, and batch operations.
Follows unified repository structure pattern.

This module is the main entry point. Implementation is split across:
- instrument_crud.py — query, batch processing, stats, cleanup, ErrorWarningCounter
- instrument_sync.py — CEFI/TRADFI/DEFI venue processing
- instrument_validation.py — venue validation and filtering
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast
from uuid import uuid4

import pandas as pd
from unified_api_contracts import VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_trading_library import determine_market_category  # noqa: domain-ucs — not yet in UDC

from instruments_service.app.core.batch_processor import InstrumentBatchProcessor
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

# Import mixins for this file's SRP split
from instruments_service.app.core.instrument_crud import ErrorWarningCounter, InstrumentCrudMixin
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from instruments_service.app.core.instrument_sync import InstrumentSyncMixin
from instruments_service.app.core.instrument_validation import InstrumentValidationMixin
from instruments_service.catalogue_updater import update_catalogue_entry
from instruments_service.config import instruments_config
from instruments_service.models import InstrumentDefinition

__all__ = [
    "ErrorWarningCounter",
    "InstrumentsService",
]

logger = logging.getLogger(__name__)


class InstrumentsService(
    InstrumentCrudMixin,
    InstrumentSyncMixin,
    InstrumentValidationMixin,
):
    """
    Main orchestration service that coordinates:
    - Instrument processing (fetching from APIs, generating canonical keys)
    - Cloud storage (GCS and BigQuery)
    - Batch operations (date range processing, memory estimation)

    Functionality is composed from mixins:
    - InstrumentCrudMixin: query, batch processing, stats, cleanup
    - InstrumentSyncMixin: CEFI/TRADFI/DEFI venue processing
    - InstrumentValidationMixin: venue validation and filtering
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestration service.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - sink_bucket: Sink bucket name (optional, auto-detected)
                - analytics_dataset: Analytics dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        processing_config: dict[str, object] = {
            "project_id": str(cast(str | None, config.get("project_id")) or instruments_config.gcp_project_id),
            "enable_ccxt_integration": cast(bool, config.get("enable_ccxt_integration", True)),
            "enable_metadata_caching": cast(bool, config.get("enable_metadata_caching", True)),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping — instance assignment satisfies mixin stub
        self.venue_mapping: VenueMapping = VenueMapping()

        logger.info("✅ InstrumentsService initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        sports: bool = False,
        venues: list[str] | str | None = None,
        instrument_ids: list[str] | str | None = None,
        tradfi_venues: list[str] | None = None,
        skip_storage: bool = False,
    ) -> dict[str, object]:
        """Generate instruments for a specific date."""
        error_warning_counter = ErrorWarningCounter()
        root_logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)
        error_warning_counter.reset()
        date_str: str = date.strftime("%Y-%m-%d")

        try:
            return await self._generate_instruments_inner(
                date=date,
                date_str=date_str,
                exchanges=exchanges,
                force=force,
                cefi=cefi,
                tradfi=tradfi,
                defi=defi,
                sports=sports,
                venues=venues,
                instrument_ids=instrument_ids,
                tradfi_venues=tradfi_venues,
                skip_storage=skip_storage,
                error_warning_counter=error_warning_counter,
                root_logger=root_logger,
            )
        except (OSError, ValueError, RuntimeError) as e:
            return self._build_exception_result(e, date_str, error_warning_counter, root_logger)
        finally:
            if error_warning_counter in root_logger.handlers:
                root_logger.removeHandler(error_warning_counter)

    async def _generate_instruments_inner(
        self,
        date: datetime,
        date_str: str,
        exchanges: list[str] | None,
        force: bool,
        cefi: bool,
        tradfi: bool,
        defi: bool,
        sports: bool,
        venues: list[str] | str | None,
        instrument_ids: list[str] | str | None,
        tradfi_venues: list[str] | None,
        skip_storage: bool,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, object]:
        """Core instrument generation logic, delegating to per-mode helpers.

        In mock mode (CLOUD_MOCK_MODE=true), uses InstrumentGenerator from
        unified-internal-contracts instead of hitting real exchange APIs.
        """
        # --- MOCK MODE: generate instruments locally ---
        if instruments_config.is_mock_mode():
            return self._generate_instruments_mock(
                date,
                date_str,
                skip_storage,
                error_warning_counter,
                root_logger,
            )

        logger.info(
            "📅 Generating instruments for %s (CeFi=%s, TradFi=%s, DeFi=%s, Sports=%s)",
            date_str,
            cefi,
            tradfi,
            defi,
            sports,
        )
        venues_filter = self._normalize_venues_filter(venues, instrument_ids)
        early_result = self._check_venues_early_return(
            venues_filter,
            cefi,
            tradfi,
            defi,
            error_warning_counter,
            root_logger,
            date_str,
        )
        if early_result is not None:
            return early_result
        if not cefi and not tradfi and not defi and not sports:
            return await self._generate_all_modes(date, exchanges, force, venues, instrument_ids)
        all_instruments, cefi_exchanges = await self._collect_all_instruments(
            date,
            exchanges,
            force,
            cefi,
            tradfi,
            defi,
            sports,
            venues_filter,
            tradfi_venues,
        )
        return self._finalize_generation(
            all_instruments,
            cefi_exchanges,
            instrument_ids,
            instruments_df=None,
            tradfi=tradfi,
            cefi=cefi,
            venues_filter=venues_filter,
            date=date,
            date_str=date_str,
            skip_storage=skip_storage,
            error_warning_counter=error_warning_counter,
            root_logger=root_logger,
        )

    def _generate_instruments_mock(
        self,
        date: datetime,
        date_str: str,
        skip_storage: bool,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, object]:
        """Generate instruments using InstrumentGenerator in mock mode.

        Reads from seed data if available, otherwise generates inline.
        Writes output to the local seed path for downstream services.
        """
        from instruments_service.engine.mock_data_provider import (
            _seed_exists_for_date,
            generate_mock_instruments,
            load_seed_instruments,
            write_mock_output,
        )

        logger.info(
            "MOCK MODE: Generating instruments for %s using InstrumentGenerator",
            date_str,
        )

        # Try to load existing seed data first
        if _seed_exists_for_date(date_str):
            logger.info("MOCK MODE: Loading existing seed data for %s", date_str)
            all_instruments = load_seed_instruments(date_str)
        else:
            logger.info("MOCK MODE: No seed data found, generating inline for %s", date_str)
            all_instruments = generate_mock_instruments(date)

        if not all_instruments:
            root_logger.removeHandler(error_warning_counter)
            return {
                "status": "warning",
                "date": date_str,
                "instruments_generated": 0,
                "message": "Mock mode: no instruments generated",
                "error_count": error_warning_counter.error_count,
                "warning_count": error_warning_counter.warning_count,
            }

        # Convert to DataFrame for standard processing path
        instruments_df = self._instruments_to_dataframe(all_instruments)
        self._populate_market_category(instruments_df)

        # Write to local seed path (for downstream services)
        write_mock_output(all_instruments, date)

        # Optionally store via cloud storage (skip_storage=True in live mode)
        if not skip_storage:
            try:
                self._store_instruments(instruments_df, date, skip_storage=False)
            except (OSError, ValueError, RuntimeError) as e:
                logger.warning(
                    "MOCK MODE: Cloud storage failed (expected in local mock): %s",
                    e,
                )

        error_count = error_warning_counter.error_count
        warning_count = error_warning_counter.warning_count
        root_logger.removeHandler(error_warning_counter)

        logger.info(
            "MOCK MODE: Generated %d instruments for %s",
            len(all_instruments),
            date_str,
        )

        return {
            "status": "success",
            "date": date_str,
            "instruments_generated": len(all_instruments),
            "exchanges_processed": 0,
            "venues": list(instruments_df["venue"].unique()) if "venue" in instruments_df.columns else [],
            "mock_mode": True,
            "error_count": error_count,
            "warning_count": warning_count,
        }

    async def _generate_all_modes(
        self,
        date: datetime,
        exchanges: list[str] | None,
        force: bool,
        venues: list[str] | str | None,
        instrument_ids: list[str] | str | None,
    ) -> dict[str, object]:
        """Re-invoke generation with all mode flags enabled."""
        logger.info("📋 No mode flags specified, processing all modes (CeFi, TradFi, DeFi, Sports)")
        return await self.generate_instruments_for_date(
            date=date,
            exchanges=exchanges,
            force=force,
            cefi=True,
            tradfi=True,
            defi=True,
            sports=True,
            venues=venues,
            instrument_ids=instrument_ids,
        )

    def _check_venues_early_return(
        self,
        venues_filter: list[str],
        cefi: bool,
        tradfi: bool,
        defi: bool,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
        date_str: str,
    ) -> dict[str, object] | None:
        """Validate venues and return early if no valid venues found."""
        if not venues_filter:
            return None
        valid_venues_by_type = self._validate_venues_filter(venues_filter, cefi, tradfi, defi)
        if valid_venues_by_type is not None and not any(valid_venues_by_type.values()):
            return self._finalize_counts(
                error_warning_counter,
                root_logger,
                {
                    "status": "warning",
                    "date": date_str,
                    "instruments_generated": 0,
                    "message": "No valid venues found after filtering",
                },
            )
        return None

    def _finalize_generation(
        self,
        all_instruments: dict[str, InstrumentDefinition],
        cefi_exchanges: list[str],
        instrument_ids: list[str] | str | None,
        instruments_df: pd.DataFrame | None,
        tradfi: bool,
        cefi: bool,
        venues_filter: list[str],
        date: datetime,
        date_str: str,
        skip_storage: bool,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, object]:
        """Post-collection: filter, convert, store, and build result."""
        all_instruments = self._filter_instruments_by_ids(all_instruments, instrument_ids)
        instruments_df = self._instruments_to_dataframe(all_instruments)
        if skip_storage and not instruments_df.empty:
            self._populate_market_category(instruments_df)
        if not all_instruments:
            empty_result = self._handle_empty_instruments(
                tradfi,
                venues_filter,
                date_str,
                error_warning_counter,
                root_logger,
            )
            if empty_result:
                return empty_result
        instruments_df = self._apply_tradfi_and_session_tag(instruments_df, tradfi, venues_filter, date, date_str)
        success = self._store_and_handle_spanning(instruments_df, date, date_str, skip_storage)
        root_logger.removeHandler(error_warning_counter)
        if success and not skip_storage and not instruments_df.empty:
            self._update_catalogue(instruments_df)
        return self._build_generation_result(
            success,
            date_str,
            instruments_df,
            cefi,
            cefi_exchanges,
            skip_storage,
            error_warning_counter.error_count,
            error_warning_counter.warning_count,
        )

    def _apply_tradfi_and_session_tag(
        self,
        instruments_df: pd.DataFrame,
        tradfi: bool,
        venues_filter: list[str],
        date: datetime,
        date_str: str,
    ) -> pd.DataFrame:
        """Add TradFi placeholders and session_date_tag column."""
        if tradfi and venues_filter:
            instruments_df = self._add_tradfi_placeholders(instruments_df, venues_filter, date, date_str)
        if not instruments_df.empty and "regular_open_utc" in instruments_df.columns:
            instruments_df["session_date_tag"] = "close_date"
        return instruments_df

    def _store_and_handle_spanning(
        self,
        instruments_df: pd.DataFrame,
        date: datetime,
        date_str: str,
        skip_storage: bool,
    ) -> bool:
        """Store instruments and handle UTC-spanning if applicable."""
        success = self._store_instruments(instruments_df, date, skip_storage)
        if success and not skip_storage and not instruments_df.empty and "regular_open_utc" in instruments_df.columns:
            self._handle_utc_spanning(instruments_df, date, date_str)
        return success

    def _normalize_venues_filter(
        self,
        venues: list[str] | str | None,
        instrument_ids: list[str] | str | None,
    ) -> list[str]:
        """Normalize venues parameter and merge with instrument_id-derived venues."""
        _venues_raw: list[str] | str = venues if venues is not None else []
        if isinstance(_venues_raw, str):
            _venues_raw = [_venues_raw]
        venues_filter: list[str] = [str(v).upper() for v in _venues_raw] if _venues_raw else []
        return self._extract_venues_from_instrument_ids(instrument_ids, venues_filter)

    async def _collect_all_instruments(
        self,
        date: datetime,
        exchanges: list[str] | None,
        force: bool,
        cefi: bool,
        tradfi: bool,
        defi: bool,
        sports: bool,
        venues_filter: list[str],
        tradfi_venues: list[str] | None,
    ) -> tuple[dict[str, InstrumentDefinition], list[str]]:
        """Collect instruments from all enabled market modes."""
        all_instruments: dict[str, InstrumentDefinition] = {}
        cefi_exchanges: list[str] = []

        if cefi:
            cefi_exchanges, cefi_instruments = await self._process_cefi_results(
                exchanges,
                venues_filter,
                date,
                force,
            )
            all_instruments.update(cefi_instruments)
        if tradfi:
            all_instruments.update(await self._process_tradfi_results(venues_filter, tradfi_venues, date))
        if defi:
            all_instruments.update(await self._process_defi_results(venues_filter, date))
        if sports:
            all_instruments.update(await self._process_sports_results(date, venues_filter))

        return all_instruments, cefi_exchanges

    async def _process_cefi_results(
        self,
        exchanges: list[str] | None,
        venues_filter: list[str],
        date: datetime,
        force: bool,
    ) -> tuple[list[str], dict[str, InstrumentDefinition]]:
        """Process CeFi exchanges with error wrapping."""
        cefi_exchanges = (
            cast(list[str], exchanges)
            if exchanges is not None
            else cast(list[str], self.venue_mapping.all_tardis_exchanges)
        )
        try:
            return await self._process_cefi_exchanges(
                cefi_exchanges=cefi_exchanges,
                venues_filter=venues_filter,
                date=date,
                force=force,
            )
        except (OSError, ValueError, RuntimeError) as e:
            raise self._wrap_enhanced_error(e) from e

    async def _process_tradfi_results(
        self,
        venues_filter: list[str],
        tradfi_venues: list[str] | None,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process TradFi exchanges with error wrapping."""
        try:
            return await self._process_tradfi_exchanges(
                venues_filter=venues_filter,
                tradfi_venues=tradfi_venues,
                date=date,
            )
        except (ValueError, KeyError, TypeError, IndexError) as e:
            raise self._wrap_enhanced_error(e) from e

    async def _process_defi_results(
        self,
        venues_filter: list[str],
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Process DeFi protocols with error wrapping."""
        try:
            return await self._process_defi_protocols(venues_filter=venues_filter, date=date)
        except (OSError, ValueError, RuntimeError) as e:
            raise self._wrap_enhanced_error(e) from e

    @staticmethod
    async def _process_sports_results(
        date: datetime,
        venues_filter: list[str],
    ) -> dict[str, InstrumentDefinition]:
        """Process Sports fixtures via lazy-loaded orchestrator."""
        from instruments_service.engine.operations.instruments.orchestration.sports_orchestration import (  # noqa: lazy-load — avoids eager load of ~4MB sports registry data
            SportsOrchestrator,
        )

        sports_orchestrator = SportsOrchestrator()
        return await sports_orchestrator.process_sports(date, venues_filter)

    @staticmethod
    def _wrap_enhanced_error(e: BaseException) -> RuntimeError:
        """Wrap an exception in an EnhancedError and return a RuntimeError."""
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.HIGH,
            recovery_strategy=ErrorRecoveryStrategy.RETRY,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
        return RuntimeError(f"[{_err.correlation_id}] {_err.message}")

    @staticmethod
    def _instruments_to_dataframe(
        all_instruments: dict[str, InstrumentDefinition],
    ) -> pd.DataFrame:
        """Convert instrument definitions dict to a DataFrame."""
        instruments_list: list[dict[str, object]] = []
        for _inst_key, inst_obj in all_instruments.items():
            if hasattr(inst_obj, "model_dump"):
                instruments_list.append(cast(dict[str, object], inst_obj.model_dump()))
            else:
                instruments_list.append(cast(dict[str, object], cast(object, inst_obj)))
        return pd.DataFrame(instruments_list)

    @staticmethod
    def _populate_market_category(instruments_df: pd.DataFrame) -> None:
        """Populate missing market_category values for live mode."""

        def _row_to_market_category(row: pd.Series) -> str:  # pyright: ignore[reportMissingTypeArgument]
            raw: dict[str, object] = cast(dict[str, object], row.to_dict())
            row_dict: dict[str, object] = {}
            for k, val in raw.items():
                if val is None or (isinstance(val, float) and val != val):
                    row_dict[str(k)] = None
                else:
                    row_dict[str(k)] = str(val)
            return determine_market_category(row_dict)

        if "market_category" not in instruments_df.columns:
            instruments_df["market_category"] = ""
        mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
        if mask.any():
            instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                _row_to_market_category, axis=1
            )

    @staticmethod
    def _handle_empty_instruments(
        tradfi: bool,
        venues_filter: list[str],
        date_str: str,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, object] | None:
        """Return error result when no instruments generated, or None to continue with placeholders."""
        if tradfi and venues_filter:
            logger.warning(
                "⚠️ No instruments generated from adapters for %s. "
                "Placeholder logic will ensure files are written for requested TradFi venues.",
                date_str,
            )
            return None
        logger.error("No instruments generated for %s.", date_str)
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

    def _store_instruments(
        self,
        instruments_df: pd.DataFrame,
        date: datetime,
        skip_storage: bool,
    ) -> bool:
        """Store instruments to cloud or skip if in live mode."""
        if skip_storage:
            logger.info("📤 Skipping batch storage (live mode) - %s instruments", len(instruments_df))
            return True
        logger.info("📤 Storing %s instruments to cloud...", len(instruments_df))
        return cast(
            bool,
            self.cloud_storage.store_instruments(  # pyright: ignore[reportUnknownMemberType]
                instruments_df=instruments_df, table_name="instruments", date=date
            ),
        )

    @staticmethod
    def _update_catalogue(instruments_df: pd.DataFrame) -> None:
        """Post-batch hook: update data-catalogue YAML with fresh metadata."""
        _row_count = len(instruments_df)
        _categories: list[str] = (
            [str(c) for c in instruments_df["market_category"].unique().tolist()]
            if "market_category" in instruments_df.columns
            else []
        )
        for _cat in _categories:
            _dataset_id = f"instruments_{_cat.lower()}"
            update_catalogue_entry(dataset_id=_dataset_id, row_count=_row_count)

    @staticmethod
    def _build_generation_result(
        success: bool,
        date_str: str,
        instruments_df: pd.DataFrame,
        cefi: bool,
        cefi_exchanges: list[str],
        skip_storage: bool,
        error_count: int,
        warning_count: int,
    ) -> dict[str, object]:
        """Build the final generation result dict."""
        if success:
            logger.info("✅ Successfully stored instruments for %s", date_str)
            result: dict[str, object] = {
                "status": "success",
                "date": date_str,
                "instruments_generated": len(instruments_df),
                "exchanges_processed": len(cefi_exchanges) if cefi else 0,
                "venues": cast(list[str], instruments_df["venue"].unique().tolist())
                if "venue" in instruments_df.columns
                else [],
                "error_count": error_count,
                "warning_count": warning_count,
            }
            if skip_storage and not instruments_df.empty and "market_category" in instruments_df.columns:
                result["instruments_by_category"] = {
                    str(cat): grp for cat, grp in instruments_df.groupby("market_category")
                }
            return result
        logger.error("Failed to store instruments for %s", date_str)
        return {
            "status": "error",
            "date": date_str,
            "instruments_generated": len(instruments_df),
            "message": "Storage failed",
            "error_count": error_count,
            "warning_count": warning_count,
        }

    @staticmethod
    def _build_exception_result(
        e: BaseException,
        date_str: str,
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
    ) -> dict[str, object]:
        """Build error result dict from an exception."""
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        root_logger.removeHandler(error_warning_counter)
        error_count = error_warning_counter.error_count
        warning_count = error_warning_counter.warning_count
        logger.exception("Exception during instrument generation: %s", e)
        return {
            "status": "error",
            "date": date_str,
            "instruments_generated": 0,
            "message": f"Exception during processing: {e!s}",
            "error_count": error_count,
            "warning_count": warning_count,
        }

    @staticmethod
    def _finalize_counts(
        error_warning_counter: ErrorWarningCounter,
        root_logger: logging.Logger,
        result: dict[str, object],
    ) -> dict[str, object]:
        """Inject error/warning counts into result and detach counter."""
        result["error_count"] = error_warning_counter.error_count
        result["warning_count"] = error_warning_counter.warning_count
        root_logger.removeHandler(error_warning_counter)
        return result

    def _add_tradfi_placeholders(
        self,
        instruments_df: pd.DataFrame,
        venues_filter: list[str],
        date: datetime,
        date_str: str,
    ) -> pd.DataFrame:
        """Add holiday/closed placeholder rows for TradFi venues without instruments."""
        requested_tradfi: list[str] = [
            v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_databento_venues)
        ]
        if not requested_tradfi:
            return instruments_df

        target_dt, date_iso = self._resolve_tradfi_date(date)
        venues_present = self._get_present_venues(instruments_df)

        for venue_name in requested_tradfi:
            if venue_name not in venues_present:
                placeholder = self._build_tradfi_placeholder(venue_name, date_iso, target_dt)
                logger.info("📅 Adding holiday/closed placeholder for %s on %s", venue_name, date_str)
                instruments_df = pd.concat([instruments_df, pd.DataFrame([placeholder])], ignore_index=True)
        return instruments_df

    @staticmethod
    def _resolve_tradfi_date(date: datetime) -> tuple[datetime, str]:
        """Resolve target datetime and ISO string for TradFi placeholders."""
        target_dt = (
            date.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(date, datetime)
            else datetime.combine(date, datetime.min.time(), tzinfo=UTC)
        )
        if target_dt.tzinfo is None:
            target_dt = target_dt.replace(tzinfo=UTC)
        date_iso = target_dt.isoformat().replace("+00:00", "Z")
        return target_dt, date_iso

    @staticmethod
    def _get_present_venues(instruments_df: pd.DataFrame) -> set[str]:
        """Return set of venue names already present in the DataFrame."""
        if "venue" in instruments_df.columns and not instruments_df.empty:
            return {str(v) for v in cast(list[object], instruments_df["venue"].unique().tolist())}
        return set()

    @staticmethod
    def _build_tradfi_placeholder(venue_name: str, date_iso: str, target_dt: datetime) -> dict[str, object]:
        """Build a single TradFi holiday/closed placeholder dict."""
        return {
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

    def _handle_utc_spanning(self, instruments_df: pd.DataFrame, date: datetime, date_str: str) -> None:
        """Handle UTC midnight spanning for CME and ICE instruments."""
        utc_spanning_instruments: list[pd.Series] = []
        file_date: date_type = date.date() if isinstance(date, datetime) else date

        for idx, row in instruments_df.iterrows():
            _ = idx
            if row.get("regular_open_utc") and row.get("regular_close_utc"):
                try:
                    open_dt = cast(pd.Timestamp, pd.to_datetime(cast(str, row["regular_open_utc"])))
                    close_dt = cast(pd.Timestamp, pd.to_datetime(cast(str, row["regular_close_utc"])))
                    if open_dt.date() != file_date and close_dt.date() == file_date:
                        utc_spanning_instruments.append(row)
                except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e

        if utc_spanning_instruments:
            spanning_df = pd.DataFrame(utc_spanning_instruments)
            spanning_df["session_date_tag"] = "open_date"
            open_date_val = cast(date_type, pd.to_datetime(cast(str, spanning_df.iloc[0]["regular_open_utc"])).date())
            logger.info("🌙 Writing %s UTC-spanning instruments to open-date file: %s", len(spanning_df), open_date_val)
            open_date_dt = datetime.combine(open_date_val, datetime.min.time(), tzinfo=UTC)
            span_success = cast(
                bool,
                self.cloud_storage.store_instruments(  # pyright: ignore[reportUnknownMemberType]
                    instruments_df=spanning_df, table_name="instruments", date=open_date_dt
                ),
            )
            if span_success:
                logger.info("✅ Successfully wrote UTC-spanning instruments to %s", open_date_val)
            else:
                logger.warning("⚠️ Failed to write UTC-spanning instruments to %s", open_date_val)
