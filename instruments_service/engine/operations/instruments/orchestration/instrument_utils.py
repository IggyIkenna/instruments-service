"""
Instrument Utilities Module

Contains helper functions for venue filtering, data conversion, and validation used by the orchestrator.
"""

import logging
from datetime import date as date_type
from datetime import datetime
from typing import cast
from uuid import uuid4

import pandas as pd
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import VenueMapping

from instruments_service.models import InstrumentDefinition
from instruments_service.utils import ErrorWarningCounter

logger = logging.getLogger(__name__)


class InstrumentUtils:
    """Utility functions for instrument orchestration."""

    def __init__(self):
        """Initialize instrument utilities."""
        self.venue_mapping: VenueMapping = VenueMapping()

    def normalize_venues_filter(self, venues: list[str] | str | None) -> list[str]:
        """Normalize venues filter to a list of strings."""
        if venues is None:
            return []
        if isinstance(venues, str):
            return [venues]
        return venues

    def extract_venues_from_instrument_ids(
        self, instrument_ids: list[str] | str, venues_filter: list[str]
    ) -> list[str]:
        """Extract venues from instrument_ids if provided."""
        instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]

        # Extract venues from instrument keys (format: "VENUE:TYPE:SYMBOL")
        extracted_venues: list[str] = []
        for inst_id in instrument_ids_list:
            parts = str(inst_id).split(":")
            if len(parts) >= 2:
                venue_part = parts[0]
                if venue_part not in extracted_venues:
                    extracted_venues.append(venue_part)

        if extracted_venues:
            # Merge with existing venues_filter
            combined_venues = list(set(venues_filter + extracted_venues))
            logger.info("🔍 Extracted venues from instrument_ids: %s", extracted_venues)
            logger.info("🔍 Combined venues filter: %s", combined_venues)
            return combined_venues

        return venues_filter

    def validate_venues(
        self, venues_filter: list[str], cefi: bool, tradfi: bool, defi: bool
    ) -> dict[str, object] | None:
        """Validate venues against allowed venues for each market type."""
        all_allowed_venues: set[str] = set()

        if cefi:
            tardis_venues = set(cast(dict[str, str], self.venue_mapping.tardis_to_venue).values())
            cefi_onchain_venues = set(cast(list[str], self.venue_mapping.all_cefi_onchain_clob_venues))
            all_allowed_venues.update(tardis_venues)
            all_allowed_venues.update(cefi_onchain_venues)

        if tradfi:
            tradfi_venues = set(cast(list[str], self.venue_mapping.all_databento_venues))
            all_allowed_venues.update(tradfi_venues)

        if defi:
            defi_venues = set(cast(list[str], self.venue_mapping.all_defi_venues))
            all_allowed_venues.update(defi_venues)

        invalid_venues = [v for v in venues_filter if v not in all_allowed_venues]
        if invalid_venues:
            error_msg = (
                f"Invalid venues in filter: {invalid_venues}. "
                f"Valid venues for selected modes (CeFi={cefi}, TradFi={tradfi}, DeFi={defi}): "
                f"{sorted(all_allowed_venues)}"
            )
            logger.error("❌ %s", error_msg)
            return {
                "success": False,
                "error": error_msg,
                "invalid_venues": invalid_venues,
                "valid_venues": sorted(all_allowed_venues),
                "error_count": 1,
                "warning_count": 0,
            }

        return None

    def filter_by_instrument_ids(
        self, all_instruments: dict[str, InstrumentDefinition], instrument_ids: list[str] | str
    ) -> dict[str, InstrumentDefinition]:
        """Filter instruments by instrument_ids."""
        instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
        instrument_ids_set: set[str] = {str(inst_id).upper() for inst_id in instrument_ids_list}

        filtered_instruments: dict[str, InstrumentDefinition] = {}
        for inst_key, inst_obj in all_instruments.items():
            if inst_key.upper() in instrument_ids_set:
                filtered_instruments[inst_key] = inst_obj

        filtered_count = len(all_instruments) - len(filtered_instruments)
        if filtered_count > 0:
            logger.info(
                "🔍 Filtered %s instruments by instrument_ids, %s matching instruments remaining",
                filtered_count,
                len(filtered_instruments),
            )

        if filtered_instruments:
            logger.info("✅ Matching instrument_ids: %s", list(filtered_instruments.keys()))
        else:
            logger.warning(
                "⚠️ No instruments matched the specified instrument_ids: %s. Processed %s instruments but none matched.",
                instrument_ids_list,
                len(all_instruments),
            )

        return filtered_instruments

    def convert_to_dataframe(self, all_instruments: dict[str, InstrumentDefinition], date: datetime) -> pd.DataFrame:
        """Convert instruments dict to DataFrame."""
        instruments_list: list[dict[str, object]] = []
        for inst_key, inst_obj in all_instruments.items():
            _ = inst_key
            if hasattr(inst_obj, "model_dump"):
                instruments_list.append(cast(dict[str, object], inst_obj.model_dump()))
            else:
                instruments_list.append(cast(dict[str, object], inst_obj))

        instruments_df = pd.DataFrame(instruments_list)

        # Add market category if missing
        if not instruments_df.empty:
            self._add_market_category(instruments_df)

        return instruments_df

    def _add_market_category(self, instruments_df: pd.DataFrame) -> None:
        """Add market category to instruments DataFrame."""
        from unified_trading_library import determine_market_category  # noqa: domain-ucs

        def _row_to_market_category(row: pd.Series[object]) -> str:
            raw = cast(dict[str, object], row.to_dict())
            row_dict: dict[str, object] = {}
            for k, v in raw.items():
                # Use str(v) for non-None values; None stays None for market category logic
                row_dict[str(k)] = str(v) if v is not None else None
            return determine_market_category(row_dict)

        if "market_category" not in instruments_df.columns:
            instruments_df["market_category"] = ""
        mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
        if mask.any():
            instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                _row_to_market_category, axis=1
            )

    def handle_no_instruments(self, date: datetime, error_warning_counter: ErrorWarningCounter) -> dict[str, object]:
        """Handle case where no instruments generated."""
        date_str = date.strftime("%Y-%m-%d")
        logger.error(
            "❌ No instruments generated for %s. This is unexpected - check adapter/API connectivity.", date_str
        )

        return {
            "success": False,
            "count": 0,
            "date": date_str,
            "message": "No instruments generated - check adapter/API connectivity",
            "error_count": error_warning_counter.error_count,
            "warning_count": error_warning_counter.warning_count,
        }

    def add_tradfi_placeholders(self, instruments_df: pd.DataFrame) -> pd.DataFrame:
        """Add placeholders for missing TradFi venues if needed."""
        # This is a simplified version - could be expanded based on specific requirements
        # The original implementation was quite complex and venue-specific
        return instruments_df

    def handle_utc_midnight_spanning(self, instruments_df: pd.DataFrame, date: datetime) -> None:
        """Handle UTC midnight spanning for CME and ICE."""
        if instruments_df.empty:
            return

        utc_spanning_instruments: list[pd.Series] = []
        file_date: date_type = date.date() if isinstance(date, datetime) else date

        for idx, row in instruments_df.iterrows():
            _ = idx
            if row.get("regular_open_utc") and row.get("regular_close_utc"):
                try:
                    _open_raw: str = str(cast(object, row["regular_open_utc"]))
                    _close_raw: str = str(cast(object, row["regular_close_utc"]))
                    open_dt = cast(pd.Timestamp, pd.to_datetime(_open_raw))
                    close_dt = cast(pd.Timestamp, pd.to_datetime(_close_raw))

                    if open_dt.date() != file_date and close_dt.date() == file_date:
                        utc_spanning_instruments.append(row)
                        logger.debug(
                            "UTC midnight spanning: %s opens %s closes %s",
                            cast(object, row["instrument_key"]),
                            open_dt.date(),
                            close_dt.date(),
                        )
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
            logger.info(
                "🌙 Found %s UTC-spanning instruments (session spans midnight for %s)",
                len(utc_spanning_instruments),
                file_date,
            )
            # Additional processing could be added here if needed
