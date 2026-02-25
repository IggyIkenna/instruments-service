"""
Instrument Utilities Module

Contains helper functions for venue filtering, data conversion, and validation used by the orchestrator.
"""

import logging
from datetime import date as date_type
from datetime import datetime
from typing import Any, cast

import pandas as pd
from unified_market_interface import InstrumentDefinition, VenueMapping

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
            logger.info(f"🔍 Extracted venues from instrument_ids: {extracted_venues}")
            logger.info(f"🔍 Combined venues filter: {combined_venues}")
            return combined_venues

        return venues_filter

    def validate_venues(self, venues_filter: list[str], cefi: bool, tradfi: bool, defi: bool) -> dict[str, Any] | None:
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
            logger.error(f"❌ {error_msg}")
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

    def convert_to_dataframe(self, all_instruments: dict[str, InstrumentDefinition], date: datetime) -> pd.DataFrame:
        """Convert instruments dict to DataFrame."""
        instruments_list: list[dict[str, Any]] = []
        for inst_key, inst_obj in all_instruments.items():
            _ = inst_key
            if hasattr(inst_obj, "model_dump"):
                instruments_list.append(cast(dict[str, Any], inst_obj.model_dump()))
            else:
                instruments_list.append(cast(dict[str, Any], inst_obj))

        instruments_df = pd.DataFrame(instruments_list)

        # Add market category if missing
        if not instruments_df.empty:
            self._add_market_category(instruments_df)

        return instruments_df

    def _add_market_category(self, instruments_df: pd.DataFrame) -> None:
        """Add market category to instruments DataFrame."""
        from unified_cloud_services import determine_market_category

        def _row_to_market_category(row: pd.Series) -> str:  # type: ignore[type-arg]
            raw = cast(dict[str, Any], row.to_dict())
            row_dict: dict[str, str | None] = {}
            for k, v in raw.items():  # type: ignore[reportAny]
                row_dict[str(k)] = str(v) if pd.notna(v) else None  # type: ignore[reportAny]
            return determine_market_category(row_dict)  # type: ignore[arg-type]

        if "market_category" not in instruments_df.columns:
            instruments_df["market_category"] = ""
        mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
        if mask.any():
            instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                _row_to_market_category, axis=1
            )

    def handle_no_instruments(self, date: datetime, error_warning_counter: ErrorWarningCounter) -> dict[str, Any]:
        """Handle case where no instruments generated."""
        date_str = date.strftime("%Y-%m-%d")
        logger.error(
            f"❌ No instruments generated for {date_str}. This is unexpected - check adapter/API connectivity."
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
                    open_dt = cast(pd.Timestamp, pd.to_datetime(row["regular_open_utc"]))  # type: ignore[arg-type]
                    close_dt = cast(pd.Timestamp, pd.to_datetime(row["regular_close_utc"]))  # type: ignore[arg-type]

                    if open_dt.date() != file_date and close_dt.date() == file_date:
                        utc_spanning_instruments.append(row)
                        logger.debug(
                            f"🌙 UTC midnight spanning: {row['instrument_key']} "
                            f"opens {open_dt.date()} closes {close_dt.date()}"
                        )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to parse session times for {row.get('instrument_key')}: {e}")

        if utc_spanning_instruments:
            logger.info(
                f"🌙 Found {len(utc_spanning_instruments)} UTC-spanning instruments "
                f"(session spans midnight for {file_date})"
            )
            # Additional processing could be added here if needed
