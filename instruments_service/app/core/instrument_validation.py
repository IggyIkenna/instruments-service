"""
Instrument Validation — Venue Validation and Filtering Logic.

Extracted from instruments_service.py — contains venue validation against
allowed venues by market type, instrument ID extraction, and filtering.
"""

from __future__ import annotations

import logging
from typing import cast

logger = logging.getLogger(__name__)


class InstrumentValidationMixin:
    """
    Mixin providing venue validation and instrument filtering methods.

    Requires the host class to have:
        - self.venue_mapping: VenueMapping
    """

    def _validate_venues_filter(
        self,
        venues_filter: list[str],
        cefi: bool,
        tradfi: bool,
        defi: bool,
    ) -> dict[str, list[str]] | None:
        """
        Validate venues against allowed venues for each market type.

        Args:
            venues_filter: List of venue names to validate
            cefi: Whether CEFI processing is enabled
            tradfi: Whether TRADFI processing is enabled
            defi: Whether DEFI processing is enabled

        Returns:
            Dictionary mapping market type to valid venues, or None if no filter applied.
            Raises ValueError if invalid venues detected.
        """
        if not venues_filter:
            return None

        allowed_cefi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_cefi_venues))
        allowed_tradfi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_databento_venues))
        allowed_defi_venues: set[str] = set(cast(list[str], self.venue_mapping.all_defi_venues))

        # Determine which market types are being processed
        market_types_being_processed: list[str] = []
        if cefi:
            market_types_being_processed.append("CEFI")
        if tradfi:
            market_types_being_processed.append("TRADFI")
        if defi:
            market_types_being_processed.append("DEFI")

        # If no market types specified, all are processed
        if not market_types_being_processed:
            market_types_being_processed = ["CEFI", "TRADFI", "DEFI"]

        # Collect all allowed venues for the market types being processed
        allowed_venues_for_processing: set[str] = set()
        if "CEFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_cefi_venues)
        if "TRADFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_tradfi_venues)
        if "DEFI" in market_types_being_processed:
            allowed_venues_for_processing.update(allowed_defi_venues)

        # Validate each venue against allowed venues
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

        # Reject invalid venues with clear error message
        if invalid_venues:
            error_msg = (
                f"❌ Invalid venues for market types {market_types_being_processed}: {invalid_venues}\n"
                f"   Allowed CEFI venues: {sorted(allowed_cefi_venues)}\n"
                f"   Allowed TRADFI venues: {sorted(allowed_tradfi_venues)}\n"
                f"   Allowed DEFI venues: {sorted(allowed_defi_venues)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Log valid venues by market type
        for market_type, valid_venues in valid_venues_by_type.items():
            if valid_venues:
                logger.info("✅ Valid %s venues: %s", market_type, valid_venues)

        return valid_venues_by_type

    @staticmethod
    def _extract_venues_from_instrument_ids(
        instrument_ids: list[str] | str | None,
        venues_filter: list[str],
    ) -> list[str]:
        """
        Extract venue names from instrument IDs and merge with existing venue filter.

        Args:
            instrument_ids: List of instrument IDs (format: VENUE:TYPE:SYMBOL)
            venues_filter: Existing venue filter list

        Returns:
            Updated venues_filter list
        """
        if not instrument_ids:
            return venues_filter

        instrument_ids_list: list[str] = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
        venues_from_instrument_ids: set[str] = set()

        for inst_id in instrument_ids_list:
            parts = str(inst_id).split(":")
            if len(parts) >= 1:
                venue_from_id: str = parts[0].upper()
                venues_from_instrument_ids.add(venue_from_id)
                logger.debug("  Extracted venue '%s' from instrument_id: %s", venue_from_id, inst_id)

        if venues_from_instrument_ids:
            logger.info("🔍 Extracted venues from instrument_ids: %s", sorted(venues_from_instrument_ids))

            if venues_filter:
                venues_filter = [v for v in venues_filter if v in venues_from_instrument_ids]
                if not venues_filter:
                    logger.warning(
                        "⚠️ No matching venues between --venues and instrument_ids %s. Processing will be skipped.",
                        venues_from_instrument_ids,
                    )
            else:
                venues_filter = list(venues_from_instrument_ids)
                logger.info("🔍 Using venues from instrument_ids as venue filter: %s", venues_filter)

        return venues_filter

    @staticmethod
    def _filter_instruments_by_ids(
        all_instruments: dict,
        instrument_ids: list[str] | str | None,
    ) -> dict:
        """
        Filter instruments by specific instrument IDs.

        Args:
            all_instruments: Dictionary of all generated instruments
            instrument_ids: List of instrument IDs to keep

        Returns:
            Filtered dictionary of instruments
        """
        if not instrument_ids:
            return all_instruments

        instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
        instrument_ids_set: set[str] = {str(inst_id).upper() for inst_id in instrument_ids_list}

        filtered_instruments = {}
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
