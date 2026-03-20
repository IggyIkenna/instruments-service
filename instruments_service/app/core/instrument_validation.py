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

    def _resolve_allowed_venues(self) -> dict[str, set[str]]:
        """Return allowed venue sets keyed by market type."""
        return {
            "CEFI": set(cast(list[str], self.venue_mapping.all_cefi_venues)),
            "TRADFI": set(cast(list[str], self.venue_mapping.all_databento_venues)),
            "DEFI": set(cast(list[str], self.venue_mapping.all_defi_venues)),
        }

    @staticmethod
    def _determine_active_market_types(cefi: bool, tradfi: bool, defi: bool) -> list[str]:
        """Determine which market types are being processed."""
        market_types: list[str] = []
        if cefi:
            market_types.append("CEFI")
        if tradfi:
            market_types.append("TRADFI")
        if defi:
            market_types.append("DEFI")
        return market_types if market_types else ["CEFI", "TRADFI", "DEFI"]  # CORRECT-LOCAL

    @staticmethod
    def _classify_venues(
        venues_filter: list[str],
        allowed_by_type: dict[str, set[str]],
        active_market_types: list[str],
    ) -> tuple[dict[str, list[str]], list[str]]:
        """Classify venues into valid-by-type and invalid lists."""
        valid_venues_by_type: dict[str, list[str]] = {"CEFI": [], "TRADFI": [], "DEFI": []}  # CORRECT-LOCAL
        invalid_venues: list[str] = []

        for venue in venues_filter:
            venue_valid = False
            for mtype in active_market_types:
                if venue in allowed_by_type[mtype]:
                    valid_venues_by_type[mtype].append(venue)
                    venue_valid = True
            if not venue_valid:
                invalid_venues.append(venue)

        return valid_venues_by_type, invalid_venues

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

        allowed_by_type = self._resolve_allowed_venues()
        active_market_types = self._determine_active_market_types(cefi, tradfi, defi)

        valid_venues_by_type, invalid_venues = self._classify_venues(
            venues_filter, allowed_by_type, active_market_types
        )

        if invalid_venues:
            error_msg = (
                f"Invalid venues for market types {active_market_types}: {invalid_venues}\n"
                f"   Allowed CEFI venues: {sorted(allowed_by_type['CEFI'])}\n"
                f"   Allowed TRADFI venues: {sorted(allowed_by_type['TRADFI'])}\n"
                f"   Allowed DEFI venues: {sorted(allowed_by_type['DEFI'])}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        for market_type, valid_venues in valid_venues_by_type.items():
            if valid_venues:
                logger.info("Valid %s venues: %s", market_type, valid_venues)

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
            logger.info("Extracted venues from instrument_ids: %s", sorted(venues_from_instrument_ids))

            if venues_filter:
                venues_filter = [v for v in venues_filter if v in venues_from_instrument_ids]
                if not venues_filter:
                    logger.warning(
                        "No matching venues between --venues and instrument_ids %s. Processing will be skipped.",
                        venues_from_instrument_ids,
                    )
            else:
                venues_filter = list(venues_from_instrument_ids)
                logger.info("Using venues from instrument_ids as venue filter: %s", venues_filter)

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
                "Filtered %s instruments by instrument_ids, %s matching instruments remaining",
                filtered_count,
                len(filtered_instruments),
            )

        if filtered_instruments:
            logger.info("Matching instrument_ids: %s", list(filtered_instruments.keys()))
        else:
            logger.warning(
                "No instruments matched the specified instrument_ids: %s. Processed %s instruments but none matched.",
                instrument_ids_list,
                len(all_instruments),
            )

        return filtered_instruments
