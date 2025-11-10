"""
Validation Service

Service-specific validation logic for instruments.
Follows unified repository structure pattern (Layer 3 validation).
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ValidationService:
    """
    Service-specific validation for instruments.

    Provides Layer 3 validation (service-specific business rules)
    beyond schema validation (Layer 1) and domain validation (Layer 2).
    """

    def __init__(self):
        """Initialize validation service."""
        logger.debug("✅ ValidationService initialized")

    def validate_instrument_definition(
        self, instrument: Dict[str, Any]
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a single instrument definition.

        Args:
            instrument: Instrument definition dictionary

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Required fields
        required_fields = ["instrument_key", "venue", "instrument_type", "symbol"]
        for field in required_fields:
            if field not in instrument or not instrument[field]:
                return False, f"Missing required field: {field}"

        # Validate instrument_key format
        instrument_key = instrument["instrument_key"]
        if not self._validate_instrument_key_format(instrument_key):
            return False, f"Invalid instrument_key format: {instrument_key}"

        # Validate venue matches instrument_key
        venue = instrument["venue"]
        if not instrument_key.startswith(venue):
            return (
                False,
                f"Venue mismatch: instrument_key starts with {instrument_key.split(':')[0]}, but venue is {venue}",
            )

        # Validate instrument_type matches instrument_key
        instrument_type = instrument["instrument_type"]
        key_parts = instrument_key.split(":")
        if len(key_parts) < 2 or key_parts[1] != instrument_type:
            return (
                False,
                f"Instrument type mismatch: instrument_key has {key_parts[1] if len(key_parts) > 1 else 'unknown'}, but instrument_type is {instrument_type}",
            )

        # Validate symbol format based on instrument type
        symbol = instrument["symbol"]
        if not self._validate_symbol_format(symbol, instrument_type):
            return False, f"Invalid symbol format for {instrument_type}: {symbol}"

        return True, None

    def validate_instruments_dataframe(
        self, df: pd.DataFrame
    ) -> tuple[bool, List[str]]:
        """
        Validate a DataFrame of instruments.

        Args:
            df: DataFrame with instrument definitions

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        if df.empty:
            errors.append("DataFrame is empty")
            return False, errors

        # Check required columns
        required_columns = ["instrument_key", "venue", "instrument_type", "symbol"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Validate each instrument
        for idx, row in df.iterrows():
            instrument = row.to_dict()
            is_valid, error = self.validate_instrument_definition(instrument)
            if not is_valid:
                errors.append(f"Row {idx}: {error}")

        # Check for duplicate instrument_keys
        duplicates = df[df.duplicated(subset=["instrument_key"], keep=False)]
        if not duplicates.empty:
            duplicate_keys = duplicates["instrument_key"].unique().tolist()
            errors.append(
                f"Duplicate instrument_keys found: {duplicate_keys[:10]}"
            )  # Limit to first 10

        is_valid = len(errors) == 0
        return is_valid, errors

    def _validate_instrument_key_format(self, instrument_key: str) -> bool:
        """
        Validate instrument key format.

        Format: VENUE:INSTRUMENT_TYPE:SYMBOL[@CHAIN]
        """
        if not instrument_key or not isinstance(instrument_key, str):
            return False

        parts = instrument_key.split(":")
        if len(parts) < 3:
            return False

        # Check venue format (UPPERCASE with dashes)
        venue = parts[0]
        if not venue.isupper() or not venue.replace("-", "").replace("_", "").isalnum():
            return False

        # Check instrument_type format (UPPERCASE with underscores)
        instrument_type = parts[1]
        if (
            not instrument_type.isupper()
            or not instrument_type.replace("_", "").isalnum()
        ):
            return False

        # Symbol should not be empty
        symbol = parts[2]
        if not symbol:
            return False

        return True

    def _validate_symbol_format(self, symbol: str, instrument_type: str) -> bool:
        """
        Validate symbol format based on instrument type.

        Args:
            symbol: Symbol string
            instrument_type: Instrument type (SPOT_PAIR, PERPETUAL, FUTURE, OPTION, etc.)

        Returns:
            True if symbol format is valid for the instrument type
        """
        if not symbol or not isinstance(symbol, str):
            return False

        if instrument_type in ["SPOT_PAIR", "PERPETUAL"]:
            # Format: BASE-QUOTE (e.g., BTC-USDT)
            if "-" not in symbol:
                return False
            parts = symbol.split("-")
            if len(parts) != 2:
                return False
            # Both parts should be non-empty
            if not parts[0] or not parts[1]:
                return False

        elif instrument_type == "FUTURE":
            # Format: BASE-QUOTE-YYMMDD or BASE-QUOTE:YYMMDD
            if "-" not in symbol:
                return False
            # Should have at least BASE-QUOTE
            parts = symbol.split("-")
            if len(parts) < 2:
                return False

        elif instrument_type == "OPTION":
            # Format: BASE-QUOTE:YYMMDD:STRIKE:OPTION_TYPE
            if "-" not in symbol or ":" not in symbol:
                return False
            # Should have multiple components
            if symbol.count(":") < 2:
                return False

        elif instrument_type == "SPOT_ASSET":
            # Format: ASSET (e.g., BTC, ETH)
            # Should be a single asset code
            if "-" in symbol or ":" in symbol:
                return False

        # All types: symbol should not be empty
        return bool(symbol.strip())

    def validate_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[bool, Optional[str]]:
        """
        Validate date range for processing.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Tuple of (is_valid, error_message)
        """
        if start_date > end_date:
            return (
                False,
                f"Start date {start_date.date()} must be <= end date {end_date.date()}",
            )

        # Check if dates are in the future (warn but allow)
        today = datetime.now().date()
        if start_date.date() > today:
            logger.warning(f"⚠️ Start date {start_date.date()} is in the future")

        return True, None
