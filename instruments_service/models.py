"""
Data Models for Instruments Service

This module defines all data models following the INSTRUMENT_KEY_SPEC.md format
with proper expiry, call/put, and margin currency support.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator, model_validator
import logging
import pandas as pd

# Import shared enums from unified-cloud-services
from unified_cloud_services.models.instrument import (
    Venue,
    InstrumentType,
)

logger = logging.getLogger(__name__)


@dataclass
class InstrumentKey:
    """Instrument key following venue:instrument_type:symbol format"""

    venue: Venue
    instrument_type: InstrumentType
    symbol: str
    expiry: Optional[str] = None  # For futures/options
    option_type: Optional[str] = None  # C or P for options

    def __str__(self) -> str:
        """Format: venue:type:symbol:expiry:option_type"""
        parts = [self.venue.value, self.instrument_type.value, self.symbol]
        if self.expiry:
            parts.append(self.expiry)
        if self.option_type:
            parts.append(self.option_type)
        return ":".join(parts)

    @classmethod
    def from_string(cls, instrument_key_str: str) -> "InstrumentKey":
        """Parse instrument key from string"""
        parts = instrument_key_str.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid instrument key format: {instrument_key_str}")

        venue = Venue(parts[0])
        instrument_type = InstrumentType(parts[1])
        symbol = parts[2]
        expiry = parts[3] if len(parts) > 3 else None
        option_type = parts[4] if len(parts) > 4 else None

        return cls(
            venue=venue,
            instrument_type=instrument_type,
            symbol=symbol,
            expiry=expiry,
            option_type=option_type,
        )


class InstrumentDefinition(BaseModel):
    """
    Comprehensive Pydantic model for instrument definitions

    This model validates all fields that are generated and uploaded to GCS
    as instrument definitions, ensuring data integrity and schema compliance.
    """

    # Core identification fields
    instrument_key: str = Field(
        ...,
        description="Canonical instrument key in format VENUE:INSTRUMENT_TYPE:SYMBOL",
    )
    venue: str = Field(..., description="Venue identifier (e.g., BINANCE, DERIBIT)")
    instrument_type: str = Field(
        ..., description="Instrument type (e.g., SPOT_PAIR, PERPETUAL, FUTURE, OPTION)"
    )
    symbol: str = Field(
        ...,
        description="Symbol (e.g., BTC-USDT, ETH-USDT@LIN, BTC-USD@INV, BTC-USDT-20250101-50000-CALL)",
    )

    # Required fields with defaults (CORRECTED per INSTRUMENT_KEY.md)
    venue_type: str = Field(
        default="exchange", description="Type of venue: exchange, protocol, or wallet"
    )
    tardis_exchange: str = Field(default="", description="Tardis exchange identifier")
    data_provider: str = Field(default="tardis", description="Data provider source")
    asset_class: str = Field(default="crypto", description="Asset class classification")

    # Availability windows (CORRECTED - allow empty for SPOT/PERPETUAL)
    available_from_datetime: str = Field(
        ..., description="ISO datetime string when instrument became available"
    )
    available_to_datetime: Optional[str] = Field(
        default=None,
        description="ISO datetime string when instrument expires (empty for SPOT/PERPETUAL)",
    )

    # Data types available for this instrument
    data_types: str = Field(
        default="trades,book_snapshot_5",
        description="Comma-separated list of available data types",
    )

    # Asset information
    base_asset: str = Field(default="", description="Base asset symbol (e.g., BTC, ETH)")
    quote_asset: str = Field(default="", description="Quote asset symbol (e.g., USDT, USD)")
    settle_asset: str = Field(default="", description="Settlement asset symbol")

    # Exchange-specific identifiers
    exchange_raw_symbol: str = Field(
        default="",
        description="Raw exchange code from exchange API (e.g., '6A', '6E', 'ES', 'AAPL')",
    )
    databento_symbol: str = Field(
        default="",
        description="Databento query symbol format (e.g., '6A.FUT', 'ES.FUT', 'SPY', 'SPY.OPT')",
    )
    tardis_symbol: str = Field(default="", description="Symbol format used by Tardis API")

    # Trading parameters
    inverse: bool = Field(default=False, description="Whether this is an inverse contract")

    # Note: symbol_type field removed to avoid confusion with canonical instrument_type

    # Option-specific fields
    strike: str = Field(default="", description="Strike price for options")
    option_type: str = Field(default="", description="Option type (C for call, P for put)")

    # Contract-specific fields
    expiry: Optional[str] = Field(
        default=None, description="Expiry datetime for futures/options (ISO string)"
    )
    contract_size: Optional[float] = Field(default=None, description="Contract size/multiplier")
    tick_size: Optional[str] = Field(default="", description="Minimum price increment")
    underlying: Optional[str] = Field(default="", description="Underlying asset for derivatives")
    min_size: Optional[str] = Field(default="", description="Minimum order size")

    # CCXT integration fields
    ccxt_symbol: str = Field(default="", description="Symbol format for CCXT library")
    ccxt_exchange: str = Field(default="", description="Exchange identifier for CCXT library")

    # DeFi-specific fields (for DEX pools and protocol tokens)
    chain: str = Field(
        default="off-chain",
        description="Chain identifier: 'off-chain' for CeFi/TradFi, chain name for DeFi (e.g., 'ETHEREUM', 'POLKADOT', 'HYPERLIQUID')",
    )
    base_asset_contract_address: Optional[str] = Field(
        default=None, description="ERC-20 contract address for base asset (DeFi)"
    )
    quote_asset_contract_address: Optional[str] = Field(
        default=None, description="ERC-20 contract address for quote asset (DeFi)"
    )
    pool_address: Optional[str] = Field(
        default=None,
        description="Pool contract address (for DEX pairs, computed from tokens + fee)",
    )
    pool_fee_tier: Optional[int] = Field(
        default=None,
        description="Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%)",
    )

    # Lending protocol-specific fields (for AAVE, Morpho, Plasma protocols)
    flash_loan_providers: Optional[str] = Field(
        default=None,
        description="Comma-separated list of flash loan provider addresses (for lending protocols)",
    )
    instadapp_routing: Optional[str] = Field(
        default=None,
        description="Instadapp routing configuration for this reserve (if applicable)",
    )
    ltv: Optional[float] = Field(
        default=None,
        description="Loan-to-Value ratio (as decimal, e.g., 0.75 = 75%) - maximum borrowing power against collateral",
    )
    liquidation_threshold: Optional[float] = Field(
        default=None,
        description="Liquidation threshold (as decimal, e.g., 0.80 = 80%) - price at which position becomes liquidatable",
    )
    liquidation_bonus: Optional[float] = Field(
        default=None,
        description="Liquidation bonus (as decimal, e.g., 0.05 = 5%) - bonus paid to liquidators",
    )
    reserve_factor: Optional[float] = Field(
        default=None,
        description="Reserve factor (as decimal, e.g., 0.10 = 10%) - portion of interest reserved for protocol",
    )
    emode_category_id: Optional[int] = Field(
        default=None,
        description="E-mode category ID (for AAVE e-mode - efficient mode for correlated assets)",
    )
    emode_label: Optional[str] = Field(
        default=None,
        description="E-mode category label (e.g., 'Stablecoins', 'ETH correlated')",
    )
    emode_underlying: Optional[str] = Field(
        default=None,
        description="E-mode underlying asset symbol (for e-mode category)",
    )
    emode_liquidation_threshold: Optional[float] = Field(
        default=None,
        description="E-mode liquidation threshold (as decimal) - higher threshold when in e-mode",
    )
    emode_liquidation_bonus: Optional[float] = Field(
        default=None,
        description="E-mode liquidation bonus (as decimal) - bonus when in e-mode",
    )
    optimal_utilization_rate: Optional[float] = Field(
        default=None,
        description="Optimal utilization rate (as decimal, e.g., 0.80 = 80%) - utilization rate where interest rate model changes slope",
    )
    base_variable_borrow_rate: Optional[float] = Field(
        default=None,
        description="Base variable borrow rate (as decimal, e.g., 0.01 = 1%) - minimum borrow rate at 0% utilization",
    )
    variable_rate_slope1: Optional[float] = Field(
        default=None,
        description="Variable rate slope 1 (as decimal) - interest rate increase per utilization below optimal",
    )
    variable_rate_slope2: Optional[float] = Field(
        default=None,
        description="Variable rate slope 2 (as decimal) - interest rate increase per utilization above optimal",
    )

    # CEFI risk parameters (from CCXT leverage tiers)
    max_position_size: Optional[float] = Field(
        default=None,
        description="Maximum position size in quote currency (from highest tier's maxNotional)",
    )
    max_leverage: Optional[float] = Field(
        default=None,
        description="Maximum leverage available (from tier 1, highest leverage tier)",
    )
    initial_margin_rate: Optional[float] = Field(
        default=None,
        description="Initial margin rate required to open position (from tier 1, as decimal e.g., 0.01 = 1%)",
    )
    maintenance_margin_rate: Optional[float] = Field(
        default=None,
        description="Maintenance margin rate (liquidation threshold, from tier 1, as decimal e.g., 0.005 = 0.5%)",
    )
    leverage_tiers_json: Optional[str] = Field(
        default=None,
        description="JSON string of all leverage tiers for this instrument (for advanced risk calculations)",
    )

    # Note: validation_warnings removed to avoid circular reference issues

    @field_validator("instrument_key")
    @classmethod
    def validate_instrument_key(cls, v):
        """Validate instrument key format"""
        if not v or ":" not in v:
            raise ValueError(f"Invalid instrument key format: {v}")

        parts = v.split(":")
        if len(parts) < 3:
            raise ValueError(f"Instrument key must have at least 3 parts: {v}")

        # Validate venue (first part)
        venue = parts[0]
        valid_venues = [v.value for v in Venue]
        if venue not in valid_venues:
            # Warning will be collected in model_validator
            pass

        # Validate instrument type (second part)
        instrument_type = parts[1]
        valid_types = [t.value for t in InstrumentType]
        if instrument_type not in valid_types:
            # Warning will be collected in model_validator
            pass

        return v

    @field_validator("available_from_datetime")
    @classmethod
    def validate_from_datetime(cls, v):
        """Validate available_from_datetime - always required"""
        if not v:
            raise ValueError("available_from_datetime is required and cannot be empty")

        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Invalid ISO datetime format: {v}")

        return v

    @field_validator("available_to_datetime")
    @classmethod
    def validate_to_datetime(cls, v):
        """Validate available_to_datetime - optional for SPOT/PERPETUAL instruments"""
        # Allow None for perpetual instruments (no expiry)
        if v is None:
            return v

        # Allow empty string and convert to None
        if isinstance(v, str) and v.strip() == "":
            return None

        # Validate non-empty datetime strings
        if v:
            try:
                datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError(f"Invalid ISO datetime format: {v}")

        return v

    @field_validator("data_types")
    @classmethod
    def validate_data_types(cls, v):
        """Validate data types string"""
        if not v:
            raise ValueError("Data types cannot be empty")

        valid_data_types = [
            "trades",
            "book_snapshot_5",
            "derivative_ticker",
            "options_chain",
            "liquidations",
            "quotes",  # TradFi quotes (Databento) - note: actual fetching uses OHLCV for cost efficiency
            "ohlcv_1m",  # 1-minute OHLCV candles (Databento TradFi)
        ]
        types = [t.strip() for t in v.split(",")]

        for data_type in types:
            if data_type not in valid_data_types:
                # Warning will be collected in model_validator
                pass

        return v

    @field_validator("expiry")
    @classmethod
    def validate_expiry(cls, v):
        """Validate expiry datetime for futures/options"""
        if v is None or v == "":
            return v

        # Handle datetime objects (convert to ISO string)
        if isinstance(v, (datetime, pd.Timestamp)):
            return v.isoformat()

        # Handle string datetime
        if isinstance(v, str):
            try:
                datetime.fromisoformat(v.replace("Z", "+00:00"))
                return v
            except ValueError:
                raise ValueError(f"Invalid expiry datetime format: {v}")

        return v

    @field_validator("option_type")
    @classmethod
    def validate_option_type(cls, v):
        """Validate option type"""
        if v and v not in ["CALL", "PUT"]:
            raise ValueError(f"Invalid option type: {v}. Must be CALL or PUT")
        return v

    @field_validator("inverse")
    @classmethod
    def validate_inverse(cls, v):
        """Validate inverse field"""
        if not isinstance(v, bool):
            raise ValueError(f"Inverse must be boolean, got: {type(v)}")
        return v

    @field_validator("tick_size", "min_size", "underlying", "ccxt_symbol", "ccxt_exchange")
    @classmethod
    def validate_optional_strings(cls, v):
        """Validate optional string fields - convert None to empty string"""
        if v is None:
            return ""
        return str(v)

    @field_validator("contract_size", mode="before")
    @classmethod
    def validate_contract_size(cls, v):
        """Validate contract_size field - convert empty strings to None BEFORE type conversion"""
        # Convert empty string to None for optional float field
        if v == "" or v is None:
            return None

        # Try to convert to float
        if isinstance(v, str):
            try:
                # Strip whitespace first
                v_stripped = v.strip()
                if not v_stripped:
                    return None

                return float(v_stripped)
            except ValueError:
                raise ValueError(f"Invalid contract_size format: {v}")

        return v

    @model_validator(mode="after")
    def validate_instrument_consistency(self):
        """Validate overall instrument consistency"""
        # Check instrument key components
        if self.instrument_key and ":" in self.instrument_key:
            parts = self.instrument_key.split(":")
            if len(parts) >= 2:
                venue = parts[0]
                instrument_type = parts[1]

                # Check venue
                valid_venues = [v.value for v in Venue]
                if venue not in valid_venues:
                    logger.warning(f"Unknown venue in instrument key: {venue}")

                # Check instrument type
                valid_types = [t.value for t in InstrumentType]
                if instrument_type not in valid_types:
                    logger.warning(f"Unknown instrument type in instrument key: {instrument_type}")

        # Check data types
        if self.data_types:
            valid_data_types = [
                "trades",
                "book_snapshot_5",
                "derivative_ticker",
                "options_chain",
                "liquidations",
                "quotes",  # TradFi quotes (Databento) - note: actual fetching uses OHLCV for cost efficiency
                "ohlcv_1m",  # 1-minute OHLCV candles (Databento TradFi, Hyperliquid, Aster)
                "ohlcv_1h",  # 1-hour OHLCV candles (fallback for Hyperliquid, Aster)
            ]
            types = [t.strip() for t in self.data_types.split(",")]
            for data_type in types:
                if data_type not in valid_data_types:
                    logger.warning(f"Unknown data type: {data_type}")

        # Futures and options should have expiry - extract from instrument_key if missing
        if self.instrument_type in ["FUTURE", "OPTION"] and not self.expiry:
            extracted_expiry = self._extract_expiry_from_key()
            if extracted_expiry:
                self.expiry = extracted_expiry
            else:
                logger.warning(f"Futures/options should have expiry: {self.instrument_key}")

        # Options should have strike and option_type - extract from instrument_key if missing
        if self.instrument_type == "OPTION":
            # Extract strike and option_type from canonical key if missing
            if not self.strike or not self.option_type:
                extracted_strike, extracted_option_type = self._extract_option_info_from_key()

                if not self.strike and extracted_strike:
                    self.strike = extracted_strike

                if not self.option_type and extracted_option_type:
                    self.option_type = extracted_option_type

            # Still warn if we couldn't extract the information
            if not self.strike:
                logger.warning(f"Options should have strike price: {self.instrument_key}")
            if not self.option_type:
                logger.warning(f"Options should have option type: {self.instrument_key}")

        return self

    def _extract_option_info_from_key(self) -> tuple[str, str]:
        """Extract strike price and option type from canonical instrument key and symbol field.

        Expected format: VENUE:OPTION:BASE-QUOTE-EXPIRY-STRIKE-TYPE
        Example: DERIBIT:OPTION:ETH-USDC-251027-3500-CALL

        Also tries parsing from self.symbol field as backup source.

        Returns:
            tuple: (strike_price, option_type) or ("", "") if extraction fails
        """
        strike_price = ""
        option_type = ""

        # Try parsing from instrument_key first
        try:
            if self.instrument_key and ":" in self.instrument_key:
                parts = self.instrument_key.split(":")
                if len(parts) >= 3:
                    # Get the symbol part (third part): ETH-USDC-251027-3500-CALL
                    key_symbol = parts[2]
                    strike_price, option_type = self._parse_option_from_symbol_string(key_symbol)
        except Exception as e:
            logger.debug(
                f"Failed to extract option info from instrument_key {self.instrument_key}: {e}"
            )

        # If not found, try parsing from symbol field as backup
        if (not strike_price or not option_type) and hasattr(self, "symbol") and self.symbol:
            try:
                backup_strike, backup_option_type = self._parse_option_from_symbol_string(
                    self.symbol
                )
                strike_price = strike_price or backup_strike
                option_type = option_type or backup_option_type
            except Exception as e:
                logger.debug(f"Failed to extract option info from symbol {self.symbol}: {e}")

        return strike_price, option_type

    def _parse_option_from_symbol_string(self, symbol_string: str) -> tuple[str, str]:
        """Parse strike price and option type from a symbol string.

        Args:
            symbol_string: Symbol string to parse (e.g., "ETH-USDC-251027-3500-CALL", "BTC-USD-240329-120000-CALL")

        Returns:
            tuple: (strike_price, option_type) or ("", "") if parsing fails
        """
        try:
            # Split by dashes and look for strike and option type at the end
            symbol_parts = symbol_string.split("-")

            if len(symbol_parts) < 4:
                return "", ""

            # For options, expect: BASE-QUOTE-EXPIRY-STRIKE-TYPE@LIN or BASE-QUOTE-EXPIRY-STRIKE-TYPE@INV
            # Handle various formats:
            # - TRX-USDC-251026-0.304-PUT@LIN
            # - BTC-USD-240329-120000-CALL@INV
            # - ETH-25DEC25-3500-C@LIN (Deribit short format)
            option_type = ""
            strike_price = ""

            # Check last part for option type (may have @LIN/@INV suffix)
            last_part = symbol_parts[-1].upper()
            # Remove @LIN or @INV suffix if present
            if "@" in last_part:
                last_part = last_part.split("@")[0]

            if last_part in ["CALL", "PUT", "C", "P"]:
                option_type = "CALL" if last_part in ["CALL", "C"] else "PUT"

                # Find strike price - look backwards from option type for numeric value
                # Enhanced logic to handle different strike price formats use context7
                for i in range(
                    len(symbol_parts) - 2, 1, -1
                ):  # Work backwards, skip option type and stop before base-quote
                    potential_strike = symbol_parts[i]

                    # Skip date parts (various formats)
                    # YYMMDD: 240329 (6 digits)
                    # DDMMMYY: 25DEC25 (contains letters)
                    if len(potential_strike) == 6 and potential_strike.isdigit():
                        continue
                    if any(char.isalpha() for char in potential_strike):
                        continue

                    # Try to validate as strike price (numeric, including decimals)
                    try:
                        # Handle various strike formats:
                        # - Regular numbers: 3500, 120000
                        # - Decimals: 0.304, 1.25
                        # - K notation: 50K, 100k
                        # - Deribit decimal format: 1d25 -> 1.25
                        test_value = potential_strike

                        # Convert Deribit decimal notation (1d25 -> 1.25)
                        if "d" in test_value.lower():
                            test_value = test_value.lower().replace("d", ".")

                        # Convert K notation
                        if test_value.upper().endswith("K"):
                            test_value = test_value[:-1] + "000"

                        # Validate as numeric
                        float(test_value)

                        # Use the converted value as strike price
                        if "d" in potential_strike.lower():
                            strike_price = potential_strike.lower().replace("d", ".")
                        elif potential_strike.upper().endswith("K"):
                            strike_price = potential_strike[:-1] + "000"
                        else:
                            strike_price = potential_strike

                        break  # Found valid strike, stop looking

                    except ValueError:
                        # Not a valid strike price, continue looking
                        continue

            return strike_price, option_type

        except Exception as e:
            logger.debug(f"Failed to parse option info from symbol string {symbol_string}: {e}")
            return "", ""

    def _extract_expiry_from_key(self) -> str:
        """Extract expiry date from canonical instrument key.

        Expected formats:
        - FUTURES: VENUE:FUTURE:BASE-QUOTE-YYMMDD (e.g., BINANCE-FUTURES:FUTURE:BTC-USDT-260327)
        - OPTIONS: VENUE:OPTION:BASE-QUOTE-YYMMDD-STRIKE-TYPE (e.g., DERIBIT:OPTION:ETH-USDC-251027-3500-CALL)

        Returns:
            str: ISO datetime string (e.g., "2026-03-27T08:00:00Z") or "" if extraction fails
        """
        try:
            if not self.instrument_key or ":" not in self.instrument_key:
                return ""

            parts = self.instrument_key.split(":")
            if len(parts) < 3:
                return ""

            # Get the symbol part (third part)
            symbol = parts[2]
            symbol_parts = symbol.split("-")

            if len(symbol_parts) < 3:
                return ""

            # Look for YYMMDD pattern in different positions based on instrument type
            expiry_date = ""

            if self.instrument_type == "FUTURE":
                # For futures: BASE-QUOTE-YYMMDD@LIN or BASE-QUOTE-YYMMDD@INV (e.g., BTC-USDT-260327@LIN)
                # Check if last part has @LIN or @INV suffix
                if len(symbol_parts) >= 3:
                    potential_date = symbol_parts[-1]  # Could be YYMMDD or YYMMDD@LIN/YYMMDD@INV
                    # Remove @LIN or @INV suffix if present
                    if "@" in potential_date:
                        potential_date = potential_date.split("@")[0]
                    expiry_date = self._parse_yymmdd_to_iso(potential_date)

            elif self.instrument_type == "OPTION":
                # For options: BASE-QUOTE-YYMMDD-STRIKE-TYPE@LIN or BASE-QUOTE-YYMMDD-STRIKE-TYPE@INV
                # Handle decimal strikes: TRX-USDC-251026-0.304-PUT@LIN (strike has decimal)
                if len(symbol_parts) >= 4:
                    # Find the YYMMDD part - it should be the first 6-digit numeric part after base-quote
                    for i in range(2, len(symbol_parts)):  # Start from index 2 (after BASE-QUOTE)
                        part = symbol_parts[i]
                        # Remove @LIN or @INV suffix if present
                        if "@" in part:
                            part = part.split("@")[0]
                        if len(part) == 6 and part.isdigit():
                            potential_date = part
                            expiry_date = self._parse_yymmdd_to_iso(potential_date)
                            break

            return expiry_date

        except Exception as e:
            logger.debug(f"Failed to extract expiry from {self.instrument_key}: {e}")
            return ""

    def _parse_yymmdd_to_iso(self, date_str: str) -> str:
        """Parse YYMMDD format to ISO datetime string.

        Args:
            date_str: Date in YYMMDD format (e.g., "260327")

        Returns:
            str: ISO datetime string (e.g., "2026-03-27T08:00:00Z") or "" if parsing fails
        """
        try:
            if not date_str or len(date_str) != 6:
                return ""

            # Validate all digits
            if not date_str.isdigit():
                return ""

            # Parse YYMMDD
            yy = int(date_str[:2])
            mm = int(date_str[2:4])
            dd = int(date_str[4:6])

            # Convert YY to full year (assuming 20XX for now, could be adjusted)
            # 00-49 -> 2000-2049, 50-99 -> 2050-2099 (reasonable for crypto derivatives)
            if yy <= 49:
                yyyy = 2000 + yy
            else:
                yyyy = 1900 + yy  # This handles 50-99 as 1950-1999, but for crypto likely 2050+
                if yyyy < 2020:  # Adjust for crypto context - assume future dates
                    yyyy += 100  # 50-99 becomes 2050-2099

            # Validate month and day ranges
            if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                return ""

            # Format as ISO datetime (8am UTC - common expiry time)
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}T08:00:00Z"

        except Exception as e:
            logger.debug(f"Failed to parse date {date_str}: {e}")
            return ""

    class Config:
        """Pydantic configuration"""

        validate_assignment = True
        use_enum_values = True
        extra = "ignore"  # Allow extra fields to prevent validation errors
        json_encoders = {datetime: lambda v: v.isoformat(), Decimal: lambda v: float(v)}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation"""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InstrumentDefinition":
        """Create from dictionary (e.g., from DataFrame row)"""
        return cls(**data)

    def validate_required_fields(self) -> List[str]:
        """Validate required fields and return list of missing fields"""
        missing_fields = []

        # Check required string fields
        required_string_fields = [
            "instrument_key",
            "venue",
            "instrument_type",
            "available_from_datetime",
            "available_to_datetime",
            "data_types",
            "base_asset",
            "quote_asset",
            "settle_asset",
            "exchange_raw_symbol",
            "databento_symbol",
            "tardis_symbol",
            "tardis_exchange",
            "data_provider",
            "venue_type",
            "asset_class",
        ]

        for field in required_string_fields:
            value = getattr(self, field, None)
            if not value or (isinstance(value, str) and value.strip() == ""):
                missing_fields.append(field)

        return missing_fields
