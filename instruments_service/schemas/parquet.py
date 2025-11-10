"""
Parquet Schema Definition for Instruments Service

This module defines the expected schema for Parquet files stored in GCS.
These files contain instrument definitions (batch historical data).

Storage Location:
- GCS Bucket: `instruments-store` (or `instruments-store-test` for tests)
- Path Format: `instrument_availability/by_date/day-{YYYY-MM-DD}/instruments.parquet`
- Format: Parquet (with headers/column names)

Schema Source:
- Based on `InstrumentDefinition` Pydantic model (`models.py`)
- Additional fields added during storage (e.g., `timestamp`)
- All fields validated via Pydantic before storage

Validation Flow:
1. Pydantic model validation (InstrumentDefinition) - type checking, format validation
2. Storage-level checks (required columns, timestamp conversion)
3. Parquet file creation with implicit schema (column names + pandas dtypes)
"""

from typing import Dict, List, Optional
from datetime import datetime

# Parquet Schema Definition
# Maps to pandas DataFrame columns when stored to GCS

INSTRUMENTS_PARQUET_SCHEMA: List[Dict[str, str]] = [
    # ============================================================================
    # REQUIRED CORE FIELDS
    # ============================================================================
    {
        "name": "instrument_key",
        "type": "string",
        "required": True,
        "description": "Canonical instrument key in format VENUE:INSTRUMENT_TYPE:SYMBOL. Example: BINANCE-FUTURES:PERPETUAL:BTC-USDT",
    },
    {
        "name": "venue",
        "type": "string",
        "required": True,
        "description": "Venue identifier (e.g., BINANCE-FUTURES, DERIBIT, OKX). Must match first part of instrument_key",
    },
    {
        "name": "instrument_type",
        "type": "string",
        "required": True,
        "description": "Instrument type (e.g., SPOT_PAIR, PERPETUAL, FUTURE, OPTION, LST, A_TOKEN, DEBT_TOKEN). Must match second part of instrument_key",
    },
    {
        "name": "symbol",
        "type": "string",
        "required": True,
        "description": "Symbol string (e.g., BTC-USDT, ETH-USDT@LIN, BTC-USD@INV, BTC-USDT-20250101-50000-CALL). Extracted from instrument_key",
    },
    {
        "name": "available_from_datetime",
        "type": "datetime64[ns]",
        "required": True,
        "description": "ISO datetime string (timezone-naive UTC) when instrument became available. Always required.",
    },
    # ============================================================================
    # METADATA FIELDS (with defaults)
    # ============================================================================
    {
        "name": "venue_type",
        "type": "string",
        "required": False,
        "default": "exchange",
        "description": "Type of venue: 'exchange', 'protocol', or 'wallet'",
    },
    {
        "name": "tardis_exchange",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Tardis exchange identifier (e.g., 'binance', 'okex-swap', 'deribit')",
    },
    {
        "name": "data_provider",
        "type": "string",
        "required": False,
        "default": "tardis",
        "description": "Data provider source: 'tardis' or 'databento'",
    },
    {
        "name": "asset_class",
        "type": "string",
        "required": False,
        "default": "crypto",
        "description": "Asset class classification: 'crypto' or 'traditional'",
    },
    # ============================================================================
    # AVAILABILITY WINDOWS
    # ============================================================================
    {
        "name": "available_to_datetime",
        "type": "datetime64[ns]",
        "required": False,
        "nullable": True,
        "description": "ISO datetime string (timezone-naive UTC) when instrument expires. Empty/None for SPOT/PERPETUAL instruments.",
    },
    # ============================================================================
    # DATA TYPES
    # ============================================================================
    {
        "name": "data_types",
        "type": "string",
        "required": False,
        "default": "trades,book_snapshot_5",
        "description": "Comma-separated list of available data types: 'trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations'",
    },
    # ============================================================================
    # ASSET INFORMATION
    # ============================================================================
    {
        "name": "base_asset",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Base asset symbol (e.g., BTC, ETH, USDC)",
    },
    {
        "name": "quote_asset",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Quote asset symbol (e.g., USDT, USD, USDC)",
    },
    {
        "name": "settle_asset",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Settlement asset symbol (for derivatives)",
    },
    # ============================================================================
    # EXCHANGE-SPECIFIC IDENTIFIERS
    # ============================================================================
    {
        "name": "exchange_raw_symbol",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Raw exchange code from exchange API (e.g., '6A', '6E', 'ES', 'AAPL')",
    },
    {
        "name": "databento_symbol",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Databento query symbol format (e.g., '6A.FUT', 'ES.FUT', 'SPY', 'SPY.OPT')",
    },
    {
        "name": "tardis_symbol",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Symbol format used by Tardis API",
    },
    # ============================================================================
    # TRADING PARAMETERS
    # ============================================================================
    {
        "name": "inverse",
        "type": "bool",
        "required": False,
        "default": False,
        "description": "Whether this is an inverse contract (e.g., BTC-USD@INV)",
    },
    # ============================================================================
    # OPTION-SPECIFIC FIELDS
    # ============================================================================
    {
        "name": "strike",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Strike price for options (as string to support formats like '50000', '5K', '0.304')",
    },
    {
        "name": "option_type",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Option type: 'CALL' or 'PUT' (empty for non-options)",
    },
    # ============================================================================
    # CONTRACT-SPECIFIC FIELDS
    # ============================================================================
    {
        "name": "expiry",
        "type": "datetime64[ns]",
        "required": False,
        "nullable": True,
        "description": "Expiry datetime for futures/options (ISO string, timezone-naive UTC). None for SPOT/PERPETUAL.",
    },
    {
        "name": "contract_size",
        "type": "float64",
        "required": False,
        "nullable": True,
        "description": "Contract size/multiplier (e.g., 1.0, 0.1, 100)",
    },
    {
        "name": "tick_size",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Minimum price increment (as string, e.g., '0.01', '1', '0.0001')",
    },
    {
        "name": "underlying",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Underlying asset for derivatives (canonical reference or instrument_key)",
    },
    {
        "name": "min_size",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Minimum order size (as string, e.g., '0.001', '1')",
    },
    # ============================================================================
    # CCXT INTEGRATION FIELDS
    # ============================================================================
    {
        "name": "ccxt_symbol",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Symbol format for CCXT library (e.g., 'BTC/USDT:USDT' for perpetuals)",
    },
    {
        "name": "ccxt_exchange",
        "type": "string",
        "required": False,
        "default": "",
        "description": "Exchange identifier for CCXT library (e.g., 'binance', 'okx')",
    },
    # ============================================================================
    # DEFI-SPECIFIC FIELDS (for DEX pools and protocol tokens)
    # ============================================================================
    {
        "name": "base_asset_contract_address",
        "type": "string",
        "required": False,
        "nullable": True,
        "description": "ERC-20 contract address for base asset (DeFi instruments only)",
    },
    {
        "name": "quote_asset_contract_address",
        "type": "string",
        "required": False,
        "nullable": True,
        "description": "ERC-20 contract address for quote asset (DeFi instruments only)",
    },
    {
        "name": "pool_address",
        "type": "string",
        "required": False,
        "nullable": True,
        "description": "Pool contract address (for DEX pairs, computed from tokens + fee tier)",
    },
    {
        "name": "pool_fee_tier",
        "type": "int64",
        "required": False,
        "nullable": True,
        "description": "Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%). Only for DEX pools.",
    },
    # ============================================================================
    # STORAGE METADATA (added during storage)
    # ============================================================================
    {
        "name": "timestamp",
        "type": "datetime64[ns]",
        "required": True,
        "description": "Generation timestamp (timezone-naive UTC) when instrument definition was created/stored. Added automatically during storage.",
    },
]

# Schema metadata
SCHEMA_METADATA = {
    "version": "1.0",
    "last_updated": "2025-01-15",
    "source_model": "InstrumentDefinition (models.py)",
    "storage_format": "Parquet",
    "storage_location": "GCS",
    "validation": "Pydantic model validation + storage-level checks",
    "notes": [
        "All datetime fields are stored as timezone-naive UTC (datetime64[ns])",
        "String fields with defaults use empty string ('') not None",
        "Optional float fields (contract_size) can be None",
        "Optional datetime fields (expiry, available_to_datetime) can be None",
        "Parquet schema is implicit - derived from DataFrame column names and dtypes",
        "No explicit Parquet schema file is enforced - structure follows InstrumentDefinition model",
    ],
}


def get_required_columns() -> List[str]:
    """Get list of required column names."""
    return [
        field["name"]
        for field in INSTRUMENTS_PARQUET_SCHEMA
        if field.get("required", False)
    ]


def get_optional_columns() -> List[str]:
    """Get list of optional column names."""
    return [
        field["name"]
        for field in INSTRUMENTS_PARQUET_SCHEMA
        if not field.get("required", False)
    ]


def get_column_info(column_name: str) -> Optional[Dict[str, str]]:
    """Get schema information for a specific column."""
    for field in INSTRUMENTS_PARQUET_SCHEMA:
        if field["name"] == column_name:
            return field
    return None


def validate_schema_compliance(df_columns: List[str]) -> tuple[bool, List[str]]:
    """
    Validate that DataFrame columns match expected schema.

    Args:
        df_columns: List of column names from DataFrame

    Returns:
        Tuple of (is_valid, list_of_missing_required_columns)
    """
    required = set(get_required_columns())
    actual = set(df_columns)

    missing_required = list(required - actual)
    is_valid = len(missing_required) == 0

    return is_valid, missing_required


def get_schema_summary() -> Dict:
    """Get summary of schema structure."""
    return {
        "total_fields": len(INSTRUMENTS_PARQUET_SCHEMA),
        "required_fields": len(get_required_columns()),
        "optional_fields": len(get_optional_columns()),
        "required_column_names": get_required_columns(),
        "metadata": SCHEMA_METADATA,
    }
