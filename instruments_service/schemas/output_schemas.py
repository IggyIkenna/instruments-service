"""
Output Schema Definitions for Instruments Service

This module defines the strict schema for GCS parquet outputs using SchemaDefinition.
The schema includes dimension-aware nullability rules for different categories.

Categories:
- CEFI: Centralized exchange instruments (Tardis data)
- TRADFI: Traditional finance instruments (Databento data)
- DEFI: Decentralized finance instruments (on-chain data)

Usage:
    from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA
    from unified_cloud_services import ParquetSchemaEnforcer

    enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
    result = enforcer.validate_dataframe(df, {"category": "CEFI"})
"""

from unified_cloud_services import ColumnSchema, SchemaDefinition

# ==============================================================================
# INSTRUMENTS OUTPUT SCHEMA
# ==============================================================================
# Defines all columns with dimension-aware nullability rules
# Key dimensions: category (CEFI, TRADFI, DEFI)

INSTRUMENTS_SCHEMA = SchemaDefinition(
    name="instruments",
    version="1.0",
    description="Instrument definitions for all categories (CEFI, TRADFI, DEFI)",
    dimension_keys=["category"],
    columns=[
        # ==========================================================================
        # REQUIRED CORE FIELDS (always NOT NULL)
        # ==========================================================================
        ColumnSchema(
            name="instrument_key",
            dtype="string",
            nullable=False,
            description="Canonical instrument key: VENUE:INSTRUMENT_TYPE:SYMBOL",
        ),
        ColumnSchema(
            name="venue",
            dtype="string",
            nullable=False,
            description="Venue identifier (e.g., BINANCE-FUTURES, DERIBIT, CME)",
        ),
        ColumnSchema(
            name="instrument_type",
            dtype="string",
            nullable=False,
            description="Instrument type (SPOT_PAIR, PERPETUAL, FUTURE, OPTION, LST, A_TOKEN)",
        ),
        ColumnSchema(
            name="symbol",
            dtype="string",
            nullable=False,
            description="Symbol string extracted from instrument_key",
        ),
        ColumnSchema(
            name="available_from_datetime",
            dtype="datetime64[ns]",
            nullable=False,
            description="When instrument became available (timezone-naive UTC)",
        ),
        ColumnSchema(
            name="timestamp",
            dtype="datetime64[ns]",
            nullable=False,
            description="Generation timestamp when instrument definition was created/stored",
        ),
        # ==========================================================================
        # EXECUTION INSTRUCTION TYPE
        # ==========================================================================
        ColumnSchema(
            name="instruction_type",
            dtype="string",
            nullable=True,
            description="Instruction type for execution algorithm selection: TRADE (CLOB), SWAP (DEX), or ZERO_ALPHA (lending/staking)",
        ),
        # ==========================================================================
        # METADATA FIELDS (nullable by default, defaults exist)
        # ==========================================================================
        ColumnSchema(
            name="venue_type",
            dtype="string",
            nullable=True,
            description="Type of venue: 'exchange', 'protocol', or 'wallet'",
        ),
        ColumnSchema(
            name="data_provider",
            dtype="string",
            nullable=True,
            description="Data provider source: 'tardis' or 'databento'",
        ),
        ColumnSchema(
            name="asset_class",
            dtype="string",
            nullable=True,
            description="Asset class: 'crypto' or 'traditional'",
        ),
        ColumnSchema(
            name="data_types",
            dtype="string",
            nullable=True,
            description="Comma-separated list of available data types",
        ),
        # ==========================================================================
        # AVAILABILITY WINDOWS (nullable - perpetuals don't expire)
        # ==========================================================================
        ColumnSchema(
            name="available_to_datetime",
            dtype="datetime64[ns]",
            nullable=True,
            description="When instrument expires (None for SPOT/PERPETUAL)",
        ),
        # ==========================================================================
        # ASSET INFORMATION (nullable with defaults)
        # ==========================================================================
        ColumnSchema(
            name="base_asset",
            dtype="string",
            nullable=True,
            description="Base asset symbol (e.g., BTC, ETH)",
        ),
        ColumnSchema(
            name="quote_asset",
            dtype="string",
            nullable=True,
            description="Quote asset symbol (e.g., USDT, USD)",
        ),
        ColumnSchema(
            name="settle_asset",
            dtype="string",
            nullable=True,
            description="Settlement asset symbol",
        ),
        # ==========================================================================
        # EXCHANGE-SPECIFIC IDENTIFIERS
        # ==========================================================================
        ColumnSchema(
            name="exchange_raw_symbol",
            dtype="string",
            nullable=True,
            description="Raw exchange code from exchange API",
        ),
        ColumnSchema(
            name="databento_symbol",
            dtype="string",
            nullable=True,
            nullable_overrides={"TRADFI": False},  # Required for TRADFI
            description="Databento query symbol format",
        ),
        ColumnSchema(
            name="tardis_exchange",
            dtype="string",
            nullable=True,
            nullable_overrides={"CEFI": False},  # Required for CEFI
            description="Tardis exchange identifier",
        ),
        ColumnSchema(
            name="tardis_symbol",
            dtype="string",
            nullable=True,
            description="Symbol format used by Tardis API",
        ),
        # ==========================================================================
        # TRADING PARAMETERS
        # ==========================================================================
        ColumnSchema(
            name="inverse",
            dtype="bool",
            nullable=True,
            description="Whether this is an inverse contract",
        ),
        ColumnSchema(
            name="tick_size",
            dtype="string",
            nullable=True,
            description="Minimum price increment",
        ),
        ColumnSchema(
            name="min_size",
            dtype="string",
            nullable=True,
            description="Minimum order size",
        ),
        # ==========================================================================
        # OPTION-SPECIFIC FIELDS (nullable - only populated for options)
        # ==========================================================================
        ColumnSchema(
            name="strike",
            dtype="string",
            nullable=True,
            description="Strike price for options",
        ),
        ColumnSchema(
            name="option_type",
            dtype="string",
            nullable=True,
            description="Option type: 'CALL' or 'PUT'",
        ),
        # ==========================================================================
        # CONTRACT-SPECIFIC FIELDS
        # ==========================================================================
        ColumnSchema(
            name="expiry",
            dtype="datetime64[ns]",
            nullable=True,
            description="Expiry datetime for futures/options",
        ),
        ColumnSchema(
            name="contract_size",
            dtype="float64",
            nullable=True,
            description="Contract size/multiplier",
        ),
        ColumnSchema(
            name="underlying",
            dtype="string",
            nullable=True,
            description="Underlying asset for derivatives",
        ),
        # ==========================================================================
        # CCXT INTEGRATION FIELDS (CEFI only)
        # ==========================================================================
        ColumnSchema(
            name="ccxt_symbol",
            dtype="string",
            nullable=True,
            description="Symbol format for CCXT library",
        ),
        ColumnSchema(
            name="ccxt_exchange",
            dtype="string",
            nullable=True,
            description="Exchange identifier for CCXT library",
        ),
        # ==========================================================================
        # DEFI-SPECIFIC FIELDS
        # ==========================================================================
        ColumnSchema(
            name="base_asset_contract_address",
            dtype="string",
            nullable=True,
            description="ERC-20 contract address for base asset (DeFi only)",
        ),
        ColumnSchema(
            name="quote_asset_contract_address",
            dtype="string",
            nullable=True,
            description="ERC-20 contract address for quote asset (DeFi only)",
        ),
        ColumnSchema(
            name="pool_id",
            dtype="string",
            nullable=True,
            description="Full pool ID for API queries (e.g., Balancer poolEvents)",
        ),
        ColumnSchema(
            name="pool_address",
            dtype="string",
            nullable=True,
            description="Pool contract address (DeFi DEX only)",
        ),
        ColumnSchema(
            name="pool_fee_tier",
            dtype="int64",
            nullable=True,
            description="Pool fee in basis points (DeFi DEX only)",
        ),
        # ==========================================================================
        # LENDING PROTOCOL FIELDS (DEFI only)
        # ==========================================================================
        ColumnSchema(
            name="flash_loan_providers",
            dtype="string",
            nullable=True,
            description="Flash loan provider addresses",
        ),
        ColumnSchema(
            name="instadapp_routing",
            dtype="string",
            nullable=True,
            description="Instadapp routing configuration",
        ),
        ColumnSchema(
            name="ltv",
            dtype="float64",
            nullable=True,
            description="Loan-to-Value ratio",
        ),
        ColumnSchema(
            name="liquidation_threshold",
            dtype="float64",
            nullable=True,
            description="Liquidation threshold",
        ),
        ColumnSchema(
            name="liquidation_bonus",
            dtype="float64",
            nullable=True,
            description="Liquidation bonus",
        ),
        ColumnSchema(
            name="reserve_factor",
            dtype="float64",
            nullable=True,
            description="Reserve factor",
        ),
        ColumnSchema(
            name="emode_category_id",
            dtype="int64",
            nullable=True,
            description="E-mode category ID",
        ),
        ColumnSchema(
            name="emode_label",
            dtype="string",
            nullable=True,
            description="E-mode category label",
        ),
        ColumnSchema(
            name="emode_underlying",
            dtype="string",
            nullable=True,
            description="E-mode underlying asset",
        ),
        ColumnSchema(
            name="emode_liquidation_threshold",
            dtype="float64",
            nullable=True,
            description="E-mode liquidation threshold",
        ),
        ColumnSchema(
            name="emode_liquidation_bonus",
            dtype="float64",
            nullable=True,
            description="E-mode liquidation bonus",
        ),
        ColumnSchema(
            name="optimal_utilization_rate",
            dtype="float64",
            nullable=True,
            description="Optimal utilization rate",
        ),
        ColumnSchema(
            name="base_variable_borrow_rate",
            dtype="float64",
            nullable=True,
            description="Base variable borrow rate",
        ),
        ColumnSchema(
            name="variable_rate_slope1",
            dtype="float64",
            nullable=True,
            description="Variable rate slope 1",
        ),
        ColumnSchema(
            name="variable_rate_slope2",
            dtype="float64",
            nullable=True,
            description="Variable rate slope 2",
        ),
        # ==========================================================================
        # CEFI RISK PARAMETERS
        # ==========================================================================
        ColumnSchema(
            name="max_position_size",
            dtype="float64",
            nullable=True,
            description="Maximum position size in quote currency",
        ),
        ColumnSchema(
            name="max_leverage",
            dtype="float64",
            nullable=True,
            description="Maximum leverage available",
        ),
        ColumnSchema(
            name="initial_margin_rate",
            dtype="float64",
            nullable=True,
            description="Initial margin rate",
        ),
        ColumnSchema(
            name="maintenance_margin_rate",
            dtype="float64",
            nullable=True,
            description="Maintenance margin rate",
        ),
        ColumnSchema(
            name="leverage_tiers_json",
            dtype="string",
            nullable=True,
            description="JSON string of all leverage tiers",
        ),
        # ==========================================================================
        # TRADFI TRADING HOURS (TRADFI only)
        # ==========================================================================
        ColumnSchema(
            name="trading_hours_open",
            dtype="string",
            nullable=True,
            # Trading hours are optional for all categories:
            # - CEFI/DEFI: Always null (crypto trades 24/7)
            # - TRADFI: May be null if exchange trading hours not mapped
            description="Trading hours open time in UTC (TRADFI only, may be null for unmapped exchanges)",
        ),
        ColumnSchema(
            name="trading_hours_close",
            dtype="string",
            nullable=True,
            # Trading hours are optional for all categories:
            # - CEFI/DEFI: Always null (crypto trades 24/7)
            # - TRADFI: May be null if exchange trading hours not mapped
            description="Trading hours close time in UTC (TRADFI only, may be null for unmapped exchanges)",
        ),
        ColumnSchema(
            name="trading_session",
            dtype="string",
            nullable=True,
            description="Trading session identifier (TRADFI only)",
        ),
        ColumnSchema(
            name="is_trading_day",
            dtype="bool",
            nullable=True,
            description="Whether instrument trades on given date (TRADFI only)",
        ),
        ColumnSchema(
            name="holiday_calendar",
            dtype="string",
            nullable=True,
            description="Exchange holiday calendar identifier (TRADFI only)",
        ),
    ],
)


def get_instruments_schema() -> SchemaDefinition:
    """Get the instruments output schema."""
    return INSTRUMENTS_SCHEMA
