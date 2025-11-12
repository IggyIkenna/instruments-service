# API Reference

> **Related Documentation**:
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service overview and architecture
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID format and implementation details
> - [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples and patterns

---

## InstrumentProcessingService

### `__init__(config: Dict[str, Any])`

Initialize the service.

**Parameters:**
- `config`: Configuration dictionary
  - `project_id` (str, optional): GCP project ID (default: 'central-element-323112')
  - `tardis_api_key` (str, optional): Tardis API key (if not provided, uses Secret Manager)
  - `enable_ccxt_integration` (bool, optional): Enable CCXT enrichment (default: True)
  - `enable_metadata_caching` (bool, optional): Enable metadata caching (default: True)

**Raises:**
- `ValueError`: If API key cannot be retrieved

### `async process_exchange_instruments(exchange: str, target_date: datetime = None, force: bool = False) -> Dict[str, InstrumentDefinition]`

Process all instruments for an exchange.

**Parameters:**
- `exchange`: Exchange name (e.g., 'binance-futures')
- `target_date`: Target date for processing
- `force`: If True, bypass date filtering

**Returns:**
- Dictionary of `InstrumentDefinition` objects keyed by canonical instrument ID

### `async generate_instruments_for_exchanges(exchanges: List[str], target_date: datetime = None, max_parallel: int = None) -> Dict[str, InstrumentDefinition]`

Generate instruments for multiple exchanges.

**Parameters:**
- `exchanges`: List of exchange names
- `target_date`: Target date for processing
- `max_parallel`: Maximum parallel processing (not yet implemented)

**Returns:**
- Combined dictionary of all processed instruments

## CloudInstrumentStorage

### `__init__(cloud_target: CloudTarget = None)`

Initialize cloud storage.

**Parameters:**
- `cloud_target`: CloudTarget configuration (auto-detects test bucket if in test mode)

### `store_instruments(instruments_df: pd.DataFrame, table_name: str = "instruments", date: Optional[datetime] = None) -> bool`

Store instruments to GCS (batch historical data only).

**Note**: BigQuery uploads have been removed for batch processing. Batch data is stored in GCS only. Live streaming data (analytics mode) uploads to BigQuery separately.

**Parameters:**
- `instruments_df`: DataFrame with instrument definitions
- `table_name`: Table name (kept for compatibility, not used for BigQuery)
- `date`: Date for GCS path and CSV sample filename

**Returns:**
- True if successful

### `query_instruments(venue: Optional[str] = None, instrument_type: Optional[str] = None, table_name: str = "instruments") -> pd.DataFrame`

Query instruments from GCS (batch historical data).

**Note**: BigQuery queries have been removed. Batch instruments are stored in GCS only. Use GCS download methods or live streaming analytics endpoints for queries.

**Parameters:**
- `venue`: Optional venue filter
- `instrument_type`: Optional instrument type filter
- `table_name`: Table name (kept for compatibility, not used)

**Returns:**
- DataFrame with instruments (empty DataFrame - GCS query not implemented)

## InstrumentBatchProcessor

### `__init__(config: Dict[str, Any])`

Initialize batch processor.

**Parameters:**
- `config`: Configuration dictionary
  - `max_batch_size` (int, optional): Maximum batch size (default: 1000)
  - `lookback_days` (int, optional): Lookback days (default: 0)

### `get_required_periods(target_date: datetime, lookback_days: Optional[int] = None) -> List[datetime]`

Get list of dates for processing.

**Parameters:**
- `target_date`: Target date
- `lookback_days`: Optional lookback override

**Returns:**
- List of datetime objects

## InstrumentsService

Main orchestration service that coordinates processing, storage, and batch operations.

### `__init__(config: Dict[str, Any])`

Initialize the orchestration service.

**Parameters:**
- `config`: Configuration dictionary
  - `project_id` (str, optional): GCP project ID (default: 'central-element-323112')
  - `enable_ccxt_integration` (bool, optional): Enable CCXT enrichment (default: True)
  - `enable_metadata_caching` (bool, optional): Enable metadata caching (default: True)
  - `max_batch_size` (int, optional): Maximum batch size (default: 1000)
  - `lookback_days` (int, optional): Lookback days (default: 0)

### `async generate_instruments_for_date(date: datetime, exchanges: Optional[List[str]] = None, force: bool = False) -> Dict[str, Any]`

Generate instruments for a specific date.

**Parameters:**
- `date`: Target date for instrument generation
- `exchanges`: Optional list of exchanges to process (default: all)
- `force`: Force regeneration even if instruments exist

**Returns:**
- Dictionary with generation results

### `async generate_instruments_date_range(start_date: datetime, end_date: datetime, exchanges: Optional[List[str]] = None, force: bool = False) -> Dict[str, Any]`

Generate instruments for a date range.

**Parameters:**
- `start_date`: Start date
- `end_date`: End date
- `exchanges`: Optional list of exchanges to process
- `force`: Force regeneration

**Returns:**
- Dictionary with batch processing results

### `query_instruments(venue: Optional[str] = None, instrument_type: Optional[str] = None) -> pd.DataFrame`

Query stored instruments from BigQuery.

**Parameters:**
- `venue`: Optional venue filter
- `instrument_type`: Optional instrument type filter

**Returns:**
- DataFrame with instruments

## CloudDataProvider

Provides read access to instrument data from unified-cloud-services.

### `__init__(cloud_target: Optional[CloudTarget] = None)`

Initialize cloud data provider.

**Parameters:**
- `cloud_target`: Optional CloudTarget configuration (auto-detects if not provided)

### `get_instruments_from_gcs(date: datetime, gcs_path: Optional[str] = None) -> pd.DataFrame`

Get instruments from GCS for a specific date.

**Parameters:**
- `date`: Target date
- `gcs_path`: Optional custom GCS path (default: uses standard path format)

**Returns:**
- DataFrame with instruments

### `get_instruments_from_bigquery(venue: Optional[str] = None, instrument_type: Optional[str] = None, table_name: str = "instruments") -> pd.DataFrame`

Query instruments from BigQuery.

**Parameters:**
- `venue`: Optional venue filter
- `instrument_type`: Optional instrument type filter
- `table_name`: BigQuery table name (default: "instruments")

**Returns:**
- DataFrame with instruments

## ValidationService

Service-specific validation logic for instruments.

### `validate_instrument_definition(instrument: Dict[str, Any]) -> tuple[bool, Optional[str]]`

Validate a single instrument definition.

**Parameters:**
- `instrument`: Instrument definition dictionary

**Returns:**
- Tuple of (is_valid, error_message)

### `validate_instruments_dataframe(df: pd.DataFrame) -> tuple[bool, List[str]]`

Validate a DataFrame of instruments.

**Parameters:**
- `df`: DataFrame with instrument definitions

**Returns:**
- Tuple of (is_valid, list_of_errors)

## InstrumentsClient

Convenience client for downstream integration (downstream should prefer unified-cloud-services directly).

### `__init__(project_id: str = 'central-element-323112', bucket_name: str = 'market-data-tick')`

Initialize client.

**Parameters:**
- `project_id`: GCP project ID
- `bucket_name`: GCS bucket name

### `get_instruments_for_date(date: Union[str, datetime], venue: Optional[str] = None, instrument_type: Optional[str] = None, base_currency: Optional[str] = None, quote_currency: Optional[str] = None, symbol_pattern: Optional[str] = None, instrument_ids: Optional[List[str]] = None) -> pd.DataFrame`

Get canonical instrument definitions for a specific date with filtering.

**Parameters:**
- `date`: Date to get instruments for (YYYY-MM-DD string or datetime)
- `venue`: Filter by venue (BINANCE, DERIBIT, BYBIT, OKX, etc.)
- `instrument_type`: Filter by type (SPOT_PAIR, PERPETUAL, FUTURE, OPTION)
- `base_currency`: Filter by base asset (BTC, ETH, SOL, etc.)
- `quote_currency`: Filter by quote asset (USDT, USD, USDC, etc.)
- `symbol_pattern`: Regex pattern to match symbols
- `instrument_ids`: List of specific instrument IDs to include

**Returns:**
- DataFrame with filtered instrument definitions

### `get_instrument_details(date: Union[str, datetime], instrument_id: str) -> Optional[Dict[str, Any]]`

Get detailed information for a specific instrument ID.

**Parameters:**
- `date`: Date to check
- `instrument_id`: Canonical instrument ID

**Returns:**
- Dictionary with instrument details or None if not found

### `get_trading_parameters(date: Union[str, datetime], instrument_id: str) -> Optional[Dict[str, Any]]`

Get trading parameters for an instrument (tick_size, min_size, etc.).

**Parameters:**
- `date`: Date to check
- `instrument_id`: Canonical instrument ID

**Returns:**
- Dictionary with trading parameters or None if not found

### `get_summary_stats(date: Union[str, datetime]) -> Dict[str, Any]`

Get summary statistics for instruments on a specific date.

**Parameters:**
- `date`: Date to analyze

**Returns:**
- Dictionary with comprehensive statistics
