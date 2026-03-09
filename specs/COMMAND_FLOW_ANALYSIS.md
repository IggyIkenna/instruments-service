# Command Flow Analysis: Instruments Generation

## Command Overview

```bash
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24
```

This document traces the complete execution flow of the instruments generation command, showing how it uses both `instruments-service` and `unified-trading-services` projects.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          instruments-service                            │
│  ┌────────────┐    ┌──────────────┐    ┌─────────────────────┐          │
│  │    CLI     │───▶│   Handler    │───▶│ InstrumentsService  │          │
│  │  (parser)  │    │ (instrument) │    │   (orchestration)   │          │
│  └────────────┘    └──────────────┘    └─────────────────────┘          │
│                                                   │                     │
│                                                   ▼                     │
│                                    ┌──────────────────────────┐         │
│                                    │ CloudInstrumentStorage   │         │
│                                    │  (storage abstraction)   │         │
│                                    └──────────────────────────┘         │
│                                                   │                     │
└───────────────────────────────────────────────────┼─────────────────────┘
                                                    │
                                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       unified-trading-services                            │
│  ┌──────────────────────────────────┐    ┌─────────────────────┐        │
│  │ StandardizedDomainCloudService   │───▶│ UnifiedCloudService │        │
│  │   (domain validation wrapper)    │    │  (async GCS/BQ ops) │        │
│  └──────────────────────────────────┘    └─────────────────────┘        │
│                                                   │                     │
│                                                   ▼                     │
│                                    ┌──────────────────────────┐         │
│                                    │  GCS & BigQuery Clients  │         │
│                                    └──────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Flow (Step by Step)

### 1. Command Entry Point

**File**: `instruments-service/instruments_service/__main__.py`

```python
# When you run: python -m instruments_service [args]
# Python executes the __main__.py file in the package
```

**Flow**:

1. Imports `cli.main.run_cli()`
2. Executes `run_cli()` function
3. Handles KeyboardInterrupt and exceptions

---

### 2. CLI Initialization & Argument Parsing

**File**: `instruments-service/instruments_service/cli/main.py`

**Key Operations**:

#### 2.1 Environment Setup (Lines 18-32)

```python
def _load_env_early():
    # Loads .env file BEFORE any other imports
    # This ensures environment variables are available for config
```

#### 2.2 Configuration Patching (Lines 42-52)

```python
# CRITICAL PATCH: Ensures unified-trading-services uses instruments-service config
import unified_trading_services.core.market_category
unified_trading_services.core.market_category.unified_config = instruments_config
```

**Why This Matters**:

- `unified-trading-services` has default bucket configurations
- This patch ensures it uses the correct buckets from `instruments-service`
- Enables market category routing (CEFI/TRADFI/DEFI to different buckets)

#### 2.3 Argument Parsing (Lines 71-72)

```python
args = parse_arguments()  # Calls parser.py
```

**File**: `instruments-service/instruments_service/cli/parser.py`

```python
def parse_arguments():
    # Creates ArgumentParser with all CLI options
    # --mode instruments
    # --start-date 2023-05-23
    # --end-date 2023-05-24
    # Market type flags: --CEFI, --TRADFI, --DEFI

    parser.add_argument("--mode", choices=["instruments", "instruments-query"])
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument("--CEFI", action="store_true")
    parser.add_argument("--TRADFI", action="store_true")
    parser.add_argument("--DEFI", action="store_true")
    # ... more arguments

    return parser.parse_args()
```

---

### 3. Handler Selection & Initialization

**File**: `instruments-service/instruments_service/cli/main.py` (Lines 88-89)

```python
# Get handler for mode
handler: ModeHandler = get_handler_for_mode(args.mode, config)
```

**Handler Mapping**:

- `"instruments"` → `InstrumentHandler`
- `"instruments-query"` → `InstrumentsQueryHandler`

**File**: `instruments-service/instruments_service/cli/handlers/instrument_handler.py`

```python
class InstrumentHandler(ModeHandler):
    def __init__(self, config: Dict[str, Any]) -> None:
        # Initialize InstrumentsService (orchestration)
        self.instruments_service = InstrumentsService(service_config)

        # Initialize cloud storage (for CLI operations)
        self.cloud_storage = CloudInstrumentStorage()

        # Initialize venue mapping
        self.venue_mapping = VenueMapping()
```

---

### 4. Handler Execution

**File**: `instruments-service/instruments_service/cli/main.py` (Line 149)

```python
result = handler.run(**handler_kwargs)
```

**Handler Arguments** (from CLI):

```python
handler_kwargs = {
    "start_date": "2023-05-23",
    "end_date": "2023-05-24",
    "force": False,  # If --force flag provided
    "cefi": True,    # Default: process all if no flags
    "tradfi": True,
    "defi": True,
}
```

---

### 5. Instrument Generation Pipeline

**File**: `instruments-service/instruments_service/cli/handlers/instrument_handler.py`

```python
def run(self, start_date, end_date, force=False, **kwargs):
    return self._execute_instrument_generation(start_date, end_date, force, **kwargs)
```

#### 5.1 Date Range Processing (Lines 86-93)

```python
# Parse dates
start_date = parse_date("2023-05-23")  # datetime(2023, 5, 23, tzinfo=UTC)
end_date = parse_date("2023-05-24")    # datetime(2023, 5, 24, tzinfo=UTC)

# Generate date range
date_range = get_date_range(start_date, end_date)
# Returns: [datetime(2023, 5, 23), datetime(2023, 5, 24)]
```

#### 5.2 Market Type Selection (Lines 105-127)

```python
# Determine which market types to process
cefi = kwargs.get("cefi", False)
tradfi = kwargs.get("tradfi", False)
defi = kwargs.get("defi", False)

# Default: If no flags, process ALL market types
if not cefi and not tradfi and not defi:
    cefi = True
    tradfi = True
    defi = True
    logger.info("🌍 Processing ALL market types: CEFI, TRADFI, and DEFI")
```

#### 5.3 Date Loop Processing (Lines 135-218)

```python
for date in date_range:  # For each date: 2023-05-23, 2023-05-24

    # 1. Check if instruments already exist (unless --force)
    if not force:
        data_provider = CloudDataProvider()
        if data_provider.check_instruments_exist(date):
            logger.info("⏭️ Skipping - instruments exist")
            continue

    # 2. Delegate to InstrumentsService
    result = asyncio.run(
        self.instruments_service.generate_instruments_for_date(
            date=date,
            exchanges=exchanges_to_process,
            force=force,
            cefi=cefi,
            tradfi=tradfi,
            defi=defi,
        )
    )

    # 3. Track metrics
    total_generated += result.get("instruments_generated", 0)
    total_processing_errors += result.get("error_count", 0)
```

---

### 6. Orchestration Layer (InstrumentsService)

**File**: `instruments-service/instruments_service/app/core/instruments_service.py`

This is the main orchestration service that coordinates all instrument generation.

#### 6.1 Service Initialization (Lines 53-88)

```python
class InstrumentsService:
    def __init__(self, config: Dict[str, Any]):
        # Processing service (fetches instruments from APIs)
        self.processing_service = InstrumentProcessingService(config)

        # Cloud storage (writes to GCS/BigQuery)
        self.cloud_storage = CloudInstrumentStorage()

        # Batch processor (date range handling)
        self.batch_processor = InstrumentBatchProcessor(config)

        # Venue mapping (exchange → venue mapping)
        self.venue_mapping = VenueMapping()
```

#### 6.2 Date Processing (Lines 90-697)

```python
async def generate_instruments_for_date(
    self,
    date: datetime,
    exchanges: Optional[List[str]] = None,
    force: bool = False,
    cefi: bool = False,
    tradfi: bool = False,
    defi: bool = False,
) -> Dict[str, Any]:
```

**Processing Flow**:

##### A. Venue Filtering (Lines 133-266)

```python
# Extract venues from command line arguments
venues_filter = [v.upper() for v in venues] if venues else []

# Validate venues against allowed venues for each market type
if venues_filter:
    allowed_cefi_venues = self.venue_mapping.tardis_to_venue.values()
    allowed_tradfi_venues = self.venue_mapping.all_databento_venues
    allowed_defi_venues = self.venue_mapping.all_defi_venues

    # Reject invalid venues with clear error
    invalid_venues = [v for v in venues_filter
                     if v not in allowed_cefi_venues
                     and v not in allowed_tradfi_venues
                     and v not in allowed_defi_venues]

    if invalid_venues:
        raise ValueError(f"Invalid venues: {invalid_venues}")
```

##### B. CEFI Processing (Lines 270-368)

```python
if cefi:
    # Get exchanges to process
    exchanges = self.venue_mapping.all_tardis_exchanges
    # Example: ['binance', 'binance-futures', 'deribit', 'bybit', ...]

    # Process all exchanges in PARALLEL using asyncio.gather
    async def process_single_exchange(exchange: str):
        exchange_instruments = await self.processing_service.process_exchange_instruments(
            exchange=exchange,
            target_date=date,
            force=force
        )
        return exchange_instruments

    results = await asyncio.gather(
        *[process_single_exchange(ex) for ex in exchanges]
    )

    # Merge all results
    for result in results:
        all_instruments.update(result)
```

**CEFI Data Flow**:

1. Fetch from Tardis API (exchange metadata)
2. Enrich with CCXT (trading parameters)
3. Generate canonical keys
4. Return instrument definitions

##### C. TRADFI Processing (Lines 370-468)

```python
if tradfi:
    databento_exchanges = ["CME", "CBOE"]  # Default TradFi venues

    # Process CME and CBOE in PARALLEL
    async def process_databento_exchange(exchange: str):
        if exchange == "CBOE":
            # VIX index (static definition)
            vix_def = databento_adapter.create_vix_instrument_definition(date)
            return {vix_def.instrument_key: vix_def}
        else:
            # Fetch from Databento API
            databento_instruments = await self.processing_service.fetch_databento_instruments(
                exchange=exchange,
                symbols=symbols,
                target_date=date,
            )
            return databento_instruments

    results = await asyncio.gather(
        *[process_databento_exchange(ex) for ex in databento_exchanges]
    )
```

**TRADFI Data Flow**:

1. Fetch from Databento API (futures/options)
2. Create VIX definition (for CBOE)
3. Generate canonical keys
4. Return instrument definitions

##### D. DEFI Processing (Lines 469-576)

```python
if defi:
    defi_protocols = [
        ("uniswap_v3", "ETHEREUM"),
        ("curve", "ETHEREUM"),
        ("aave_v3", "ETHEREUM"),
        ("hyperliquid", None),
        ("aster", None),
        # ... more protocols
    ]

    for protocol, chain in defi_protocols:
        defi_instruments = self.processing_service.fetch_defi_instruments(
            protocol=protocol,
            chain=chain,
            target_date=date,
        )
        all_instruments.update(defi_instruments)
```

**DEFI Data Flow**:

1. Fetch from The Graph API (on-chain protocols)
2. Fetch from direct APIs (Hyperliquid, Aster)
3. Generate canonical keys
4. Return instrument definitions

##### E. Consolidation & Storage (Lines 637-682)

```python
# Convert to DataFrame
instruments_list = [inst.model_dump() for inst in all_instruments.values()]
instruments_df = pd.DataFrame(instruments_list)

# Store to cloud
success = self.cloud_storage.store_instruments(
    instruments_df=instruments_df,
    table_name="instruments",
    date=date
)

if success:
    return {
        "status": "success",
        "date": date_str,
        "instruments_generated": len(instruments_df),
        "venues": instruments_df["venue"].unique().tolist(),
    }
```

---

### 7. Cloud Storage Layer

**File**: `instruments-service/instruments_service/app/core/cloud_instrument_storage.py`

This is where the connection to `unified-trading-services` happens.

#### 7.1 Initialization (Lines 41-100)

```python
class CloudInstrumentStorage:
    def __init__(self, cloud_target: CloudTarget = None):
        # Import unified-trading-services
        from unified_trading_services import StandardizedDomainCloudService, CloudTarget

        # Detect test environment
        environment = get_config("ENVIRONMENT", "development").lower()

        # Configure CloudTarget
        if cloud_target is None:
            cloud_target = CloudTarget(
                project_id=get_config("GCP_PROJECT_ID", "{project_id}"),  # Replace {project_id} with actual project ID
                gcs_bucket=get_config("INSTRUMENTS_GCS_BUCKET", "instruments-store"),
                bigquery_dataset=get_config("INSTRUMENTS_BIGQUERY_DATASET", "instruments"),
                bigquery_location=get_config("BIGQUERY_LOCATION", "asia-northeast1"),
            )

        # Create domain cloud service
        self.cloud_service = StandardizedDomainCloudService(
            domain="instruments",
            cloud_target=cloud_target
        )
```

**Key Configuration Sources**:

- `ENVIRONMENT` → Controls test vs prod bucket
- `INSTRUMENTS_GCS_BUCKET` → Main GCS bucket
- `INSTRUMENTS_BIGQUERY_DATASET` → BigQuery dataset
- `BIGQUERY_LOCATION` → Region for BigQuery

#### 7.2 Store Instruments (Lines 103-340)

```python
def store_instruments(
    self,
    instruments_df: pd.DataFrame,
    table_name: str,
    date: datetime,
) -> bool:
    """Store instruments to GCS and BigQuery."""
```

**Storage Flow**:

##### A. Market Category Determination (Lines 118-142)

```python
# Import from unified-trading-services
from unified_trading_services import (
    determine_market_category,
    get_bucket_for_category,
)

# Determine category for each instrument
instruments_df["market_category"] = instruments_df.apply(
    lambda row: determine_market_category(
        venue=row.get("venue"),
        instrument_type=row.get("instrument_type"),
    ),
    axis=1
)

# Get unique categories
categories = instruments_df["market_category"].unique()
# Example: ['CEFI', 'TRADFI', 'DEFI']
```

##### B. Category-Based Storage (Lines 144-238)

```python
for category in categories:
    # Filter instruments by category
    category_df = instruments_df[
        instruments_df["market_category"] == category
    ]

    # Get category-specific bucket
    category_bucket = get_bucket_for_category(category)
    # Example: CEFI → "instruments-store-cefi"
    #          TRADFI → "instruments-store-tradfi"
    #          DEFI → "instruments-store-defi"

    # Create category-specific CloudTarget
    category_target = CloudTarget(
        project_id=self.cloud_service.cloud_target.project_id,
        gcs_bucket=category_bucket,
        bigquery_dataset=self.cloud_service.cloud_target.bigquery_dataset,
        bigquery_location=self.cloud_service.cloud_target.bigquery_location,
    )

    # Upload to GCS
    gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"
    self.cloud_service.upload_to_gcs(
        data=category_df,
        gcs_path=gcs_path,
        format="parquet",
        metadata={
            "date": date_str,
            "category": category,
            "instrument_count": str(len(category_df)),
        }
    )

    # Upload to BigQuery
    self.cloud_service.upload_to_bigquery(
        data=category_df,
        table_name=table_name,
        write_mode="safe",
        partition_field="timestamp",
    )
```

**Storage Paths**:

- **GCS**: `gs://<category-bucket>/instrument_availability/by_date/day=2023-05-23/instruments.parquet`
- **BigQuery**: `<project>.<dataset>.instruments` (partitioned by timestamp)

---

### 8. Unified Cloud Services Layer

**File**: `unified-trading-services/unified_trading_services/domain/standardized_service.py`

This is the abstraction layer that provides domain-specific validation and wraps async operations.

#### 8.1 Service Architecture

```python
class StandardizedDomainCloudService:
    """
    Wrapper around UnifiedCloudService with domain-specific validation.
    Provides synchronous public API that wraps async operations internally.
    """

    def __init__(self, domain: str, cloud_target: CloudTarget):
        self.domain = domain  # "instruments"
        self.cloud_target = cloud_target

        # Create domain-specific UnifiedCloudService
        self.unified_service = create_domain_cloud_service(domain)

        # Initialize domain validation
        self.domain_validation = DomainValidationService(domain=domain)
```

#### 8.2 Upload to GCS (Lines 198-229)

```python
def upload_to_gcs(
    self,
    data: pd.DataFrame | bytes | str,
    gcs_path: str,
    format: str = "parquet",
    metadata: dict[str, str] | None = None,
) -> str:
    """
    Upload to GCS with runtime target configuration.
    Synchronous wrapper around async UnifiedCloudService.
    """

    async def _upload():
        return await self.unified_service.upload_to_gcs(
            target=self.cloud_target,  # Category-specific bucket
            data=data,
            gcs_path=gcs_path,
            format=format,
            metadata=metadata,
        )

    return self._run_async(_upload())
```

**Key Features**:

- **Synchronous API**: Easy to use from synchronous code
- **Async Internally**: Optimized performance with async I/O
- **Runtime Target**: Uses category-specific bucket from cloud_target

#### 8.3 Upload to BigQuery (Lines 254-320)

```python
def upload_to_bigquery(
    self,
    data: pd.DataFrame,
    table_name: str,
    write_mode: str = "safe",
    partition_field: str = "timestamp",
    clustering_fields: list[str] | None = None,
    validate: bool = True,
) -> dict[str, Any]:
    """
    Upload to BigQuery with domain-specific validation.
    """

    # Apply domain-specific validation
    if validate:
        validation_result = self.domain_validation.validate_dataframe(data)
        if not validation_result.is_valid:
            raise ValueError(f"Validation failed: {validation_result.errors}")

    async def _upload():
        return await self.unified_service.upload_to_bigquery(
            target=self.cloud_target,
            data=data,
            table_name=table_name,
            write_mode=write_mode,
            partition_field=partition_field,
            clustering_fields=clustering_fields,
        )

    return self._run_async(_upload())
```

**Domain Validation** (for "instruments" domain):

- Required columns: `instrument_key`, `venue`, `instrument_type`, `timestamp`
- Data type checks
- Value range validation
- Null checks

---

### 9. Core Cloud Operations

**File**: `unified-trading-services/unified_trading_services/core/unified_cloud_service.py`

This is the lowest-level cloud service that directly interacts with GCS and BigQuery clients.

#### 9.1 GCS Upload (Lines 201-290)

```python
async def upload_to_gcs(
    self,
    data: Union[pd.DataFrame, bytes, str],
    target: CloudTarget,
    gcs_path: str,
    format: str = "parquet",
    metadata: dict[str, str] | None = None,
) -> str:
    """Upload data to GCS with runtime target configuration"""

    # Enforce concurrency limit
    async with self._upload_semaphore:
        # Get GCS client
        gcs_client = self._get_gcs_client()
        bucket = gcs_client.bucket(target.gcs_bucket)
        blob = bucket.blob(gcs_path)

        # Set metadata
        if metadata:
            blob.metadata = metadata

        # Handle DataFrame → Parquet
        if isinstance(data, pd.DataFrame) and format == "parquet":
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
                data.to_parquet(
                    tmp_file.name,
                    index=False,
                    engine="pyarrow",
                    compression="snappy",
                )
                blob.upload_from_filename(tmp_file.name)

        return f"gs://{target.gcs_bucket}/{gcs_path}"
```

**Key Features**:

- **Connection Pooling**: Reuses GCS client across uploads
- **Concurrency Control**: Semaphore limits concurrent uploads
- **Format Support**: Parquet, CSV, JSON, pickle, joblib
- **Temporary Files**: Handles serialization to temp files

#### 9.2 BigQuery Upload (Lines 334-500)

```python
async def upload_to_bigquery(
    self,
    data: pd.DataFrame,
    target: CloudTarget,
    table_name: str,
    write_mode: str = "safe",
    partition_field: str = "timestamp",
    clustering_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Upload DataFrame to BigQuery"""

    # Prepare DataFrame for BigQuery
    data = prepare_dataframe_for_bigquery(data)

    # Get BigQuery client
    bq_client = self._get_bigquery_client()

    # Build table ID
    table_id = f"{target.project_id}.{target.bigquery_dataset}.{table_name}"

    # Write modes:
    # - "safe": DELETE existing rows for this date, then INSERT
    # - "append": Append to existing table
    # - "skip": Skip if table exists

    if write_mode == "safe":
        # Delete existing rows for this date
        delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE DATE({partition_field}) = '{date}'
        """
        bq_client.query(delete_query).result()

    # Configure table partitioning
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ),
        clustering_fields=clustering_fields,
    )

    # Upload DataFrame
    job = bq_client.load_table_from_dataframe(
        data,
        table_id,
        job_config=job_config,
    )

    job.result()  # Wait for completion

    return {
        "status": "success",
        "table_id": table_id,
        "rows_uploaded": len(data),
    }
```

**Key Features**:

- **Safe Write Mode**: Deletes existing data for date before inserting
- **Partitioning**: Daily partitions by timestamp field
- **Clustering**: Optimizes query performance
- **Connection Pooling**: Reuses BigQuery client

---

## Summary: Two-Project Integration

### Separation of Concerns

```
┌──────────────────────────────────────────────────────────────────┐
│                    instruments-service                            │
│                                                                   │
│  Responsibilities:                                               │
│  • CLI interface & argument parsing                              │
│  • Domain logic (instrument generation)                          │
│  • API integrations (Tardis, Databento, The Graph)              │
│  • Canonical key generation                                      │
│  • Venue & instrument type mapping                               │
│  • Market category determination (CEFI/TRADFI/DEFI)             │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
                            ▼
                   (uses as dependency)
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│                  unified-trading-services                           │
│                                                                   │
│  Responsibilities:                                               │
│  • GCS upload/download operations                                │
│  • BigQuery upload/query operations                              │
│  • Connection pooling & optimization                             │
│  • Domain validation (schema checks)                             │
│  • Error handling & retry logic                                  │
│  • Observability & performance monitoring                        │
│  • Secret management (API keys)                                  │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Key Integration Points

1. **Configuration Patching** (main.py:42-52)
   - Ensures `unified-trading-services` uses `instruments-service` config
   - Critical for category-based bucket routing

2. **CloudInstrumentStorage** (cloud_instrument_storage.py)
   - Bridge between `instruments-service` and `unified-trading-services`
   - Creates `StandardizedDomainCloudService` instance
   - Manages category-based storage routing

3. **Market Category Classification** (unified-trading-services)
   - `determine_market_category()`: Classifies instruments as CEFI/TRADFI/DEFI
   - `get_bucket_for_category()`: Routes to category-specific GCS buckets
   - Enables independent batch processing per category

4. **Domain Validation** (unified-trading-services)
   - Schema validation before BigQuery uploads
   - Type checking and null validation
   - Prevents bad data from entering data warehouse

---

## Data Flow: Single Instrument

```
1. CLI Command
   ├─ --mode instruments
   ├─ --start-date 2023-05-23
   └─ --end-date 2023-05-24

2. Handler Execution
   └─ InstrumentHandler.run()
      └─ For each date in range:

3. Orchestration (InstrumentsService)
   ├─ Process CEFI (Tardis)
   │  ├─ Fetch from Tardis API
   │  ├─ Enrich with CCXT
   │  └─ Generate canonical keys
   │
   ├─ Process TRADFI (Databento)
   │  ├─ Fetch from Databento API
   │  ├─ Create VIX definition
   │  └─ Generate canonical keys
   │
   └─ Process DEFI (The Graph)
      ├─ Fetch from The Graph API
      ├─ Fetch from direct APIs
      └─ Generate canonical keys

4. Storage (CloudInstrumentStorage)
   └─ For each category (CEFI/TRADFI/DEFI):
      ├─ Determine market category
      ├─ Get category-specific bucket
      ├─ Upload to GCS (parquet)
      └─ Upload to BigQuery (partitioned)

5. Cloud Operations (StandardizedDomainCloudService)
   ├─ Validate schema (domain validation)
   ├─ Prepare DataFrame for BigQuery
   └─ Delegate to UnifiedCloudService

6. GCS/BigQuery Clients (UnifiedCloudService)
   ├─ Connection pooling
   ├─ Concurrent upload optimization
   ├─ Error handling & retry
   └─ Performance monitoring
```

---

## File Structure Reference

### instruments-service

```
instruments_service/
├── cli/
│   ├── main.py                    # CLI entry point, patching, orchestration
│   ├── parser.py                  # Argument parsing, validation
│   └── handlers/
│       └── instrument_handler.py  # Date loop, delegation to InstrumentsService
│
├── app/
│   └── core/
│       ├── instruments_service.py           # Orchestration (CEFI/TRADFI/DEFI)
│       ├── cloud_instrument_storage.py      # Bridge to unified-trading-services
│       ├── instrument_processing_service.py # API integrations
│       └── cloud_data_provider.py           # Read operations
│
├── config/
│   └── venue_mapping.py          # Exchange → Venue mapping
│
└── settings.py                   # Configuration (buckets, datasets, API keys)
```

### unified-trading-services

```
unified_trading_services/
├── domain/
│   ├── standardized_service.py   # Domain-aware wrapper (sync API)
│   └── validation.py             # Schema validation per domain
│
├── core/
│   ├── unified_cloud_service.py  # Async GCS/BQ operations
│   ├── cloud_config.py           # CloudTarget configuration
│   ├── market_category.py        # Category classification & routing
│   └── sampling_service.py       # CSV sampling for development
│
└── models/
    ├── error.py                  # Error handling models
    └── instrument.py             # Venue & InstrumentType enums
```

---

## Key Design Patterns

### 1. **Dependency Injection**

- `instruments-service` depends on `unified-trading-services`
- `unified-trading-services` is reusable across all services
- No circular dependencies

### 2. **Configuration Patching**

- Ensures correct bucket configuration
- Enables category-based routing
- Critical for multi-market support

### 3. **Async/Sync Bridge**

- Public API is synchronous (easy to use)
- Internal operations are async (performance)
- `_run_async()` handles event loop management

### 4. **Category-Based Storage**

- Each market category (CEFI/TRADFI/DEFI) has its own bucket
- Enables independent batch processing
- Simplifies data organization

### 5. **Domain Validation**

- Schema validation before uploads
- Prevents bad data early
- Domain-specific rules per service

---

## Environment Variables Reference

```bash
# GCP Configuration
GCP_PROJECT_ID={project_id}  # Replace with actual project ID
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# GCS Configuration
INSTRUMENTS_GCS_BUCKET=instruments-store
INSTRUMENTS_GCS_BUCKET_CEFI=instruments-store-cefi
INSTRUMENTS_GCS_BUCKET_TRADFI=instruments-store-tradfi
INSTRUMENTS_GCS_BUCKET_DEFI=instruments-store-defi

# BigQuery Configuration
INSTRUMENTS_BIGQUERY_DATASET=instruments
BIGQUERY_LOCATION=asia-northeast1

# Environment
ENVIRONMENT=development  # or "production" or "test"

# API Keys (stored in Secret Manager)
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
GRAPH_SECRET_NAME=graph-api-key
```

---

## Performance Optimizations

### 1. **Parallel Exchange Processing**

```python
# Process all exchanges concurrently
results = await asyncio.gather(
    *[process_single_exchange(ex) for ex in exchanges]
)
```

### 2. **Connection Pooling**

- GCS client reused across uploads
- BigQuery client reused across queries
- HTTP session pooling for API calls

### 3. **Concurrency Control**

```python
# Limit concurrent uploads to avoid rate limits
async with self._upload_semaphore:
    await upload_to_gcs(...)
```

### 4. **Batch Processing**

- Group instruments by category
- Upload entire category at once
- Minimize API calls

---

## Error Handling

### Levels of Error Handling

1. **CLI Level** (main.py)
   - Catches top-level exceptions
   - Logs errors with full traceback
   - Returns exit code

2. **Handler Level** (instrument_handler.py)
   - Tracks error counts per date
   - Continues processing on error
   - Provides summary statistics

3. **Service Level** (instruments_service.py)
   - Catches per-exchange errors
   - Logs with context
   - Returns partial results

4. **Cloud Level** (unified_cloud_service.py)
   - Retry logic with exponential backoff
   - Connection error recovery
   - Rate limit handling

---

## Testing Considerations

### Test Environment Detection

```python
# Detects test environment in multiple ways
is_test = (
    environment in ["test", "testing"]  # ENVIRONMENT=test
    or "pytest" in os.environ.get("_", "")  # pytest execution
    or "PYTEST_CURRENT_TEST" in os.environ  # pytest marker
)
```

### Test Bucket Usage

```python
if is_test:
    bucket = get_config("INSTRUMENTS_GCS_BUCKET_TEST", "instruments-store-test")
else:
    bucket = get_config("INSTRUMENTS_GCS_BUCKET", "instruments-store")
```

---

## Conclusion

The command flow demonstrates a clean separation of concerns:

- **instruments-service**: Domain logic & API integrations
- **unified-trading-services**: Cloud operations & infrastructure

The integration is achieved through:

1. Configuration patching (ensures correct bucket routing)
2. CloudInstrumentStorage (bridge class)
3. StandardizedDomainCloudService (unified interface)
4. Market category classification (CEFI/TRADFI/DEFI routing)

This architecture enables:

- ✅ Reusable cloud infrastructure
- ✅ Independent batch processing per category
- ✅ Clean domain separation
- ✅ Easy testing and mocking
- ✅ Performance optimization (async, pooling, concurrency)
