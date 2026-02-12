"""
Data type and processing configuration for instruments-service.

GCS buckets, BigQuery dataset, and processing flags.
The actual values are loaded from environment variables via InstrumentsServiceConfig.
"""

# Processing defaults (overridable via env)
DEFAULT_ENABLE_CCXT_INTEGRATION = True
DEFAULT_ENABLE_METADATA_CACHING = True
DEFAULT_CACHE_TTL_HOURS = 24
DEFAULT_MAX_BATCH_SIZE = 1000
