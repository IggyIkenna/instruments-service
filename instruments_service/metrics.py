"""Prometheus metrics for instruments-service."""

from prometheus_client import Counter, Histogram

RECORDS_PROCESSED = Counter(
    "instruments_service_records_processed_total",
    "Total number of instrument records processed",
    ["status"],  # labels: success / error
)

PROCESSING_LATENCY = Histogram(
    "instruments_service_processing_latency_seconds",
    "Instrument processing latency in seconds",
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
)
