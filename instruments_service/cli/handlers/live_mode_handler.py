"""
Live Mode Handler for instruments-service.

Runs continuously on wall clock aligned intervals (:00, :15, :30, :45) following
codex batch-live symmetry principles.

Per codex:
- Cloud-agnostic: uses upload_to_storage() not direct google.cloud
- Split libraries: unified-config-interface, unified-events-interface, unified-trading-library
- UTC datetime: all timestamps use timezone.utc
- No hardcoded project IDs: uses unified_config
"""

import asyncio
import logging
import threading
from datetime import UTC, datetime, timedelta
from queue import Queue
from typing import TypedDict, cast
from uuid import uuid4

import pandas as pd
from unified_events_interface import JsonValue, log_event, publish_coordination_event, setup_events
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

# Unified cloud services (cloud-agnostic)
from unified_trading_library import GCSEventSink, upload_to_storage

from instruments_service.app.core.instruments_service import InstrumentsService
from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import get_config

logger = logging.getLogger(__name__)

# Constants
LIVE_DIRECTORY_PREFIX = "live/"


class _PersistenceItem(TypedDict):  # CORRECT-LOCAL: private service-internal queue item, not a domain contract
    """Item queued for async GCS persistence."""

    data: pd.DataFrame
    path: str
    bucket: str
    category: str
    timestamp: datetime


class LiveModeHandler(ModeHandler):
    """
    Live mode handler - runs continuously on wall clock aligned intervals.

    Per codex batch-live symmetry:
    - 90% code reuse (same InstrumentsService engine)
    - 4 seams differ: data source (same), data sink (async), persistence (thread), trigger (wall clock)
    """

    def __init__(self, config: dict[str, object]):
        super().__init__(config)

        # Initialize instruments service (expects dict, not Pydantic object)
        self.instruments_service = InstrumentsService(config)

        # Persistence thread (Queue holds PersistenceItem or None for stop signal)
        self.persistence_queue: Queue[_PersistenceItem | None] | None = None
        self.persistence_thread: threading.Thread | None = None

        logger.debug("✅ LiveModeHandler initialized")

    def run(self, **kwargs: object) -> dict[str, HandlerResultValue]:
        """
        Run live mode handler.

        Returns:
            Dictionary with execution results
        """
        # Extract args (cast from object for type checker; CLI provides correct types)
        interval = cast(int, kwargs.get("interval", 15))
        categories = cast(list[str], kwargs.get("category", ["CEFI", "TRADFI", "DEFI", "SPORTS"]))
        venues = cast(list[str] | None, kwargs.get("venues"))

        # Run async handler
        return asyncio.run(self._run_live_mode(interval, categories, venues))

    async def _run_live_mode(
        self, interval: int, categories: list[str], venues: list[str] | None
    ) -> dict[str, HandlerResultValue]:
        """
        Main live mode execution loop.

        Args:
            interval: Minutes between runs (default 15)
            categories: List of categories to process
            venues: Optional list of specific venues

        Returns:
            Execution results
        """
        config = get_config()

        # Setup events (direct import per dependency matrix)
        _sink = GCSEventSink(
            project_id=config.gcp_project_id,
            bucket=getattr(config, "events_bucket", f"{config.gcp_project_id}-events"),
            service_name="instruments-service",
        )
        setup_events(sink=_sink, mode="live", service_name="instruments-service")

        log_event(
            "LIVE_MODE_STARTED",
            details=cast(
                dict[str, JsonValue],
                {
                    "interval_minutes": interval,
                    "categories": categories,
                    "venues": venues if venues else "all",
                    "retention_days": 30,
                },
            ),
        )

        # Start persistence thread
        self.persistence_queue = Queue()
        self.persistence_thread = self._start_persistence_thread()

        cycle_count = 0
        try:
            while True:
                # Wait for next aligned timestamp
                sleep_seconds, next_run = self._calculate_next_aligned_time(interval)

                log_event(
                    "LIVE_WAITING_FOR_ALIGNMENT",
                    details=cast(
                        dict[str, JsonValue],
                        {
                            "next_run_utc": next_run.isoformat(),
                            "sleep_seconds": sleep_seconds,
                            "target_minute": next_run.minute,
                        },
                    ),
                )

                await asyncio.sleep(sleep_seconds)

                # Verify alignment
                actual_time = datetime.now(UTC)
                cycle_count += 1

                log_event(
                    "LIVE_CYCLE_STARTED",
                    details=cast(
                        dict[str, JsonValue],
                        {"cycle": cycle_count, "timestamp": actual_time.isoformat(), "minute": actual_time.minute},
                    ),
                )

                # Process instruments (reuse batch engine)
                await self._process_cycle(actual_time, categories, venues, cycle_count)

        except KeyboardInterrupt:
            log_event("LIVE_MODE_STOPPED", details=cast(dict[str, JsonValue], {"total_cycles": cycle_count}))
            logger.info("Stopped by user after %s cycles", cycle_count)
            return {"status": "stopped", "cycles": cycle_count}

        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            log_event("LIVE_MODE_FAILED", severity="CRITICAL", details=cast(dict[str, JsonValue], {"error": str(e)}))
            logger.exception("Live mode failed")
            return {"status": "failed", "error": str(e)}
        finally:
            self._cleanup()

    async def _process_cycle(self, timestamp: datetime, categories: list[str], venues: list[str] | None, cycle: int):
        """Process one live cycle."""
        try:
            # Convert categories list to boolean flags (method expects cefi/tradfi/defi/sports flags)
            cefi = "CEFI" in categories
            tradfi = "TRADFI" in categories
            defi = "DEFI" in categories
            sports = "SPORTS" in categories

            # Generate instruments (reuse batch engine per codex symmetry)
            # skip_storage=True: live mode writes to live/ path via persistence queue, not batch path
            result = await self.instruments_service.generate_instruments_for_date(
                date=timestamp,
                cefi=cefi,
                tradfi=tradfi,
                defi=defi,
                sports=sports,
                venues=venues,
                force=True,
                skip_storage=True,
            )

            if result.get("status") == "success":
                total_instruments = 0
                instruments_by_category_raw = result.get("instruments_by_category")
                if instruments_by_category_raw is None:
                    raise ValueError("instruments_by_category is required when status is success")
                if not isinstance(instruments_by_category_raw, dict):
                    raise TypeError(
                        f"instruments_by_category must be dict, got {type(instruments_by_category_raw).__name__}"
                    )
                instruments_by_category: dict[str, pd.DataFrame] = cast(
                    dict[str, pd.DataFrame],
                    instruments_by_category_raw,
                )
                for category, instruments_df in instruments_by_category.items():
                    # Queue for async persistence (null check before .put())
                    if self.persistence_queue is not None:
                        gcs_path = self._get_live_gcs_path(timestamp)
                        bucket_name = get_config().get_bucket_for_category(category)
                        self.persistence_queue.put(
                            {
                                "data": instruments_df,
                                "path": gcs_path,
                                "bucket": bucket_name,
                                "category": category,
                                "timestamp": timestamp,
                            }
                        )
                    total_instruments += len(instruments_df)

                # Publish coordination event (live only)
                publish_coordination_event(
                    event_type="INSTRUMENTS_READY",
                    payload=cast(
                        dict[str, JsonValue],
                        {
                            "timestamp": timestamp.isoformat(),
                            "minute": timestamp.minute,
                            "categories": list(instruments_by_category.keys()),
                            "count": total_instruments,
                        },
                    ),
                )

                log_event(
                    "LIVE_CYCLE_COMPLETED",
                    details=cast(
                        dict[str, JsonValue],
                        {"cycle": cycle, "instruments": total_instruments, "minute": timestamp.minute},
                    ),
                )

            else:
                log_event("LIVE_CYCLE_FAILED", severity="ERROR", details=cast(dict[str, JsonValue], {"cycle": cycle}))

        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            log_event(
                "LIVE_CYCLE_EXCEPTION",
                severity="ERROR",
                details=cast(dict[str, JsonValue], {"cycle": cycle, "error": str(e)}),
            )
            logger.exception("Cycle %s failed", cycle)

    def _calculate_next_aligned_time(self, interval_minutes: int = 15) -> tuple[float, datetime]:
        """Calculate sleep until next wall clock aligned timestamp."""
        now = datetime.now(UTC)
        current_minute = now.minute
        minutes_to_next = interval_minutes - (current_minute % interval_minutes)

        if minutes_to_next == 0 and now.second < 1:
            minutes_to_next = interval_minutes

        next_run = now + timedelta(minutes=minutes_to_next)
        next_run = next_run.replace(second=0, microsecond=0)

        sleep_seconds = (next_run - now).total_seconds()
        return sleep_seconds, next_run

    def _get_live_gcs_path(self, timestamp: datetime) -> str:
        """Generate GCS path for live mode with 15-min Hive partitioning."""
        minute_partition = f"{timestamp.hour:02d}{timestamp.minute:02d}"
        filename = f"instruments_{timestamp:%Y%m%d_%H%M%S}.parquet"

        day_str = timestamp.date()
        return (
            f"{LIVE_DIRECTORY_PREFIX}instrument_availability/by_date/day={day_str}/minute={minute_partition}/{filename}"
        )

    def _start_persistence_thread(self) -> threading.Thread:
        """Start background thread for non-blocking GCS writes."""
        queue = self.persistence_queue
        if queue is None:
            raise RuntimeError("persistence_queue must be set before starting persistence thread")

        def worker():
            while True:
                item: _PersistenceItem | None = queue.get()

                if item is None:  # Stop signal
                    break

                try:
                    parquet_bytes: bytes = item["data"].to_parquet()
                    upload_to_storage(
                        bucket=item["bucket"],
                        path=item["path"],
                        data=parquet_bytes,
                        content_type="application/octet-stream",
                    )
                    log_event(
                        "DATA_PERSISTED",
                        details=cast(
                            dict[str, JsonValue],
                            {"path": f"gs://{item['bucket']}/{item['path']}", "rows": len(item["data"])},  # noqa: gs-uri — log payload only, I/O via UCI
                        ),
                    )

                except (ConnectionError, TimeoutError, OSError, ValueError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.MEDIUM,
                        recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                    log_event(
                        "PERSIST_FAILED",
                        severity="ERROR",
                        details=cast(dict[str, JsonValue], {"path": item["path"], "error": str(e)}),
                    )

        thread = threading.Thread(target=worker, daemon=True, name="GCS-Persistence")
        thread.start()
        return thread

    def _cleanup(self):
        """Cleanup resources."""
        if self.persistence_queue:
            self.persistence_queue.put(None)
        if self.persistence_thread:
            self.persistence_thread.join(timeout=30)

        log_event("STOPPED")
