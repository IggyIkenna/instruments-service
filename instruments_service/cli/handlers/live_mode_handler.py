"""
Live Mode Handler for instruments-service.

Runs continuously on wall clock aligned intervals (:00, :15, :30, :45) following
codex batch-live symmetry principles.

Per codex:
- Cloud-agnostic: uses get_storage_client() not direct google.cloud
- Split libraries: unified-config-interface, unified-events-interface, unified-cloud-services
- UTC datetime: all timestamps use timezone.utc
- No hardcoded project IDs: uses unified_config
"""

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import Any

# Split libraries (per codex: use directly)
try:
    from unified_events_interface import log_event, publish_coordination_event, setup_events

    HAS_EVENTS_INTERFACE = True
except ImportError:
    from unified_cloud_services import log_event

    setup_events = None
    publish_coordination_event = None
    HAS_EVENTS_INTERFACE = False

try:
    from unified_config_interface import load_config as load_config_interface

    HAS_CONFIG_INTERFACE = True
except ImportError:
    load_config_interface = None
    HAS_CONFIG_INTERFACE = False

# Unified cloud services (cloud-agnostic)
from unified_cloud_services import get_storage_client

# Service imports
from instruments_service.app.core.instruments_service import InstrumentsService
from instruments_service.cli.base_handler import ModeHandler
from instruments_service.config import get_config

logger = logging.getLogger(__name__)

# Constants
LIVE_DIRECTORY_PREFIX = "live/"


class LiveModeHandler(ModeHandler):
    """
    Live mode handler - runs continuously on wall clock aligned intervals.

    Per codex batch-live symmetry:
    - 90% code reuse (same InstrumentsService engine)
    - 4 seams differ: data source (same), data sink (async), persistence (thread), trigger (wall clock)
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)

        # Initialize instruments service (expects dict, not Pydantic object)
        self.instruments_service = InstrumentsService(config)

        # Persistence thread
        self.persistence_queue: Queue | None = None
        self.persistence_thread: threading.Thread | None = None

        logger.debug("✅ LiveModeHandler initialized")

    def run(self, **kwargs) -> dict[str, Any]:
        """
        Run live mode handler.

        Returns:
            Dictionary with execution results
        """
        # Extract args
        interval = kwargs.get("interval", 15)
        category = kwargs.get("category", ["CEFI", "TRADFI", "DEFI"])
        venues = kwargs.get("venues")

        # Run async handler
        return asyncio.run(self._run_live_mode(interval, category, venues))

    async def _run_live_mode(self, interval: int, categories: list[str], venues: list[str] | None) -> dict[str, Any]:
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

        # Setup events (per codex: use split library if available)
        if HAS_EVENTS_INTERFACE and setup_events:
            # Handle both project_id and gcp_project_id attributes (compatibility)
            project_id = getattr(config, "project_id", None) or getattr(config, "gcp_project_id", None)
            setup_events(mode="live", service_name="instruments-service", project_id=project_id)
            logger.info("✅ Events setup via unified-events-interface")
        else:
            logger.warning("⚠️ unified-events-interface not available")

        log_event(
            "LIVE_MODE_STARTED",
            details={
                "interval_minutes": interval,
                "categories": categories,
                "venues": venues if venues else "all",
                "retention_days": 30,
            },
        )

        # Start persistence thread
        self.persistence_queue = Queue()
        self.persistence_thread = self._start_persistence_thread()

        try:
            cycle_count = 0

            while True:
                # Wait for next aligned timestamp
                sleep_seconds, next_run = self._calculate_next_aligned_time(interval)

                log_event(
                    "LIVE_WAITING_FOR_ALIGNMENT",
                    details={
                        "next_run_utc": next_run.isoformat(),
                        "sleep_seconds": sleep_seconds,
                        "target_minute": next_run.minute,
                    },
                )

                await asyncio.sleep(sleep_seconds)

                # Verify alignment
                actual_time = datetime.now(timezone.utc)
                cycle_count += 1

                log_event(
                    "LIVE_CYCLE_STARTED",
                    details={"cycle": cycle_count, "timestamp": actual_time.isoformat(), "minute": actual_time.minute},
                )

                # Process instruments (reuse batch engine)
                await self._process_cycle(actual_time, categories, venues, cycle_count)

        except KeyboardInterrupt:
            log_event("LIVE_MODE_STOPPED", details={"total_cycles": cycle_count})
            logger.info(f"Stopped by user after {cycle_count} cycles")
            return {"status": "stopped", "cycles": cycle_count}

        except Exception as e:
            log_event("LIVE_MODE_FAILED", severity="CRITICAL", details={"error": str(e)})
            logger.exception("Live mode failed")
            return {"status": "failed", "error": str(e)}

        finally:
            self._cleanup()

    async def _process_cycle(self, timestamp: datetime, categories: list[str], venues: list[str] | None, cycle: int):
        """Process one live cycle."""
        try:
            current_date = timestamp.date()

            # Convert categories list to boolean flags (method expects cefi/tradfi/defi flags)
            cefi = "CEFI" in categories
            tradfi = "TRADFI" in categories
            defi = "DEFI" in categories

            # Generate instruments (reuse batch engine per codex symmetry)
            result = await self.instruments_service.generate_instruments_for_date(
                date=current_date, cefi=cefi, tradfi=tradfi, defi=defi, venues=venues, force=True
            )

            if result.get("success"):
                total_instruments = 0

                # Queue for async persistence
                for category, instruments_df in result.get("instruments_by_category", {}).items():
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
                if HAS_EVENTS_INTERFACE and publish_coordination_event:
                    publish_coordination_event(
                        event_type="INSTRUMENTS_READY",
                        payload={
                            "timestamp": timestamp.isoformat(),
                            "minute": timestamp.minute,
                            "categories": list(result.instruments_by_category.keys()),
                            "count": total_instruments,
                        },
                    )

                log_event(
                    "LIVE_CYCLE_COMPLETED",
                    details={"cycle": cycle, "instruments": total_instruments, "minute": timestamp.minute},
                )

            else:
                log_event("LIVE_CYCLE_FAILED", severity="ERROR", details={"cycle": cycle})

        except Exception as e:
            log_event("LIVE_CYCLE_EXCEPTION", severity="ERROR", details={"cycle": cycle, "error": str(e)})
            logger.exception(f"Cycle {cycle} failed")

    def _calculate_next_aligned_time(self, interval_minutes: int = 15) -> tuple[float, datetime]:
        """Calculate sleep until next wall clock aligned timestamp."""
        now = datetime.now(timezone.utc)
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

        return f"{LIVE_DIRECTORY_PREFIX}instrument_availability/by_date/day={timestamp.date()}/minute={minute_partition}/{filename}"

    def _start_persistence_thread(self) -> threading.Thread:
        """Start background thread for non-blocking GCS writes."""

        def worker():
            storage_client = get_storage_client()  # Cloud-agnostic per codex

            while True:
                item = self.persistence_queue.get()

                if item is None:  # Stop signal
                    break

                try:
                    bucket = storage_client.bucket(item["bucket"])
                    blob = bucket.blob(item["path"])

                    blob.upload_from_string(item["data"].to_parquet(), content_type="application/octet-stream")

                    log_event(
                        "DATA_PERSISTED",
                        details={"path": f"gs://{item['bucket']}/{item['path']}", "rows": len(item["data"])},
                    )

                except Exception as e:
                    log_event("PERSIST_FAILED", severity="ERROR", details={"path": item["path"], "error": str(e)})

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
