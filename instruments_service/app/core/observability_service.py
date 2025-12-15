"""
Observability Service for Instruments Service

Centralizes observability operations (logging, monitoring, performance tracking)
by wrapping unified-cloud-services components.

Benefits:
- Single point of observability logic
- Consistent logging/monitoring patterns across all operations
- Centralized performance tracking and metrics collection
- Wraps UCS PerformanceMonitor and MemoryMonitor

Pattern: Wraps unified-cloud-services observability components
         (same pattern as market-tick-data-handler)
"""

import asyncio
import functools
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

# Import UCS observability components
from unified_cloud_services import (
    MemoryMonitor,
    PerformanceMetrics,
    PerformanceMonitor,
    SystemMetrics,
)

logger = logging.getLogger(__name__)


@dataclass
class OperationContext:
    """Context for tracking operation execution."""

    operation_name: str
    start_time: datetime
    handler_name: Optional[str] = None
    operation_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    memory_checkpoints: List[Dict[str, Any]] = field(default_factory=list)
    log_entries: List[Dict[str, Any]] = field(default_factory=list)
    success: bool = True
    error: Optional[str] = None
    end_time: Optional[datetime] = None

    @property
    def duration(self) -> Optional[float]:
        """Calculate operation duration in seconds."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class ObservabilityService:
    """
    Centralized observability service for instruments-service.

    Wraps unified-cloud-services PerformanceMonitor and MemoryMonitor
    to provide consistent observability across all operations.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize observability service.

        Args:
            config: Configuration with observability settings:
                - enable_performance_monitoring: bool (default: True)
                - enable_memory_monitoring: bool (default: True)
                - memory_threshold_percent: float (default: 85.0)
                - performance_monitoring_interval: int seconds (default: 30)
                - log_level: str (default: 'INFO')
        """
        self.config = config
        self.enable_performance_monitoring = config.get(
            "enable_performance_monitoring", True
        )
        self.enable_memory_monitoring = config.get("enable_memory_monitoring", True)
        self.memory_threshold = config.get("memory_threshold_percent", 85.0)
        self.monitoring_interval = config.get("performance_monitoring_interval", 30)
        self.log_level = config.get("log_level", "INFO")

        # Lazy-initialized monitoring components (from UCS)
        self._performance_monitor: Optional[PerformanceMonitor] = None
        self._memory_monitor: Optional[MemoryMonitor] = None
        self._operation_contexts: Dict[str, OperationContext] = {}

        logger.info(
            f"✅ ObservabilityService initialized: "
            f"performance={self.enable_performance_monitoring}, "
            f"memory={self.enable_memory_monitoring}, "
            f"threshold={self.memory_threshold}%"
        )

    @property
    def performance_monitor(self) -> PerformanceMonitor:
        """Lazy initialization of UCS PerformanceMonitor."""
        if self._performance_monitor is None and self.enable_performance_monitoring:
            self._performance_monitor = PerformanceMonitor()
        return self._performance_monitor

    @property
    def memory_monitor(self) -> MemoryMonitor:
        """Lazy initialization of UCS MemoryMonitor."""
        if self._memory_monitor is None and self.enable_memory_monitoring:
            self._memory_monitor = MemoryMonitor(threshold_percent=self.memory_threshold)
        return self._memory_monitor

    def create_operation_logger(
        self, operation: str, handler: Optional[str] = None
    ) -> logging.Logger:
        """
        Create logger for specific operation/handler.

        Args:
            operation: Operation name
            handler: Optional handler name

        Returns:
            Logger with operation context
        """
        logger_name = f"{handler}.{operation}" if handler else f"instruments.{operation}"
        operation_logger = logging.getLogger(logger_name)

        class OperationAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                return f"[{operation}] {msg}", kwargs

        return OperationAdapter(operation_logger, {})

    @contextmanager
    def track_operation(
        self, operation_name: str, handler_name: Optional[str] = None, **metadata
    ):
        """
        Central operation tracking context manager.

        Usage:
            with observability.track_operation('generate_instruments') as ctx:
                # Operation code here
                ctx.add_metric('instruments_processed', 100)
        """
        operation_id = f"{operation_name}_{int(time.time() * 1000)}"
        context = OperationContext(
            operation_name=operation_name,
            handler_name=handler_name,
            operation_id=operation_id,
            start_time=datetime.now(timezone.utc),
            metadata=metadata,
        )

        self._operation_contexts[operation_id] = context

        # Start performance monitoring
        if self.enable_performance_monitoring and self._performance_monitor:
            self.performance_monitor.start_monitoring(self.monitoring_interval)

        # Initial memory checkpoint
        if self.enable_memory_monitoring and self._memory_monitor:
            memory_stats = {
                "memory_usage_percent": self.memory_monitor.get_memory_usage_percent(),
                "memory_usage_bytes": self.memory_monitor.get_memory_usage_bytes(),
                "available_memory_bytes": self.memory_monitor.get_available_memory_bytes(),
            }
            context.memory_checkpoints.append(
                {
                    "checkpoint": "start",
                    "timestamp": context.start_time,
                    **memory_stats,
                }
            )

        operation_logger = self.create_operation_logger(operation_name, handler_name)
        operation_logger.info(
            f"🚀 Started operation: {operation_name}",
            extra={"operation_id": operation_id, "metadata": metadata},
        )

        try:
            # Enhanced context with helper methods
            class EnhancedContext:
                def __init__(self, base_context, service):
                    for attr in [
                        "operation_name",
                        "start_time",
                        "handler_name",
                        "operation_id",
                        "metadata",
                        "performance_metrics",
                        "memory_checkpoints",
                        "log_entries",
                        "success",
                        "error",
                        "end_time",
                    ]:
                        setattr(self, attr, getattr(base_context, attr))
                    self._service = service

                @property
                def duration(self) -> Optional[float]:
                    if self.end_time and self.start_time:
                        return (self.end_time - self.start_time).total_seconds()
                    return None

                def log_progress(self, message: str, **extra_data):
                    operation_logger.info(
                        f"📊 {message}",
                        extra={"operation_id": operation_id, "progress": True, **extra_data},
                    )

                def add_metric(self, name: str, value: Any):
                    self.metadata[f"metric_{name}"] = value

                def memory_checkpoint(self, checkpoint_name: str):
                    if self._service.enable_memory_monitoring:
                        memory_stats = {
                            "memory_usage_percent": self._service.memory_monitor.get_memory_usage_percent(),
                            "memory_usage_bytes": self._service.memory_monitor.get_memory_usage_bytes(),
                            "available_memory_bytes": self._service.memory_monitor.get_available_memory_bytes(),
                        }
                        self.memory_checkpoints.append(
                            {
                                "checkpoint": checkpoint_name,
                                "timestamp": datetime.now(timezone.utc),
                                **memory_stats,
                            }
                        )

                def warn_if_memory_high(self) -> bool:
                    if self._service.enable_memory_monitoring:
                        if self._service.memory_monitor.is_memory_threshold_exceeded():
                            operation_logger.warning(
                                "⚠️ Memory usage above threshold",
                                extra={"memory_warning": True},
                            )
                            return True
                    return False

            enhanced_context = EnhancedContext(context, self)
            yield enhanced_context

        except Exception as e:
            context.success = False
            context.error = str(e)
            if "enhanced_context" in locals():
                enhanced_context.success = False
                enhanced_context.error = str(e)
            operation_logger.error(
                f"❌ Operation failed: {e}",
                extra={"operation_id": operation_id, "error": str(e)},
                exc_info=True,
            )
            raise

        finally:
            context.end_time = datetime.now(timezone.utc)
            if "enhanced_context" in locals():
                enhanced_context.end_time = context.end_time

            # Stop performance monitoring
            if self.enable_performance_monitoring and self._performance_monitor:
                self.performance_monitor.stop_monitoring()

            # Final memory checkpoint
            if self.enable_memory_monitoring and self._memory_monitor:
                memory_stats = {
                    "memory_usage_percent": self.memory_monitor.get_memory_usage_percent(),
                    "memory_usage_bytes": self.memory_monitor.get_memory_usage_bytes(),
                    "available_memory_bytes": self.memory_monitor.get_available_memory_bytes(),
                }
                context.memory_checkpoints.append(
                    {
                        "checkpoint": "end",
                        "timestamp": context.end_time,
                        **memory_stats,
                    }
                )

            # Log completion
            duration = context.duration
            if context.success:
                operation_logger.info(
                    f"✅ Completed operation: {operation_name} ({duration:.2f}s)",
                    extra={
                        "operation_id": operation_id,
                        "duration": duration,
                        "success": True,
                        "metadata": context.metadata,
                    },
                )
            else:
                operation_logger.error(
                    f"❌ Failed operation: {operation_name} ({duration:.2f}s)",
                    extra={
                        "operation_id": operation_id,
                        "duration": duration,
                        "success": False,
                        "error": context.error,
                        "metadata": context.metadata,
                    },
                )

            self._operation_contexts.pop(operation_id, None)

    def log_operation_summary(
        self,
        operation: str,
        results: Dict[str, Any],
        handler: Optional[str] = None,
    ):
        """Log standardized operation summary."""
        operation_logger = self.create_operation_logger(operation, handler)

        total_items = results.get("total_items", 0)
        processed_items = results.get("processed_items", 0)
        failed_items = results.get("failed_items", 0)
        duration = results.get("duration", 0)

        summary_msg = (
            f"📋 Operation Summary: {operation} | "
            f"Processed: {processed_items}/{total_items} | "
            f"Failed: {failed_items} | "
            f"Duration: {duration:.2f}s"
        )

        if failed_items > 0:
            operation_logger.warning(
                summary_msg, extra={"operation_summary": True, "results": results}
            )
        else:
            operation_logger.info(
                summary_msg, extra={"operation_summary": True, "results": results}
            )

    def log_progress(
        self,
        operation: str,
        current: int,
        total: int,
        handler: Optional[str] = None,
        **extra_data,
    ):
        """Log progress with percentage."""
        operation_logger = self.create_operation_logger(operation, handler)

        percentage = (current / total * 100) if total > 0 else 0
        progress_msg = f"📊 Progress: {operation} - {current}/{total} ({percentage:.1f}%)"

        operation_logger.info(
            progress_msg,
            extra={
                "progress": True,
                "current": current,
                "total": total,
                "percentage": percentage,
                **extra_data,
            },
        )

    def get_system_metrics(self) -> Optional[SystemMetrics]:
        """Get current system metrics from UCS PerformanceMonitor."""
        if self.enable_performance_monitoring and self._performance_monitor:
            try:
                if hasattr(self._performance_monitor, "get_current_system_metrics"):
                    return self._performance_monitor.get_current_system_metrics()
                elif hasattr(self._performance_monitor, "_collect_system_metrics"):
                    return self._performance_monitor._collect_system_metrics()
            except Exception as e:
                logger.debug(f"Failed to collect system metrics: {e}")
        return None

    def get_memory_status(self) -> Dict[str, Any]:
        """Get current memory status."""
        if self.enable_memory_monitoring and self._memory_monitor:
            return {
                "memory_usage_percent": self.memory_monitor.get_memory_usage_percent(),
                "memory_usage_bytes": self.memory_monitor.get_memory_usage_bytes(),
                "available_memory_bytes": self.memory_monitor.get_available_memory_bytes(),
                "threshold_exceeded": self.memory_monitor.is_memory_threshold_exceeded(),
            }
        return {}

    def get_operation_stats(self) -> Dict[str, Any]:
        """Get current operation statistics."""
        return {
            "active_operations": len(self._operation_contexts),
            "operations": list(self._operation_contexts.keys()),
            "performance_monitoring": self.enable_performance_monitoring,
            "memory_monitoring": self.enable_memory_monitoring,
        }

    def create_success_result(
        self, data: Any, operation: Optional[str] = None, **metrics
    ) -> Dict[str, Any]:
        """Create standardized success result with observability metadata."""
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": operation,
            "observability_metrics": metrics,
        }

        if isinstance(data, dict):
            result.update(data)
            if "status" not in result:
                result["status"] = "success"
        else:
            result.update({"status": "success", "data": data})

        # Add system metrics if available
        system_metrics = self.get_system_metrics()
        if system_metrics:
            result["system_metrics"] = {
                "cpu_percent": system_metrics.cpu_percent,
                "memory_percent": system_metrics.memory_percent,
                "memory_available_mb": system_metrics.memory_available_mb,
            }

        return result

    def create_error_result(
        self, error: Exception, operation: Optional[str] = None, **context
    ) -> Dict[str, Any]:
        """Create standardized error result."""
        return {
            "status": "error",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": operation,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "context": context,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if self._performance_monitor:
            try:
                if hasattr(self._performance_monitor, "cleanup"):
                    self._performance_monitor.cleanup()
                else:
                    self._performance_monitor.stop_monitoring()
            except Exception as e:
                logger.warning(f"⚠️ Performance monitor cleanup issue: {e}")

        if self._memory_monitor:
            memory_usage = self._memory_monitor.get_memory_usage_percent()
            logger.info(f"🧹 Final memory status: {memory_usage}% usage")

        logger.info("🧹 ObservabilityService cleanup completed")


# =================================================================
# DECORATOR FOR AUTOMATIC OPERATION TRACKING
# =================================================================


def observe_operation(
    operation_name: Optional[str] = None, handler_name: Optional[str] = None
):
    """
    Decorator for automatic operation observability.

    Usage:
        @observe_operation('fetch_instruments', 'tardis_adapter')
        async def fetch_function():
            # Function automatically tracked with observability
    """

    def decorator(func: Callable):
        op_name = operation_name or func.__name__

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                observability_service = None
                for arg in args:
                    if hasattr(arg, "observability") and hasattr(
                        arg.observability, "track_operation"
                    ):
                        observability_service = arg.observability
                        break

                if observability_service:
                    with observability_service.track_operation(op_name, handler_name):
                        return await func(*args, **kwargs)
                else:
                    return await func(*args, **kwargs)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                observability_service = None
                for arg in args:
                    if hasattr(arg, "observability") and hasattr(
                        arg.observability, "track_operation"
                    ):
                        observability_service = arg.observability
                        break

                if observability_service:
                    with observability_service.track_operation(op_name, handler_name):
                        return func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            return sync_wrapper

    return decorator







