"""
Cloud Data Provider

Provides read access to instrument data via UCI protocol APIs.
Each domain has its own routing key (instruments domain).
"""

import logging
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from unified_cloud_interface import DataSource, get_analytics_client, get_data_source
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

from instruments_service.config import instruments_config
from instruments_service.utils.dump_to_csv import dump_to_csv

logger = logging.getLogger(__name__)


class CloudDataProvider:
    """
    Provides read access to instrument data via UCI protocol APIs.

    Each category maps to its own UCI routing key (cefi, tradfi, defi).
    """

    def __init__(self, testing_mode: bool = False):
        """
        Initialize cloud data provider.

        Args:
            testing_mode: When True, uses test buckets instead of production buckets
        """
        self._testing_mode = testing_mode

        logger.info("CloudDataProvider initialized")

    def get_instruments_from_gcs(
        self, date: datetime, gcs_path: str | None = None, category: str | None = None
    ) -> pd.DataFrame:
        """Get instruments from GCS for a specific date."""
        date_str = date.strftime("%Y-%m-%d")
        if category:
            return self.get_instruments_from_category(date, category)
        routing_key = "cefi"
        try:
            logger.info("Loading instruments for %s (routing_key=%s)", date_str, routing_key)
            data_source: DataSource = get_data_source(routing_key=routing_key, prefix="instrument_availability/by_date")
            raw: object = data_source.read(partition={"day": date_str}, format="parquet")
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning("No instruments found for %s", date_str)
            else:
                logger.info("Loaded %s instruments from storage", len(df))
                dump_to_csv(
                    df,
                    filename=f"instruments_service_data_{date.strftime('%Y%m%d')}_{datetime.now(UTC).strftime('%H%M%S')}.csv",
                )
            return df
        except FileNotFoundError:
            logger.info("No instruments found (not found) for %s", date_str)
            return pd.DataFrame()
        except (OSError, PermissionError) as e:
            return self._handle_storage_read_error(e, date_str)

    def get_instruments_from_category(self, date: datetime, category: str, gcs_path: str | None = None) -> pd.DataFrame:
        """Get instruments from category-specific routing key for a specific date."""
        date_str = date.strftime("%Y-%m-%d")
        routing_key = category.lower()
        partition = self._build_partition(date_str, gcs_path)
        try:
            logger.info("Loading %s instruments for %s", category, date_str)
            data_source: DataSource = get_data_source(routing_key=routing_key, prefix="instrument_availability/by_date")
            raw: object = data_source.read(partition=partition, format="parquet")
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning("No %s instruments found for %s", category, date_str)
            else:
                logger.info("Loaded %s %s instruments from storage", len(df), category)
                dump_to_csv(
                    df,
                    filename=f"instruments_service_{category.lower()}_data_{date.strftime('%Y%m%d')}_{datetime.now(UTC).strftime('%H%M%S')}.csv",
                )
            return df
        except FileNotFoundError:
            logger.info("No %s instruments found (not found) for %s", category, date_str)
            return pd.DataFrame()
        except (OSError, PermissionError) as e:
            return self._handle_storage_read_error(e, date_str, category)

    @staticmethod
    def _build_partition(date_str: str, gcs_path: str | None) -> dict[str, str]:
        """Build partition dict, extracting venue from gcs_path if present."""
        partition: dict[str, str] = {"day": date_str}
        if gcs_path is not None and "venue=" in gcs_path:
            for part in gcs_path.split("/"):
                if part.startswith("venue="):
                    partition["venue"] = part[len("venue=") :]
                    break
        return partition

    def get_instruments_from_bigquery(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        table_name: str = "instruments",
    ) -> pd.DataFrame:
        """Query instruments from analytics backend."""
        try:
            query, parameters = self._build_analytics_query(venue, instrument_type, table_name)
            logger.info("Querying instruments from analytics: %s", table_name)
            rows: object = get_analytics_client().execute_query(query, params=parameters)
            rows_list = rows if isinstance(rows, list) else []
            result: pd.DataFrame = pd.DataFrame(rows_list)
            logger.info("Queried %s instruments from analytics", len(result))
            return result
        except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("Failed to query instruments from analytics: %s", e)
            return pd.DataFrame()

    @staticmethod
    def _build_analytics_query(
        venue: str | None,
        instrument_type: str | None,
        table_name: str,
    ) -> tuple[str, dict[str, object]]:
        """Build BigQuery SQL and parameters for instrument query."""
        analytics_dataset = instruments_config.analytics_dataset or "instruments"
        query = f"SELECT * FROM `{analytics_dataset}.{table_name}` WHERE 1=1"  # nosec B608
        parameters: dict[str, object] = {}
        if venue:
            query += " AND venue = @venue"
            parameters["venue"] = venue
        if instrument_type:
            query += " AND instrument_type = @instrument_type"
            parameters["instrument_type"] = instrument_type
        query += " ORDER BY instrument_key"
        return query, parameters

    def check_instruments_exist(
        self,
        date: datetime,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> bool:
        """Check if instruments exist for a specific date."""
        date_str = date.strftime("%Y-%m-%d")
        if categories is None:
            categories = ["CEFI", "TRADFI", "DEFI"]  # CORRECT-LOCAL
            check_all = False
        else:
            check_all = True
        if venues:
            return self._check_venue_exists(date, date_str, categories, venues)
        return self._check_category_exists(date, date_str, categories, check_all)

    def _check_venue_exists(
        self,
        date: datetime,
        date_str: str,
        categories: list[str],
        venues: list[str],
    ) -> bool:
        """Check venue-level existence for all category x venue combinations."""
        logger.debug("Checking venue-level existence for %s: categories=%s, venues=%s", date_str, categories, venues)
        for category in categories:
            for venue in venues:
                venue_folder = venue.replace("/", "-").replace("\\", "-")
                gcs_path = f"instrument_availability/by_date/day={date_str}/venue={venue_folder}/instruments.parquet"
                try:
                    df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)
                    if df.empty:
                        logger.debug("Venue instruments NOT found: %s/%s for %s", category, venue, date_str)
                        return False
                    logger.debug("Venue instruments found: %s/%s for %s", category, venue, date_str)
                except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                    self._log_enhanced_warning(e, f"Could not check {category}/{venue} for {date_str}")
                    return False
        return True

    def _check_category_exists(
        self,
        date: datetime,
        date_str: str,
        categories: list[str],
        check_all: bool,
    ) -> bool:
        """Check date-level aggregate existence per category."""
        gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"
        found_categories: list[str] = []
        for category in categories:
            try:
                df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)
                if not df.empty:
                    logger.debug("Instruments found in %s for %s", category, date_str)
                    found_categories.append(category)
                    if not check_all:
                        return True
            except (OSError, ValueError, RuntimeError) as e:
                self._log_enhanced_warning(e, f"Could not check {category} for {date_str}")
                continue
        if check_all:
            all_found = len(found_categories) == len(categories)
            if not all_found:
                logger.debug("Missing categories for %s: %s", date_str, set(categories) - set(found_categories))
            return all_found
        logger.debug("No instruments found for %s in any category", date_str)
        return False

    @staticmethod
    def _log_enhanced_warning(e: BaseException, context_msg: str) -> None:
        """Log a warning with EnhancedError metadata."""
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        logger.debug(context_msg + ": %s", e)

    def _handle_storage_read_error(
        self,
        e: OSError | PermissionError,
        date_str: str,
        category: str | None = None,
    ) -> pd.DataFrame:
        """Handle storage read errors, returning empty DataFrame for 404s."""
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        error_msg = str(e)
        label = f"{category} " if category else ""
        if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
            logger.info("No %sinstruments found (404) for %s", label, date_str)
            return pd.DataFrame()
        logger.error("Failed to load %sinstruments from storage: %s", label, e)
        return pd.DataFrame()
