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
        """
        Get instruments from GCS for a specific date.

        Args:
            date: Target date
            gcs_path: Optional custom GCS path prefix override (ignored; kept for API compatibility)
            category: Optional market category ("CEFI", "TRADFI", "DEFI") to read from category-specific bucket

        Returns:
            DataFrame with instruments
        """
        date_str = date.strftime("%Y-%m-%d")

        # If category specified, use category-specific source
        if category:
            return self.get_instruments_from_category(date, category)

        # Default: use cefi routing key
        routing_key = "cefi"
        try:
            logger.info("Loading instruments for %s (routing_key=%s)", date_str, routing_key)
            data_source: DataSource = get_data_source(
                routing_key=routing_key, prefix="instrument_availability/by_date"
            )
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
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("No instruments found (404) for %s", date_str)
                return pd.DataFrame()
            logger.error("Failed to load instruments from storage: %s", e)
            return pd.DataFrame()

    def get_instruments_from_category(self, date: datetime, category: str, gcs_path: str | None = None) -> pd.DataFrame:
        """
        Get instruments from category-specific routing key for a specific date.

        Args:
            date: Target date
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            gcs_path: Ignored (kept for API compatibility)

        Returns:
            DataFrame with instruments from the specified category routing key
        """
        date_str = date.strftime("%Y-%m-%d")
        routing_key = category.lower()

        # Parse venue from gcs_path if provided (venue-level path format)
        # e.g. "instrument_availability/by_date/day=2024-01-01/venue=BINANCE-SPOT/instruments.parquet"
        partition: dict[str, str] = {"day": date_str}
        if gcs_path is not None and "venue=" in gcs_path:
            # Extract venue value from the path
            for part in gcs_path.split("/"):
                if part.startswith("venue="):
                    partition["venue"] = part[len("venue="):]
                    break

        try:
            logger.info("Loading %s instruments for %s", category, date_str)
            data_source: DataSource = get_data_source(
                routing_key=routing_key, prefix="instrument_availability/by_date"
            )
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
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("No %s instruments found (404) for %s", category, date_str)
                return pd.DataFrame()
            logger.error("Failed to load %s instruments from storage: %s", category, e)
            return pd.DataFrame()

    def get_instruments_from_bigquery(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        table_name: str = "instruments",
    ) -> pd.DataFrame:
        """
        Query instruments from analytics backend.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            table_name: Table name (default: "instruments")

        Returns:
            DataFrame with instruments
        """
        try:
            analytics_dataset = instruments_config.analytics_dataset or "instruments"
            query = f"""
            SELECT * FROM `{analytics_dataset}.{table_name}`
            WHERE 1=1
            """

            parameters: dict[str, object] = {}

            if venue:
                query += " AND venue = @venue"
                parameters["venue"] = venue

            if instrument_type:
                query += " AND instrument_type = @instrument_type"
                parameters["instrument_type"] = instrument_type

            query += " ORDER BY instrument_key"

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
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("Failed to query instruments from analytics: %s", e)
            return pd.DataFrame()

    def check_instruments_exist(
        self,
        date: datetime,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> bool:
        """
        Check if instruments exist for a specific date.

        When venues is specified, checks for venue-level files (new structure).
        When venues is None, checks for date-level aggregate file (legacy behavior).

        Args:
            date: Target date
            categories: Optional list of categories to check (e.g., ["CEFI", "TRADFI"]).
                       If None, checks ALL categories and returns True if ANY exist.
                       If specified, returns True only if ALL specified categories exist.
            venues: Optional list of venues to check (e.g., ["BINANCE-SPOT", "BYBIT"]).
                   When specified, checks venue-level files instead of date-level aggregate.
                   Returns True only if ALL specified venues exist.

        Returns:
            True if instruments exist (logic depends on categories/venues parameters)
        """
        date_str = date.strftime("%Y-%m-%d")

        # Default: check all categories
        if categories is None:
            categories = ["CEFI", "TRADFI", "DEFI"]
            check_all = False  # Return True if ANY exist (legacy behavior)
        else:
            check_all = True  # Return True only if ALL specified categories exist

        # When venues specified, use venue-level paths (new structure)
        # This enables granular skip logic matching the category x venue x date sharding
        if venues:
            logger.debug(
                "🔍 Checking venue-level existence for %s: categories=%s, venues=%s", date_str, categories, venues
            )
            for category in categories:
                for venue in venues:
                    # Sanitize venue name for folder (replace slashes, etc.)
                    venue_folder = venue.replace("/", "-").replace("\\", "-")
                    # Use key=value format for BigQuery hive partitioning
                    gcs_path = (
                        f"instrument_availability/by_date/day={date_str}/venue={venue_folder}/instruments.parquet"
                    )

                    try:
                        df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)

                        if not df.empty:
                            logger.debug("📊 Venue instruments found: %s/%s for %s", category, venue, date_str)
                            # For venue-level checks, we need ALL venues to exist
                            # (matches the sharding logic - each shard is a specific venue)
                        else:
                            logger.debug("📊 Venue instruments NOT found: %s/%s for %s", category, venue, date_str)
                            return False  # Any missing venue means data doesn't exist
                    except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                        _err = EnhancedError(
                            message=str(e),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(e).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        logger.debug("Could not check %s/%s for %s: %s", category, venue, date_str, e)
                        return False  # Error checking = treat as not existing
            # All venues found
            return True

        # Legacy behavior: check date-level aggregate file
        gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"
        found_categories: list[str] = []

        for category in categories:
            try:
                # Use get_instruments_from_category which handles bucket selection (test vs prod)
                # and uses the correct category-specific bucket
                df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)

                if not df.empty:
                    logger.debug("📊 Instruments found in %s for %s", category, date_str)
                    found_categories.append(category)

                    # Legacy behavior: return True on first find
                    if not check_all:
                        return True
            except (OSError, ValueError, RuntimeError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                # Log but continue checking other categories
                logger.debug("Could not check %s for %s: %s", category, date_str, e)
                continue
        if check_all:
            # Only return True if ALL specified categories were found
            all_found = len(found_categories) == len(categories)
            if not all_found:
                missing = set(categories) - set(found_categories)
                logger.debug("📊 Missing categories for %s: %s", date_str, missing)
            return all_found

        logger.debug("📊 No instruments found for %s in any category", date_str)
        return False
