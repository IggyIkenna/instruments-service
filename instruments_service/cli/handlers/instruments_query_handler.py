"""
Instruments Query Handler

Provides CLI access to instruments client for querying canonical instrument definitions.
Supports comprehensive filtering and analysis operations.
"""

import logging
import json
from typing import Dict, Any, Optional, List

from ..base_handler import ModeHandler

logger = logging.getLogger(__name__)


class InstrumentsQueryHandler(ModeHandler):
    """Query instrument definitions with comprehensive filtering capabilities."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # Import client (lazy to avoid circular dependencies)
        # Use unified-cloud-services domain client instead of service client
        from unified_cloud_services import create_instruments_client

        self.client = create_instruments_client()
        logger.debug("✅ InstrumentsQueryHandler initialized")

    def run(self, start_date, end_date=None, **kwargs) -> Dict[str, Any]:
        """Execute instruments query operation."""
        # Use single date if end_date not provided
        if not end_date:
            end_date = start_date

        query_type = kwargs.get("query_type", "list")

        if query_type == "list":
            return self._query_instruments_list(start_date, end_date, **kwargs)
        elif query_type == "summary":
            return self._query_summary_stats(start_date, **kwargs)
        elif query_type == "details":
            return self._query_instrument_details(start_date, **kwargs)
        elif query_type == "trading-params":
            return self._query_trading_parameters(start_date, **kwargs)
        elif query_type == "data-types":
            return self._query_by_data_type(start_date, **kwargs)
        elif query_type == "expiring":
            return self._query_expiring_instruments(start_date, **kwargs)
        else:
            raise ValueError(f"Unknown query_type: {query_type}")

    def _query_instruments_list(self, start_date, end_date, **kwargs) -> Dict[str, Any]:
        """Query instruments with filtering and return list."""

        # Extract filter parameters
        venue = kwargs.get("venues")  # CLI uses 'venues'
        if isinstance(venue, list):
            venue = ",".join(venue)

        instrument_type = kwargs.get("instrument_types")  # CLI uses 'instrument_types'
        if isinstance(instrument_type, list):
            instrument_type = ",".join(instrument_type)

        base_currency = kwargs.get("base_currency")
        quote_currency = kwargs.get("quote_currency")
        symbol_pattern = kwargs.get("symbol_pattern")
        instrument_ids = kwargs.get("instrument_ids")

        # Query instruments
        if start_date == end_date:
            # Single date query
            instruments_df = self.client.get_instruments_for_date(
                date=start_date,
                venue=venue,
                instrument_type=instrument_type,
                base_currency=base_currency,
                quote_currency=quote_currency,
                symbol_pattern=symbol_pattern,
                instrument_ids=instrument_ids,
            )
        else:
            # Date range query
            instruments_df = self.client.get_instruments_date_range(
                start_date=start_date,
                end_date=end_date,
                venue=venue,
                instrument_type=instrument_type,
                base_currency=base_currency,
                quote_currency=quote_currency,
                symbol_pattern=symbol_pattern,
                instrument_ids=instrument_ids,
            )

        # Format output
        output_format = kwargs.get("output_format", "summary")

        if output_format == "json":
            # Return as JSON-serializable data
            result_data = instruments_df.to_dict("records")
        elif output_format == "csv":
            # Save to CSV file
            csv_file = kwargs.get("output_file", f"instruments_{start_date}.csv")
            instruments_df.to_csv(csv_file, index=False)
            result_data = {"csv_file": csv_file, "rows": len(instruments_df)}
        else:
            # Return summary
            result_data = {
                "instruments_found": len(instruments_df),
                "sample_instruments": (
                    instruments_df.head(10)["instrument_key"].tolist()
                    if not instruments_df.empty
                    else []
                ),
                "venues": (
                    instruments_df["venue"].unique().tolist()
                    if not instruments_df.empty
                    else []
                ),
                "instrument_types": (
                    instruments_df["instrument_type"].unique().tolist()
                    if not instruments_df.empty
                    else []
                ),
            }

        return {
            "status": "success",
            "success": True,
            "query_type": "list",
            "date_range": f"{start_date} to {end_date}",
            "filters_applied": {
                "venue": venue,
                "instrument_type": instrument_type,
                "base_currency": base_currency,
                "quote_currency": quote_currency,
                "symbol_pattern": symbol_pattern,
                "instrument_ids": len(instrument_ids) if instrument_ids else None,
            },
            "results": result_data,
        }

    def _query_summary_stats(self, date, **kwargs) -> Dict[str, Any]:
        """Query summary statistics for a date."""

        stats = self.client.get_summary_stats(date)

        logger.info(f"📊 Summary stats for {date}:")
        logger.info(f"   Total instruments: {stats.get('total_instruments', 0)}")
        logger.info(f"   Venues: {stats.get('venues', 0)}")
        logger.info(f"   Instrument types: {stats.get('instrument_types', 0)}")

        return {
            "status": "success",
            "success": True,
            "query_type": "summary",
            "date": date,
            "results": stats,
        }

    def _query_instrument_details(self, date, **kwargs) -> Dict[str, Any]:
        """Query detailed information for specific instrument."""

        instrument_id = kwargs.get("instrument_id")
        if not instrument_id:
            raise ValueError("instrument_id required for details query")

        details = self.client.get_instrument_details(date, instrument_id)

        if details:
            logger.info(f"✅ Found instrument details for {instrument_id}")
        else:
            logger.warning(f"⚠️ Instrument not found: {instrument_id} on {date}")

        return {
            "status": "success",
            "success": True,
            "query_type": "details",
            "date": date,
            "instrument_id": instrument_id,
            "results": details,
        }

    def _query_trading_parameters(self, date, **kwargs) -> Dict[str, Any]:
        """Query trading parameters for specific instrument."""

        instrument_id = kwargs.get("instrument_id")
        if not instrument_id:
            raise ValueError("instrument_id required for trading-params query")

        params = self.client.get_trading_parameters(date, instrument_id)

        if params:
            logger.info(f"📊 Trading parameters for {instrument_id}:")
            logger.info(f"   Tick size: {params.get('tick_size', 'N/A')}")
            logger.info(f"   Min size: {params.get('min_size', 'N/A')}")
            logger.info(f"   Data types: {params.get('data_types', [])}")

        return {
            "status": "success",
            "success": True,
            "query_type": "trading-params",
            "date": date,
            "instrument_id": instrument_id,
            "results": params,
        }

    def _query_by_data_type(self, date, **kwargs) -> Dict[str, Any]:
        """Query instruments by data type availability."""

        data_type = kwargs.get("data_type")
        if not data_type:
            raise ValueError("data_type required for data-types query")

        venue = kwargs.get("venues")
        if isinstance(venue, list):
            venue = ",".join(venue)

        instruments_df = self.client.get_instruments_by_data_type(
            date=date, data_type=data_type, venue=venue, limit=kwargs.get("limit", 1000)
        )

        logger.info(f"📊 Found {len(instruments_df)} instruments with {data_type} data")

        return {
            "status": "success",
            "success": True,
            "query_type": "data-types",
            "date": date,
            "data_type": data_type,
            "venue": venue,
            "results": {
                "instruments_found": len(instruments_df),
                "instruments": (
                    instruments_df["instrument_key"].tolist()
                    if not instruments_df.empty
                    else []
                ),
            },
        }

    def _query_expiring_instruments(self, date, **kwargs) -> Dict[str, Any]:
        """Query instruments expiring soon."""

        days_until_expiry = kwargs.get("days_until_expiry", 30)
        instrument_type = kwargs.get("instrument_types")
        if isinstance(instrument_type, list):
            instrument_type = ",".join(instrument_type)

        expiring_df = self.client.get_expiring_instruments(
            date=date,
            days_until_expiry=days_until_expiry,
            instrument_type=instrument_type,
        )

        logger.info(
            f"📊 Found {len(expiring_df)} instruments expiring within {days_until_expiry} days"
        )

        return {
            "status": "success",
            "success": True,
            "query_type": "expiring",
            "date": date,
            "days_until_expiry": days_until_expiry,
            "results": {
                "instruments_found": len(expiring_df),
                "expiring_instruments": (
                    expiring_df[["instrument_key", "available_to_datetime"]].to_dict(
                        "records"
                    )
                    if not expiring_df.empty
                    else []
                ),
            },
        }
