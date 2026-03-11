"""
Orchestrator Helper Functions

Contains utility functions for the orchestrator including:
- Venue validation and filtering
- DataFrame operations
- Placeholder management
- UTC midnight spanning handling
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast

import pandas as pd
from unified_trading_library import determine_market_category  # noqa: domain-ucs

from instruments_service.app.core.instrument_crud import ErrorWarningCounter
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.dump_to_csv import dump_to_csv

logger = logging.getLogger(__name__)


def normalize_venues_filter(venues: list[str] | str | None) -> list[str]:
    """
    Normalize venues filter to list of uppercase strings.

    Args:
        venues: Venues to normalize (string, list, or None)

    Returns:
        List of normalized venue names
    """
    if venues is None:
        return []
    elif isinstance(venues, str):
        return [venues.upper()]
    else:
        return [str(v).upper() for v in venues if v]


def extract_venues_from_instrument_ids(instrument_ids: list[str] | str) -> list[str]:
    """
    Extract venue names from instrument IDs.

    Args:
        instrument_ids: List of instrument IDs or single ID

    Returns:
        List of unique venue names
    """
    instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
    venues: set[str] = set()

    for inst_id in instrument_ids_list:
        if ":" in str(inst_id):
            venue = str(inst_id).split(":")[0].upper()
            venues.add(venue)

    return list(venues)


def validate_venues(
    venues_filter: list[str], venue_mapping: object, cefi: bool, tradfi: bool, defi: bool
) -> tuple[list[str], list[str] | None]:
    """
    Validate and filter venues based on category flags.

    Args:
        venues_filter: List of venues to validate
        venue_mapping: VenueMapping instance
        cefi: Whether to process CeFi venues
        tradfi: Whether to process TradFi venues
        defi: Whether to process DeFi venues

    Returns:
        Tuple of (valid_venues, tradfi_venues or None)
    """
    all_valid_venues = set()

    if cefi:
        all_valid_venues.update(cast(list[str], venue_mapping.all_tardis_exchanges))
    if tradfi:
        all_valid_venues.update(cast(list[str], venue_mapping.all_databento_venues))
    if defi:
        all_valid_venues.update(cast(list[str], venue_mapping.all_defi_venues))

    # Validate provided venues
    invalid_venues = [v for v in venues_filter if v not in all_valid_venues]
    if invalid_venues:
        logger.warning("⚠️ Invalid/unsupported venues in filter: %s", invalid_venues)

    valid_venues = [v for v in venues_filter if v in all_valid_venues]

    # Check if only TradFi venues specified
    tradfi_venues = None
    if valid_venues:
        tradfi_only = all(v in cast(list[str], venue_mapping.all_databento_venues) for v in valid_venues)
        if tradfi_only:
            tradfi_venues = valid_venues
            logger.info("🔍 Only TradFi venues specified: %s", tradfi_venues)

    return valid_venues, tradfi_venues


def filter_by_instrument_ids(
    all_instruments: dict[str, InstrumentDefinition], instrument_ids: list[str] | str
) -> dict[str, InstrumentDefinition]:
    """
    Filter instruments by instrument IDs.

    Args:
        all_instruments: Dictionary of all instruments
        instrument_ids: IDs to filter by

    Returns:
        Filtered instruments dictionary
    """
    instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [str(instrument_ids)]
    instrument_ids_set: set[str] = {str(inst_id).upper() for inst_id in instrument_ids_list}

    filtered_instruments: dict[str, InstrumentDefinition] = {}
    for inst_key, inst_obj in all_instruments.items():
        if inst_key.upper() in instrument_ids_set:
            filtered_instruments[inst_key] = inst_obj

    filtered_count = len(all_instruments) - len(filtered_instruments)
    if filtered_count > 0:
        logger.info(
            "🔍 Filtered %s instruments by instrument_ids, %s matching instruments remaining",
            filtered_count,
            len(filtered_instruments),
        )

    if filtered_instruments:
        logger.info("✅ Matching instrument_ids: %s", list(filtered_instruments.keys()))
    else:
        logger.warning(
            "⚠️ No instruments matched the specified instrument_ids: %s. Processed %s instruments but none matched.",
            instrument_ids_list,
            len(all_instruments),
        )

    return filtered_instruments


def convert_to_dataframe(all_instruments: dict[str, InstrumentDefinition], skip_storage: bool) -> pd.DataFrame:
    """
    Convert instruments dict to DataFrame.

    Args:
        all_instruments: Dictionary of instruments
        skip_storage: Whether storage is being skipped

    Returns:
        DataFrame of instruments
    """
    instruments_list: list[dict[str, object]] = []
    for inst_key, inst_obj in all_instruments.items():
        _ = inst_key
        if hasattr(inst_obj, "model_dump"):
            instruments_list.append(cast(dict[str, object], inst_obj.model_dump()))
        else:
            instruments_list.append(cast(dict[str, object], inst_obj))

    instruments_df = pd.DataFrame(instruments_list)

    if skip_storage and not instruments_df.empty:

        def _row_to_market_category(row: pd.Series[object]) -> str:
            raw = cast(dict[str, object], row.to_dict())
            row_dict: dict[str, object] = {}
            for k, v in raw.items():
                row_dict[str(k)] = str(v) if v is not None else None
            return determine_market_category(row_dict)

        if "market_category" not in instruments_df.columns:
            instruments_df["market_category"] = ""
        mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
        if mask.any():
            instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                _row_to_market_category, axis=1
            )

    # CSV sampling for generated instruments data
    if not instruments_df.empty:
        dump_to_csv(
            instruments_df, filename=f"instruments_service_generated_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.csv"
        )

    return instruments_df


def handle_no_instruments(
    date_str: str,
    tradfi: bool,
    venues_filter: list[str],
    error_warning_counter: ErrorWarningCounter,
    root_logger: logging.Logger,
) -> dict[str, object]:
    """
    Handle case where no instruments generated.

    Args:
        date_str: Date string
        tradfi: Whether TradFi processing was enabled
        venues_filter: Venues filter list
        error_warning_counter: Error/warning counter
        root_logger: Root logger instance

    Returns:
        Error response dict if appropriate, empty dict otherwise
    """
    if tradfi and venues_filter:
        logger.warning(
            "⚠️ No instruments generated from adapters for %s. This can happen on weekday holidays when Databento returns empty. Placeholder logic will ensure files are written for requested TradFi venues.",
            date_str,
        )
    else:
        logger.error(
            "❌ No instruments generated for %s. This is unexpected - check adapter/API connectivity.", date_str
        )
        error_count = error_warning_counter.error_count
        warning_count = error_warning_counter.warning_count
        root_logger.removeHandler(error_warning_counter)
        return {
            "status": "error",
            "date": date_str,
            "instruments_generated": 0,
            "message": "No instruments generated - check adapter/API connectivity",
            "error_count": error_count,
            "warning_count": warning_count,
        }

    return {}


def add_tradfi_placeholders(
    instruments_df: pd.DataFrame, date: datetime, venues_filter: list[str], venue_mapping: object
) -> pd.DataFrame:
    """
    Add placeholders for missing TradFi venues.

    Args:
        instruments_df: DataFrame of instruments
        date: Target date
        venues_filter: List of venue filters
        venue_mapping: VenueMapping instance

    Returns:
        DataFrame with placeholders added
    """
    requested_tradfi: list[str] = [v for v in venues_filter if v in cast(list[str], venue_mapping.all_databento_venues)]
    logger.debug("🔍 Placeholder check: Requested TradFi venues: %s", requested_tradfi)

    if requested_tradfi:
        if isinstance(date, datetime):
            target_dt = date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_dt = datetime.combine(date, datetime.min.time(), tzinfo=UTC)
        if target_dt.tzinfo is None:
            target_dt = target_dt.replace(tzinfo=UTC)
        date_iso = target_dt.isoformat().replace("+00:00", "Z")

        if "venue" not in instruments_df.columns or instruments_df.empty:
            logger.info("🔍 DataFrame empty or no venue column - adding placeholders for all requested venues")
            venues_present: set[str] = set()
        else:
            unique_venues: list[object] = cast(list[object], cast(object, instruments_df["venue"].unique().tolist()))
            venues_present = {str(v) for v in unique_venues}
            logger.debug("🔍 Venues present in DataFrame: %s", venues_present)

        for venue_name in requested_tradfi:
            if venue_name not in venues_present:
                placeholder: dict[str, object] = {
                    "instrument_key": f"{venue_name}:MARKET_CLOSED:PLACEHOLDER",
                    "venue": venue_name,
                    "instrument_type": "MARKET_CLOSED",
                    "symbol": "PLACEHOLDER",
                    "available_from_datetime": date_iso,
                    "timestamp": target_dt,
                    "market_category": "TRADFI",
                    "data_provider": "databento",
                    "asset_class": "traditional",
                    "venue_type": "exchange",
                    "chain": "off-chain",
                    "databento_symbol": "PLACEHOLDER",
                    "tardis_exchange": "",
                    "is_trading_day": False,
                    "trading_hours_open": "holiday",
                    "trading_hours_close": "holiday",
                    "holiday_calendar": venue_name,
                    "trading_session": "regular",
                    "regular_open_utc": None,
                    "regular_close_utc": None,
                    "auction_open_utc": None,
                    "auction_close_utc": None,
                    "early_close_utc": None,
                }
                logger.info(
                    "📅 Adding holiday/closed placeholder for %s on %s (no instruments from adapter so file would be missing)",
                    venue_name,
                    date.strftime("%Y-%m-%d"),
                )
                instruments_df = pd.concat([instruments_df, pd.DataFrame([placeholder])], ignore_index=True)
            else:
                logger.debug("✅ %s already has instruments - no placeholder needed", venue_name)

    return instruments_df


def handle_utc_midnight_spanning(instruments_df: pd.DataFrame, date: datetime, cloud_storage: object) -> None:
    """
    Handle UTC midnight spanning for CME and ICE.

    Args:
        instruments_df: DataFrame of instruments
        date: Target date
        cloud_storage: CloudInstrumentStorage instance
    """
    utc_spanning_instruments: list[pd.Series] = []
    file_date: date_type = date.date() if isinstance(date, datetime) else date

    for idx, row in instruments_df.iterrows():
        _ = idx
        if row.get("regular_open_utc") and row.get("regular_close_utc"):
            try:
                _open_raw: str = str(cast(object, row["regular_open_utc"]))
                _close_raw: str = str(cast(object, row["regular_close_utc"]))
                open_dt = cast(pd.Timestamp, pd.to_datetime(_open_raw))
                close_dt = cast(pd.Timestamp, pd.to_datetime(_close_raw))

                if open_dt.date() != file_date and close_dt.date() == file_date:
                    utc_spanning_instruments.append(row)
                    logger.debug(
                        "UTC midnight spanning: %s opens %s closes %s",
                        cast(object, row["instrument_key"]),
                        open_dt.date(),
                        close_dt.date(),
                    )
            except (ValueError, TypeError, KeyError) as e:
                logger.warning("⚠️ Failed to parse session times for %s: %s", row.get("instrument_key"), e)

    if utc_spanning_instruments:
        spanning_df = pd.DataFrame(utc_spanning_instruments)
        spanning_df["session_date_tag"] = "open_date"

        _span_open_str: str = str(cast(object, spanning_df.iloc[0]["regular_open_utc"]))
        open_date_val = cast(date_type, pd.to_datetime(_span_open_str).date())
        logger.info(
            "🌙 Writing %s UTC-spanning instruments to open-date file: %s (session opens %s, closes %s)",
            len(spanning_df),
            open_date_val,
            open_date_val,
            file_date,
        )

        open_date_dt = datetime.combine(open_date_val, datetime.min.time(), tzinfo=UTC)
        span_success = cast(
            bool,
            cloud_storage.store_instruments(instruments_df=spanning_df, table_name="instruments", date=open_date_dt),
        )

        if span_success:
            logger.info("✅ Successfully wrote UTC-spanning instruments to %s", open_date_val)
        else:
            logger.warning("⚠️ Failed to write UTC-spanning instruments to %s", open_date_val)
