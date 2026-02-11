"""
Symbol resolver - resolve Databento instrument_id to raw_symbol via symbology API.

Extracted from DatabentoAdapter._resolve_instrument_id_to_raw_symbol.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


def resolve_instrument_id_to_raw_symbol(
    client: Any,
    base_client: Any,
    instrument_id: int,
    exchange: str,
    dataset: str,
    target_date: Optional[datetime] = None,
) -> Optional[str]:
    """
    Resolve instrument_id to raw_symbol using Databento symbology API.

    Args:
        client: Databento Historical client (db.Historical)
        base_client: Base client with ip_rate_limiter
        instrument_id: Databento instrument ID
        exchange: Exchange name
        dataset: Databento dataset ID
        target_date: Target date for symbology resolution

    Returns:
        Raw symbol string (e.g., "ESZ0 C3620") or None if resolution fails
    """
    try:
        if target_date:
            start_date_str = target_date.strftime("%Y-%m-%d")
            end_date = target_date + timedelta(days=1)
            end_date_str = end_date.strftime("%Y-%m-%d")
        else:
            today = datetime.now(timezone.utc)
            start_date_str = today.strftime("%Y-%m-%d")
            end_date = today + timedelta(days=1)
            end_date_str = end_date.strftime("%Y-%m-%d")

        logger.debug(
            f"Calling symbology.resolve: instrument_id={instrument_id}, "
            f"stype_in=instrument_id, stype_out=raw_symbol, dataset={dataset}, "
            f"start_date={start_date_str}, end_date={end_date_str}"
        )

        base_client.ip_rate_limiter.acquire("symbology")
        resolved = client.symbology.resolve(
            symbols=[str(instrument_id)],
            stype_in="instrument_id",
            stype_out="raw_symbol",
            dataset=dataset,
            start_date=start_date_str,
            end_date=end_date_str,
        )

        logger.debug(f"Symbology resolution response type: {type(resolved)}, value: {resolved}")

        if resolved:
            if isinstance(resolved, dict):
                input_key = str(instrument_id)
                if input_key in resolved:
                    output_symbols = resolved[input_key]
                    if isinstance(output_symbols, list):
                        if len(output_symbols) > 0:
                            return str(output_symbols[0])
                    elif isinstance(output_symbols, str):
                        if len(output_symbols) > 0:
                            return output_symbols
                    elif isinstance(output_symbols, dict):
                        if "S" in output_symbols:
                            symbol_value = output_symbols["S"]
                            if isinstance(symbol_value, str) and len(symbol_value) > 0:
                                return symbol_value
                            elif isinstance(symbol_value, list) and len(symbol_value) > 0:
                                return str(symbol_value[0])
                        if len(output_symbols) > 0:
                            for key, value in output_symbols.items():
                                if key.startswith("D") and key[1:].isdigit():
                                    continue
                                if isinstance(value, str) and len(value) > 0:
                                    return value
                                elif isinstance(value, list) and len(value) > 0:
                                    return str(value[0])
                    else:
                        try:
                            if hasattr(output_symbols, "__len__") and len(output_symbols) > 0:
                                try:
                                    first_elem = (
                                        output_symbols.iloc[0] if hasattr(output_symbols, "iloc") else output_symbols[0]
                                    )
                                    return str(first_elem)
                                except (KeyError, IndexError, TypeError):
                                    pass
                            result = str(output_symbols)
                            if result and result != "None" and result != "nan":
                                return result
                        except Exception as e:
                            logger.debug(f"Failed to convert output_symbols to string: {e}")

                for key, value in resolved.items():
                    if isinstance(value, list) and len(value) > 0:
                        return str(value[0])
                    elif isinstance(value, str) and value:
                        return value
                    elif isinstance(value, dict):
                        if "S" in value:
                            symbol_value = value["S"]
                            if isinstance(symbol_value, str) and len(symbol_value) > 0:
                                return symbol_value
                            elif isinstance(symbol_value, list) and len(symbol_value) > 0:
                                return str(symbol_value[0])
                        if len(value) > 0:
                            for k, v in value.items():
                                if k.startswith("D") and k[1:].isdigit():
                                    continue
                                if isinstance(v, str) and len(v) > 0:
                                    return v
                                elif isinstance(v, list) and len(v) > 0:
                                    return str(v[0])
                    else:
                        try:
                            result = str(value)
                            if result and result != "None":
                                return result
                        except Exception as e:
                            logger.debug(f"Failed to convert value {value} to string: {e}")
                            continue
            elif isinstance(resolved, list) and len(resolved) > 0:
                return str(resolved[0])
            elif isinstance(resolved, str):
                return resolved
            else:
                return str(resolved)

        logger.warning(f"Symbology resolution returned empty result for instrument_id {instrument_id}")
    except Exception as e:
        logger.warning(
            f"Symbology resolution failed for instrument_id {instrument_id}: {e}",
            exc_info=True,
        )

    return None
