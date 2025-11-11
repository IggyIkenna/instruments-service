"""
Date Filter Service

Provides centralized date filtering logic for instruments.
Ensures uniform filtering by available_from_datetime and available_to_datetime
across all data sources (DeFi, CeFi, TradFi).
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DateFilterService:
    """
    Centralized date filtering service for instruments.
    
    Provides:
    - Uniform date filtering by available_from_datetime and available_to_datetime
    - Protocol-specific default dates
    - Date range validation
    """

    def __init__(self):
        """Initialize Date Filter service."""
        # Protocol-specific default launch dates
        # These are accurate launch dates verified from protocol documentation and blockchain data
        # Format: ISO datetime strings in UTC
        self._protocol_defaults: Dict[str, Dict[str, Optional[str]]] = {
            "hyperliquid": {
                "available_from": "2024-01-01T00:00:00Z",  # Hyperliquid launched in early 2024
                "available_to": None,
            },
            "aster": {
                "available_from": "2021-08-01T00:00:00Z",  # Aster launched August 2021
                "available_to": None,
            },
            "uniswap_v2": {
                "available_from": "2020-05-01T00:00:00Z",  # Uniswap V2 launch: May 2020
                "available_to": None,
            },
            "uniswap_v3": {
                "available_from": "2021-05-05T00:00:00Z",  # Uniswap V3 launch: May 5, 2021
                "available_to": None,
            },
            "uniswap_v4": {
                "available_from": "2025-01-31T00:00:00Z",  # Uniswap V4 launch: January 31, 2025
                "available_to": None,
            },
            "curve": {
                "available_from": "2020-01-01T00:00:00Z",  # Curve Finance launch: January 2020
                "available_to": None,
            },
            "balancer": {
                "available_from": "2021-05-11T00:00:00Z",  # Balancer V2 launch: May 11, 2021
                "available_to": None,
            },
            "aave_v3": {
                "available_from": "2023-01-27T00:00:00Z",  # AAVE V3 Ethereum launch: January 27, 2023
                "available_to": None,
            },
            "ethena": {
                "available_from": "2024-02-16T00:00:00Z",  # Ethena launch (first benchmark data): February 16, 2024
                "available_to": None,
            },
            "etherfi": {
                "available_from": "2024-05-12T00:00:00Z",  # EtherFi weETH launch: May 12, 2024
                "available_to": None,
            },
            "lido": {
                "available_from": "2020-12-01T00:00:00Z",  # Lido stETH launch: December 2020
                "available_to": None,
            },
            "morpho": {
                "available_from": "2022-07-01T00:00:00Z",  # Morpho launch: July 2022
                "available_to": None,
            },
        }
        
        logger.info("✅ DateFilterService initialized")

    def filter_instruments_by_date(
        self,
        instruments: Dict[str, Dict[str, Any]],
        target_date: datetime,
        protocol: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Filter instruments by target date using available_from_datetime and available_to_datetime.
        
        Args:
            instruments: Dictionary mapping instrument_key to instrument definition
            target_date: Target date to filter by (must be timezone-aware)
            protocol: Optional protocol name for default date handling
            
        Returns:
            Filtered dictionary of instruments available on target_date
        """
        if not instruments:
            return {}
        
        # Ensure target_date is timezone-aware
        if target_date.tzinfo is None:
            target_date = target_date.replace(tzinfo=timezone.utc)
        
        filtered = {}
        filtered_count = 0
        
        for inst_key, inst_data in instruments.items():
            try:
                # Get available_from_datetime
                available_from_str = inst_data.get("available_from_datetime")
                if not available_from_str:
                    # Use protocol default if available
                    if protocol and protocol.lower() in self._protocol_defaults:
                        default_from = self._protocol_defaults[protocol.lower()].get("available_from")
                        if default_from:
                            available_from_str = default_from
                            inst_data["available_from_datetime"] = default_from
                
                # Parse available_from_datetime
                available_from = None
                if available_from_str:
                    try:
                        # Handle ISO format strings
                        if isinstance(available_from_str, str):
                            # Remove timezone info if present for parsing
                            if available_from_str.endswith("Z"):
                                available_from_str = available_from_str[:-1] + "+00:00"
                            available_from = datetime.fromisoformat(available_from_str.replace("Z", "+00:00"))
                            if available_from.tzinfo is None:
                                available_from = available_from.replace(tzinfo=timezone.utc)
                        else:
                            available_from = available_from_str
                    except (ValueError, AttributeError) as e:
                        logger.debug(f"Failed to parse available_from_datetime '{available_from_str}': {e}")
                        # Use protocol default or skip
                        if protocol and protocol.lower() in self._protocol_defaults:
                            default_from = self._protocol_defaults[protocol.lower()].get("available_from")
                            if default_from:
                                try:
                                    available_from = datetime.fromisoformat(default_from.replace("Z", "+00:00"))
                                    if available_from.tzinfo is None:
                                        available_from = available_from.replace(tzinfo=timezone.utc)
                                except ValueError:
                                    pass
                
                # Get available_to_datetime
                available_to_str = inst_data.get("available_to_datetime")
                available_to = None
                if available_to_str:
                    try:
                        if isinstance(available_to_str, str):
                            if available_to_str.endswith("Z"):
                                available_to_str = available_to_str[:-1] + "+00:00"
                            available_to = datetime.fromisoformat(available_to_str.replace("Z", "+00:00"))
                            if available_to.tzinfo is None:
                                available_to = available_to.replace(tzinfo=timezone.utc)
                        else:
                            available_to = available_to_str
                    except (ValueError, AttributeError) as e:
                        logger.debug(f"Failed to parse available_to_datetime '{available_to_str}': {e}")
                
                # Filter by date
                # Instrument is available if:
                # 1. available_from is None or target_date >= available_from
                # 2. available_to is None or target_date <= available_to
                is_available = True
                
                if available_from and target_date < available_from:
                    is_available = False
                
                if available_to and target_date > available_to:
                    is_available = False
                
                if is_available:
                    filtered[inst_key] = inst_data
                else:
                    filtered_count += 1
                    
            except Exception as e:
                logger.debug(f"Error filtering instrument {inst_key} by date: {e}")
                # Include instrument if filtering fails (safer to include than exclude)
                filtered[inst_key] = inst_data
        
        if filtered_count > 0:
            logger.debug(
                f"📅 Date filter: {len(filtered)}/{len(instruments)} instruments available on {target_date.strftime('%Y-%m-%d')}"
            )
        
        return filtered

    def get_protocol_default_date(self, protocol: str, field: str = "available_from") -> Optional[str]:
        """
        Get default date for a protocol.
        
        Args:
            protocol: Protocol name
            field: Field name ('available_from' or 'available_to')
            
        Returns:
            Default date string or None
        """
        protocol_lower = protocol.lower()
        if protocol_lower in self._protocol_defaults:
            return self._protocol_defaults[protocol_lower].get(field)
        return None

    def set_protocol_default_date(self, protocol: str, field: str, date_str: str):
        """
        Set default date for a protocol.
        
        Args:
            protocol: Protocol name
            field: Field name ('available_from' or 'available_to')
            date_str: ISO format date string
        """
        protocol_lower = protocol.lower()
        if protocol_lower not in self._protocol_defaults:
            self._protocol_defaults[protocol_lower] = {}
        self._protocol_defaults[protocol_lower][field] = date_str
        logger.info(f"Set default {field} for {protocol}: {date_str}")

