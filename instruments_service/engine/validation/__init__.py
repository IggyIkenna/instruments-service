"""
Validation module for instruments-service.

This module provides validation utilities for:
- Dependency checking (external APIs, infrastructure)
- Selective API key validation (only for required venues)
"""

from unified_trading_library import (
    BaseDependencyChecker,
    DependencyReport,
    DependencyStatus,
)

from instruments_service.engine.validation.selective_validator import (
    get_venues_for_category,
    validate_required_api_keys,
)

__all__ = [
    "BaseDependencyChecker",
    "DependencyReport",
    "DependencyStatus",
    "get_venues_for_category",
    "validate_required_api_keys",
]
