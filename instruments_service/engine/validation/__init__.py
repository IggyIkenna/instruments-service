"""
Validation module for instruments-service.

This module provides validation utilities for:
- Dependency checking (external APIs, infrastructure)
- Selective API key validation (only for required venues)
"""

from instruments_service.engine.validation.dependency_checker import (
    DependencyChecker,
    DependencyReport,
    DependencyStatus,
    check_dependencies,
)
from instruments_service.engine.validation.selective_validator import (
    get_venues_for_category,
    validate_required_api_keys,
)

__all__ = [
    "DependencyChecker",
    "DependencyReport",
    "DependencyStatus",
    "check_dependencies",
    "get_venues_for_category",
    "validate_required_api_keys",
]
