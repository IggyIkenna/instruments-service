"""Sports-domain sub-package for instruments-service.

Houses the fixture-completeness oracle and other sports-specific validation
utilities.  Import from this package, not from sub-modules directly.
"""

from instruments_service.sports.fixture_completeness import (
    CompletenessReport,
    FixtureDefect,
    FixtureDefectKind,
    validate_fixture_completeness,
)

__all__ = [
    "CompletenessReport",
    "FixtureDefect",
    "FixtureDefectKind",
    "validate_fixture_completeness",
]
