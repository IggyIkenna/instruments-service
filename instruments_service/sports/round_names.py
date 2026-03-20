"""Tournament Round Names — re-exported from UAC canonical SSOT.

The canonical round name registry (ROUND_NAMES, ROUND_PREFIXES) and resolution
logic (resolve_round_name, RoundMatch, is_known_round) now live in
unified-api-contracts canonical/domain/sports/round_names.py.

This module re-exports for backward compatibility within instruments-service.
"""

from unified_api_contracts.sports import (  # noqa: deep-import — UAC sports is the correct domain facade
    ROUND_NAMES as ROUND_NAMES,
)
from unified_api_contracts.sports import (
    ROUND_PREFIXES as ROUND_PREFIXES,
)
from unified_api_contracts.sports import (
    RoundMatch as RoundMatch,
)
from unified_api_contracts.sports import (
    is_known_round as is_known_round,
)
from unified_api_contracts.sports import (
    resolve_round_name as resolve_round_name,
)

__all__ = [
    "ROUND_NAMES",
    "ROUND_PREFIXES",
    "RoundMatch",
    "is_known_round",
    "resolve_round_name",
]
