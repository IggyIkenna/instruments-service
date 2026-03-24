"""Schema definitions for instruments-service.

INSTRUMENTS_SCHEMA is the canonical output contract for instrument parquet files.
The SSOT lives in unified_internal_contracts.domain.instruments — do not define
a local copy here. This module re-exports it for backward-compatible imports.
"""

from unified_internal_contracts.domain.instruments import INSTRUMENTS_SCHEMA

__all__ = ["INSTRUMENTS_SCHEMA"]
