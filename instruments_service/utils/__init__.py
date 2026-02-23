"""
Utility modules for instruments-service.

Re-exports from unified-cloud-services to avoid duplication.
Local implementations kept only for instruments-specific functionality.
"""

# Re-export from split libraries (DateFilterService in UDS, SubgraphService in UMI)
from unified_domain_services.instrument_date_filter import DateFilterService
from unified_market_interface import SubgraphService

# Optional: http_session and web3_client moved to unified-market-interface (UCS v1.6.0)
# Services that need them should import from unified_market_interface.utils when available
try:
    from unified_cloud_services import get_http_session, get_web3_client  # pyright: ignore
    from unified_cloud_services.core.http_session_pool import (
        clear_pool as clear_http_pool,  # pyright: ignore[reportUnknownVariableType]
    )
    from unified_cloud_services.core.web3_client_pool import (
        clear_pool as clear_web3_pool,  # pyright: ignore[reportUnknownVariableType]
    )

    _HAS_HTTP_WEB3 = True
except ImportError:
    clear_http_pool = None  # pyright: ignore[reportConstantRedefinition]
    clear_web3_pool = None  # pyright: ignore[reportConstantRedefinition]
    get_http_session = None  # pyright: ignore[reportConstantRedefinition]
    get_web3_client = None  # pyright: ignore[reportConstantRedefinition]
    _HAS_HTTP_WEB3 = False

# Local implementations (instruments-service specific)
from instruments_service.utils.ccxt_service import CCXTService

__all__ = [
    # Re-exported from unified-cloud-services
    "SubgraphService",
    "DateFilterService",
    "get_web3_client",
    "clear_web3_pool",
    "get_http_session",
    "clear_http_pool",
    # Local services
    "CCXTService",
]
