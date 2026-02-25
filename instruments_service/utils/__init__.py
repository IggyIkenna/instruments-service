"""
Utility modules for instruments-service.

Re-exports from unified-cloud-services to avoid duplication.
Local implementations kept only for instruments-specific functionality.
"""

# Re-export from split libraries (DateFilterService in UDS, SubgraphService in UMI)
from unified_domain_services import DateFilterService
from unified_market_interface import SubgraphService

# Optional UCS imports (http_utils/web3_utils may not exist in all UCS versions)
try:
    from unified_cloud_services import clear_http_pool, clear_pool, clear_web3_pool, get_http_session, get_web3_client

    _HAS_HTTP_WEB3 = True
except ImportError:
    clear_http_pool = None  # pyright: ignore[reportConstantRedefinition]
    clear_web3_pool = None  # pyright: ignore[reportConstantRedefinition]
    get_http_session = None  # pyright: ignore[reportConstantRedefinition]
    get_web3_client = None  # pyright: ignore[reportConstantRedefinition]
    clear_pool = None  # pyright: ignore[reportConstantRedefinition]
    _HAS_HTTP_WEB3 = False

# Local implementations (instruments-service specific)
from instruments_service.utils.ccxt_service import CCXTService

__all__ = [
    # Local services
    "CCXTService",
    "DateFilterService",
    # Re-exported from split libraries
    "SubgraphService",
    "clear_http_pool",
    "clear_pool",
    "clear_web3_pool",
    "get_http_session",
    "get_web3_client",
]
