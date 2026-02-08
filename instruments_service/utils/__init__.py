"""
Utility modules for instruments-service.

Re-exports from unified-cloud-services to avoid duplication.
Local implementations kept only for instruments-specific functionality.
"""

# Re-export from unified-cloud-services (centralized implementations)
from unified_cloud_services import (
    DateFilterService,
    SubgraphService,
    get_http_session,
    get_web3_client,
)

# Import local clear functions that wrap UCS internals
from unified_cloud_services.core.http_session_pool import clear_pool as clear_http_pool
from unified_cloud_services.core.web3_client_pool import clear_pool as clear_web3_pool

# Local implementations (instruments-service specific)
from instruments_service.utils.ccxt_service import CCXTService

__all__ = [
    # Re-exported from unified-cloud-services
    "get_web3_client",
    "clear_web3_pool",
    "get_http_session",
    "clear_http_pool",
    "SubgraphService",
    "DateFilterService",
    # Local services
    "CCXTService",
]
