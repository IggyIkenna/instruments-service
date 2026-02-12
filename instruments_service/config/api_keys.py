"""
API keys and secret configuration for instruments-service.

Secret names and API URL defaults used by InstrumentsServiceConfig.
The actual values are loaded from environment variables via Pydantic Settings.
"""

# Default secret names (overridable via env)
DEFAULT_GRAPH_SECRET_NAME = "graph-api-key"
DEFAULT_AAVESCAN_API_URL = "https://api.aavescan.com/v2"
DEFAULT_THEGRAPH_GATEWAY_TEMPLATE = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
