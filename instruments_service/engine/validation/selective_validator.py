"""
Selective API Key Validation

Only validates API keys for data sources required by requested venues.
Prevents misleading API key errors and reduces Secret Manager API calls.

Complies with:
- Cursor rules: cloud-agnostic (use get_secret_client)
- Codex: 07-security/secrets-management.md
- Audit: SEC-01, SEC-02 (secrets from Secret Manager)
"""

import logging
from typing import Protocol
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import DataSourceMapping
from unified_trading_library import get_secret_client

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


def validate_required_api_keys(venues: list[str], project_id: str | None = None) -> dict[str, str]:
    """
    Validate and fetch API keys for requested venues only.

    Args:
        venues: List of venues to process
        project_id: GCP project ID (defaults to config)

    Returns:
        Dict mapping data source to API key

    Raises:
        ValueError: If required API key is missing for a venue
    """
    if not project_id:
        project_id = instruments_config.gcp_project_id

    # Get required secrets for venues
    required_secrets: dict[str, str] = DataSourceMapping.get_required_secrets(venues)

    if not required_secrets:
        logger.info("No API keys required for requested venues")
        return {}

    logger.info("Validating API keys for data sources: %s", list(required_secrets.keys()))

    api_keys: dict[str, str] = {}
    errors: list[str] = []

    for data_source, _ in required_secrets.items():
        try:
            # Map data source to config secret name
            if data_source == "tardis":
                config_secret_name = instruments_config.tardis_secret_name
            elif data_source == "databento":
                config_secret_name = instruments_config.databento_secret_name
            elif data_source == "thegraph":
                config_secret_name = instruments_config.graph_secret_name
            elif data_source == "alchemy":
                config_secret_name = "alchemy-api-key"
            elif data_source == "barchart":
                config_secret_name = "barchart-api-key"
            else:
                logger.warning("Unknown data source: %s", data_source)
                continue

            # Fetch API key
            api_key = get_secret_client(
                project_id=project_id,
            ).get_secret(config_secret_name)

            if api_key:
                api_keys[data_source] = api_key.strip()
                logger.info("✅ Validated API key for %s", data_source)
            else:
                error_msg = f"Missing API key for {data_source} \
                    (venues: {[v for v in venues if DataSourceMapping.get_data_source_for_venue(v) == data_source]})"
                errors.append(error_msg)
                logger.error(error_msg)

        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = f"Failed to fetch API key for {data_source}: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
    if errors:
        raise ValueError(f"API key validation failed: {'; '.join(errors)}")

    logger.info("✅ Validated %s API keys for %s venues", len(api_keys), len(venues))
    return api_keys


class VenueMappingProtocol(Protocol):
    """Protocol for VenueMapping with category venue attributes."""

    @property
    def all_tardis_exchanges(self) -> list[str]: ...
    @property
    def all_databento_venues(self) -> list[str]: ...
    @property
    def all_defi_venues(self) -> list[str]: ...


def get_venues_for_category(category: str, venue_mapping: VenueMappingProtocol) -> list[str]:
    """
    Get all venues for a category.

    Args:
        category: Category name ("CEFI", "TRADFI", "DEFI")
        venue_mapping: VenueMapping instance

    Returns:
        List of venue names
    """
    if category == "CEFI":
        return venue_mapping.all_tardis_exchanges
    elif category == "TRADFI":
        return venue_mapping.all_databento_venues
    elif category == "DEFI":
        return venue_mapping.all_defi_venues
    else:
        raise ValueError(f"Unknown category: {category}")
