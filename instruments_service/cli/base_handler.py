"""
CLI Base Handler

Abstract base class for all CLI mode handlers following unified repository structure.
Provides the common interface contract that all handlers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ModeHandler(ABC):
    """
    Abstract base class for different CLI operation modes.

    This class provides the common interface and shared functionality
    for all CLI handlers. Each handler represents a specific operation mode
    (e.g., instruments, instruments-query).

    Attributes:
        config: Configuration dictionary containing all settings

    Example:
        class MyHandler(ModeHandler):
            def run(self, **kwargs):
                # Implement specific mode logic
                return {"status": "success"}
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the handler with configuration.

        Args:
            config: Configuration dictionary containing all required settings
                   including GCP credentials, project ID, etc.

        Raises:
            ValueError: If required configuration is missing
        """
        if not config:
            raise ValueError("Configuration is required")

        self.config = config

        # Validate critical configuration
        self._validate_config()

        logger.debug(f"Initialized {self.__class__.__name__}")

    def _validate_config(self) -> None:
        """
        Validate critical configuration parameters.

        Raises:
            ValueError: If required configuration is missing or invalid
        """
        # Basic validation - can be extended by subclasses
        if not isinstance(self.config, dict):
            raise ValueError("Configuration must be a dictionary")

    @abstractmethod
    def run(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Execute the specific mode logic.

        This method must be implemented by all subclasses to define
        the specific business logic for each CLI operation mode.

        Args:
            **kwargs: Variable keyword arguments passed from CLI parsing.
                     Common arguments include:
                     - start_date: Start date for data operations
                     - end_date: End date for data operations
                     - force: Force re-processing flag

        Returns:
            Dict containing operation results with at least:
            - status: "success" | "error" | "warning"
            - message: Human-readable result description
            - Additional mode-specific result data

        Raises:
            NotImplementedError: If not implemented in subclass
            Exception: Mode-specific exceptions based on operation

        Example:
            result = handler.run(
                start_date="2023-10-26",
                end_date="2023-10-27",
                force=False
            )
            # Returns: {
            #     "status": "success",
            #     "message": "Processed 15 instruments",
            #     "processed_count": 15,
            #     "failed_count": 0
            # }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement run() method"
        )

    def cleanup(self) -> None:
        """
        Cleanup resources after operation completion.

        This method is called after run() completes (successfully or with errors)
        to ensure proper resource cleanup. Override in subclasses if specific
        cleanup is needed.

        Default implementation performs basic cleanup logging.
        """
        logger.debug(f"Cleaning up {self.__class__.__name__}")

    def __repr__(self) -> str:
        """String representation of the handler."""
        return f"{self.__class__.__name__}()"
