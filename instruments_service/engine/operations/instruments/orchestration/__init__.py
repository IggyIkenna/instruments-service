"""Instruments Orchestration Module

Coordinates instrument generation workflow across market types (CeFi, TradFi, DeFi).
Split into multiple files for maintainability.
"""

from .orchestrator import InstrumentsOrchestrator, InstrumentStorageProtocol

__all__ = ["InstrumentStorageProtocol", "InstrumentsOrchestrator"]
