"""
Instrument Processors Package

Category-specific processors for instrument operations:
- BaseInstrumentProcessor: Shared functionality
- CeFiInstrumentProcessor: CeFi-specific logic
- TradFiInstrumentProcessor: TradFi-specific logic
- DeFiInstrumentProcessor: DeFi-specific logic
"""

from instruments_service.engine.operations.instruments.processors.base_processor import (
    BaseInstrumentProcessor,
    InstrumentProcessingConfig,
)
from instruments_service.engine.operations.instruments.processors.cefi_processor import CeFiInstrumentProcessor
from instruments_service.engine.operations.instruments.processors.defi_processor import DeFiInstrumentProcessor
from instruments_service.engine.operations.instruments.processors.tradfi_processor import TradFiInstrumentProcessor

__all__ = [
    "BaseInstrumentProcessor",
    "CeFiInstrumentProcessor",
    "DeFiInstrumentProcessor",
    "InstrumentProcessingConfig",
    "TradFiInstrumentProcessor",
]
