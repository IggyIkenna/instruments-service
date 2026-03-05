"""
Aggregate Operation

Instrument aggregation logic extracted from CLI handlers.
Pure engine logic - no CLI concerns, delegates storage to adapters.
"""

from instruments_service.engine.operations.aggregate.aggregator import InstrumentAggregator

__all__ = ["InstrumentAggregator"]
