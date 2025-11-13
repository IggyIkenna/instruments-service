#!/usr/bin/env python
import sys

sys.path.insert(
    0, "/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/instruments-service"
)

# Force fresh import
if "instruments_service.cli.handlers" in sys.modules:
    del sys.modules["instruments_service.cli.handlers"]
if "instruments_service.cli.handlers.__init__" in sys.modules:
    del sys.modules["instruments_service.cli.handlers.__init__"]

from instruments_service.cli.handlers import get_handler_for_mode
import traceback

# Check registry before calling
from instruments_service.cli.handlers import _handler_registry

print(f"Registry before call: {_handler_registry}")

try:
    handler = get_handler_for_mode("instruments-query", {})
    print("✅ Handler created successfully")
    print(f"Handler type: {type(handler)}")
    print(f"Registry after call: {_handler_registry}")
except Exception as e:
    print("❌ Error creating handler:")
    traceback.print_exc()
    print(f"Registry after error: {_handler_registry}")
