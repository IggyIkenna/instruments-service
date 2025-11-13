#!/usr/bin/env python
import sys
import traceback

# Clear any cached modules
modules_to_clear = [k for k in sys.modules.keys() if "instruments_service.cli.handlers" in k]
for mod in modules_to_clear:
    del sys.modules[mod]

# Now import and trace
print("Importing handlers module...")
try:
    from instruments_service.cli.handlers.instrument_handler import InstrumentHandler

    print(f"✅ InstrumentHandler: {InstrumentHandler}")
except Exception as e:
    print(f"❌ Error importing InstrumentHandler: {e}")
    traceback.print_exc()

try:
    from instruments_service.cli.handlers.instruments_query_handler import InstrumentsQueryHandler

    print(f"✅ InstrumentsQueryHandler: {InstrumentsQueryHandler}")
except Exception as e:
    print(f"❌ Error importing InstrumentsQueryHandler: {e}")
    traceback.print_exc()

# Now check registry
from instruments_service.cli.handlers import _handler_registry, register_handler

print(f"\nRegistry before manual registration: {_handler_registry}")

# Manually register
try:
    register_handler("instruments", InstrumentHandler)
    print(f"✅ Registered InstrumentHandler")
    register_handler("instruments-query", InstrumentsQueryHandler)
    print(f"✅ Registered InstrumentsQueryHandler")
    print(f"\nRegistry after manual registration: {_handler_registry}")
except Exception as e:
    print(f"❌ Error during registration: {e}")
    traceback.print_exc()
