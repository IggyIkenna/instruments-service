#!/usr/bin/env python
import sys
import traceback

# Clear modules
modules_to_clear = [k for k in sys.modules.keys() if "instruments_service.cli.handlers" in k]
for mod in modules_to_clear:
    del sys.modules[mod]

from instruments_service.cli.handlers import get_handler_for_mode, _handler_registry

print(f"Registry before: {_handler_registry}")

# Add try-except around the import
import sys

original_import = __import__


def debug_import(name, *args, **kwargs):
    if "instrument_handler" in name or "instruments_query_handler" in name:
        print(f"🔍 Importing: {name}")
    return original_import(name, *args, **kwargs)


# Monkey patch import temporarily
sys.modules["__builtins__"].__import__ = debug_import

try:
    handler = get_handler_for_mode("instruments-query", {})
    print(f"✅ Handler created: {type(handler)}")
    print(f"Registry after: {_handler_registry}")
except Exception as e:
    print(f"❌ Error: {e}")
    traceback.print_exc()
    print(f"Registry after error: {_handler_registry}")
