#!/usr/bin/env python
from instruments_service.cli.handlers import get_handler_for_mode
import traceback

try:
    handler = get_handler_for_mode('instruments-query', {})
    print('✅ Handler created successfully')
    print(f'Handler type: {type(handler)}')
except Exception as e:
    print('❌ Error creating handler:')
    traceback.print_exc()

