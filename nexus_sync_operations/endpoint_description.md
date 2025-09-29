## Service: [GreetingService](https://github.com/temporalio/samples-python/blob/main/nexus_sync_operations/service.py)
- operation: `get_languages` - Query: Get list of supported languages
- operation: `get_language` - Query: Get current language setting
- operation: `set_language` - Update: Change the language setting (using activity)
- operation: `approve` - Signal: Approve the workflow for greeting release
- operation: `fetch_greeting_translation` - Update: Fetch greeting after approval (waits for approval)
- operation: `get_operation_log` - Query: Get log of all operations performed