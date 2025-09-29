"""
This is a workflow that calls nexus operations. The caller does not have information about how these
operations are implemented by the nexus service.
"""

from typing import List

from message_passing.introduction import Language
from message_passing.introduction.workflows import (
    ApproveInput,
    GetLanguagesInput,
    SetLanguageInput,
)
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nexus_sync_operations.service import GreetingService

NEXUS_ENDPOINT = "nexus-sync-operations-nexus-endpoint"


@workflow.defn
class CallerWorkflow:
    @workflow.run
    async def run(self) -> List[str]:
        nexus_client = workflow.create_nexus_client(
            service=GreetingService,
            endpoint=NEXUS_ENDPOINT,
        )

        # Get supported languages
        supported_languages = await nexus_client.execute_operation(
            GreetingService.get_languages, GetLanguagesInput(include_unsupported=False)
        )
        print(f"supported languages: {supported_languages}")

        # Set language to Arabic
        previous_language = await nexus_client.execute_operation(
            GreetingService.set_language,
            SetLanguageInput(language=Language.ARABIC),
        )
        assert (
            await nexus_client.execute_operation(GreetingService.get_language, None)
            == Language.ARABIC
        )
        print(f"language changed: {previous_language.name} -> {Language.ARABIC.name}")

        # Send approval signal
        await nexus_client.execute_operation(
            GreetingService.approve, ApproveInput(name="Nexus Caller")
        )
        print("Sent approval signal")

        # Fetch greeting translation
        greeting = await nexus_client.execute_operation(
            GreetingService.fetch_greeting_translation, None
        )
        print(f"Fetched greeting: {greeting}")

        # Get operation log to verify everything worked
        operation_log = await nexus_client.execute_operation(
            GreetingService.get_operation_log, None
        )
        print(f"Operation log: {operation_log}")

        return operation_log
