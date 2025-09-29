import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from message_passing.introduction import Language
    from message_passing.introduction.activities import call_greeting_service


@dataclass
class GetLanguagesInput:
    include_unsupported: bool


@dataclass
class SetLanguageInput:
    language: Language


@dataclass
class ApproveInput:
    name: str


@workflow.defn
class GreetingWorkflow:
    """
    A never-ending entity workflow that manages greeting translations in multiple
    languages.

    It exposes queries, signals, and updates to interact with the workflow state.
    The workflow maintains a log of all operations performed on it.

    This is a true entity workflow that runs indefinitely, processing operations
    as they come in rather than completing after a single approval.
    """

    def __init__(self) -> None:
        self.approved_for_release = False
        self.approver_name: Optional[str] = None
        self.greetings = {
            Language.CHINESE: "你好，世界",
            Language.ENGLISH: "Hello, world",
        }
        self.language = Language.ENGLISH
        self.lock = asyncio.Lock()  # used by the async handler below
        self.operation_log: List[str] = []  # Log of operations performed

    @workflow.run
    async def run(self) -> List[str]:
        # This is a never-ending entity workflow that runs indefinitely
        # It will only return the log if explicitly requested (e.g., via cancellation)
        self.operation_log.append("Workflow started")
        try:
            # Run forever, processing operations as they come
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            # Return the log when the workflow is cancelled
            self.operation_log.append("Workflow cancelled")
            return self.operation_log

        # This should never be reached in normal operation
        return self.operation_log

    @workflow.query
    def get_languages(self, input: GetLanguagesInput) -> List[Language]:
        # 👉 A Query handler returns a value: it can inspect but must not mutate the Workflow state.
        if input.include_unsupported:
            return sorted(Language)
        else:
            return sorted(self.greetings)

    @workflow.query
    def get_operation_log(self) -> List[str]:
        """Get the log of operations performed on this workflow."""
        return self.operation_log.copy()

    @workflow.signal
    def approve(self, input: ApproveInput) -> None:
        # 👉 A Signal handler mutates the Workflow state but cannot return a value.
        self.approved_for_release = True
        self.approver_name = input.name
        self.operation_log.append(
            f"Approved by {input.name if input.name else 'anonymous'}"
        )

    @workflow.update
    def set_language(self, input: SetLanguageInput) -> Language:
        # 👉 An Update handler can mutate the Workflow state and return a value.
        previous_language, self.language = self.language, input.language
        self.operation_log.append(
            f"Language changed from {previous_language.name} to {input.language.name}"
        )
        return previous_language

    @set_language.validator
    def validate_language(self, input: SetLanguageInput) -> None:
        if input.language not in self.greetings:
            # 👉 In an Update validator you raise any exception to reject the Update.
            raise ValueError(f"{input.language.name} is not supported")

    @workflow.update
    async def set_language_using_activity(self, input: SetLanguageInput) -> Language:
        # 👉 This update handler is async, so it can execute an activity.
        if input.language not in self.greetings:
            # 👉 We use a lock so that, if this handler is executed multiple
            # times, each execution can schedule the activity only when the
            # previously scheduled activity has completed. This ensures that
            # multiple calls to set_language are processed in order.
            async with self.lock:
                greeting = await workflow.execute_activity(
                    call_greeting_service,
                    input.language,
                    start_to_close_timeout=timedelta(seconds=10),
                )
                # 👉 The requested language might not be supported by the remote
                # service. If so, we raise ApplicationError, which will fail the
                # Update. The WorkflowExecutionUpdateAccepted event will still
                # be added to history. (Update validators can be used to reject
                # updates before any event is written to history, but they
                # cannot be async, and so we cannot use an update validator for
                # this purpose.)
                if greeting is None:
                    raise ApplicationError(
                        f"Greeting service does not support {input.language.name}"
                    )
                self.greetings[input.language] = greeting
        previous_language, self.language = self.language, input.language
        self.operation_log.append(
            f"Language changed from {previous_language.name} to {input.language.name} (using activity)"
        )
        return previous_language

    @workflow.update
    async def fetch_greeting_translation(self) -> str:
        """
        Fetch the current greeting translation.
        This waits for approval, returns the greeting, then resets approval.
        """
        # Wait for approval
        await workflow.wait_condition(lambda: self.approved_for_release)

        # Get the greeting
        greeting = self.greetings[self.language]

        # Log the operation
        self.operation_log.append(
            f"Fetched greeting '{greeting}' in {self.language.name} for {self.approver_name or 'anonymous'}"
        )

        # Reset approval state for next time
        self.approved_for_release = False
        self.approver_name = None

        return greeting

    @workflow.query
    def get_language(self) -> Language:
        return self.language
