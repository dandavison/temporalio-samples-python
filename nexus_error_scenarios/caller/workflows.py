import nexusrpc
from temporalio import workflow
from temporalio.exceptions import ApplicationError, NexusOperationError

with workflow.unsafe.imports_passed_through():
    from nexus_error_scenarios.service import (
        ErrorScenarioInput,
        ErrorScenarioNexusService,
    )

NEXUS_ENDPOINT = "nexus-error-scenarios-nexus-endpoint"

SYNC_SCENARIOS = {"sync-handler-error", "sync-operation-error"}


@workflow.defn
class CallerWorkflow:
    def __init__(self):
        self.nexus_client = workflow.create_nexus_client(
            service=ErrorScenarioNexusService,
            endpoint=NEXUS_ENDPOINT,
        )

    @workflow.run
    async def run(self, scenario: str) -> str:
        op = (
            ErrorScenarioNexusService.sync_operation
            if scenario in SYNC_SCENARIOS
            else ErrorScenarioNexusService.workflow_run_operation
        )
        try:
            result = await self.nexus_client.execute_operation(
                op,
                ErrorScenarioInput(scenario),
            )
            return f"Operation succeeded: {result.message}"
        except NexusOperationError as err:
            cause = err.cause
            if isinstance(cause, ApplicationError):
                return (
                    f"Caught NexusOperationError caused by ApplicationError: "
                    f"message='{cause.message}', "
                    f"type='{cause.type}', "
                    f"non_retryable={cause.non_retryable}"
                )
            if isinstance(cause, nexusrpc.HandlerError):
                return (
                    f"Caught NexusOperationError caused by HandlerError: "
                    f"message='{cause}', "
                    f"type={cause.type}"
                )
            return (
                f"Caught NexusOperationError: "
                f"message='{err.message}', "
                f"cause={type(cause).__name__}: {cause}"
            )
