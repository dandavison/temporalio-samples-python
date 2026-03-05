from temporalio import workflow
from temporalio.exceptions import ApplicationError, NexusOperationError

with workflow.unsafe.imports_passed_through():
    from nexus_error_scenarios.service import (
        ErrorScenarioInput,
        ErrorScenarioNexusService,
    )

NEXUS_ENDPOINT = "nexus-error-scenarios-nexus-endpoint"


@workflow.defn
class CallerWorkflow:
    def __init__(self):
        self.nexus_client = workflow.create_nexus_client(
            service=ErrorScenarioNexusService,
            endpoint=NEXUS_ENDPOINT,
        )

    @workflow.run
    async def run(self, scenario: str) -> str:
        try:
            result = await self.nexus_client.execute_operation(
                ErrorScenarioNexusService.error_scenario_operation,
                ErrorScenarioInput(scenario),
            )
            return f"Operation succeeded: {result.message}"
        except NexusOperationError as err:
            if isinstance(err.cause, ApplicationError):
                return (
                    f"Caught NexusOperationError caused by ApplicationError: "
                    f"message='{err.cause.message}', "
                    f"type='{err.cause.type}', "
                    f"non_retryable={err.cause.non_retryable}"
                )
            return f"Caught NexusOperationError: {err}"
