from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nexus_error_scenarios.service import ErrorScenarioInput, ErrorScenarioOutput


@workflow.defn
class ErrorScenarioWorkflow:
    @workflow.run
    async def run(self, input: ErrorScenarioInput) -> ErrorScenarioOutput:
        if input.scenario == "application-error":
            raise ApplicationError(
                "intentional failure from handler workflow",
                type="InvalidInput",
                non_retryable=True,
            )
        return ErrorScenarioOutput(message=f"Completed scenario: {input.scenario}")
