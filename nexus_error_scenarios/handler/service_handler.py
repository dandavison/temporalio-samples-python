from __future__ import annotations

import uuid

import nexusrpc
from temporalio import nexus

from nexus_error_scenarios.handler.workflows import ErrorScenarioWorkflow
from nexus_error_scenarios.service import (
    ErrorScenarioInput,
    ErrorScenarioNexusService,
    ErrorScenarioOutput,
)


@nexusrpc.handler.service_handler(service=ErrorScenarioNexusService)
class ErrorScenarioNexusServiceHandler:
    @nexus.workflow_run_operation
    async def workflow_run_operation(
        self, ctx: nexus.WorkflowRunOperationContext, input: ErrorScenarioInput
    ) -> nexus.WorkflowHandle[ErrorScenarioOutput]:
        if input.scenario == "handler-error":
            raise nexusrpc.HandlerError(
                "handler rejected the request",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
            )
        return await ctx.start_workflow(
            ErrorScenarioWorkflow.run,
            input,
            id=str(uuid.uuid4()),
        )

    @nexusrpc.handler.sync_operation
    async def sync_operation(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ErrorScenarioInput
    ) -> ErrorScenarioOutput:
        if input.scenario == "sync-handler-error":
            raise nexusrpc.HandlerError(
                "sync operation not found error",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            )
        if input.scenario == "sync-operation-error":
            raise nexusrpc.OperationError(
                "sync operation failed",
                state=nexusrpc.OperationErrorState.FAILED,
            )
        return ErrorScenarioOutput(message=f"Completed scenario: {input.scenario}")
