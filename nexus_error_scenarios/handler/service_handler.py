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
    async def error_scenario_operation(
        self, ctx: nexus.WorkflowRunOperationContext, input: ErrorScenarioInput
    ) -> nexus.WorkflowHandle[ErrorScenarioOutput]:
        return await ctx.start_workflow(
            ErrorScenarioWorkflow.run,
            input,
            id=str(uuid.uuid4()),
        )
