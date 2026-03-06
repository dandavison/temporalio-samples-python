import asyncio
import uuid
from typing import Optional

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from nexus_error_scenarios.caller.workflows import CallerWorkflow
from nexus_error_scenarios.service import SCENARIOS

NAMESPACE = "nexus-error-scenarios-caller-namespace"
TASK_QUEUE = "nexus-error-scenarios-caller-task-queue"

SCENARIO_DESCRIPTIONS = {
    "application-error": (
        "Handler workflow raises ApplicationError("
        '"intentional failure from handler workflow", '
        'type="InvalidInput", non_retryable=True)'
    ),
    "application-error-default": (
        "Handler workflow raises ApplicationError("
        '"handler workflow failed") with default settings'
    ),
    "handler-error": (
        "workflow_run_operation handler raises HandlerError("
        '"handler rejected the request", type=BAD_REQUEST) '
        "before starting the workflow"
    ),
    "sync-handler-error": (
        "sync_operation handler raises HandlerError("
        '"sync operation not found error", type=NOT_FOUND)'
    ),
    "sync-operation-error": (
        "sync_operation handler raises OperationError("
        '"sync operation failed", state=FAILED)'
    ),
}


async def execute_caller_workflow(
    client: Optional[Client] = None,
    scenario: str = "application-error",
) -> str:
    if not client:
        config = ClientConfig.load_client_connect_config()
        config.setdefault("target_host", "localhost:7233")
        config.setdefault("namespace", NAMESPACE)
        client = await Client.connect(**config)

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CallerWorkflow],
    ):
        return await client.execute_workflow(
            CallerWorkflow.run,
            arg=scenario,
            id=str(uuid.uuid4()),
            task_queue=TASK_QUEUE,
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        for scenario in SCENARIOS:
            print(f"\n--- Scenario: {scenario} ---")
            print(f"  Handler: {SCENARIO_DESCRIPTIONS[scenario]}")
            result = loop.run_until_complete(execute_caller_workflow(scenario=scenario))
            print(f"  Caller:  {result}")
    except KeyboardInterrupt:
        loop.run_until_complete(loop.shutdown_asyncgens())
