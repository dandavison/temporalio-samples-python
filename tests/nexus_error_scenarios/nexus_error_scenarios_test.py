import asyncio
import sys

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

import nexus_error_scenarios.caller.app
import nexus_error_scenarios.caller.workflows
import nexus_error_scenarios.handler.worker
from tests.helpers.nexus import create_nexus_endpoint, delete_nexus_endpoint

EXPECTED = {
    "application-error": (
        "Caught NexusOperationError caused by ApplicationError: "
        "message='intentional failure from handler workflow', "
        "type='InvalidInput', "
        "non_retryable=True"
    ),
    "application-error-default": (
        "Caught NexusOperationError caused by ApplicationError: "
        "message='handler workflow failed', "
        "type='None', "
        "non_retryable=False"
    ),
    "handler-error": (
        "Caught NexusOperationError caused by HandlerError: "
        "message='handler error (BAD_REQUEST): handler rejected the request', "
        "type=HandlerErrorType.BAD_REQUEST"
    ),
    "sync-handler-error": (
        "Caught NexusOperationError caused by HandlerError: "
        "message='handler error (NOT_FOUND): sync operation not found error', "
        "type=HandlerErrorType.NOT_FOUND"
    ),
    "sync-operation-error": (
        "Caught NexusOperationError caused by ApplicationError: "
        "message='sync operation failed', "
        "type='NexusFailure', "
        "non_retryable=True"
    ),
}


@pytest.fixture
async def nexus_endpoint(client: Client, env: WorkflowEnvironment):
    if env.supports_time_skipping:
        pytest.skip("Nexus tests don't work under the Java test server")
    if sys.version_info[:2] < (3, 10):
        pytest.skip("Sample is written for Python >= 3.10")

    create_response = await create_nexus_endpoint(
        name=nexus_error_scenarios.caller.workflows.NEXUS_ENDPOINT,
        task_queue=nexus_error_scenarios.handler.worker.TASK_QUEUE,
        client=client,
    )
    handler_worker_task = asyncio.create_task(
        nexus_error_scenarios.handler.worker.main(client)
    )
    await asyncio.sleep(1)
    yield
    nexus_error_scenarios.handler.worker.interrupt_event.set()
    await handler_worker_task
    nexus_error_scenarios.handler.worker.interrupt_event.clear()
    await delete_nexus_endpoint(
        id=create_response.endpoint.id,
        version=create_response.endpoint.version,
        client=client,
    )


@pytest.mark.parametrize("scenario", list(EXPECTED.keys()))
async def test_nexus_error_scenario(
    client: Client, nexus_endpoint: None, scenario: str
):
    result = await nexus_error_scenarios.caller.app.execute_caller_workflow(
        client,
        scenario=scenario,
    )
    assert result == EXPECTED[scenario]
