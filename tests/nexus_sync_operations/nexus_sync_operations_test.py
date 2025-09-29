import asyncio
import uuid

import nexus_sync_operations.caller.workflows
import nexus_sync_operations.handler.worker
import pytest
from nexus_sync_operations.caller.workflows import CallerWorkflow
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.helpers.nexus import create_nexus_endpoint, delete_nexus_endpoint


async def test_nexus_sync_operations(client: Client, env: WorkflowEnvironment):
    if env.supports_time_skipping:
        pytest.skip("Nexus tests don't work under the Java test server")

    create_response = await create_nexus_endpoint(
        name=nexus_sync_operations.caller.workflows.NEXUS_ENDPOINT,
        task_queue=nexus_sync_operations.handler.worker.TASK_QUEUE,
        client=client,
    )
    try:
        # Start the handler worker
        handler_worker_task = asyncio.create_task(
            nexus_sync_operations.handler.worker.main(
                client,
            )
        )

        # Give the handler worker time to start up
        await asyncio.sleep(0.5)

        # Run the caller workflow using a worker
        async with Worker(
            client,
            task_queue="test-caller-task-queue",
            workflows=[CallerWorkflow],
        ):
            # Execute the caller workflow
            operation_log = await client.execute_workflow(
                CallerWorkflow.run,
                id=str(uuid.uuid4()),
                task_queue="test-caller-task-queue",
            )

            # Verify the operation log contains expected entries
            assert "Workflow started" in operation_log
            assert any(
                "Language changed from ENGLISH to ARABIC" in entry
                for entry in operation_log
            )
            assert any("Approved by Nexus Caller" in entry for entry in operation_log)
            assert any(
                "Fetched greeting" in entry and "in ARABIC" in entry
                for entry in operation_log
            )

            print(f"Operation log from test: {operation_log}")

        # Clean up
        nexus_sync_operations.handler.worker.interrupt_event.set()
        await handler_worker_task
        nexus_sync_operations.handler.worker.interrupt_event.clear()
    finally:
        await delete_nexus_endpoint(
            id=create_response.endpoint.id,
            version=create_response.endpoint.version,
            client=client,
        )
