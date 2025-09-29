import asyncio
import logging
from typing import Optional

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.worker import Worker

from message_passing.introduction.activities import call_greeting_service
from message_passing.introduction.workflows import GreetingWorkflow
from nexus_sync_operations.handler.service_handler import GreetingServiceHandler

interrupt_event = asyncio.Event()

NAMESPACE = "nexus-sync-operations-handler-namespace"
TASK_QUEUE = "nexus-sync-operations-handler-task-queue"


async def main(client: Optional[Client] = None):
    logging.basicConfig(level=logging.INFO)

    client = client or await Client.connect(
        "localhost:7233",
        namespace=NAMESPACE,
    )

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[call_greeting_service],
    ):
        long_running_workflow_handle = await client.start_workflow(
            GreetingWorkflow.run,
            id="nexus-sync-operations-greeting-workflow",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            nexus_service_handlers=[
                GreetingServiceHandler(long_running_workflow_handle)
            ],
        ):
            logging.info("Worker started, ctrl+c to exit")
            await interrupt_event.wait()
            logging.info("Shutting down")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
