import uuid

from temporalio.client import Client
from temporalio.worker import Worker

from hello.hello_list_workflows import GreetingWorkflow, wait_for_workflow_on_task_queue


async def test_list_workflows_by_task_queue(client: Client):
    task_queue_name = str(uuid.uuid4())
    workflow_id = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[GreetingWorkflow],
    ):
        handle = await client.start_workflow(
            GreetingWorkflow.run,
            "World",
            id=workflow_id,
            task_queue=task_queue_name,
        )

        info = await wait_for_workflow_on_task_queue(
            client,
            task_queue_name,
            workflow_id,
        )

        assert info.id == workflow_id
        assert info.task_queue == task_queue_name
        assert info.workflow_type == "GreetingWorkflow"

        assert await handle.result() == "Hello, World!"
