import uuid
from concurrent.futures import ThreadPoolExecutor

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from hello.hello_activity import (
    ActivityInput,
    MyWorkflow,
    my_activity,
)


async def test_execute_workflow(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[MyWorkflow],
        activity_executor=ThreadPoolExecutor(5),
        activities=[my_activity],
    ):
        assert "Hello, World!" == await client.execute_workflow(
            MyWorkflow.run,
            "World",
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
        )


@activity.defn(name="compose_greeting")
async def compose_greeting_mocked(input: ActivityInput) -> str:
    return f"{input.greeting}, {input.name} from mocked activity!"


async def test_mock_activity(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[MyWorkflow],
        activities=[compose_greeting_mocked],
    ):
        assert "Hello, World from mocked activity!" == await client.execute_workflow(
            MyWorkflow.run,
            "World",
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
        )
