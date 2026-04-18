import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from hello_standalone_activity.my_activity import MyJobInput, my_job_handler


async def test_execute_standalone_activity(client: Client):
    pytest.skip(
        "Standalone Activity is not yet supported by `temporal server start-dev`"
    )
    task_queue_name = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        activities=[my_job_handler],
        activity_executor=ThreadPoolExecutor(5),
    ):
        result = await client.execute_activity(
            my_job_handler,
            args=[MyJobInput("Hello", "World")],
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
            start_to_close_timeout=timedelta(seconds=10),
        )
        assert result == "Hello, World!"
