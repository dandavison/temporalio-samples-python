import asyncio

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from hello_standalone_activity.my_activity import (
    my_activity_1,
    my_activity_2,
    my_job_handler,
)
from hello_standalone_activity.my_workflow import MyJobHandler


class JobHandlerWorkflow:
    pass


async def run_activity_worker():
    connect_config = ClientConfig.load_client_connect_config()
    client = await Client.connect(**connect_config)

    await Worker(
        client,
        task_queue="my-task-queue",
        activities=[my_job_handler],
    ).run()


async def run_workflow_worker():
    connect_config = ClientConfig.load_client_connect_config()
    client = await Client.connect(**connect_config)

    await Worker(
        client,
        task_queue="my-task-queue",
        workflows=[MyJobHandler],
        activities=[
            my_activity_1,
            my_activity_2,
        ],
    ).run()


if __name__ == "__main__":
    asyncio.run(run_activity_worker())
