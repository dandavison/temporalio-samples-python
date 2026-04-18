import asyncio
from datetime import timedelta

from temporalio.client import Client
from temporalio.envconfig import ClientConfig

from hello_standalone_activity.my_activity import MyJobInput, my_job_handler


async def my_application():
    connect_config = ClientConfig.load_client_connect_config()
    connect_config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**connect_config)

    handle = await client.start_activity(
        my_job_handler,
        MyJobInput("my-val"),
        id="my-job-identifier",
        task_queue="my-task-queue",
        start_to_close_timeout=timedelta(seconds=5),
    )

    result = await handle.result()

    print(f"Activity result: {result}")


if __name__ == "__main__":
    asyncio.run(my_application())
