import asyncio
from datetime import timedelta

from temporalio.client import Client
from temporalio.envconfig import ClientConfig

from hello_standalone_activity.my_activity import MyJobInput, my_job_handler


async def my_application():
    connect_config = ClientConfig.load_client_connect_config()
    connect_config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**connect_config)

    activity_result = await client.execute_activity(
        my_job_handler,
        args=[MyJobInput("Hello", "World")],
        id="my-standalone-activity-id",
        task_queue="my-task-queue",
        start_to_close_timeout=timedelta(seconds=10),
    )
    print(f"Activity result: {activity_result}")


if __name__ == "__main__":
    asyncio.run(my_application())
