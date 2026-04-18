import asyncio

from temporalio.client import Client
from temporalio.envconfig import ClientConfig

from hello_standalone_activity.my_workflow import MyJobHandler, MyJobInput


async def my_application():
    connect_config = ClientConfig.load_client_connect_config()
    connect_config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**connect_config)

    handle = await client.start_workflow(
        MyJobHandler.run,
        MyJobInput("my-val"),
        id="my-job-identifier",
        task_queue="my-task-queue",
    )

    result = await handle.result()

    print(f"Activity result: {result}")


if __name__ == "__main__":
    asyncio.run(my_application())
