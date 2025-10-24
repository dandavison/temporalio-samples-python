import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
async def my_activity(input: int) -> int:
    print(f"Activity input:  {input}")
    return input + 1


@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            my_activity,
            1,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def my_client_code(client: Client):
    result = await client.execute_workflow(
        MyWorkflow.run,
        id="wid",
        task_queue="tq",
    )
    print(f"Activity result: {result}")


async def main():
    client = await Client.connect("localhost:12345")
    async with Worker(
        client,
        task_queue="tq",
        workflows=[MyWorkflow],
        activities=[my_activity],
        max_concurrent_activity_task_polls=2,
        max_concurrent_workflow_task_polls=2,
    ):
        await my_client_code(client)


if __name__ == "__main__":
    asyncio.run(main())
