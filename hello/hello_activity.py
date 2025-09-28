import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
async def coin_flip() -> float:
    import random

    return random.random()


@workflow.defn
class RandomNumberOfAttemptsWorkflow:
    @workflow.run
    async def run(self) -> int:
        attempts = 0
        while True:
            attempts += 1
            random_number = await workflow.execute_activity(
                coin_flip,
                start_to_close_timeout=timedelta(seconds=10),
            )
            if random_number < 0.5:
                return attempts


async def main():
    client = await Client.connect("localhost:7233")
    async with Worker(
        client,
        task_queue="coin-flip-task-queue",
        workflows=[RandomNumberOfAttemptsWorkflow],
        activities=[coin_flip],
    ):
        print(
            await client.execute_workflow(
                RandomNumberOfAttemptsWorkflow.run,
                id="coin-flip-workflow-id",
                task_queue="coin-flip-task-queue",
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
