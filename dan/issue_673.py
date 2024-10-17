import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from dan import utils
from dan.constants import TASK_QUEUE, WORKFLOW_ID


@activity.defn
async def test_activity() -> None:
    return


@workflow.defn
class Workflow:
    def __init__(self) -> None:
        self.running_count = 0
        self.state = "stopped"

    @workflow.run
    async def run(self) -> None:
        await asyncio.sleep(1)

    @workflow.signal
    async def start(self) -> None:
        await self.run_one()

    @workflow.update
    async def resume(self) -> None:
        if self.state == "running":
            return
        await self.run_one()

    async def run_one(self):
        self.running_count += 1
        self.state = "running"
        await workflow.execute_activity(
            test_activity,
            start_to_close_timeout=timedelta(seconds=1),
        )
        self.running_count -= 1
        if self.running_count == 0:
            self.state = "stopped"


activities = [test_activity]


async def original_main() -> None:
    client = await Client.connect("localhost:7233")
    async with Worker(
        client=client,
        task_queue=TASK_QUEUE,
        workflows=[Workflow],
        activities=activities,
    ):
        await starter()


async def starter():
    client = await utils.connect("Client")
    workflow_handle = await utils.start_workflow(Workflow.run, client=client)
    await workflow_handle.signal(Workflow.start)
    await asyncio.sleep(0.5)
    await workflow_handle.execute_update(Workflow.resume)
    await workflow_handle.result()

    workflows = client.list_workflows(f"WorkflowId = '{WORKFLOW_ID}'")
    histories = workflows.map_histories()
    replayer = Replayer(workflows=[Workflow])
    await replayer.replay_workflows(histories)


if __name__ == "__main__":
    asyncio.run(starter())
