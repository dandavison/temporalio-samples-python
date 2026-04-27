import asyncio
import uuid

from temporalio import workflow
from temporalio.client import Client, WorkflowExecution
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await asyncio.sleep(1)
        return f"Hello, {name}!"


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)
    task_queue = f"hello-list-workflows-task-queue-{uuid.uuid4()}"
    workflow_id = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[GreetingWorkflow],
    ):
        handle = await client.start_workflow(
            GreetingWorkflow.run,
            "World",
            id=workflow_id,
            task_queue=task_queue,
        )

        await wait_for_workflow_on_task_queue(client, task_queue, workflow_id)

        for info in await list_workflows_on_task_queue(client, task_queue):
            status = info.status.name if info.status else "UNKNOWN"
            print(
                f"WorkflowID: {info.id}, Type: {info.workflow_type}, TaskQueue: {info.task_queue}, Status: {status}"
            )

        print(f"Result: {await handle.result()}")


async def list_workflows_on_task_queue(
    client: Client, task_queue: str
) -> list[WorkflowExecution]:
    return [
        info
        async for info in client.list_workflows(
            query=f"TaskQueue = '{task_queue}'",
        )
    ]


async def wait_for_workflow_on_task_queue(
    client: Client, task_queue: str, workflow_id: str
) -> WorkflowExecution:
    for _ in range(50):
        for info in await list_workflows_on_task_queue(client, task_queue):
            if info.id == workflow_id:
                return info
        await asyncio.sleep(0.1)
    raise RuntimeError(f"Workflow {workflow_id} did not become visible")


if __name__ == "__main__":
    asyncio.run(main())
