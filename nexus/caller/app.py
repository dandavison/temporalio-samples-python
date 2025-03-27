import asyncio
import sys

from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from nexus.caller.workflows import (
    Echo2CallerWorkflow,
    EchoCallerWorkflow,
    HelloCallerWorkflow,
)

interrupt_event = asyncio.Event()


async def execute_echo_caller_workflow():
    client = await Client.connect("localhost:7233", namespace="my-caller-namespace")
    task_queue = "my-caller-task-queue"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[EchoCallerWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        print("🟠 Caller worker started")
        result = await client.execute_workflow(
            EchoCallerWorkflow.run,
            "hello",
            id="my-caller-workflow-id",
            task_queue=task_queue,
        )
        print("🟢 workflow result:", result)


async def execute_hello_caller_workflow():
    client = await Client.connect("localhost:7233", namespace="my-caller-namespace")
    task_queue = "my-caller-task-queue"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[HelloCallerWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        print("🟠 Caller worker started")
        result = await client.execute_workflow(
            HelloCallerWorkflow.run,
            "world",
            id="my-caller-workflow-id",
            task_queue=task_queue,
        )
        print("🟢 workflow result:", result)


async def execute_echo2_caller_workflow():
    client = await Client.connect("localhost:7233", namespace="my-caller-namespace")
    task_queue = "my-caller-task-queue"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Echo2CallerWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        print("🟠 Caller worker started")
        result = await client.execute_workflow(
            Echo2CallerWorkflow.run,
            "hello",
            id="my-caller-workflow-id",
            task_queue=task_queue,
        )
        print("🟢 workflow result:", result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m nexus.caller.app [echo|hello]")
        sys.exit(1)

    [wf_name] = sys.argv[1:]
    fn = {
        "echo": execute_echo_caller_workflow,
        "hello": execute_hello_caller_workflow,
        "echo2": execute_echo2_caller_workflow,
    }[wf_name]

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(fn())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
