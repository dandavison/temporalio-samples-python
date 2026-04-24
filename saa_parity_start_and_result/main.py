import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

from saa_parity_start_and_result.activities import add
from saa_parity_start_and_result.workflow import AddWorkflow

TASK = f"py-saa-parity-start_and_result-{uuid.uuid4()}"


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[AddWorkflow],
        activities=[add],
    ):
        wf_id = f"saa-start-result-wf-{uuid.uuid4()}"
        handle: WorkflowHandle = await client.start_workflow(
            AddWorkflow.run,
            args=[3, 4],
            id=wf_id,
            task_queue=TASK,
        )
        wf_result = await handle.result()
        if wf_result != 7:
            print(f"MISMATCH: workflow result {wf_result!r}, expected 7")
            sys.exit(1)

        saa_result = await client.execute_activity(
            add,
            args=[3, 4],
            id=f"sa-{uuid.uuid4()}",
            task_queue=TASK,
            start_to_close_timeout=timedelta(seconds=10),
        )
        if saa_result != 7:
            print(f"MISMATCH: SAA result {saa_result!r}, expected 7")
            sys.exit(1)

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
