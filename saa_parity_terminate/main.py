import asyncio
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import (
    ActivityExecutionStatus,
    Client,
    WorkflowExecutionStatus,
    WorkflowHandle,
)
from temporalio.worker import Worker

from saa_parity_terminate.activities import stubborn
from saa_parity_terminate.workflow import StubbornWorkflow

TASK = f"py-saa-parity-terminate-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    stc = timedelta(seconds=30)

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[StubbornWorkflow],
        activities=[stubborn],
        activity_executor=ThreadPoolExecutor(5),
    ):
        wfh: WorkflowHandle = await client.start_workflow(
            StubbornWorkflow.run,
            id=f"stub-wf-{uuid.uuid4()}",
            task_queue=TASK,
        )
        await asyncio.sleep(0.5)
        await wfh.terminate()
        for _ in range(200):
            winfo = await wfh.describe()
            if winfo.status == WorkflowExecutionStatus.TERMINATED:
                break
            await asyncio.sleep(0.1)
        else:
            winfo = await wfh.describe()
            _mismatch(
                f"WF: status expected TERMINATED, last seen {winfo.status!r}"
            )
        if winfo.status != WorkflowExecutionStatus.TERMINATED:
            _mismatch(
                f"WF: status expected {WorkflowExecutionStatus.TERMINATED!r}, got {winfo.status!r}"
            )
        try:
            await wfh.result()
            _mismatch("WF: expected result() to fail after terminate")
        except BaseException:
            pass

        ah = await client.start_activity(
            stubborn,
            id=f"stub-saa-{uuid.uuid4()}",
            task_queue=TASK,
            start_to_close_timeout=stc,
        )
        await asyncio.sleep(0.5)
        await ah.terminate(reason="test")
        d = await ah.describe()
        for _ in range(200):
            if d.status != ActivityExecutionStatus.RUNNING:
                break
            await asyncio.sleep(0.1)
            d = await ah.describe()
        if d.status == ActivityExecutionStatus.COMPLETED:
            _mismatch(
                "SAA: completed successfully; expected no successful result / termination path"
            )
        if d.status != ActivityExecutionStatus.TERMINATED:
            _mismatch(
                f"SAA: status expected {ActivityExecutionStatus.TERMINATED!r} "
                f"(non-running, non-completed), got {d.status!r}"
            )
        try:
            await ah.result()
            _mismatch("SAA: expected result() to fail after terminate")
        except BaseException:
            pass

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
