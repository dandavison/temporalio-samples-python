import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

from saa_parity_heartbeat_details_checkpoint.activities import counter
from saa_parity_heartbeat_details_checkpoint.workflow import CounterWorkflow

TASK = f"py-saa-parity-heartbeat_details_checkpoint-{uuid.uuid4()}"
RP = RetryPolicy(
    initial_interval=timedelta(milliseconds=50),
    maximum_attempts=5,
    backoff_coefficient=1.0,
)


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[CounterWorkflow],
        activities=[counter],
    ):
        wid = f"chk-wf-{uuid.uuid4()}"
        wh: WorkflowHandle = await client.start_workflow(
            CounterWorkflow.run,
            id=wid,
            task_queue=TASK,
        )
        wf_r = await wh.result()
        if wf_r != 10:
            _mismatch(f"WF result {wf_r!r}, expected 10")

        saa_id = f"chk-saa-{uuid.uuid4()}"
        sh = await client.start_activity(
            counter,
            10,
            id=saa_id,
            task_queue=TASK,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=RP,
        )
        saa_r = await sh.result()
        if saa_r != 10:
            _mismatch(f"SAA result {saa_r!r}, expected 10")
        sd = await sh.describe()
        if sd.attempt < 2:
            _mismatch(f"SAA describe attempt {sd.attempt}, expected >= 2")

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
