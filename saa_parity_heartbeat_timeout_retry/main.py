import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, TimeoutError, TimeoutType
from temporalio.worker import Worker

from saa_parity_heartbeat_timeout_retry.activities import heartbeater
from saa_parity_heartbeat_timeout_retry.workflow import HeartbeaterWorkflow

TASK = f"py-saa-parity-heartbeat_timeout_retry-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


def _has_heartbeat_timeout(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, TimeoutError) and cur.type == TimeoutType.HEARTBEAT:
            return True
        if isinstance(cur, ActivityError):
            cur = cur.cause
            continue
        cur = cur.__cause__
    return False


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    rp = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(milliseconds=100))

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[HeartbeaterWorkflow],
        activities=[heartbeater],
    ):
        wh: WorkflowHandle = await client.start_workflow(
            HeartbeaterWorkflow.run,
            id=f"hb-wf-{uuid.uuid4()}",
            task_queue=TASK,
        )
        wf_r = await wh.result()
        if wf_r != "ok":
            _mismatch(f"WF result {wf_r!r}, expected ok")

        saa_id = f"hb-saa-{uuid.uuid4()}"
        sh = await client.start_activity(
            heartbeater,
            id=saa_id,
            task_queue=TASK,
            start_to_close_timeout=timedelta(seconds=10),
            heartbeat_timeout=timedelta(seconds=1),
            retry_policy=rp,
        )
        saa_r = await sh.result()
        if saa_r != "ok":
            _mismatch(f"SAA result {saa_r!r}, expected ok")
        sd = await sh.describe()
        if sd.attempt < 2:
            _mismatch(f"SAA final attempt {sd.attempt}, expected >= 2")
        if sd.last_failure is None or not _has_heartbeat_timeout(sd.last_failure):
            _mismatch(
                f"SAA last_failure missing heartbeat timeout: {sd.last_failure!r}"
            )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
