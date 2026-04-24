import asyncio
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import (
    ActivityFailureError,
    Client,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, TimeoutError, TimeoutType
from temporalio.worker import Worker

from saa_parity_schedule_to_start_no_worker.activities import unreachable
from saa_parity_schedule_to_start_no_worker.workflow import NoWorkerActivityWorkflow

WORKER_TASK = f"py-saa-parity-schedule_to_start_no_worker-w-{uuid.uuid4()}"
DEAD = f"py-saa-parity-no-worker-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


def _is_sts_in_chain(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ActivityError):
            c = cur.cause
            if isinstance(c, TimeoutError) and c.type == TimeoutType.SCHEDULE_TO_START:
                return True
        if isinstance(cur, TimeoutError) and cur.type == TimeoutType.SCHEDULE_TO_START:
            return True
        nxt: BaseException | None = cur.__cause__
        if nxt is None and hasattr(cur, "cause"):
            c2 = cur.cause
            if c2 is not None:
                nxt = c2
        cur = nxt
    return False


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    rp = RetryPolicy(maximum_attempts=1)
    sts = timedelta(seconds=2)
    stc = timedelta(seconds=5)

    async with Worker(
        client,
        task_queue=WORKER_TASK,
        workflows=[NoWorkerActivityWorkflow],
        activities=[unreachable],
        activity_executor=ThreadPoolExecutor(3),
    ):
        wfh: WorkflowHandle = await client.start_workflow(
            NoWorkerActivityWorkflow.run,
            args=[DEAD],
            id=f"nf-wf-{uuid.uuid4()}",
            task_queue=WORKER_TASK,
        )
        try:
            await wfh.result()
            _mismatch("WF: expected failure for no-worker activity")
        except WorkflowFailureError as wfe:
            if not _is_sts_in_chain(wfe.cause):
                _mismatch(
                    f"WF: expected ActivityError/TimeoutError SCHEDULE_TO_START, got {wfe.cause!r}"
                )

        try:
            await client.execute_activity(
                unreachable,
                id=f"nf-saa-{uuid.uuid4()}",
                task_queue=DEAD,
                schedule_to_start_timeout=sts,
                start_to_close_timeout=stc,
                retry_policy=rp,
            )
            _mismatch("SAA: expected failure for no-worker task queue")
        except ActivityFailureError as afe:
            if not _is_sts_in_chain(afe.cause):
                _mismatch(
                    f"SAA: expected ActivityError/TimeoutError SCHEDULE_TO_START, got {afe.cause!r}"
                )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
