import asyncio
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    ActivityExecutionStatus,
    ActivityFailureError,
    Client,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Worker

from saa_parity_retry_policy_nonretryable.activities import fail_business
from saa_parity_retry_policy_nonretryable.workflow import NonRetryableWorkflow

TASK = f"py-saa-parity-retry_policy_nonretryable-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


def _unwrap_app(exc: BaseException) -> ApplicationError | None:
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ApplicationError):
            return cur
        if isinstance(cur, ActivityError):
            cur = cur.cause
            continue
        cur = cur.__cause__
    return None


async def _count_activity_schedules(client: Client, wid: str, rid: str) -> int:
    h = client.get_workflow_handle(wid, run_id=rid)
    hist = await h.fetch_history()
    return sum(
        1
        for e in hist.events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    )


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    rp = RetryPolicy(
        maximum_attempts=5, initial_interval=timedelta(milliseconds=50)
    )

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[NonRetryableWorkflow],
        activities=[fail_business],
        activity_executor=ThreadPoolExecutor(2),
    ):
        wid = f"nr-wf-{uuid.uuid4()}"
        wh: WorkflowHandle = await client.start_workflow(
            NonRetryableWorkflow.run,
            id=wid,
            task_queue=TASK,
        )
        rid = wh.first_execution_run_id or (await wh.describe()).run_id
        try:
            await wh.result()
            _mismatch("WF: expected failure")
        except WorkflowFailureError as wfe:
            wae = wfe.cause
            if not isinstance(wae, ActivityError):
                _mismatch(f"WF: expected ActivityError, got {wae!r}")
            wapp = _unwrap_app(wae)
            if not wapp or wapp.type != "BusinessRuleFailed":
                _mismatch(
                    f"WF: expected ApplicationError BusinessRuleFailed, got {wapp!r}"
                )
        n_s = await _count_activity_schedules(client, wid, rid)
        if n_s > 1:
            _mismatch(
                f"WF: non-retryable should not reschedule; schedules={n_s}"
            )

        saa_id = f"nr-saa-{uuid.uuid4()}"
        th = await client.start_activity(
            fail_business,
            id=saa_id,
            task_queue=TASK,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp,
        )
        try:
            await th.result()
            _mismatch("SAA: expected failure from start_activity result")
        except ActivityFailureError as afe:
            sapp = _unwrap_app(afe.cause)
            if not sapp or sapp.type != "BusinessRuleFailed":
                _mismatch(
                    f"SAA: expected BusinessRuleFailed, got {sapp!r} cause={afe.cause!r}"
                )

        d = await th.describe()
        for _ in range(30):
            if d.status != ActivityExecutionStatus.RUNNING:
                break
            await asyncio.sleep(0.05)
            d = await th.describe()
        if d.attempt != 1:
            _mismatch(f"SAA describe attempt expected 1, got {d.attempt}")
        if d.last_failure is not None:
            lapp = _unwrap_app(d.last_failure)
            if not lapp or lapp.type != "BusinessRuleFailed":
                _mismatch(
                    f"SAA last_failure not BusinessRuleFailed: {d.last_failure!r}"
                )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
