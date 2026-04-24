import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

from saa_parity_activity_info.activities import get_info
from saa_parity_activity_info.workflow import InfoWorkflow

TASK = f"py-saa-parity-activity_info-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    stc = timedelta(seconds=10)

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[InfoWorkflow],
        activities=[get_info],
    ):
        wfh: WorkflowHandle = await client.start_workflow(
            InfoWorkflow.run,
            id=f"info-wf-{uuid.uuid4()}",
            task_queue=TASK,
        )
        wfd = await wfh.result()

        saa = await client.execute_activity(
            get_info,
            id=f"info-saa-{uuid.uuid4()}",
            task_queue=TASK,
            start_to_close_timeout=stc,
        )

    if not wfd.get("workflow_id"):
        _mismatch("WF: workflow_id expected truthy")
    if not wfd.get("workflow_run_id"):
        _mismatch("WF: workflow_run_id expected truthy")
    if wfd.get("in_workflow") is not True:
        _mismatch(f"WF: in_workflow expected True, got {wfd.get('in_workflow')!r}")
    if wfd.get("attempt") != 1:
        _mismatch(f"WF: attempt {wfd.get('attempt')!r} expected 1")

    wid = saa.get("workflow_id")
    wrid = saa.get("workflow_run_id")
    if wid not in (None, ""):
        _mismatch(f"SAA: workflow_id expected None or '', got {wid!r}")
    if wrid not in (None, ""):
        _mismatch(f"SAA: workflow_run_id expected None or '', got {wrid!r}")
    if not saa.get("activity_run_id"):
        _mismatch(f"SAA: activity_run_id expected truthy, got {saa.get('activity_run_id')!r}")
    if saa.get("in_workflow") is not False:
        _mismatch(f"SAA: in_workflow expected False, got {saa.get('in_workflow')!r}")
    if saa.get("attempt") != 1:
        _mismatch(f"SAA: attempt {saa.get('attempt')!r} expected 1")

    if wfd.get("activity_type") != saa.get("activity_type"):
        _mismatch(
            f"activity_type WF {wfd.get('activity_type')!r} != SAA {saa.get('activity_type')!r}"
        )

    wto = wfd.get("start_to_close_timeout")
    sto = saa.get("start_to_close_timeout")
    if wto != sto:
        _mismatch(
            f"start_to_close_timeout: WF {wto!r} != SAA {sto!r} (expected same)"
        )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
