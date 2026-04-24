import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

from saa_parity_async_completion.activities import async_add, set_token_queue
from saa_parity_async_completion.workflow import AsyncAddWorkflow

TASK = f"py-saa-parity-async_completion-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    stc = timedelta(seconds=60)

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[AsyncAddWorkflow],
        activities=[async_add],
    ):
        # --- Workflow: token completion ---
        q_wf = asyncio.Queue[tuple[str, str, bytes]]()
        set_token_queue(q_wf)
        wfh: WorkflowHandle = await client.start_workflow(
            AsyncAddWorkflow.run,
            id=f"aa-wf-{uuid.uuid4()}",
            task_queue=TASK,
        )
        _aid, _arid, tok_wf = await asyncio.wait_for(q_wf.get(), timeout=30.0)
        await client.get_async_activity_handle(task_token=tok_wf).complete(7)
        wf_r = await wfh.result()
        if wf_r != 7:
            _mismatch(f"WF async complete: expected 7, got {wf_r!r}")

        # --- SAA: complete via task token ---
        q1 = asyncio.Queue[tuple[str, str, bytes]]()
        set_token_queue(q1)
        h1 = await client.start_activity(
            async_add,
            args=[3, 4],
            id=f"aa-saa1-{uuid.uuid4()}",
            task_queue=TASK,
            start_to_close_timeout=stc,
        )
        _a1, _r1, tok1 = await asyncio.wait_for(q1.get(), timeout=30.0)
        await client.get_async_activity_handle(task_token=tok1).complete(7)
        r1 = await h1.result()
        if r1 != 7:
            _mismatch(f"SAA token: expected 7, got {r1!r}")

        # --- SAA: id reference (empty workflow_id) ---
        q2 = asyncio.Queue[tuple[str, str, bytes]]()
        set_token_queue(q2)
        h2 = await client.start_activity(
            async_add,
            args=[3, 4],
            id=f"aa-saa2-{uuid.uuid4()}",
            task_queue=TASK,
            start_to_close_timeout=stc,
        )
        act_id, run_id, _tok2 = await asyncio.wait_for(q2.get(), timeout=30.0)
        await client.get_async_activity_handle(
            workflow_id="",
            run_id=run_id,
            activity_id=act_id,
        ).complete(7)
        r2 = await h2.result()
        if r2 != 7:
            _mismatch(f"SAA id ref: expected 7, got {r2!r}")

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
