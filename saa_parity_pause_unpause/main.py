import asyncio
import os
import sys
import uuid
from datetime import timedelta

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import (
    PauseActivityRequest,
    UnpauseActivityRequest,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError
from temporalio.worker import Worker

from saa_parity_pause_unpause.activities import pauser
from saa_parity_pause_unpause.workflow import PauserWorkflow

TASK = f"py-saa-parity-pause_unpause-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


def _result_file() -> str:
    return os.environ.get(
        "SAA_PARITY_PAUSE_UNPAUSE_RESULT",
        "/tmp/saa-parity/progress/python-pause_unpause-RESULT.md",
    )


async def _pause(
    client: Client,
    *,
    wf: str,
    run: str,
    act_id: str,
    act_type: str = "pauser",
) -> str | None:
    req = PauseActivityRequest(
        namespace=client.namespace,
        id=act_id,
        type=act_type,
        reason="saa_parity",
    )
    req.execution.CopyFrom(WorkflowExecution(workflow_id=wf, run_id=run))
    try:
        await client.workflow_service.pause_activity(req, retry=True)
    except RPCError as e:
        return f"PauseActivity {e.status!r} {e.message!r}"
    return None


async def _unpause(
    client: Client, *, wf: str, run: str, act_id: str, act_type: str = "pauser"
) -> str | None:
    req = UnpauseActivityRequest(
        namespace=client.namespace,
        id=act_id,
        type=act_type,
    )
    req.execution.CopyFrom(WorkflowExecution(workflow_id=wf, run_id=run))
    try:
        await client.workflow_service.unpause_activity(req, retry=True)
    except RPCError as e:
        return f"UnpauseActivity {e.status!r} {e.message!r}"
    return None


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    w_act_id = "pause-activity-1"
    log_lines: list[str] = []

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[PauserWorkflow],
        activities=[pauser],
    ):
        wid = f"pu-wf-{uuid.uuid4()}"
        wh: WorkflowHandle = await client.start_workflow(
            PauserWorkflow.run,
            id=wid,
            task_queue=TASK,
        )
        rid = wh.first_execution_run_id or (await wh.describe()).run_id
        await asyncio.sleep(0.5)
        wf_p = await _pause(
            client, wf=wid, run="", act_id=w_act_id
        )
        if wf_p is not None:
            wf_p = await _pause(
                client, wf=wid, run=rid, act_id=w_act_id
            )
        await asyncio.sleep(1.0)
        wf_u = await _unpause(
            client, wf=wid, run="", act_id=w_act_id
        )
        if wf_u is not None:
            wf_u = await _unpause(
                client, wf=wid, run=rid, act_id=w_act_id
            )
        wf_r = await wh.result()
        if wf_r != "done":
            _mismatch(f"WF result {wf_r!r}, expected done")
        log_lines.append(f"WF pause: {wf_p or 'ok'}")
        log_lines.append(f"WF unpause: {wf_u or 'ok'}")

        saa_id = f"pu-saa-{uuid.uuid4()}"
        sh = await client.start_activity(
            pauser,
            id=saa_id,
            task_queue=TASK,
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=5),
        )
        rdesc = await sh.describe()
        a_run = rdesc.activity_run_id or ""
        await asyncio.sleep(0.5)
        s_wf = ""
        s_p: str | None
        s_u: str | None
        if a_run:
            preq = PauseActivityRequest(
                namespace=client.namespace,
                id=saa_id,
                type="pauser",
                reason="saa_parity",
            )
            preq.execution.CopyFrom(
                WorkflowExecution(workflow_id=s_wf, run_id=a_run)
            )
            try:
                await client.workflow_service.pause_activity(preq, retry=True)
                s_p = None
            except RPCError as e:
                s_p = f"PauseActivity {e.status!r} {e.message!r}"
        else:
            s_p = "no activity_run_id on describe"
        if a_run:
            ureq = UnpauseActivityRequest(
                namespace=client.namespace, id=saa_id, type="pauser"
            )
            ureq.execution.CopyFrom(
                WorkflowExecution(workflow_id=s_wf, run_id=a_run)
            )
            try:
                await client.workflow_service.unpause_activity(ureq, retry=True)
                s_u = None
            except RPCError as e:
                s_u = f"UnpauseActivity {e.status!r} {e.message!r}"
        else:
            s_u = "no activity_run_id for unpause"
        log_lines.append(
            f"SAA pause (workflow_id={s_wf!r} run_id={a_run!r}): {s_p or 'ok'}"
        )
        log_lines.append(f"SAA unpause: {s_u or 'ok'}")

        sr = await sh.result()
        if sr != "done":
            _mismatch(f"SAA result {sr!r}, expected done")

    with open(_result_file(), "w", encoding="utf-8") as f:
        f.write("# pause_unpause probe\n\n")
        for line in log_lines:
            f.write(f"- {line}\n")

    wf_pause_ok = wf_p is None
    wf_unpause_ok = wf_u is None
    s_pause_ok = s_p is None
    s_unpause_ok = s_u is None
    if (
        wf_pause_ok != s_pause_ok
        or wf_unpause_ok != s_unpause_ok
    ):
        _mismatch(
            f"WF pause ok={wf_pause_ok} unpause={wf_unpause_ok} "
            f"vs SAA pause ok={s_pause_ok} unpause={s_unpause_ok} — see {_result_file()}"
        )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
