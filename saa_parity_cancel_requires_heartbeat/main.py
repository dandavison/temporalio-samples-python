import asyncio
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import CancelledError as TemporalCancelledError
from temporalio.worker import Worker

from saa_parity_cancel_requires_heartbeat.activities import long_task
from saa_parity_cancel_requires_heartbeat.workflow import LongTaskWorkflow

TASK = f"py-saa-parity-cancel_requires_heartbeat-{uuid.uuid4()}"


def _mismatch(msg: str) -> None:
    print(f"MISMATCH: {msg}")
    sys.exit(1)


def _is_cancel_family(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, (asyncio.CancelledError, TemporalCancelledError)):
            return True
        nxt: BaseException | None = cur.__cause__
        if nxt is None and hasattr(cur, "cause"):
            c = cur.cause
            if c is not None:
                nxt = c
        cur = nxt
    return False


async def run() -> None:
    client = await Client.connect("localhost:7233", namespace="default")
    stc = timedelta(seconds=8)

    async with Worker(
        client,
        task_queue=TASK,
        workflows=[LongTaskWorkflow],
        activities=[long_task],
    ):
        for do_hb in (False, True):
            hb_opt = timedelta(seconds=5) if do_hb else None

            wh: WorkflowHandle = await client.start_workflow(
                LongTaskWorkflow.run,
                args=[do_hb],
                id=f"long-wf-{uuid.uuid4()}",
                task_queue=TASK,
            )
            await asyncio.sleep(0.5)
            await wh.cancel()
            try:
                wval = await wh.result()
                wf_ok = True
                wf_err: BaseException | None = None
            except BaseException as e:
                wf_ok = False
                wf_err = e

            sah = await client.start_activity(
                long_task,
                args=[do_hb],
                id=f"long-saa-{uuid.uuid4()}",
                task_queue=TASK,
                start_to_close_timeout=stc,
                heartbeat_timeout=hb_opt,
            )
            await asyncio.sleep(0.5)
            await sah.cancel()
            try:
                sval = await sah.result()
                saa_ok = True
                saa_err: BaseException | None = None
            except BaseException as e:
                saa_ok = False
                saa_err = e

            if wf_ok != saa_ok:
                _mismatch(
                    f"hb={do_hb}: WF ok={wf_ok} vs SAA ok={saa_ok} "
                    f"(wf_err={wf_err!r} saa_err={saa_err!r})"
                )

            if not do_hb:
                if not wf_ok or not saa_ok:
                    _mismatch(
                        f"hb=False: expected both success, wf={wval if wf_ok else wf_err!r} "
                        f"saa={sval if saa_ok else saa_err!r}"
                    )
                if wval != "normal-done" or sval != "normal-done":
                    _mismatch(
                        f"hb=False: expected normal-done, wf={wval!r} saa={sval!r}"
                    )
            else:
                if wf_ok or saa_ok:
                    _mismatch(
                        f"hb=True: expected both to fail, wf={wval if wf_ok else 'err'} "
                        f"saa={sval if saa_ok else 'err'}"
                    )
                assert wf_err is not None and saa_err is not None
                w_chain = _is_cancel_family(wf_err)
                s_chain = _is_cancel_family(saa_err)
                if not w_chain or not s_chain:
                    _mismatch(
                        f"hb=True: expected cancel-family, wf={wf_err!r} saa={saa_err!r}"
                    )

    print("PASS")


if __name__ == "__main__":
    asyncio.run(run())
