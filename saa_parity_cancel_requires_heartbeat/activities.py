import asyncio
import time

from temporalio import activity
from temporalio.exceptions import CancelledError as TemporalCancelledError


@activity.defn
async def long_task(do_heartbeat: bool) -> str:
    if not do_heartbeat:
        await asyncio.get_running_loop().run_in_executor(None, time.sleep, 5.0)
        return "normal-done"

    deadline = time.monotonic() + 5.0
    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            activity.heartbeat("ping")
    except (asyncio.CancelledError, TemporalCancelledError):
        raise
    return "should-not-happen"
