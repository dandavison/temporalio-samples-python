import asyncio

from temporalio import activity


@activity.defn
async def pauser() -> str:
    for _ in range(150):
        await asyncio.sleep(0.2)
        activity.heartbeat("tick")
    return "done"
