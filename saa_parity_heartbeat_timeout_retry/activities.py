import asyncio

from temporalio import activity


@activity.defn
async def heartbeater() -> str:
    i = activity.info()
    if i.attempt == 1:
        await asyncio.sleep(3)
        return "should not reach"
    activity.heartbeat("hb")
    return "ok"
