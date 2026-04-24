import asyncio

from temporalio import activity


@activity.defn
async def counter(target: int) -> int:
    i = activity.info()
    attempt = i.attempt
    start = int(i.heartbeat_details[0]) if i.heartbeat_details else 0
    if attempt > 1 and start < target // 2:
        raise RuntimeError(
            f"heartbeat checkpoint: start {start} < {target // 2} on attempt {attempt}"
        )
    activity.logger.info(
        "counter attempt=%s start=%s target=%s", attempt, start, target
    )
    for n in range(start, target):
        activity.heartbeat(n)
        if attempt == 1 and n == target // 2:
            raise RuntimeError("simulated crash")
        await asyncio.sleep(0.05)
    return target
