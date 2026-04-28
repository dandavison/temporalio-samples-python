import asyncio
from dataclasses import dataclass

from temporalio import activity


@dataclass
class CancellableInput:
    duration_seconds: int


@activity.defn
async def cancellable_activity(input: CancellableInput) -> str:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + input.duration_seconds
    iterations = 0
    while loop.time() < deadline:
        await asyncio.sleep(0.5)
        activity.heartbeat(iterations)
        iterations += 1
    return f"completed after {iterations} heartbeats"
