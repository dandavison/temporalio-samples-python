"""Confirm: cancel-to-delivery time = next heartbeat throttle window."""

import asyncio
import time
import uuid
from datetime import timedelta

from temporalio import activity
from temporalio.client import ActivityFailureError, Client
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker


@activity.defn
async def heartbeats_forever() -> str:
    while True:
        await asyncio.sleep(0.3)
        activity.heartbeat()


async def measure(client: Client, settle_seconds: float) -> float:
    task_queue = str(uuid.uuid4())
    async with Worker(
        client, task_queue=task_queue, activities=[heartbeats_forever]
    ):
        handle = await client.start_activity(
            heartbeats_forever,
            id=str(uuid.uuid4()),
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=180),
        )
        await asyncio.sleep(settle_seconds)
        t0 = time.monotonic()
        await handle.cancel()
        try:
            await asyncio.wait_for(handle.result(), timeout=60)
        except ActivityFailureError as e:
            if isinstance(e.cause, CancelledError):
                return time.monotonic() - t0
        except asyncio.TimeoutError:
            try:
                await handle.terminate()
            except Exception:
                pass
            return float("inf")
        return -1


async def main():
    client = await Client.connect("localhost:7233")
    print("settle_before_cancel  cancel_to_delivery")
    for settle in [0.0, 0.1, 0.5, 1.0, 2.0, 5.0]:
        elapsed = await measure(client, settle)
        print(f"  {settle:>6.1f}s            {elapsed:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
