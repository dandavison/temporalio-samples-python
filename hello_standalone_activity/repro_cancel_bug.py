"""
Bug repro: cancel() on a running Standalone Activity is silently ignored by the worker.

Expected: activity receives CancelledError within 1-2 heartbeat cycles of handle.cancel()
Actual:   server transitions to CANCEL_REQUESTED but worker keeps running to completion

Verified against:
  SDK:    temporalio 1.26.0
  Server: CLI main (Server 1.31.0-154.0)

Root cause:
  _heartbeat_async in the Python worker sends RecordActivityHeartbeat fire-and-forget.
  The response (which carries the cancel flag) is never read. For workflow activities,
  cancel is delivered as a dedicated poll task pushed to the task queue. For SAA,
  no cancel task appears to be enqueued — leaving the heartbeat response as the only
  delivery path, which the SDK ignores.

Run:
  Terminal 1: cd standalone-activity-demo/cli && ./temporal server start-dev
  Terminal 2: cd standalone-activity-demo && uv run hello_standalone_activity/worker.py
  Terminal 3: cd standalone-activity-demo && uv run hello_standalone_activity/repro_cancel_bug.py
"""

import asyncio
from datetime import timedelta

from temporalio.client import ActivityFailureError, Client
from temporalio.envconfig import ClientConfig
from temporalio.exceptions import CancelledError

from hello_standalone_activity.activities import CancellableInput, cancellable_activity
from hello_standalone_activity.scenarios import TASK_QUEUE, uid


async def main():
    connect_config = ClientConfig.load_client_connect_config()
    connect_config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**connect_config)

    handle = await client.start_activity(
        cancellable_activity,
        args=[CancellableInput(duration_seconds=60)],
        id=f"cancel-repro-{uid()}",
        task_queue=TASK_QUEUE,
        start_to_close_timeout=timedelta(seconds=120),
    )
    print(f"Activity started: {handle.id}")
    print("Activity heartbeats every 0.5s. Waiting 3s before cancelling...\n")

    await asyncio.sleep(3)

    desc_before = await handle.describe()
    print(f"run_state before cancel: {int(desc_before.run_state)}  (2 = STARTED)")

    await handle.cancel()
    print("handle.cancel() called — server should deliver CancelledError on next heartbeat\n")

    await asyncio.sleep(2)
    desc_after = await handle.describe()
    print(f"run_state after 2s:      {int(desc_after.run_state)}  (3 = CANCEL_REQUESTED)")
    print("Server has the cancel flag. Worker should have acted on it by now.\n")

    # Activity heartbeats every 0.5s. Cancel should arrive within 1 heartbeat cycle.
    # We wait up to 10s — more than enough.
    try:
        await asyncio.wait_for(handle.result(), timeout=10)
        print("[BUG] Activity returned a result — cancel was ignored by worker")
    except TimeoutError:
        print("[BUG] Activity still running after 10s — cancel never delivered to worker")
        await handle.terminate(reason="repro cleanup")
    except ActivityFailureError as e:
        if isinstance(e.cause, CancelledError):
            print("[OK] Activity cancelled correctly — bug may be fixed")
        else:
            print(f"[UNEXPECTED] {type(e.cause).__name__}: {e.cause}")


if __name__ == "__main__":
    asyncio.run(main())
