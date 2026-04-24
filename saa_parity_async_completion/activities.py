import asyncio

from temporalio import activity

_token_queue: asyncio.Queue[tuple[str, str, bytes]] | None = None


def set_token_queue(q: asyncio.Queue[tuple[str, str, bytes]]) -> None:
    global _token_queue
    _token_queue = q


@activity.defn
async def async_add(a: int, b: int) -> int:
    del a, b
    i = activity.info()
    if _token_queue is not None:
        await _token_queue.put(
            (i.activity_id, i.activity_run_id or "", i.task_token)
        )
    activity.raise_complete_async()
