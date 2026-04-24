from temporalio import activity


@activity.defn
async def add(a: int, b: int) -> int:
    return a + b
