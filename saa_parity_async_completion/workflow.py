from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_async_completion.activities import async_add


@workflow.defn
class AsyncAddWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            async_add,
            args=[3, 4],
            start_to_close_timeout=timedelta(seconds=60),
        )
