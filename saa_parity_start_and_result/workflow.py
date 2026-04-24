from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_start_and_result.activities import add


@workflow.defn
class AddWorkflow:
    @workflow.run
    async def run(self, a: int, b: int) -> int:
        return await workflow.execute_activity(
            add,
            args=[a, b],
            start_to_close_timeout=timedelta(seconds=10),
        )
