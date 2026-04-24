from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_terminate.activities import stubborn


@workflow.defn
class StubbornWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            stubborn,
            start_to_close_timeout=timedelta(seconds=30),
        )
