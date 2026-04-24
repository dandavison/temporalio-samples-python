from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_activity_info.activities import get_info


@workflow.defn
class InfoWorkflow:
    @workflow.run
    async def run(self) -> dict:
        return await workflow.execute_activity(
            get_info,
            start_to_close_timeout=timedelta(seconds=10),
        )
