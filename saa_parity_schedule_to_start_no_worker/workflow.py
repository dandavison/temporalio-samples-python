from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from saa_parity_schedule_to_start_no_worker.activities import unreachable


@workflow.defn
class NoWorkerActivityWorkflow:
    @workflow.run
    async def run(self, dead_queue: str) -> str:
        return await workflow.execute_activity(
            unreachable,
            task_queue=dead_queue,
            schedule_to_start_timeout=timedelta(seconds=2),
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
