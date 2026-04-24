from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from saa_parity_heartbeat_details_checkpoint.activities import counter


@workflow.defn
class CounterWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            counter,
            10,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50),
                maximum_attempts=5,
                backoff_coefficient=1.0,
            ),
        )
