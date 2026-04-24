from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from saa_parity_heartbeat_timeout_retry.activities import heartbeater


@workflow.defn
class HeartbeaterWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            heartbeater,
            start_to_close_timeout=timedelta(seconds=10),
            heartbeat_timeout=timedelta(seconds=1),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(milliseconds=100),
            ),
        )
