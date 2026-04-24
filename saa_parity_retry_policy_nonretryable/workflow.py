from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from saa_parity_retry_policy_nonretryable.activities import fail_business


@workflow.defn
class NonRetryableWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            fail_business,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=5, initial_interval=timedelta(milliseconds=50)
            ),
        )
