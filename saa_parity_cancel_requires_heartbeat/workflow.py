from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_cancel_requires_heartbeat.activities import long_task


@workflow.defn
class LongTaskWorkflow:
    @workflow.run
    async def run(self, do_heartbeat: bool) -> str:
        hb = timedelta(seconds=5) if do_heartbeat else None
        return await workflow.execute_activity(
            long_task,
            args=[do_heartbeat],
            start_to_close_timeout=timedelta(seconds=8),
            heartbeat_timeout=hb,
        )
