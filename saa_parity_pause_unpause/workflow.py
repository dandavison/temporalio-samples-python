from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from saa_parity_pause_unpause.activities import pauser


@workflow.defn
class PauserWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            pauser,
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=5),
            activity_id="pause-activity-1",
        )
