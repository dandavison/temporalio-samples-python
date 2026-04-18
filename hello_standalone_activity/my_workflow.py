from datetime import timedelta

from temporalio import workflow

from hello_standalone_activity.my_activity import MyJobInput, my_job_handler


@workflow.defn
class MyJobHandler:
    @workflow.run
    async def run(self, input: MyJobInput) -> str:
        return await workflow.execute_activity(
            my_job_handler,
            input,
            start_to_close_timeout=timedelta(seconds=10),
        )
