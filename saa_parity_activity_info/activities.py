from typing import Any

from temporalio import activity


@activity.defn
async def get_info() -> dict[str, Any]:
    i = activity.info()
    stc = i.start_to_close_timeout
    return {
        "workflow_id": i.workflow_id,
        "workflow_run_id": i.workflow_run_id,
        "activity_id": i.activity_id,
        "activity_run_id": i.activity_run_id,
        "task_queue": i.task_queue,
        "activity_type": i.activity_type,
        "attempt": i.attempt,
        "start_to_close_timeout": (stc.total_seconds() if stc is not None else None),
        "in_workflow": i.in_workflow,
    }
