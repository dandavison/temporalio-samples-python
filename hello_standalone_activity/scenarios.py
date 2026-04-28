import uuid

TASK_QUEUE = "my-standalone-activity-task-queue"


def uid() -> str:
    return str(uuid.uuid4())[:8]
