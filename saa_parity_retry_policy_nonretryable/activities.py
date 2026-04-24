from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn
def fail_business() -> str:
    raise ApplicationError(
        "nope", type="BusinessRuleFailed", non_retryable=True
    )
