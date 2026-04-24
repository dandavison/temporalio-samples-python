from temporalio import activity


@activity.defn
def unreachable() -> str:
    return "unreachable"
