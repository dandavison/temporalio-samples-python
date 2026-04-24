import time

from temporalio import activity


@activity.defn
def stubborn() -> None:
    while True:
        time.sleep(0.1)
