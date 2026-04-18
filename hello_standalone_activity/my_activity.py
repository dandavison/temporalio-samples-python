from dataclasses import dataclass

from temporalio import activity


@dataclass
class MyJobInput:
    field_1: str


@activity.defn
def my_job_handler(input: MyJobInput) -> str:
    return input.field_1


@activity.defn
def my_activity_1(input: MyJobInput) -> str:
    return input.field_1


@activity.defn
def my_activity_2(input: MyJobInput) -> str:
    return input.field_1
