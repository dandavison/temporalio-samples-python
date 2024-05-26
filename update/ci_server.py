import asyncio
from datetime import datetime, timedelta
import logging
from sys import version
from typing import Iterator, Optional, Union

from attr import dataclass
from temporalio import common, workflow, activity
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

JobId = int
ClusterSlot = str

@dataclass
class Job:
    id: JobId
    depends_on: list[JobId]
    after_time: datetime
    name: str
    run: str
    python_interpreter_version: Optional[str]


@dataclass
class JobOutput:
    status: int
    stdout: str
    stderr: str    


###############################################################################
## SDK internals
##
class WorkflowEvent: pass
class UpdateRejected(WorkflowEvent): pass
class UpdateEnqueued(WorkflowEvent): pass
class UpdateDequeued(WorkflowEvent): pass
class UpdateCompleted(WorkflowEvent): pass
class Signal(WorkflowEvent): pass
class SignalHandlerReturned(WorkflowEvent): pass
class TimerFired(WorkflowEvent): pass
class CustomFutureResolved(WorkflowEvent): pass

EventStream = Iterator[Union[UpdateRejected, UpdateEnqueued, UpdateDequeued, UpdateCompleted, Signal, SignalHandlerReturned, TimerFired, CustomFutureResolved]]

class Selector:
    def get_event_stream(self) -> EventStream:
        ...
        

###############################################################################
## User's code
##

def make_event_stream() -> EventStream:
    selector = Selector()
    return selector.get_event_stream()


event_stream = make_event_stream()

@workflow.defn
class CIServer:

    def __init__(self):
        self.jobs: dict[JobId, Job] = {}

    # Design notes
    # ------------
    # Updates always have handler functions, and an update handler function is still just a normal
    # handler function: it implements the handling logic and the return value.
    #
    # The handler will be invoked automatically by the SDK when the underlying event stream yields
    # an UpdateDequeued event for this update ID. The user does not have to know anything about
    # this: by default, handlers are executed before other workflow code, in order of update
    # arrival.
    
    # The SDK is capable of automatically draining the event stream before workflow return / CAN,
    # including an option for this draining to result in serial execution of the handlers (i.e.
    # waiting for all async work scheduled by the handler to complete before the next handler is
    # invoked, and not allowing the workflow to complete until all such work is complete.) Default
    # behavior TBD.
    #
    # The event stream thus provides a way for users to wait until a specific message handler has
    # completed, or until all message handlers have completed. These can be exposed via convenient
    # `wait_for_X()` APIs, rather than interacting with the raw event stream
    #
    # Furthermore, users can optionally implement the EventStream themselves. This gives them
    # precise control over the ordering of handler invocation with respect to all other workflow
    # events (e.g. other update completions, and custom futures such as timers and
    # workflow.wait_condition).
    #
    # TODO: does handler invocation remain automatic on yielding Dequeue, or is that too magical. An
    # alternative would be for users to be able to call update.handle() on an update object obtained
    # from an event object yielded by the event stream.
    
    @workflow.update(event_stream=event_stream) # type: ignore
    async def run_shell_script_job(self, job: Job) -> JobOutput:
        """
        Run shell script job when permitted by job dependency graph (see `jobs.depends_on`) and not
        before `job.after_time`.
        """
        if (security_errors := await workflow.execute_activity(run_shell_script_security_linter, args=[job.run])):
            return JobOutput(1, "", security_errors)           
        while not (slot := await workflow.execute_activity(request_cluster_slot, arg=job, start_to_close_timeout=timedelta(seconds=10))):
            await asyncio.sleep(10)
        return await workflow.execute_activity(run_job, args=[job, slot], start_to_close_timeout=timedelta(seconds=10))

    @workflow.update
    async def run_python_job(self, job: Job) -> JobOutput:
        """
        Run python job when permitted by job dependency graph (see `jobs.depends_on`) and not before
        `job.after_time`.
        """
        if not await workflow.execute_activity(check_python_interpreter_version, args=[job.python_interpreter_version]):
            return JobOutput(1, "", f"Python interpreter version {version} is not available")           
        while not (slot := await workflow.execute_activity(request_cluster_slot, arg=job, start_to_close_timeout=timedelta(seconds=10))):
            await asyncio.sleep(10)
        return await workflow.execute_activity(run_job, args=[job, slot], start_to_close_timeout=timedelta(seconds=10))

    @workflow.run
    async def run(self):
        while not workflow.info().is_continue_as_new_suggested():
            await workflow.wait_condition(lambda: len(self.jobs) > 0)
        workflow.continue_as_new()

@activity.defn
async def run_job(job: Job, cluster_slot: ClusterSlot) -> JobOutput:
    return JobOutput(0, "job output", "")    
    
@activity.defn
async def request_cluster_slot(job: Job) -> ClusterSlot:
    return "cluster-slot-token-abc123"


@activity.defn
async def run_shell_script_security_linter(code: str) -> str:
    # The user's organization requires that all shell scripts pass an in-house linter that checks
    # for shell scripting constructions deemed insecure.
    await asyncio.sleep(0.1)
    return ""


@activity.defn
async def check_python_interpreter_version(version: str) -> bool:
    await asyncio.sleep(0.1)
    version_is_available = True
    return version_is_available


async def app(wf: WorkflowHandle):
    pass


async def main():
    client = await Client.connect("localhost:7233")
    async with Worker(
        client,
        task_queue="tq",
        workflows=[CIServer],
    ):
        wf = await client.start_workflow(
            CIServer.run,
            id="wid",
            task_queue="tq",
            id_reuse_policy=common.WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
        )
        await app(wf)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
