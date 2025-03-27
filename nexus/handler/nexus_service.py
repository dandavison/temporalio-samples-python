"""
Notes:

Sync operations:
---------------
Implementations are free to make arbitrary network calls, or perform CPU-bound
computations such as this one. Total execution duration must not exceed 10s. To
perform Temporal client calls such as signaling/querying/listing workflows, use
self.client.


Workflow operations:
---------------------
The task queue defaults to the task queue being used by the Nexus worker.
"""

from __future__ import annotations

import nexusrpc.handler
import temporalio.common
import temporalio.nexus.handler

from nexus.handler.dbclient import MyDBClient
from nexus.handler.workflows import HelloWorkflow
from nexus.service import interface
from nexus.service.interface import (
    EchoInput,
    EchoOutput,
    HelloInput,
    HelloOutput,
)


# Inheriting from the protocol here is optional. Users who do it will get the
# operation definition itself type-checked in situ against the interface (*).
# Call-sites using instances of the operation are always type-checked.
#
# (*) However, in VSCode/Pyright this is done only when type-checking is set to
# 'strict'.
class EchoOperation(nexusrpc.handler.Operation[EchoInput, EchoOutput]):
    def __init__(self, service: MyNexusService):
        self.service = service

    async def start(
        self, input: EchoInput, options: nexusrpc.handler.StartOperationOptions
    ) -> EchoOutput:
        return EchoOutput(message=f"Echo {input.message}!")

    async def cancel(
        self, token: str, options: nexusrpc.handler.CancelOperationOptions
    ) -> None:
        raise NotImplementedError

    async def fetch_info(
        self, token: str, options: nexusrpc.handler.FetchOperationInfoOptions
    ) -> nexusrpc.handler.OperationInfo:
        raise NotImplementedError

    async def fetch_result(
        self, token: str, options: nexusrpc.handler.FetchOperationResultOptions
    ) -> EchoOutput:
        raise NotImplementedError


# Inheriting from the protocol here is optional. Users who do it will get the
# operation definition itself type-checked in situ against the interface (*).
# Call-sites using instances of the operation are always type-checked.
#
# (*) However, in VSCode/Pyright this is done only when type-checking is set to
# 'strict'.
class HelloOperation:  # (nexusrpc.handler.Operation[HelloInput, HelloOutput]):
    def __init__(self, service: "MyNexusService"):
        self.service = service

    async def start(
        self, input: HelloInput, options: nexusrpc.handler.StartOperationOptions
    ) -> temporalio.nexus.handler.AsyncWorkflowOperationResult[HelloOutput]:
        self.service.db_client.execute("<some query>")
        workflow_id = "default-workflow-id"
        return await temporalio.nexus.handler.start_workflow(
            HelloWorkflow.run, input, workflow_id, options
        )

    async def cancel(
        self, token: str, options: nexusrpc.handler.CancelOperationOptions
    ) -> None:
        return await temporalio.nexus.handler.cancel_workflow(token, options)

    async def fetch_info(
        self, token: str, options: nexusrpc.handler.FetchOperationInfoOptions
    ) -> nexusrpc.handler.OperationInfo:
        return await temporalio.nexus.handler.fetch_workflow_info(token, options)

    async def fetch_result(
        self, token: str, options: nexusrpc.handler.FetchOperationResultOptions
    ) -> HelloOutput:
        return await temporalio.nexus.handler.fetch_workflow_result(token, options)


@nexusrpc.handler.service(interface=interface.MyNexusService)
class MyNexusService:
    def __init__(self, db_client: MyDBClient):
        # An example of something that might be held by the service instance.
        self.db_client = db_client

    # A sync operation defined by explicitly implementing the Operation interface
    @nexusrpc.handler.operation
    def echo(self) -> nexusrpc.handler.Operation[EchoInput, EchoOutput]:
        return EchoOperation(self)

    # An async operation defined by explicitly implementing the Operation interface
    @nexusrpc.handler.operation
    def hello(self) -> nexusrpc.handler.Operation[HelloInput, HelloOutput]:
        return HelloOperation(self)

    # A convenience shorthand for defining a sync operation by providing the
    # start method only.
    #
    # A start method defined in this way has access to the service instance, but
    # not to the operation instance (users who need the latter should implement
    # the Operation interface directly).
    @nexusrpc.handler.sync_operation
    async def echo2(
        self, input: EchoInput, _: nexusrpc.handler.StartOperationOptions
    ) -> EchoOutput:
        return EchoOutput(message=f"Echo {input.message} [via shorthand]!")
