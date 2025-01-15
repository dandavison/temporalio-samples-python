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


class EchoOperation:
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

    # Nexus operations do not need to implement fetch_result when they are called from
    # a Workflow. We implement it here to check the design in the non-Temporal context.
    async def fetch_result(
        self, token: str, options: nexusrpc.handler.FetchOperationResultOptions
    ) -> EchoOutput:
        raise NotImplementedError


class HelloOperation:
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

    # Nexus operations do not need to implement fetch_result when they are called from
    # a Workflow. We implement it here to check the design in the non-Temporal context.
    async def fetch_result(
        self, token: str, options: nexusrpc.handler.FetchOperationResultOptions
    ) -> HelloOutput:
        return await temporalio.nexus.handler.fetch_workflow_result(token, options)


@nexusrpc.handler.service(interface=interface.MyNexusService)
class MyNexusService:
    def __init__(self, db_client: MyDBClient):
        self.db_client = db_client

    @nexusrpc.handler.operation
    def echo(self) -> EchoOperation:
        return EchoOperation(self)

    @nexusrpc.handler.operation
    def hello(self) -> HelloOperation:
        return HelloOperation(self)
