"""
This file demonstrates how to implement a Nexus service that is backed by a long-running workflow
and exposes operations that perform updates, and queries against that workflow.
"""

from __future__ import annotations

import nexusrpc
from message_passing.introduction import Language
from message_passing.introduction.workflows import (
    ApproveInput,
    GetLanguagesInput,
    GreetingWorkflow,
    SetLanguageInput,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy

from nexus_sync_operations.service import GreetingService


@nexusrpc.handler.service_handler(service=GreetingService)
class GreetingServiceHandler:
    def __init__(
        self,
        greeting_workflow_handle: WorkflowHandle[GreetingWorkflow, str],
    ):
        self.greeting_workflow_handle = greeting_workflow_handle

    @classmethod
    async def create(cls, client: Client, task_queue: str) -> GreetingServiceHandler:
        # Obtain a workflow handle to the long-running workflow that backs this service, starting
        # the workflow if it is not already running.
        wf_handle = await client.start_workflow(
            GreetingWorkflow.run,
            id="nexus-sync-operations-greeting-workflow",
            task_queue=task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return cls(wf_handle)

    # 👉 This is a handler for a nexus operation whose internal implementation involves executing a
    # query against a long-running workflow that is private to the nexus service.
    @nexusrpc.handler.sync_operation
    async def get_languages(
        self, ctx: nexusrpc.handler.StartOperationContext, input: GetLanguagesInput
    ) -> list[Language]:
        return await self.greeting_workflow_handle.query(
            GreetingWorkflow.get_languages, input
        )

    # 👉 This is a handler for a nexus operation whose internal implementation involves executing a
    # query against a long-running workflow that is private to the nexus service.
    @nexusrpc.handler.sync_operation
    async def get_language(
        self, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> Language:
        return await self.greeting_workflow_handle.query(GreetingWorkflow.get_language)

    # 👉 This is a handler for a nexus operation whose internal implementation involves executing an
    # update against a long-running workflow that is private to the nexus service.
    @nexusrpc.handler.sync_operation
    async def set_language(
        self,
        ctx: nexusrpc.handler.StartOperationContext,
        input: SetLanguageInput,
    ) -> Language:
        return await self.greeting_workflow_handle.execute_update(
            GreetingWorkflow.set_language_using_activity, input
        )

    @nexusrpc.handler.sync_operation
    async def approve(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ApproveInput
    ) -> None:
        await self.greeting_workflow_handle.signal(GreetingWorkflow.approve, input)

    @nexusrpc.handler.sync_operation
    async def fetch_greeting_translation(
        self, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> str:
        return await self.greeting_workflow_handle.execute_update(
            GreetingWorkflow.fetch_greeting_translation
        )

    @nexusrpc.handler.sync_operation
    async def get_operation_log(
        self, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> list[str]:
        return await self.greeting_workflow_handle.query(
            GreetingWorkflow.get_operation_log
        )
