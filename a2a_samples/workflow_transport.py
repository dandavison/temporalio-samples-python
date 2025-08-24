from collections.abc import AsyncGenerator

from a2a.client import ClientConfig
from a2a.client.middleware import ClientCallContext, ClientCallInterceptor
from a2a.client.transports.base import ClientTransport
from a2a.types import (
    AgentCard,
    DataPart,
    GetTaskPushNotificationConfigParams,
    Message,
    MessageSendParams,
    Part,
    Role,
    Task,
    TaskArtifactUpdateEvent,
    TaskIdParams,
    TaskPushNotificationConfig,
    TaskQueryParams,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from temporalio import workflow


class WorkflowNexusTransport(ClientTransport):
    """
    A transport that uses the Nexus RPC framework to call operations on a workflow.
    """

    def __init__(
        self,
        endpoint: str,
    ):
        self.endpoint = endpoint

    async def send_message(
        self,
        request: MessageSendParams,
        *,
        context: ClientCallContext | None = None,
    ) -> Task | Message:
        """Sends a non-streaming message request to the agent."""

        assert len(request.message.parts) == 1
        [part] = request.message.parts
        assert isinstance(part.root, DataPart)
        message_data = part.root.data

        nexus_client = workflow.create_nexus_client(
            endpoint=self.endpoint,
            service=message_data["service"],
        )
        nexus_op = await nexus_client.start_operation(
            message_data["operation"], message_data["input"]
        )
        if nexus_op.operation_token:
            return Task(
                id=nexus_op.operation_token,
                context_id="TODO",
                status=TaskStatus(
                    # In the future, if a Nexus task is made durable before delivery to worker, then
                    # this state would be 'submitted'.
                    state=TaskState.working,
                ),
            )
        else:
            result = await nexus_op
            return Message(
                message_id="TODO",
                parts=[Part(root=DataPart(data=result))],
                role=Role.agent,
            )

    async def send_message_streaming(
        self,
        request: MessageSendParams,
        *,
        context: ClientCallContext | None = None,
    ) -> AsyncGenerator[
        Message | Task | TaskStatusUpdateEvent | TaskArtifactUpdateEvent
    ]:
        """Sends a streaming message request to the agent and yields responses as they arrive."""
        raise NotImplementedError

    async def get_task(
        self,
        request: TaskQueryParams,
        *,
        context: ClientCallContext | None = None,
    ) -> Task:
        """Retrieves the current state and history of a specific task."""
        raise NotImplementedError

    async def cancel_task(
        self,
        request: TaskIdParams,
        *,
        context: ClientCallContext | None = None,
    ) -> Task:
        """Requests the agent to cancel a specific task."""
        raise NotImplementedError

    async def set_task_callback(
        self,
        request: TaskPushNotificationConfig,
        *,
        context: ClientCallContext | None = None,
    ) -> TaskPushNotificationConfig:
        """Sets or updates the push notification configuration for a specific task."""
        raise NotImplementedError

    async def get_task_callback(
        self,
        request: GetTaskPushNotificationConfigParams,
        *,
        context: ClientCallContext | None = None,
    ) -> TaskPushNotificationConfig:
        """Retrieves the push notification configuration for a specific task."""
        raise NotImplementedError

    async def resubscribe(
        self,
        request: TaskIdParams,
        *,
        context: ClientCallContext | None = None,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent
    ]:
        """Reconnects to get task updates."""
        raise NotImplementedError

    async def get_card(
        self,
        *,
        context: ClientCallContext | None = None,
    ) -> AgentCard:
        """Retrieves the AgentCard."""
        raise NotImplementedError

    async def close(self) -> None:
        """Closes the transport."""
        raise NotImplementedError


def create_workflow_nexus_transport(
    _card: AgentCard,
    url: str,
    _config: ClientConfig,
    _interceptors: list[ClientCallInterceptor],
):
    return WorkflowNexusTransport(endpoint=url)
