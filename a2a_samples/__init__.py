import asyncio
import uuid
from dataclasses import dataclass

from a2a.client import ClientConfig
from a2a.client.client_factory import ClientFactory, minimal_agent_card
from a2a.types import DataPart, Message, Part, Role
from pydantic import BaseModel
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from a2a_samples.service import TestServiceHandler
from a2a_samples.workflow_transport import create_workflow_nexus_transport

NEXUS_ENDPOINT_NAME = "a2a-nexus-endpoint"
NAMESPACE = "a2a-namespace"
TASK_QUEUE = "a2a-handler-task-queue"


class MyInput(BaseModel):
    name: str


class MyOutput(BaseModel):
    message: str


@dataclass
class MCPCallerWorkflowInput:
    endpoint: str


# sandbox disabled due to use of ThreadLocal by sniffio
# TODO: make this unnecessary
@workflow.defn(sandboxed=False)
class MCPCallerWorkflow:
    @workflow.run
    async def run(self, input: MCPCallerWorkflowInput) -> list[Message]:
        print("🌈 MCPCallerWorkflow.run")

        transport_name = "temporal-workflow-nexus-transport"
        config = ClientConfig(supported_transports=[transport_name])
        factory = ClientFactory(config)
        factory.register(transport_name, create_workflow_nexus_transport)
        card = minimal_agent_card(NEXUS_ENDPOINT_NAME, [transport_name])
        client = factory.create(card)

        messages = []
        async for message in client.send_message(
            Message(
                message_id=str(uuid.uuid4()),
                parts=[
                    Part(
                        root=DataPart(
                            data={
                                "service": "test-service",
                                "operation": "greet",
                                "input": {"name": "World"},
                            }
                        )
                    )
                ],
                role=Role.user,
            )
        ):
            messages.append(message)
        return messages


async def main() -> None:
    client = await Client.connect(
        target_host="localhost:7233",
        namespace=NAMESPACE,
        data_converter=pydantic_data_converter,
    )

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[MCPCallerWorkflow],
        nexus_service_handlers=[TestServiceHandler()],
    ):
        result = await client.execute_workflow(
            MCPCallerWorkflow.run,
            arg=MCPCallerWorkflowInput(endpoint=NEXUS_ENDPOINT_NAME),
            id=str(uuid.uuid4()),
            task_queue=TASK_QUEUE,
        )
        assert len(result) == 1
        [message] = result
        assert isinstance(message, Message)
        assert len(message.parts) == 1
        [part] = message.parts
        assert isinstance(part.root, DataPart)
        assert part.root.data == {"message": "Hello, World"}


if __name__ == "__main__":
    asyncio.run(main())
