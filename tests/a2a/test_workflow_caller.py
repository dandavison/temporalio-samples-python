import uuid
from dataclasses import dataclass

from a2a.client import ClientConfig
from a2a.client.client_factory import ClientFactory, minimal_agent_card
from a2a.types import DataPart, Message, Part, Role
from nexusrpc import Operation, service
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation
from pydantic import BaseModel
from temporalio import workflow
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from a2a_samples.workflow_transport import create_workflow_nexus_transport


class MyInput(BaseModel):
    name: str


class MyOutput(BaseModel):
    message: str


@service(name="test-service")
class TestService:
    greet: Operation[MyInput, MyOutput]


@service_handler(service=TestService)
class TestServiceHandler:
    @sync_operation
    async def greet(self, ctx: StartOperationContext, input: MyInput) -> MyOutput:
        """
        This is a test operation.
        """
        return MyOutput(message=f"Hello, {input.name}")


@dataclass
class MCPCallerWorkflowInput:
    endpoint: str


# sandbox disabled due to use of ThreadLocal by sniffio
# TODO: make this unnecessary
@workflow.defn(sandboxed=False)
class MCPCallerWorkflow:
    @workflow.run
    async def run(self, input: MCPCallerWorkflowInput) -> list[Message]:
        config = ClientConfig(
            supported_transports=["temporal-workflow-nexus-transport"]
        )
        factory = ClientFactory(config)
        factory.register(
            "temporal-workflow-nexus-transport", create_workflow_nexus_transport
        )
        card = minimal_agent_card("endpoint", ["temporal-workflow-nexus-transport"])
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


async def test_workflow_caller() -> None:
    endpoint_name = "endpoint"
    task_queue = "handler-queue"

    async with await WorkflowEnvironment.start_local(
        data_converter=pydantic_data_converter,
        dev_server_existing_path="/opt/homebrew/bin/temporal",
    ) as env:
        await env.client.operator_service.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=EndpointSpec(
                    name=endpoint_name,
                    target=EndpointTarget(
                        worker=EndpointTarget.Worker(
                            namespace=env.client.namespace,
                            task_queue=task_queue,
                        )
                    ),
                )
            )
        )

        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[MCPCallerWorkflow],
            nexus_service_handlers=[TestServiceHandler()],
        ):
            result = await env.client.execute_workflow(
                MCPCallerWorkflow.run,
                arg=MCPCallerWorkflowInput(endpoint=endpoint_name),
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
            assert len(result) == 1
            [message] = result
            assert isinstance(message, Message)
            assert len(message.parts) == 1
            [part] = message.parts
            assert isinstance(part.root, DataPart)
            assert part.root.data == {"message": "Hello, World"}
