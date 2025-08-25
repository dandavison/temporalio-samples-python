from a2a.types import (
    DataPart,
    Message,
    Part,
    Role,
)
from nexusrpc import Operation, service
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation
from pydantic import BaseModel

NEXUS_ENDPOINT_NAME = "a2a-nexus-endpoint"
NAMESPACE = "a2a-namespace"
TASK_QUEUE = "a2a-handler-task-queue"


class MyInput(BaseModel):
    name: str


@service(name="test-service")
class TestService:
    greet: Operation[MyInput, Message]


@service_handler(service=TestService)
class TestServiceHandler:
    @sync_operation
    async def greet(self, ctx: StartOperationContext, input: MyInput) -> Message:
        """
        This is a test operation.
        """
        return Message(
            message_id="TODO",
            parts=[Part(root=DataPart(data={"message": f"Hello, {input.name}"}))],
            role=Role.agent,
        )
