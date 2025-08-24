from nexusrpc import Operation, service
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation
from pydantic import BaseModel


class MyInput(BaseModel):
    name: str


class MyOutput(BaseModel):
    message: str


@service(name="modified-service-name")
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
