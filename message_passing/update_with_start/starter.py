import asyncio

from temporalio import common
from temporalio.client import Client

from message_passing.update_with_start import TASK_QUEUE
from message_passing.update_with_start.workflows import (
    LockService,
    ShoppingCartWorkflow,
    TransactionRequest,
    TransactionWorkflow,
)


async def financial_transaction_with_early_return():
    client = await Client.connect("localhost:7233")
    # The user wants to kick off a long-running transaction workflow and get an early-return result.

    # This example is slightly different because they will want the final workflow result, as well
    # as the update result.

    transaction = client.with_start_workflow(
        TransactionWorkflow.run,
        args=[TransactionRequest(amount=77.7)],
        id="transaction-abc123",
        task_queue=TASK_QUEUE,
    )

    # Send the MultiOp gRPC
    confirmation_token = await transaction.execute_update(
        TransactionWorkflow.get_confirmation
    )
    final_report = await transaction.get_workflow_handle().result()

    print(f"got confirmation token: {confirmation_token}")
    print(f"got final report: {final_report}")


async def use_a_lock_service():
    client = await Client.connect("localhost:7233")

    # The user wants to acquire a lock lease from a lock service

    # - No network call here
    # - WithStartWorkflowHandle is a restricted interface that only offers update APIs and a way to
    #   get the real workflow handle.
    lock_service = client.with_start_workflow(
        LockService.run,
        id="lock-service-id",
        id_conflict_policy=common.WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue="uws",
    )

    lock = await lock_service.execute_update(LockService.acquire_lock, "client-1")

    print(f"acquired lock: {lock}")


async def shopping_cart():
    client = await Client.connect("localhost:7233")

    shopping_cart = client.with_start_workflow(
        ShoppingCartWorkflow.run,
        id="shopping-cart-id",
        id_conflict_policy=common.WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue="uws",
    )
    











































    new_subtotal = shopping_cart.


async def main():
    await financial_transaction_with_early_return()
    await use_a_lock_service()
    await shopping_cart()


if __name__ == "__main__":
    asyncio.run(main())
