import asyncio

from temporalio import common
from temporalio.client import Client, StartWorkflowOperation

from message_passing.update_with_start import TASK_QUEUE
from message_passing.update_with_start.workflows import (
    LockService,
    ShoppingCartItem,
    ShoppingCartWorkflow,
    TransactionRequest,
    TransactionWorkflow,
)


async def financial_transaction_with_early_return():
    client = await Client.connect("localhost:7233")
    # The user wants to kick off a long-running transaction workflow and get an early-return result.

    # This example is slightly different because they will want the final workflow result, as well
    # as the update result.

    start_op = StartWorkflowOperation(
        TransactionWorkflow.run,
        args=[TransactionRequest(amount=77.7)],
        id="transaction-abc123",
        id_conflict_policy=common.WorkflowIDConflictPolicy.FAIL,
        task_queue=TASK_QUEUE,
    )

    # Send the MultiOp gRPC
    confirmation_token = await client.execute_update_with_start(
        TransactionWorkflow.get_confirmation, start_workflow_operation=start_op
    )
    wf_handle = await start_op.workflow_handle()
    final_report = await wf_handle.result()

    print(f"got confirmation token: {confirmation_token}")
    print(f"got final report: {final_report}")


async def financial_transaction_with_early_return_2():
    # Same, but using asyncio.gather.
    client = await Client.connect("localhost:7233")

    start_op = StartWorkflowOperation(
        TransactionWorkflow.run,
        args=[TransactionRequest(amount=77.7)],
        id="transaction-abc123-2",
        id_conflict_policy=common.WorkflowIDConflictPolicy.FAIL,
        task_queue=TASK_QUEUE,
    )

    wf_handle, confirmation_token = await asyncio.gather(
        start_op.workflow_handle(),
        client.execute_update_with_start(
            TransactionWorkflow.get_confirmation, start_workflow_operation=start_op
        ),
    )

    print(f"got confirmation token: {confirmation_token}")
    final_report = await wf_handle.result()
    print(f"got final report: {final_report}")


async def use_a_lock_service():
    client = await Client.connect("localhost:7233")

    # The user wants to acquire a lock lease from a lock service

    # - No network call here
    # - WithStartWorkflowHandle is a restricted interface that only offers update APIs and a way to
    #   get the real workflow handle.
    start_op = StartWorkflowOperation(
        LockService.run,
        id="lock-service-id",
        id_conflict_policy=common.WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue="uws",
    )

    lock = await client.execute_update_with_start(
        LockService.acquire_lock, "client-1", start_workflow_operation=start_op
    )

    print(f"acquired lock: {lock}")


async def shopping_cart():
    client = await Client.connect("localhost:7233")

    def create_start_op():
        return StartWorkflowOperation(
            ShoppingCartWorkflow.run,
            id="shopping-cart-id",
            id_conflict_policy=common.WorkflowIDConflictPolicy.USE_EXISTING,
            task_queue="uws",
        )

    crisps = ShoppingCartItem(sku="sku-123", quantity=1, price=77.7)
    start_op_1 = create_start_op()
    subtotal_1 = await client.execute_update_with_start(
        ShoppingCartWorkflow.add_item, crisps, start_workflow_operation=start_op_1
    )

    jam = ShoppingCartItem(sku="sku-456", quantity=1, price=77.7)
    start_op_2 = create_start_op()
    subtotal_2 = await client.execute_update_with_start(
        ShoppingCartWorkflow.add_item, jam, start_workflow_operation=start_op_2
    )

    # Get the real workflow handle that we'll need to send a signal
    wf_handle = await start_op_1.workflow_handle()
    await wf_handle.signal(ShoppingCartWorkflow.finalize)
    order = await wf_handle.result()

    print(f"subtotals were, {[subtotal_1, subtotal_2]}")
    print(f"final order: {order}")


async def sad_path_1():
    client = await Client.connect("localhost:7233")

    start_op = StartWorkflowOperation(
        ShoppingCartWorkflow.run,
        id="shopping-cart-id",
        id_conflict_policy=common.WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue="uws",
    )

    wf_handle = await start_op.workflow_handle()
    await wf_handle.result()


async def main():
    print("💰")
    await financial_transaction_with_early_return()
    print("💰 2")
    await financial_transaction_with_early_return_2()
    print("🔒")
    await use_a_lock_service()
    print("🛒")
    await shopping_cart()
    # print("💥")
    # await sad_path_1()


if __name__ == "__main__":
    asyncio.run(main())
