import asyncio

from temporalio.client import Client, WorkflowUpdateStage

from message_passing.introduction.workflow import (
    ApproveInput,
    GetLanguagesInput,
    GreetingWorkflow,
    Language,
)


async def main():
    client = await Client.connect("localhost:7233")
    wf_handle = await client.start_workflow(
        GreetingWorkflow.run,
        id="greeting-workflow-1234",
        task_queue="message-passing-introduction-task-queue",
    )

    supported_languages = await wf_handle.query(
        GreetingWorkflow.get_languages, GetLanguagesInput(include_unsupported=False)
    )
    print(f"supported languages: {supported_languages}")

    previous_language = await wf_handle.execute_update(
        GreetingWorkflow.set_language, Language.CHINESE
    )
    current_language = await wf_handle.query(GreetingWorkflow.get_language)
    print(f"language changed: {previous_language.name} -> {current_language.name}")

    update_handle = await wf_handle.start_update(
        GreetingWorkflow.set_language,
        Language.ENGLISH,
        wait_for_stage=WorkflowUpdateStage.ACCEPTED,
    )
    previous_language = await update_handle.result()
    current_language = await wf_handle.query(GreetingWorkflow.get_language)
    print(f"language changed: {previous_language.name} -> {current_language.name}")

    await wf_handle.signal(GreetingWorkflow.approve, ApproveInput(name=""))
    print(await wf_handle.result())


if __name__ == "__main__":
    asyncio.run(main())
