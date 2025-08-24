import logging
from typing import cast

from litellm import acompletion
from litellm.types.utils import ModelResponse
from temporalio import activity

logger = logging.getLogger(__name__)


@activity.defn
async def llm(prompt: str) -> str:
    """Query the LLM with a prompt.

    Returns:
        The LLM's response as a string
    """
    logger.info("Querying LLM with prompt: %s", prompt)
    response = cast(
        ModelResponse,
        await acompletion(
            model="anthropic/claude-3-5-sonnet-20240620",
            messages=[
                {"role": "user", "content": prompt},
            ],
        ),
    )
    logger.info("LLM response: %s", response)
    return response["choices"][0]["message"]["content"]
