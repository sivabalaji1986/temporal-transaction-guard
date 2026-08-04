from typing import TYPE_CHECKING

from temporalio import activity

from app.config import settings
from app.models import InvestigationSummary

if TYPE_CHECKING:
    from pydantic_ai import Agent

_agent: "Agent | None" = None


def _get_agent() -> "Agent":
    # pydantic-ai (and its OpenAI-compatible model/provider classes) are
    # imported lazily here, on first activity invocation, rather than at
    # module import time. This module is imported into the workflow file
    # (as a passthrough) purely so `generate_explanation` can be referenced
    # by `workflow.execute_activity`; merely importing
    # `pydantic_ai.models.openai` / `pydantic_ai.providers.openai` has a
    # process-wide side effect (it installs a beartype import hook via a
    # transitive dependency) that conflicts with Temporal's sandboxed
    # workflow re-import of unrelated modules -- including our own `app`
    # package -- causing a circular-import crash during workflow
    # validation. Deferring the imports (and construction) to first call
    # keeps that side effect out of workflow import/validation entirely,
    # since activities never run inside the sandbox.
    global _agent
    if _agent is None:
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider

        _agent = Agent(
            OpenAIChatModel(
                settings.ollama_model,
                provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
            ),
            output_type=InvestigationSummary,
            system_prompt=(
                "A bank's fraud engine has already decided to hold a transaction. "
                "Your job is only to EXPLAIN that decision clearly -- to the customer "
                "in plain language, and to fraud-ops staff as a short internal summary "
                "-- and to pick the best notification channel. You do NOT decide "
                "whether to hold the transaction; that decision has already been made."
            ),
        )
    return _agent


@activity.defn
async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
    result = await _get_agent().run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Write the customer_explanation, ops_summary, and notification_type."
    )
    return result.output
