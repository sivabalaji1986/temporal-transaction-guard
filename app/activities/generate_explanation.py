from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from temporalio import activity

from app.config import settings
from app.models import InvestigationSummary

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    output_type=InvestigationSummary,
    system_prompt=(
        "An upstream fraud-detection system identifies candidate suspicious "
        "transactions and provides a fraud score and trigger reason for each "
        "one. Our Temporal Workflow -- not this agent -- performs the "
        "deterministic threshold check on that score, decides whether to "
        "hold, and places the hold. You are only called after that hold "
        "decision has already been made and the hold is already in place. "
        "Your only job is to generate three things: a customer-friendly "
        "explanation, a short internal fraud-ops summary, and the best "
        "notification channel. You must never decide whether to hold, "
        "release, block, or escalate a transaction -- those decisions "
        "belong entirely to the Workflow, not to you."
    ),
)


@activity.defn
async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
    result = await _agent.run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Write the customer_explanation, ops_summary, and notification_type."
    )
    return result.output
