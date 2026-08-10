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
        "An upstream fraud-detection system flagged this transaction as a "
        "candidate that may require further action, and our own deterministic "
        "threshold check has already decided to place a hold on it. Your job "
        "is only to EXPLAIN that hold clearly -- to the customer in plain "
        "language, and to fraud-ops staff as a short internal summary -- and "
        "to pick the best notification channel. You do NOT decide whether to "
        "hold the transaction; that decision has already been made, and it "
        "was not made by you."
    ),
)


@activity.defn
async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
    result = await _agent.run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Write the customer_explanation, ops_summary, and notification_type."
    )
    return result.output
