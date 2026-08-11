from typing import Literal

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import UsageLimits
from temporalio import activity

from app.config import settings
from app.models import InvestigationSummary

# Bounds the Agent's tool-calling/reasoning loop for a single Activity
# invocation. The normal path needs at most 3 model requests (decide to call
# a tool, decide to call the other tool or go straight to output, produce the
# final structured output) and 2 tool calls (one per read-only tool below).
# request_limit=6 / tool_calls_limit=4 give roughly 2x headroom over that
# normal path while still stopping a pathological tool loop well short of
# consuming the whole Activity timeout. Exceeding either limit raises
# pydantic_ai.exceptions.UsageLimitExceeded (verified against the installed
# pydantic-ai-slim==2.22.0 wheel), which is just another Agent failure as far
# as the Workflow is concerned -- it propagates the same way an Ollama outage
# would, and is handled by the existing `except ActivityError` fallback in
# fraud_hold_workflow.py.
_AGENT_USAGE_LIMITS = UsageLimits(request_limit=6, tool_calls_limit=4)

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    output_type=InvestigationSummary,
    # deps_type=str: the customer_id for the transaction under investigation.
    # It's bound once via `_agent.run(deps=customer_id, ...)` below and read
    # from RunContext.deps inside the tools -- never exposed as a tool
    # parameter the model could fill in itself. This means the model can only
    # ever look up the customer this Activity was actually called for, not an
    # arbitrary customer_id of its choosing.
    deps_type=str,
    system_prompt=(
        "An upstream fraud-detection system identifies candidate suspicious "
        "transactions and provides a fraud score and trigger reason for each "
        "one. Our Temporal Workflow -- not this agent -- performs the "
        "deterministic threshold check on that score, decides whether to "
        "hold, and places the hold. You are only called after that hold "
        "decision has already been made and the hold is already in place. "
        "You may call your read-only tools, in any order, zero or more "
        "times, to gather extra context about this customer before writing "
        "your response -- for example their recent transaction activity or "
        "their preferred notification channel. Your only job is to produce "
        "three things: a customer-friendly explanation, a short internal "
        "fraud-ops summary, and the best notification channel. You must "
        "never decide whether to hold, release, block, or escalate a "
        "transaction -- those decisions belong entirely to the Workflow, "
        "not to you. The customer-facing explanation must stay concise and "
        "appropriate for an end customer: never paste raw tool output, "
        "internal identifiers, or other internal-only details directly "
        "into it -- summarize what's relevant in plain language instead."
    ),
)


# Small, deterministic, in-memory mock data, keyed by customer_id -- stands
# in for a real customer-history/preference lookup. An unknown customer_id
# (not one of these keys) is a legitimate case, not an error, so both tools
# fall back to a safe default instead of raising: an empty transaction list,
# and an existing valid notification channel.
_MOCK_RECENT_TRANSACTIONS: dict[str, list[dict[str, str | float]]] = {
    "CUST-101": [
        {"merchant": "Acme Coffee", "amount": 4.50, "currency": "USD", "country": "US"},
        {"merchant": "Riverside Grocer", "amount": 62.10, "currency": "USD", "country": "US"},
        {"merchant": "Unnamed Kiosk", "amount": 310.00, "currency": "EUR", "country": "DE"},
    ],
    "CUST-202": [
        {"merchant": "Downtown Pharmacy", "amount": 18.25, "currency": "USD", "country": "US"},
        {"merchant": "Lakeside Diner", "amount": 27.80, "currency": "USD", "country": "US"},
    ],
}

_MOCK_CHANNEL_PREFERENCES: dict[str, Literal["sms", "email", "push"]] = {
    "CUST-101": "email",
    "CUST-202": "push",
}

_DEFAULT_CHANNEL_PREFERENCE: Literal["sms", "email", "push"] = "sms"


@_agent.tool
async def lookup_recent_transactions(ctx: RunContext[str]) -> list[dict[str, str | float]]:
    """Read-only mock: this customer's recent transactions (merchant, amount, currency, country)."""
    return _MOCK_RECENT_TRANSACTIONS.get(ctx.deps, [])


@_agent.tool
async def lookup_customer_channel_preference(
    ctx: RunContext[str],
) -> Literal["sms", "email", "push"]:
    """Read-only mock: this customer's preferred notification channel."""
    return _MOCK_CHANNEL_PREFERENCES.get(ctx.deps, _DEFAULT_CHANNEL_PREFERENCE)


@activity.defn
async def generate_explanation(
    fraud_score: float, trigger_reason: str, customer_id: str
) -> InvestigationSummary:
    # Both tool calls (if the Agent makes them) and the final structured
    # output happen inside this single Activity invocation -- see the module
    # docstring note in fraud_hold_workflow.py and AGENTS.md for why a
    # Temporal retry of this Activity re-runs the whole Agent loop, including
    # any tool calls already made, and why that's safe here (the tools are
    # read-only and idempotent).
    result = await _agent.run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Gather any useful context via your tools, then write the "
        "customer_explanation, ops_summary, and notification_type.",
        deps=customer_id,
        usage_limits=_AGENT_USAGE_LIMITS,
    )
    return result.output
