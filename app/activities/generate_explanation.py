import asyncio
from datetime import timedelta
from typing import Literal

from pydantic_ai import Agent, RunContext
from pydantic_ai.durable_exec.temporal import TemporalDurability
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import UsageLimits
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from app.config import settings
from app.models import InvestigationSummary

# Bounds the Agent's tool-calling/reasoning loop for a single agent.run()
# call. The normal path needs at most 3 model requests (decide to call a
# tool, decide to call the other tool or go straight to output, produce the
# final structured output) and 2 tool calls (one per read-only tool below).
# request_limit=6 / tool_calls_limit=4 give roughly 2x headroom over that
# normal path while still stopping a pathological tool loop well short of
# consuming the Agent phase's worst-case wall-clock budget (see
# _MODEL_ACTIVITY_CONFIG / _BASE_ACTIVITY_CONFIG below). Exceeding either
# limit raises pydantic_ai.exceptions.UsageLimitExceeded (verified against
# the installed pydantic-ai-slim==2.22.0 wheel) -- a plain AgentRunError
# subclass, propagating directly from `agent.run()` in Workflow code, caught
# the same way as an ActivityError by fraud_hold_workflow.py's fallback.
_AGENT_USAGE_LIMITS = UsageLimits(request_limit=6, tool_calls_limit=4)

# Explicit, bounded Temporal activity configs for the fine-grained model/tool
# Activities TemporalDurability creates -- Temporal's own default RetryPolicy
# has maximum_attempts=0 (unlimited), so these must be set explicitly or the
# Agent phase's wall-clock budget would be unbounded.
#
# Worst-case Agent-phase budget (maximum path permitted by the UsageLimits
# above, assuming every single invocation fails once and retries, and
# demo_model_retry_interval_seconds is left at its default of 1 -- see below):
#   model:  6 x (60s + 1s backoff + 60s) = 6 x 121s = 726s
#   tools:  4 x (10s + 1s backoff + 10s) = 4 x  21s =  84s
#   total:  810s <= 900s (the required Agent-phase ceiling)
# Real local measurement (qwen3.5:latest, this repo's configured default,
# forced-cold-start, real system prompt + tool schemas): 13.6-17.1s worst
# observed, so 60s gives ~3.5x headroom over real observed behavior, not
# just a guess. See AGENTS.md for the full comparison of alternatives.
_BASE_ACTIVITY_CONFIG: ActivityConfig = {
    "start_to_close_timeout": timedelta(seconds=10),
    "retry_policy": RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1)),
}
_MODEL_ACTIVITY_CONFIG: ActivityConfig = {
    "start_to_close_timeout": timedelta(seconds=60),
    "retry_policy": RetryPolicy(
        maximum_attempts=2,
        initial_interval=timedelta(seconds=settings.demo_model_retry_interval_seconds),
    ),
}

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    # Stable, explicit name: this becomes the Temporal activity-name prefix
    # (agent__fraud_hold_investigator__*) that in-flight Workflow histories
    # depend on. Treat it as a durable contract -- see AGENTS.md -- do not
    # rename without a migration plan.
    name="fraud_hold_investigator",
    output_type=InvestigationSummary,
    # deps_type=str: the customer_id for the transaction under investigation.
    # It's bound once via `_agent.run(deps=customer_id, ...)` and read from
    # RunContext.deps inside the tools -- never exposed as a tool parameter
    # the model could fill in itself, and never part of the model-visible
    # tool schema, even now that tool calls are genuine Temporal Activities.
    # This means the model can only ever look up the customer this run was
    # actually called for, not an arbitrary customer_id of its choosing.
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
    # Routes model requests and tool calls through separate Temporal
    # Activities when running inside a Workflow (transparent -- a plain
    # in-process call -- outside of one, e.g. in tests that call
    # `_agent.run(...)` directly). See fraud_hold_workflow.py for where
    # `_agent.run(...)` is actually called from Workflow code, and
    # AGENTS.md for the retry-granularity implications.
    capabilities=[
        TemporalDurability(
            activity_config=_BASE_ACTIVITY_CONFIG,
            model_activity_config=_MODEL_ACTIVITY_CONFIG,
        )
    ],
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
    # settings.demo_failover_delay_seconds is 0 in every normal run
    # (default, and every automated test). It exists solely for the manual
    # Worker-pod-failover demo (README, "Demo C") -- set via the
    # DEMO_FAILOVER_DELAY_SECONDS env var, never in application code, so a
    # demo operator has a wide, predictable window to identify and kill a
    # Worker while this Activity is genuinely in-flight, without needing to
    # race a tight timing window or patch any framework internals. It only
    # ever delays this one read-only, side-effect-free Activity -- it never
    # touches Workflow code, customer_id handling, or the TemporalDurability
    # model/tool boundary itself.
    if settings.demo_failover_delay_seconds > 0:
        await asyncio.sleep(settings.demo_failover_delay_seconds)
    return _MOCK_CHANNEL_PREFERENCES.get(ctx.deps, _DEFAULT_CHANNEL_PREFERENCE)
