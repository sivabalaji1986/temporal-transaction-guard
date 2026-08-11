# tests/test_generate_explanation_agent.py
"""Tests for the tool-using PydanticAI Agent inside generate_explanation.py.

These call `generate_explanation(...)` directly (not through a Temporal
Workflow/Worker) and swap out the real Ollama model for one of PydanticAI's
deterministic test models via `_agent.override(model=...)`. No real Ollama,
Temporal server, or Docker is required or contacted.

Two different test models are used deliberately, because they prove
different things:

- `TestModel` calls every registered tool once by default and returns
  schema-valid dummy data. It's good for proving tools are registered,
  callable, and that a schema-valid `InvestigationSummary` comes back --
  but its dummy output doesn't depend on what the tools actually returned,
  so it can't prove tool *results* influenced the final answer.
- `FunctionModel` lets a test script the "model's" behavior turn by turn,
  including reading real `ToolReturnPart` values out of the message
  history. That's what's needed to prove a real, mocked tool return value
  (e.g. the actual channel `lookup_customer_channel_preference` returns)
  actually ends up in the final `InvestigationSummary`.
"""

from typing import Any

import pytest
from pydantic_ai.exceptions import UnexpectedModelBehavior, UsageLimitExceeded
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

from app.activities.generate_explanation import (
    _AGENT_USAGE_LIMITS,
    _agent,
    generate_explanation,
    lookup_customer_channel_preference,
    lookup_recent_transactions,
)


def _tool_result(messages: list[ModelMessage], tool_name: str) -> Any:
    """Pull a real ToolReturnPart's content out of message history, if present."""
    for message in messages:
        for part in getattr(message, "parts", []):
            if isinstance(part, ToolReturnPart) and part.tool_name == tool_name:
                return part.content
    return None


@pytest.mark.asyncio
async def test_agent_can_invoke_both_read_only_tools():
    # TestModel's default call_tools="all" calls every registered tool once
    # before producing a schema-valid InvestigationSummary -- this proves
    # both tools are actually registered on the Agent and callable, and that
    # the Agent's output still satisfies the InvestigationSummary schema
    # once tools are in the loop.
    with _agent.override(model=TestModel()):
        result = await generate_explanation(85, "UNUSUAL_LOCATION", "CUST-101")
    assert result.notification_type in {"sms", "email", "push"}
    assert isinstance(result.customer_explanation, str)
    assert isinstance(result.ops_summary, str)


@pytest.mark.asyncio
async def test_real_tool_return_value_influences_final_output():
    # This is the test that TestModel cannot provide: it proves the *actual*
    # return value of the real lookup_customer_channel_preference tool (not
    # a value the test invents) flows all the way into the final structured
    # output. The scripted model calls the real tool, reads its real
    # ToolReturnPart back out of message history, and uses that value --
    # not a hardcoded one -- when producing notification_type.
    def scripted_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        real_channel = _tool_result(messages, "lookup_customer_channel_preference")
        if real_channel is not None:
            output_tool = info.output_tools[0]
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=output_tool.name,
                        args={
                            "customer_explanation": "We paused this transaction for review.",
                            "ops_summary": "Transaction flagged and held pending review.",
                            "notification_type": real_channel,
                        },
                    )
                ]
            )
        return ModelResponse(
            parts=[ToolCallPart(tool_name="lookup_customer_channel_preference", args={})]
        )

    with _agent.override(model=FunctionModel(scripted_model)):
        result = await generate_explanation(85, "UNUSUAL_LOCATION", "CUST-101")

    # lookup_customer_channel_preference's real (mocked) return value.
    assert result.notification_type == "email"


@pytest.mark.asyncio
async def test_invalid_notification_type_is_rejected_not_silently_accepted():
    # notification_type is Literal["sms", "email", "push"]. A model that
    # insists on an out-of-range value should never produce a successful
    # InvestigationSummary -- PydanticAI's own output validation must retry
    # and eventually give up, surfacing an error instead of silently
    # accepting bad data.
    def bad_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        output_tool = info.output_tools[0]
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=output_tool.name,
                    args={
                        "customer_explanation": "x",
                        "ops_summary": "y",
                        "notification_type": "carrier_pigeon",
                    },
                )
            ]
        )

    with _agent.override(model=FunctionModel(bad_model)):
        with pytest.raises(UnexpectedModelBehavior):
            await generate_explanation(85, "UNUSUAL_LOCATION", "CUST-101")


@pytest.mark.asyncio
async def test_customer_explanation_does_not_leak_raw_tool_data():
    # lookup_recent_transactions returns raw-looking merchant/amount/country
    # records -- exactly the kind of internal detail the system prompt tells
    # the Agent not to paste verbatim into the customer-facing explanation.
    # This test scripts a *compliant* final answer (a clean summary, the
    # behavior the system prompt asks for) and asserts none of the raw tool
    # payload leaks into customer_explanation, while ops_summary is allowed
    # to be more detailed. It encodes the intended contract and guards
    # against a code regression (e.g. someone later wiring raw tool output
    # directly into customer_explanation) -- it cannot, by itself, prove a
    # live LLM will always follow the instruction, since here the test
    # controls what the scripted "model" returns. Proving that against a
    # live model is an evaluation concern, out of scope for this demo.
    raw_marker = "Unnamed Kiosk"

    def scripted_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tx_result = _tool_result(messages, "lookup_recent_transactions")
        if tx_result is not None:
            assert any(raw_marker in str(tx.get("merchant", "")) for tx in tx_result)
            output_tool = info.output_tools[0]
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=output_tool.name,
                        args={
                            "customer_explanation": (
                                "We paused this transaction because it looked unusual "
                                "compared to your normal spending. We'll follow up shortly."
                            ),
                            "ops_summary": (
                                f"Recent activity reviewed, including a transaction at "
                                f"{raw_marker!r}; flagged for UNUSUAL_LOCATION."
                            ),
                            "notification_type": "email",
                        },
                    )
                ]
            )
        return ModelResponse(
            parts=[ToolCallPart(tool_name="lookup_recent_transactions", args={})]
        )

    with _agent.override(model=FunctionModel(scripted_model)):
        result = await generate_explanation(85, "UNUSUAL_LOCATION", "CUST-101")

    assert raw_marker not in result.customer_explanation
    assert raw_marker in result.ops_summary


@pytest.mark.asyncio
async def test_agent_loop_is_bounded():
    # A pathological model that always calls a real tool again and never
    # produces final output must be stopped by the Agent's configured
    # usage_limits (imported from the real module, not reimplemented here)
    # well short of an unbounded loop.
    def infinite_loop_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[ToolCallPart(tool_name="lookup_recent_transactions", args={})]
        )

    with _agent.override(model=FunctionModel(infinite_loop_model)):
        with pytest.raises(UsageLimitExceeded):
            await generate_explanation(85, "UNUSUAL_LOCATION", "CUST-101")

    # Documents the actual configured bound so this test breaks loudly if
    # someone loosens it without updating this test/the docs together.
    assert _AGENT_USAGE_LIMITS.request_limit == 6
    assert _AGENT_USAGE_LIMITS.tool_calls_limit == 4


def test_tools_are_importable_and_registered():
    # Confirms both read-only tools referenced throughout this file (and in
    # the docs) are the same functions actually registered on the Agent --
    # catches accidental renames/typos that would otherwise only surface as
    # a confusing runtime error deep inside PydanticAI.
    registered = set(_agent._function_toolset.tools.keys())
    assert lookup_recent_transactions.__name__ in registered
    assert lookup_customer_channel_preference.__name__ in registered
