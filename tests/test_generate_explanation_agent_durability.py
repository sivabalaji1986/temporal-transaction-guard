# tests/test_generate_explanation_agent_durability.py
"""Proves the fine-grained TemporalDurability boundary itself: that model
requests and tool calls become separate Temporal Activities, and that a
completed Activity is not re-executed merely because a later step fails and
retries.

This is deliberately a *different* concern from
tests/test_fraud_hold_workflow.py (which proves FraudHoldWorkflow's business
logic -- ordering, fallback, release/block/escalate -- using the real
production `_agent` via monkeypatch + UnsandboxedWorkflowRunner). This file
instead builds a small, dedicated test-only Agent + Workflow pair, mirroring
production's shape (same deps_type, same TemporalDurability capability
pattern), because what's being proven here is a property of the *mechanism*
-- not of FraudHoldWorkflow's own orchestration, which is already covered
elsewhere. No real Ollama, Temporal server, or Docker is required or
contacted; `WorkflowEnvironment.start_time_skipping()` (the same hermetic
test harness already used throughout this project) plus a scripted
`FunctionModel` is enough.

Design note on how the scripted model is swapped per test: `TemporalDurability`
warns that a *live* Model instance passed per-run (`agent.run(model=...)`)
must be pre-registered (it can't be serialized across the activity boundary
on the fly) -- untested and risky to rely on here. Instead, the test Agent's
*primary* model (fixed once, at module-import time) is a `FunctionModel`
that delegates to whatever function `_current_script[0]` currently points
to. Each test reassigns that module-level reference before running. This is
safe because the actual model-request Activity execution -- unlike Workflow
code -- is not sandboxed/re-imported, so it reads this module's live state
directly.
"""

import uuid
from typing import Literal

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.durable_exec.temporal import (
    PydanticAIPlugin,
    PydanticAIWorkflow,
    TemporalDurability,
)
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import UsageLimits
from temporalio import workflow
from temporalio.client import WorkflowHistoryEventFilterType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker


class _Summary(BaseModel):
    notification_type: Literal["sms", "email", "push"]


_TOOL_CALL_LOG: list[str] = []

# Reassigned by each test before it runs; see module docstring for why.
_current_script: list = [None]


def _delegating_model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return _current_script[0](messages, info)


_test_agent = Agent(
    FunctionModel(_delegating_model_fn),
    name="durability_prototype_agent",
    deps_type=str,
    output_type=_Summary,
    system_prompt="test",
    capabilities=[TemporalDurability()],
)


@_test_agent.tool
async def lookup(ctx: RunContext[str]) -> str:
    _TOOL_CALL_LOG.append(f"lookup({ctx.deps})")
    return f"data-for-{ctx.deps}"


@workflow.defn
class _DurabilityTestWorkflow(PydanticAIWorkflow):
    __pydantic_ai_agents__ = [_test_agent]

    @workflow.run
    async def run(self, customer_id: str) -> str:
        result = await _test_agent.run(
            "investigate",
            deps=customer_id,
            usage_limits=UsageLimits(request_limit=6, tool_calls_limit=4),
        )
        return result.output.notification_type


def _tool_result(messages: list[ModelMessage], tool_name: str) -> str | None:
    for message in messages:
        for part in getattr(message, "parts", []):
            if isinstance(part, ToolReturnPart) and part.tool_name == tool_name:
                return part.content
    return None


@pytest.mark.asyncio
async def test_completed_tool_activity_is_not_reexecuted_on_later_failure():
    """The headline test this whole migration exists to prove:

    model step succeeds -> tool step succeeds -> the *next* model step fails
    once and retries -> the already-completed tool Activity is NOT
    re-executed -> the Agent finishes correctly.

    Verified two independent ways: a Python-level invocation counter, and
    Temporal's own Event History (ActivityTaskScheduled counts) -- "the
    Workflow eventually completed" alone would not be sufficient proof.
    """
    _TOOL_CALL_LOG.clear()
    model_call_count = {"n": 0}

    def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        model_call_count["n"] += 1
        real_result = _tool_result(messages, "lookup")
        if real_result is None:
            # Turn 1: call the tool.
            return ModelResponse(parts=[ToolCallPart(tool_name="lookup", args={})])
        # Turn 2: fail on the first attempt only, forcing Temporal to retry
        # just this model-request Activity -- not the already-completed
        # tool Activity.
        if model_call_count["n"] == 2:
            raise RuntimeError("simulated transient failure on the second model turn")
        output_tool = info.output_tools[0]
        return ModelResponse(
            parts=[ToolCallPart(tool_name=output_tool.name, args={"notification_type": "email"})]
        )

    _current_script[0] = scripted

    async with await WorkflowEnvironment.start_time_skipping(plugins=[PydanticAIPlugin()]) as env:
        task_queue = str(uuid.uuid4())
        wf_id = str(uuid.uuid4())
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[_DurabilityTestWorkflow],
        ):
            result = await env.client.execute_workflow(
                _DurabilityTestWorkflow.run,
                "CUST-101",
                id=wf_id,
                task_queue=task_queue,
            )

        # Independent proof #2: Temporal's own Event History, not just our
        # own bookkeeping.
        handle = env.client.get_workflow_handle(wf_id)
        tool_schedules = 0
        model_schedules = 0
        async for event in handle.fetch_history_events(
            event_filter_type=WorkflowHistoryEventFilterType.ALL_EVENT
        ):
            if event.HasField("activity_task_scheduled_event_attributes"):
                activity_type = event.activity_task_scheduled_event_attributes.activity_type.name
                if "call_tool" in activity_type:
                    tool_schedules += 1
                elif "model_request" in activity_type:
                    model_schedules += 1

    assert result == "email"
    # Independent proof #1: our own invocation counter.
    assert _TOOL_CALL_LOG == ["lookup(CUST-101)"], (
        f"tool was invoked more than once: {_TOOL_CALL_LOG}"
    )
    assert tool_schedules == 1, (
        f"tool Activity scheduled {tool_schedules} times, expected exactly 1"
    )
    assert model_schedules == 2, (
        f"expected exactly 2 model-request Activity schedules "
        f"(one per agent turn), got {model_schedules}"
    )


@pytest.mark.asyncio
async def test_agent_loop_is_bounded_through_the_durable_boundary():
    """Companion to test_agent_loop_is_bounded in
    test_generate_explanation_agent.py (which proves the bound outside a
    Workflow): this proves the same UsageLimits bound stops a pathological
    loop when the Agent is actually running through real Temporal
    Activities, not just as a plain in-process call.
    """

    def infinite_loop(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(tool_name="lookup", args={})])

    _current_script[0] = infinite_loop

    async with await WorkflowEnvironment.start_time_skipping(plugins=[PydanticAIPlugin()]) as env:
        task_queue = str(uuid.uuid4())
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[_DurabilityTestWorkflow],
        ):
            with pytest.raises(Exception) as exc_info:
                await env.client.execute_workflow(
                    _DurabilityTestWorkflow.run,
                    "CUST-101",
                    id=str(uuid.uuid4()),
                    task_queue=task_queue,
                )
    # The Workflow fails with the real UsageLimitExceeded as its cause
    # (PydanticAIPlugin's workflow_failure_exception_types preserves it,
    # rather than the workflow task retrying forever). It crosses back as a
    # generic ApplicationError (Temporal's cross-boundary failure envelope),
    # not literally reconstructed as the Python UsageLimitExceeded class --
    # its type name and message are preserved in the ApplicationError's
    # own message instead.
    cause = getattr(exc_info.value, "cause", None)
    assert cause is not None
    assert "UsageLimitExceeded" in str(cause)
    assert "tool_calls_limit" in str(cause)


@pytest.mark.asyncio
async def test_replay_determinism():
    """Captures a completed durability-boundary Workflow's real Event
    History and replays it with `temporalio.worker.Replayer` -- the
    standard, core-Temporal-SDK mechanism for proving Workflow code is
    genuinely replay-safe, independent of anything PydanticAI-specific.
    A failed replay here would mean the migration introduced nondeterminism
    that ordinary tests (which only check the *result*) would not catch.
    """

    def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        real_result = _tool_result(messages, "lookup")
        if real_result is None:
            return ModelResponse(parts=[ToolCallPart(tool_name="lookup", args={})])
        output_tool = info.output_tools[0]
        return ModelResponse(
            parts=[ToolCallPart(tool_name=output_tool.name, args={"notification_type": "push"})]
        )

    _current_script[0] = scripted

    async with await WorkflowEnvironment.start_time_skipping(plugins=[PydanticAIPlugin()]) as env:
        task_queue = str(uuid.uuid4())
        wf_id = str(uuid.uuid4())
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[_DurabilityTestWorkflow],
        ):
            await env.client.execute_workflow(
                _DurabilityTestWorkflow.run,
                "CUST-202",
                id=wf_id,
                task_queue=task_queue,
            )

        handle = env.client.get_workflow_handle(wf_id)
        history = await handle.fetch_history()

    replayer = Replayer(workflows=[_DurabilityTestWorkflow], plugins=[PydanticAIPlugin()])
    # Raises on any detected nondeterminism; a clean return means the
    # recorded history replays safely against this Workflow's current code.
    await replayer.replay_workflow(history)
