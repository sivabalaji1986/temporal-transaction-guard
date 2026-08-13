# tests/test_fraud_hold_workflow.py
"""Workflow-level tests for FraudHoldWorkflow: threshold branching,
place_hold-before-Agent ordering, release/block/escalate resolution, and the
Agent-failure fallback.

These exercise the *real* production `FraudHoldWorkflow` and the real
`_agent` module binding it calls -- not a parallel test-only Workflow class.
To do that with a deterministic model instead of live Ollama, each test:

1. builds a small test Agent (its own FunctionModel, its own
   TemporalDurability capability -- mirroring production's activity
   configs for fidelity, imported from generate_explanation.py rather than
   restated here);
2. uses pytest's `monkeypatch` fixture to reassign
   `app.workflows.fraud_hold_workflow._agent` (the name FraudHoldWorkflow.run
   actually calls) and `FraudHoldWorkflow.__pydantic_ai_agents__` (what
   PydanticAIPlugin reads to register Worker activities) to that test
   Agent, before the Worker is constructed;
3. constructs the test Worker with `workflow_runner=UnsandboxedWorkflowRunner()`.

Step 3 is required, not optional: Temporal's default sandboxed workflow
runner re-imports workflow-defining modules into an isolated copy, so
`monkeypatch.setattr` on the outer module (step 2) would not be visible to
the actually-executing Workflow code without it -- confirmed by first
reproducing the failure this causes (a `NotFoundError` scheduling an
activity under the *production* Agent's name) before adding this. Test-only:
required so pytest's `monkeypatch` of the production Agent is visible to
Workflow execution. The production Worker (app/worker.py) remains
sandboxed; replay/determinism is validated separately, with the normal
sandboxed runner, in tests/test_generate_explanation_agent_durability.py's
`test_replay_determinism`. See AGENTS.md for why this doesn't weaken that
safety net.
"""

import uuid
from collections.abc import Callable

import pytest
from pydantic_ai import Agent
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin, TemporalDurability
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from app.activities.generate_explanation import _BASE_ACTIVITY_CONFIG, _MODEL_ACTIVITY_CONFIG
from app.models import CustomerResponse, InvestigationSummary, Transaction
from app.workflows import fraud_hold_workflow as fraud_hold_workflow_module
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

FRAUD_SCORE_THRESHOLD = 70


def make_test_agent(calls: list[str], fail: bool = False) -> Agent:
    """A test-only Agent standing in for the real one -- registered on the
    Worker via monkeypatch (see module docstring), never calling Ollama.
    Goes straight to final output on the first turn (no tool calls) since
    these tests are about Workflow orchestration, not Agent/tool internals
    -- those are covered in test_generate_explanation_agent.py and
    test_generate_explanation_agent_durability.py.
    """

    def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append("agent_investigation")
        if fail:
            raise RuntimeError("simulated Ollama outage")
        output_tool = info.output_tools[0]
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=output_tool.name,
                    args={
                        "customer_explanation": "test explanation",
                        "ops_summary": "test ops summary",
                        "notification_type": "sms",
                    },
                )
            ]
        )

    return Agent(
        FunctionModel(scripted),
        name="test_fraud_hold_investigator",
        deps_type=str,
        output_type=InvestigationSummary,
        system_prompt="test",
        # Reuses production's real activity configs (Option A: 60s/2
        # attempts model, 10s/2 attempts base) rather than restating them,
        # so a real exhausted-retries ActivityError is exercised with the
        # same timing production actually uses.
        capabilities=[
            TemporalDurability(
                activity_config=_BASE_ACTIVITY_CONFIG,
                model_activity_config=_MODEL_ACTIVITY_CONFIG,
            )
        ],
    )


def make_mock_activities() -> tuple[list[str], list[str], list[Callable]]:
    calls: list[str] = []
    notify_messages: list[str] = []

    @activity.defn(name="record_no_hold_outcome")
    async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
        calls.append("record_no_hold_outcome")

    @activity.defn(name="place_hold")
    async def place_hold(transaction_id: str) -> None:
        calls.append("place_hold")

    @activity.defn(name="notify_customer")
    async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
        calls.append("notify_customer")
        notify_messages.append(message)

    @activity.defn(name="release")
    async def release(transaction_id: str) -> None:
        calls.append("release")

    @activity.defn(name="block")
    async def block(transaction_id: str) -> None:
        calls.append("block")

    @activity.defn(name="escalate")
    async def escalate(transaction_id: str) -> None:
        calls.append("escalate")

    return calls, notify_messages, [
        record_no_hold_outcome,
        place_hold,
        notify_customer,
        release,
        block,
        escalate,
    ]


def make_transaction(transaction_id: str, fraud_score: float) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        fraud_score=fraud_score,
        trigger_reason="TEST_REASON",
        customer_id="CUST-TEST",
    )


@pytest.mark.asyncio
async def test_below_threshold_records_no_hold_outcome(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # Test-only: required so pytest monkeypatch of the production Agent is visible
            # to Workflow execution. Production Worker remains sandboxed; replay/determinism
            # is validated separately using the normal Temporal runner.
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await env.client.execute_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-BELOW", 50), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
    assert result == "no_hold_needed"
    assert calls == ["record_no_hold_outcome"]


@pytest.mark.asyncio
async def test_it_was_me_releases(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # UnsandboxedWorkflowRunner(): see rationale comment on the first
            # test above (test_below_threshold_records_no_hold_outcome).
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            handle = await env.client.start_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-ITWASME", 90), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
            await handle.signal(
                FraudHoldWorkflow.customer_responded,
                CustomerResponse(response="it_was_me"),
            )
            result = await handle.result()
    assert result == "released"
    assert "release" in calls
    assert "block" not in calls
    assert "escalate" not in calls
    # place_hold must happen before the Agent investigation: the hold
    # protects funds and must not depend on the LLM call succeeding or
    # being fast.
    assert calls.index("place_hold") < calls.index("agent_investigation")


@pytest.mark.asyncio
async def test_not_me_blocks(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # UnsandboxedWorkflowRunner(): see rationale comment on the first
            # test above (test_below_threshold_records_no_hold_outcome).
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            handle = await env.client.start_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-NOTME", 90), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
            await handle.signal(
                FraudHoldWorkflow.customer_responded,
                CustomerResponse(response="not_me"),
            )
            result = await handle.result()
    assert result == "blocked"
    assert "block" in calls
    assert "release" not in calls
    assert "escalate" not in calls
    # place_hold must happen before the Agent investigation: the hold
    # protects funds and must not depend on the LLM call succeeding or
    # being fast.
    assert calls.index("place_hold") < calls.index("agent_investigation")


@pytest.mark.asyncio
async def test_timeout_escalates(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # UnsandboxedWorkflowRunner(): see rationale comment on the first
            # test above (test_below_threshold_records_no_hold_outcome).
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await env.client.execute_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-TIMEOUT", 95), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
    assert result == "escalated_no_response"
    assert "escalate" in calls


@pytest.mark.asyncio
async def test_ai_failure_falls_back_and_still_resolves(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls, fail=True)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # UnsandboxedWorkflowRunner(): see rationale comment on the first
            # test above (test_below_threshold_records_no_hold_outcome).
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            handle = await env.client.start_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-AIFAIL", 90), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
            await handle.signal(
                FraudHoldWorkflow.customer_responded,
                CustomerResponse(response="it_was_me"),
            )
            result = await handle.result()
    assert result == "released"
    assert "release" in calls
    # The Agent investigation was actually attempted (and failed every
    # time, exhausting the real model-request Activity's own retries) --
    # the workflow didn't skip it, it caught the resulting ActivityError
    # and fell back, then still notified, waited, and resolved normally.
    assert "agent_investigation" in calls
    assert "notify_customer" in calls
    # The customer must have been notified with the fallback message, not
    # an AI-generated one (the AI mock's success message is "test
    # explanation" -- confirm that never went out, and the actual fallback
    # text from the workflow's except block did).
    assert len(notify_messages) == 1
    assert notify_messages[0] != "test explanation"
    assert "temporarily paused" in notify_messages[0]
