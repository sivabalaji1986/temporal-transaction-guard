# tests/test_fraud_hold_workflow.py
import uuid
from typing import Callable

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.models import CustomerResponse, InvestigationSummary, Transaction
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

FRAUD_SCORE_THRESHOLD = 70


def make_mock_activities() -> tuple[list[str], list[Callable]]:
    calls: list[str] = []

    @activity.defn(name="record_no_hold_outcome")
    async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
        calls.append("record_no_hold_outcome")

    @activity.defn(name="generate_explanation")
    async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
        calls.append("generate_explanation")
        return InvestigationSummary(
            customer_explanation="test explanation",
            ops_summary="test ops summary",
            notification_type="sms",
        )

    @activity.defn(name="place_hold")
    async def place_hold(transaction_id: str) -> None:
        calls.append("place_hold")

    @activity.defn(name="notify_customer")
    async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
        calls.append("notify_customer")

    @activity.defn(name="release")
    async def release(transaction_id: str) -> None:
        calls.append("release")

    @activity.defn(name="block")
    async def block(transaction_id: str) -> None:
        calls.append("block")

    @activity.defn(name="escalate")
    async def escalate(transaction_id: str) -> None:
        calls.append("escalate")

    return calls, [
        record_no_hold_outcome,
        generate_explanation,
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
async def test_below_threshold_records_no_hold_outcome():
    calls, activities = make_mock_activities()
    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
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
async def test_it_was_me_releases():
    calls, activities = make_mock_activities()
    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
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
    # place_hold must happen before generate_explanation: the hold protects
    # funds and must not depend on the LLM call succeeding or being fast.
    assert calls.index("place_hold") < calls.index("generate_explanation")


@pytest.mark.asyncio
async def test_not_me_blocks():
    calls, activities = make_mock_activities()
    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
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
    # place_hold must happen before generate_explanation: the hold protects
    # funds and must not depend on the LLM call succeeding or being fast.
    assert calls.index("place_hold") < calls.index("generate_explanation")


@pytest.mark.asyncio
async def test_timeout_escalates():
    calls, activities = make_mock_activities()
    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
        ):
            result = await env.client.execute_workflow(
                FraudHoldWorkflow.run,
                args=[make_transaction("TXN-TIMEOUT", 95), FRAUD_SCORE_THRESHOLD],
                id=str(uuid.uuid4()),
                task_queue=task_queue,
            )
    assert result == "escalated_no_response"
    assert "escalate" in calls
