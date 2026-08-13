from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from app.config import settings
from app.models import CustomerResponse, Transaction
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

_client: Client | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = await Client.connect(
        settings.temporal_address,
        data_converter=pydantic_data_converter,
        plugins=[PydanticAIPlugin()],
    )
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/transactions/hold")
async def hold_transaction(transaction: Transaction) -> dict:
    assert _client is not None
    try:
        handle = await _client.start_workflow(
            FraudHoldWorkflow.run,
            args=[transaction, settings.fraud_score_threshold],
            id=transaction.transaction_id,
            task_queue=settings.task_queue,
            # REJECT_DUPLICATE governs closed (completed) workflows with this
            # ID: without it, the default (ALLOW_DUPLICATE) would let a
            # retried submission silently start a brand-new execution once
            # the original has already finished, defeating the idempotency
            # this endpoint is meant to provide. The default id_conflict_policy
            # already rejects starting over a *currently running* execution
            # with the same ID (raising the same WorkflowAlreadyStartedError
            # caught below) -- this policy covers the other half: a duplicate
            # submitted after the original has already completed.
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        return {"workflow_id": handle.id, "status": "started"}
    except WorkflowAlreadyStartedError:
        # transaction_id is used as the Workflow ID on purpose -- it's a
        # natural idempotency key. Whether the duplicate was submitted while
        # the original is still running or after it already completed, this
        # returns the existing workflow's ID instead of erroring,
        # demonstrating Temporal's dedup-by-Workflow-ID guarantee rather than
        # treating it as an edge case to reject.
        return {"workflow_id": transaction.transaction_id, "status": "already_started"}


@app.post("/transactions/{transaction_id}/respond")
async def respond_to_transaction(transaction_id: str, response: CustomerResponse) -> dict:
    assert _client is not None
    handle = _client.get_workflow_handle(transaction_id)
    try:
        await handle.signal(FraudHoldWorkflow.customer_responded, response)
    except RPCError:
        # Signaling a transaction_id with no matching workflow (unknown or
        # already-completed) raises here rather than returning a normal
        # response -- surface it as a clean 404 instead of an unhandled 500.
        raise HTTPException(status_code=404, detail=f"No transaction found for {transaction_id!r}")
    return {"status": "signal_sent"}
