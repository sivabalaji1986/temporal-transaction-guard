"""FraudHoldWorkflow orchestrates what happens after an existing fraud engine
flags a transaction: a deterministic score check decides whether to place a
hold; if held, the transaction is held immediately (so fund protection never
waits on the LLM), then a pydantic-ai agent (via a local Ollama model)
generates a customer-facing explanation and an internal ops summary -- falling
back to a fixed, deterministic explanation instead of failing the workflow if
that call exhausts its retries (e.g. Ollama is unreachable) -- the customer is
notified, and the workflow durably waits up to 24 hours for the customer's
"it was me" / "not me" response -- resolving to release, block, or escalate on
timeout. Because Temporal persists workflow progress independently of the
worker process, this all resumes correctly even if the worker is killed and
restarted mid-hold.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from app.models import CustomerResponse, InvestigationSummary, Transaction

with workflow.unsafe.imports_passed_through():
    from app.activities.generate_explanation import generate_explanation
    from app.activities.hold import block, escalate, place_hold, release
    from app.activities.log_outcome import record_no_hold_outcome
    from app.activities.notify import notify_customer


@workflow.defn
class FraudHoldWorkflow:
    def __init__(self) -> None:
        self._response: CustomerResponse | None = None

    @workflow.signal
    def customer_responded(self, response: CustomerResponse) -> None:
        self._response = response

    @workflow.run
    async def run(self, transaction: Transaction, fraud_score_threshold: float) -> str:
        # Deterministic threshold check: this stays as plain workflow logic,
        # not an activity. It's pure (touches no external system) and must
        # produce the exact same result every time this workflow is replayed
        # from history. Wrapping it in an activity would add a scheduling
        # round trip for zero benefit, and risks the replayed decision
        # diverging from the original if the threshold value were ever read
        # from somewhere mutable instead of the immutable start argument.
        if transaction.fraud_score < fraud_score_threshold:
            await workflow.execute_activity(
                record_no_hold_outcome,
                args=[transaction.transaction_id, transaction.fraud_score],
                start_to_close_timeout=timedelta(seconds=10),
            )
            return "no_hold_needed"

        # Place the hold before generating the LLM explanation: the hold is
        # what actually protects funds, and that protection shouldn't depend
        # on Ollama being reachable. If the explanation call is slow or
        # fails, the hold is already in place; only the customer-facing
        # explanation and notification are delayed.
        await workflow.execute_activity(
            place_hold,
            transaction.transaction_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        # If the LLM call fails even after retries (e.g. Ollama is down),
        # fall back to a deterministic explanation instead of letting the
        # ActivityError propagate. The hold has already been placed; failing
        # the whole workflow here would leave the transaction held forever
        # with no notification, no escalation, and no way to resolve it
        # short of manual intervention in Temporal.
        try:
            investigation: InvestigationSummary = await workflow.execute_activity(
                generate_explanation,
                args=[transaction.fraud_score, transaction.trigger_reason],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    maximum_attempts=3, initial_interval=timedelta(seconds=1)
                ),
            )
        except ActivityError:
            investigation = InvestigationSummary(
                customer_explanation=(
                    "We temporarily paused this transaction because it was "
                    "flagged by our fraud monitoring system. We'll follow up "
                    "shortly."
                ),
                ops_summary=(
                    f"generate_explanation failed after retries for "
                    f"transaction {transaction.transaction_id}; used "
                    f"fallback message."
                ),
                notification_type="sms",
            )

        await workflow.execute_activity(
            notify_customer,
            args=[
                transaction.customer_id,
                investigation.customer_explanation,
                investigation.notification_type,
            ],
            start_to_close_timeout=timedelta(seconds=10),
        )

        try:
            await workflow.wait_condition(
                lambda: self._response is not None,
                timeout=timedelta(hours=24),
            )
        except asyncio.TimeoutError:
            await workflow.execute_activity(
                escalate,
                transaction.transaction_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            return "escalated_no_response"

        assert self._response is not None
        if self._response.response == "it_was_me":
            await workflow.execute_activity(
                release,
                transaction.transaction_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            return "released"

        await workflow.execute_activity(
            block,
            transaction.transaction_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return "blocked"
