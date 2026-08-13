"""FraudHoldWorkflow orchestrates what happens after an upstream fraud engine
identifies a candidate transaction that may require further action and hands
it off with a fraud score and trigger reason -- this workflow does not
recompute that score. Its own deterministic threshold check decides whether
to place a hold; if held, the transaction is held immediately (so fund
protection never waits on the LLM), then a tool-using pydantic-ai Agent (via
a local Ollama model) may gather read-only customer context before producing
a customer-facing explanation and an internal ops summary -- the Agent never
decides whether to hold, release, block, or escalate, only the Workflow does
-- falling back to a fixed, deterministic explanation instead of failing the
workflow if that call exhausts its retries (e.g. Ollama is unreachable) --
the customer is notified, and the workflow durably waits up to 24 hours for
the customer's "it was me" / "not me" response -- resolving to release,
block, or escalate on timeout. Because Temporal persists workflow progress
independently of the worker process, this all resumes correctly even if the
worker is killed and restarted mid-hold.

The Agent's `agent.run(...)` call below executes directly in this Workflow's
code (via the `TemporalDurability` capability configured in
generate_explanation.py, and `PydanticAIWorkflow`/`__pydantic_ai_agents__`
below): each model request and each tool call becomes its own Temporal
Activity, individually recorded in Event History -- not one opaque Activity
containing the whole loop. This means a Worker failure partway through an
investigation does not force the whole investigation to restart: whatever
model/tool steps already completed are preserved in history and are not
re-executed on replay/resume, only the interrupted step retries. See
AGENTS.md for why this is safe (the tools are read-only) and
docs/LEARNING_GUIDE.md Section 2.6 for the full explanation.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError

from app.models import CustomerResponse, InvestigationSummary, Transaction

with workflow.unsafe.imports_passed_through():
    from pydantic_ai.durable_exec.temporal import PydanticAIWorkflow
    from pydantic_ai.exceptions import AgentRunError

    from app.activities.generate_explanation import _AGENT_USAGE_LIMITS, _agent
    from app.activities.hold import block, escalate, place_hold, release
    from app.activities.log_outcome import record_no_hold_outcome
    from app.activities.notify import notify_customer


@workflow.defn
class FraudHoldWorkflow(PydanticAIWorkflow):
    # Tells PydanticAIPlugin (see app/worker.py) which Agent(s)' Temporal
    # activities to register on the Worker -- it walks this list at Worker
    # construction time via `TemporalDurability.from_agent(...)`, so no
    # manual activity registration is needed for the Agent's model/tool
    # activities (unlike place_hold/notify_customer/etc. below, which are
    # plain Activities and still are registered manually in worker.py).
    __pydantic_ai_agents__ = [_agent]

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

        # Place the hold before running the Agent: the hold is what actually
        # protects funds, and that protection shouldn't depend on Ollama
        # being reachable. If the investigation is slow or fails, the hold
        # is already in place; only the customer-facing explanation and
        # notification are delayed.
        await workflow.execute_activity(
            place_hold,
            transaction.transaction_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        # If the Agent fails even after its activities' retries (e.g. Ollama
        # is down, the model never produces valid structured output, or the
        # Agent's usage_limits bound is hit -- see generate_explanation.py),
        # fall back to a deterministic explanation instead of letting the
        # error propagate. The hold has already been placed; failing the
        # whole workflow here would leave the transaction held forever with
        # no notification, no escalation, and no way to resolve it short of
        # manual intervention in Temporal.
        #
        # Two exception types are caught deliberately: `ActivityError` is
        # raised when an individual model-request or tool-call Activity
        # exhausts its own retries (see _MODEL_ACTIVITY_CONFIG /
        # _BASE_ACTIVITY_CONFIG in generate_explanation.py); `AgentRunError`
        # (and subclasses, e.g. UsageLimitExceeded, UnexpectedModelBehavior)
        # is raised directly by the Agent's continuation loop running as
        # Workflow code, not wrapped in an Activity failure. Neither
        # `asyncio.CancelledError` nor an unrelated Temporal infrastructure
        # failure is caught here -- only these two specific, expected Agent
        # failure modes.
        try:
            result = await _agent.run(
                f"fraud_score={transaction.fraud_score}, "
                f"trigger_reason={transaction.trigger_reason}. Gather any "
                "useful context via your tools, then write the "
                "customer_explanation, ops_summary, and notification_type.",
                deps=transaction.customer_id,
                usage_limits=_AGENT_USAGE_LIMITS,
            )
            investigation: InvestigationSummary = result.output
        except (ActivityError, AgentRunError):
            investigation = InvestigationSummary(
                customer_explanation=(
                    "We temporarily paused this transaction because it was "
                    "flagged by our fraud monitoring system. We'll follow up "
                    "shortly."
                ),
                ops_summary=(
                    f"Agent investigation failed after retries for "
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
        except TimeoutError:
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
