# AGENTS.md

Load-bearing design rules for this repo. Read before changing workflow or
activity logic. For a general walkthrough, see `README.md` and
`docs/LEARNING_GUIDE.md` instead — this file is guardrails only.

## Rules

1. **Agentic investigation, deterministic action.** `generate_explanation`'s
   PydanticAI Agent may call read-only tools to gather context, but it never
   decides whether to hold, release, block, or escalate a transaction, and
   it must never be given a write-side/banking tool. Every action decision
   stays in the Workflow.

2. **`place_hold` always runs before `generate_explanation`.** Fund
   protection must not depend on the LLM (or its tool calls) being
   reachable. If the Agent Activity is slow or fails, the hold is already in
   place.

3. **The fraud-score threshold check is deterministic workflow logic, not an
   Activity.** It's pure, touches no external system, and must replay to the
   same result every time. Don't move it into an Activity.

4. **Use `pydantic-ai-slim[openai,temporal]`, never the full `pydantic-ai`
   package.** The full package pulls in the `mcp` extra, which imports
   `beartype.claw` as a global import hook. That hook poisons Temporal's
   sandboxed workflow importer and crashes the worker permanently after a
   restart. This took three attempts to diagnose — don't reintroduce it by
   adding `pydantic-ai` as a transitive or direct dependency. The `temporal`
   extra is required for `pydantic_ai.durable_exec.temporal`
   (`TemporalDurability`, `PydanticAIWorkflow`, `PydanticAIPlugin`).

5. **The Agent's tools stay read-only.** Don't add a write tool (real or
   mocked) that a future change could accidentally let the Agent call. With
   `TemporalDurability`, each model request and each tool call runs as its
   own Temporal Activity (see
   `app/activities/generate_explanation.py`'s `_agent`), so a retry now
   re-executes only the failed step, not the whole Agent loop — tool calls
   already completed and recorded in history are not re-run. The read-only
   requirement is still load-bearing though: don't assume idempotency covers
   you if a future tool has a side effect, because a *model request* retry
   can still cause the Agent to re-decide which tool to call next.

6. **The Agent's tool-calling loop is explicitly bounded**, via
   `usage_limits=UsageLimits(request_limit=6, tool_calls_limit=4)` on
   `_agent.run(...)` in `generate_explanation.py` — not left unbounded.
   Each model-request Activity uses `start_to_close_timeout=60s` with 2
   attempts and each tool-call Activity uses `10s` with 2 attempts (see
   `_MODEL_ACTIVITY_CONFIG`/`_BASE_ACTIVITY_CONFIG` in
   `generate_explanation.py`), for a worst case of 810s, kept under the
   workflow-level 900s ceiling. If you add more tools, raise the round-trip
   count, or change a model's typical latency, recompute this worst case
   deliberately — don't just delete or silently widen the limits. The
   model-request retry backoff (the `1s` in that calculation) is
   configurable via `settings.demo_model_retry_interval_seconds`
   (`DEMO_MODEL_RETRY_INTERVAL_SECONDS`), default `1` — a demo/observability
   knob like `DEMO_FAILOVER_DELAY_SECONDS`, not something to widen for
   routine tuning. Raising it also raises the 810s worst case and must be
   recomputed the same way. Because `Settings` reads `.env` from the current
   working directory regardless of context, leaving either `DEMO_*` variable
   set in your local `.env` after a demo silently slows down (or, for
   `DEMO_FAILOVER_DELAY_SECONDS`, can badly inflate) a native `pytest tests/`
   run too, not just the Docker/native "stack" — reset both to `0`/`1` when
   you're done with a demo.

7. **The Agent's stable name is a durability contract, not cosmetic.**
   `Agent(..., name="fraud_hold_investigator")` in `generate_explanation.py`
   determines the Temporal Activity type names PydanticAI registers
   (`agent__fraud_hold_investigator__model_request`,
   `agent__fraud_hold_investigator__toolset__<agent>__call_tool`). Renaming
   it is a breaking change for any in-flight Workflow execution — history
   events reference the old Activity type names, so replay against a
   renamed Agent fails. Treat a rename like a workflow-versioning event, not
   a routine refactor.

8. **Never use `pydantic_ai.durable_exec.temporal.TemporalAgent`.** It's
   deprecated in favor of the capability-based `Agent(...,
   capabilities=[TemporalDurability(...)])` pattern this repo uses, and its
   own source marks it for removal in a future major version.

9. **`customer_id` reaches tools only via `RunContext.deps`, never as a
   tool argument the model can see or set.** `_agent.run(..., deps=
   transaction.customer_id)` in `fraud_hold_workflow.py` and every
   `@_agent.tool` signature (`ctx: RunContext[str]`, reading `ctx.deps`)
   must keep this shape. This also holds across the Activity boundary:
   `deps` rides as a separate, typed Activity parameter, not inside
   `tool_args`. Don't add a `customer_id: str` parameter to a tool function
   — that would let the model supply or guess it.

10. **`UnsandboxedWorkflowRunner()` in `tests/test_fraud_hold_workflow.py`
    is intentional, not a shortcut to remove.** Temporal's default
    `SandboxedWorkflowRunner` re-imports workflow-defining modules into an
    isolated copy, so `monkeypatch.setattr()` on `_agent` /
    `__pydantic_ai_agents__` from the test process doesn't reach the
    sandboxed copy the Workflow actually runs. The comment at the test
    `Worker(...)` construction explains this:

    > Test-only: required so pytest monkeypatch of the production Agent is
    > visible to Workflow execution. Production Worker remains sandboxed;
    > replay/determinism is validated separately using the normal Temporal
    > runner.

    Production `app/worker.py` must stay on the normal sandboxed runner.
    Replay/determinism safety is validated separately, under the sandboxed
    runner, by `test_replay_determinism` in
    `tests/test_generate_explanation_agent_durability.py` (via
    `temporalio.worker.Replayer`) — don't let a future cleanup remove
    `UnsandboxedWorkflowRunner()` from the Workflow-path tests thinking it's
    redundant with that determinism test; they cover different things.

11. **All existing tests must stay green**, currently 15 across
    `tests/test_fraud_hold_workflow.py` (5, Workflow-level, mocked
    activities + monkeypatched test Agent),
    `tests/test_generate_explanation_agent.py` (7, Agent-level,
    `TestModel`/`FunctionModel`, no Temporal Workflow, no real Ollama), and
    `tests/test_generate_explanation_agent_durability.py` (3, fine-grained
    durability proofs: per-Activity retry doesn't re-run completed tool
    calls, `UsageLimitExceeded` propagates through the Activity boundary,
    and replay determinism via `Replayer`). Adding a new test for a real
    correctness gap is fine — just call it out explicitly rather than
    silently changing the expected test count, and don't add a test that
    requires a live Ollama, Temporal server, or Docker.
