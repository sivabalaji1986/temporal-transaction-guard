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

4. **Use `pydantic-ai-slim[openai]`, never the full `pydantic-ai` package.**
   The full package pulls in the `mcp` extra, which imports `beartype.claw`
   as a global import hook. That hook poisons Temporal's sandboxed workflow
   importer and crashes the worker permanently after a restart. This took
   three attempts to diagnose — don't reintroduce it by adding `pydantic-ai`
   as a transitive or direct dependency.

5. **The Agent's tools stay read-only and stay inside the single
   `generate_explanation` Activity.** Don't split tool calls into separate
   Temporal Activities, and don't add a write tool (real or mocked) that a
   future change could accidentally let the Agent call. Because the whole
   Agent run (all tool calls plus the final structured output) lives in one
   Activity, a Temporal retry re-runs the entire Agent loop, including any
   tool calls already made — this is only safe because the tools are
   read-only and idempotent. If a future tool has a side effect, this
   assumption breaks and needs revisiting.

6. **The Agent's tool-calling loop is explicitly bounded**, via
   `usage_limits=UsageLimits(request_limit=6, tool_calls_limit=4)` on
   `_agent.run(...)` in `generate_explanation.py` — not left unbounded. If
   you add more tools or expect more round trips, raise these deliberately
   and re-check `start_to_close_timeout` in `fraud_hold_workflow.py`
   against the new worst case; don't just delete the limit.

7. **All existing tests must stay green**, currently 11 across
   `tests/test_fraud_hold_workflow.py` (5, Workflow-level, mocked
   activities) and `tests/test_generate_explanation_agent.py` (6, Agent-level,
   `TestModel`/`FunctionModel` — no real Ollama). Adding a new test for a
   real correctness gap is fine — just call it out explicitly rather than
   silently changing the expected test count, and don't add a test that
   requires a live Ollama, Temporal server, or Docker.
