# AGENTS.md

Load-bearing design rules for this repo. Read before changing workflow or
activity logic. For a general walkthrough, see `README.md` and
`docs/LEARNING_GUIDE.md` instead — this file is guardrails only.

## Rules

1. **The AI explains; it never decides.** `generate_explanation` produces a
   customer-facing explanation and an ops summary for a decision the workflow
   has already made. It must never be given the authority to decide whether
   to hold, release, block, or escalate a transaction.

2. **`place_hold` always runs before `generate_explanation`.** Fund
   protection must not depend on the LLM being reachable. If the explanation
   call is slow or fails, the hold is already in place.

3. **The fraud-score threshold check is deterministic workflow logic, not an
   Activity.** It's pure, touches no external system, and must replay to the
   same result every time. Don't move it into an Activity.

4. **Use `pydantic-ai-slim[openai]`, never the full `pydantic-ai` package.**
   The full package pulls in the `mcp` extra, which imports `beartype.claw`
   as a global import hook. That hook poisons Temporal's sandboxed workflow
   importer and crashes the worker permanently after a restart. This took
   three attempts to diagnose — don't reintroduce it by adding `pydantic-ai`
   as a transitive or direct dependency.

5. **All 5 existing tests in `tests/test_fraud_hold_workflow.py` must stay
   green.** Adding a new test for a real correctness gap is fine — just call
   it out explicitly rather than silently changing the expected test count.
