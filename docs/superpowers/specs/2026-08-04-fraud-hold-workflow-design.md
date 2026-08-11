# Fraud Hold Workflow — Design Spec

## Purpose

A minimal, readable Python demo of Temporal's durable execution, applied to a banking
fraud-hold scenario. The upstream fraud engine does not send every transaction — it
identifies candidate transactions that may require further action and hands each one
off with a `fraudScore`, `triggerReason`, and `customerId`. This project is not a
fraud-detection engine and does not recompute that score. It is only responsible for
applying a deterministic hold policy on top of it (hold at/above a configured
threshold, otherwise don't) and, for held transactions, what happens next: explain
the hold to the customer via a local LLM, notify them, wait durably for their
response (with a timeout), and resolve the case. This is a teaching demo, not
production code — every external effect is mocked with a print statement and a short
sleep.

> Narrative note (2026-08-10): this Purpose section was reworded post-launch to make
> the upstream boundary explicit — the fraud engine identifies *candidates*, it does
> not decide to hold; this project's threshold check is what decides that. The rest
> of this spec (including embedded code snippets) reflects the design as of
> 2026-08-04 and is intentionally left as a historical record — see `AGENTS.md` and
> the current source for the up-to-date system prompt wording.

## Stack

All dependencies and images are pinned to their latest stable release as of
2026-08-04, for reproducibility (this repo accompanies a published article).

- Python 3.11+
- Temporal Python SDK: `temporalio==1.31.0`
- FastAPI: `fastapi==0.141.1`, `uvicorn==0.52.1` (two endpoints only)
- `pydantic==2.13.4` for all data passed between workflow/activities/API
- `pydantic-settings==2.14.2` for env-driven config (separate package from
  `pydantic` in Pydantic v2 — `BaseSettings` was moved out in v2)
- `pydantic-ai-slim[openai]==2.22.0` (NOT the full `pydantic-ai` package — that
  bundles an unused `mcp` extra which pulls in `fastmcp`/`beartype`, and
  `beartype`'s global import hook broke Temporal's sandboxed workflow importer;
  see `requirements.txt`'s comment and commit `0e84137` for the full diagnosis),
  using a local Ollama model via its OpenAI-compatible endpoint
  (`OLLAMA_BASE_URL`, default `http://localhost:11434/v1`), model name
  from `OLLAMA_MODEL` env var (default `llama3.1`)
- Docker + docker-compose for: Temporal dev server (`temporalio/temporal:1.8.2`,
  running `temporal server start-dev`), the FastAPI app, and the worker. Ollama
  runs on the host; the worker reaches it via `host.docker.internal`.

### `requirements.txt` (exact pins)

```
temporalio==1.31.0
fastapi==0.141.1
uvicorn==0.52.1
pydantic==2.13.4
pydantic-settings==2.14.2
pydantic-ai-slim[openai]==2.22.0
pytest==9.1.1
pytest-asyncio==1.4.0
```

(`pytest`/`pytest-asyncio` are for the Tests section below; `temporalio`'s test
utilities, incl. `WorkflowEnvironment` and time-skipping, ship in the base
package — no separate extra needed.)

## Folder Structure

```
temporal-transaction-guard/
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── app/
│   ├── main.py
│   ├── models.py
│   ├── config.py
│   ├── worker.py
│   ├── workflows/
│   │   └── fraud_hold_workflow.py
│   └── activities/
│       ├── generate_explanation.py
│       ├── hold.py
│       ├── notify.py
│       └── log_outcome.py
├── scripts/
│   └── send_signal.py
└── tests/
    └── test_fraud_hold_workflow.py
```

## Data Models (`app/models.py`)

- `Transaction`: `transaction_id: str`, `fraud_score: float`, `trigger_reason: str`,
  `customer_id: str` (aliased to `transactionId`/`fraudScore`/`triggerReason`/
  `customerId` for the JSON wire format via Pydantic `alias`/`populate_by_name`).
- `InvestigationSummary`: `customer_explanation: str`, `ops_summary: str`,
  `notification_type: Literal["sms", "email", "push"]` — the structured output of
  the pydantic-ai agent.
- `CustomerResponse`: `response: Literal["it_was_me", "not_me"]`.

## Config (`app/config.py`)

`from pydantic_settings import BaseSettings` (the `pydantic-settings` package —
`BaseSettings` was moved out of `pydantic` itself in Pydantic v2). Settings read
from env (see `.env.example`):

- `ollama_base_url` (default `http://localhost:11434/v1`)
- `ollama_model` (default `llama3.1`)
- `temporal_address` (default `localhost:7233`)
- `task_queue` (default `fraud-hold-task-queue`)
- `fraud_score_threshold` (default `70`)

### Pydantic data converter (required for every Temporal `Client`)

Temporal's default data converter only knows how to serialize dataclasses and
plain JSON-compatible types — it does not know how to serialize a Pydantic
`BaseModel` (`Transaction`, `InvestigationSummary`, `CustomerResponse`) on its
own. Every place a `temporalio.client.Client` is constructed
(`app/main.py`, `app/worker.py`, `scripts/send_signal.py`, and the test file's
`WorkflowEnvironment.start_time_skipping(...)`) must pass
`data_converter=pydantic_data_converter` from
`temporalio.contrib.pydantic`, e.g.:

```python
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

client = await Client.connect(
    settings.temporal_address, data_converter=pydantic_data_converter
)
```

Without this, passing the Pydantic models as workflow/activity/signal
arguments will fail at runtime. (Verified against the `temporalio==1.31.0`
wheel — `temporalio/contrib/pydantic.py`.)

## Workflow Logic (`app/workflows/fraud_hold_workflow.py`)

Module docstring summarizes the full flow (as in Purpose above).

1. Receive `Transaction` as workflow input.
2. **Deterministic threshold check** (`transaction.fraud_score >= settings-provided
   threshold`, passed into the workflow as a plain arg at start time — not read from
   env inside the workflow, to keep the workflow deterministic on replay). This
   check stays as plain `if` logic inside the workflow, with a comment explaining
   why: it's pure, touches no external system, and must produce the same result
   on every replay — wrapping it in an activity would add a scheduling round-trip
   for zero benefit and risk the two diverging.
3. Below threshold: call `record_no_hold_outcome` activity, workflow completes.
4. At/above threshold:
   a. Call `place_hold` activity first — the hold is what actually protects
      funds, and that protection must not depend on the LLM call below
      succeeding or being fast.
   b. Call `generate_explanation` activity → `InvestigationSummary`. This is the
      pydantic-ai / Ollama call — renamed from "investigate" because the agent's
      job is to *explain* a decision already made in step 2, not to investigate
      or re-decide fraud (consistent with the "AI explains, doesn't decide" rule
      in Purpose). If this activity fails after exhausting its retries (e.g.
      Ollama is unreachable), catch the resulting `ActivityError` and fall back
      to a fixed, deterministic `InvestigationSummary` instead of failing the
      workflow — the hold has already been placed at this point, so failing
      the whole workflow here would leave it stuck forever with no
      notification, no escalation, and no way to resolve it short of manual
      intervention in Temporal.
   c. Call `notify_customer` activity with the (real or fallback)
      investigation's customer-facing explanation and notification type.
   d. Set up a `self._response: CustomerResponse | None = None` field, set by the
      `customer_responded` signal handler. Wait with:
      ```python
      try:
          await workflow.wait_condition(
              lambda: self._response is not None,
              timeout=timedelta(hours=24),
          )
      except asyncio.TimeoutError:
          await workflow.execute_activity(
              escalate, transaction.transaction_id,
              start_to_close_timeout=timedelta(seconds=10),
          )
          return "escalated_no_response"
      ```
      `workflow.wait_condition`'s `timeout` param handles the 24h wait directly —
      no separate `asyncio.wait_for` wrapping needed. `asyncio.TimeoutError` is
      what it raises on expiry.
   e. If the signal arrived before timeout, resolve:
      - `it_was_me` → `release` activity
      - `not_me` → `block` activity
5. Workflow returns a short result string describing the outcome (for visibility
   in `workflow describe` / the Web UI).

## Activities

All activities are `async def`, using `await asyncio.sleep(1)` for the mocked
delay, plus a `print(...)` describing the mocked effect. They're async (not sync)
so the Worker doesn't need an explicit `activity_executor`
(`ThreadPoolExecutor`) — the Temporal Python Worker only requires one for
*synchronous* activities; async activities run directly on the asyncio event
loop, which keeps the demo's worker setup a couple of lines shorter.

- `generate_explanation.py`: `async def generate_explanation(...)` builds a
  pydantic-ai `Agent` with an `OpenAIChatModel` + `OpenAIProvider`
  (base_url=`OLLAMA_BASE_URL`, api_key="ollama") pointed at Ollama,
  `output_type=InvestigationSummary`, called via `await agent.run(...)` (result's
  `.output` holds the `InvestigationSummary`). Prompt includes `fraud_score` and
  `trigger_reason`, explicitly instructs the model to *explain*, not *decide*.
  (Note: pydantic-ai 2.22.0 renamed the old `OpenAIModel` class to
  `OpenAIChatModel` — verified against the installed wheel.)
- `hold.py`: `place_hold(transaction_id)`, `release(transaction_id)`,
  `block(transaction_id)`, `escalate(transaction_id)` — each prints + sleeps.
- `notify.py`: `notify_customer(customer_id, message, notification_type)` — prints
  + sleeps.
- `log_outcome.py`: `record_no_hold_outcome(transaction_id, fraud_score)` — prints
  + sleeps.

### Activity timeouts and retries

Every `workflow.execute_activity(...)` call requires a `start_to_close_timeout`
(the Temporal SDK raises at workflow-run time if it's missing). Since these are
all mocked ~1-second effects, timeouts are generous but not unbounded:

| Activity | `start_to_close_timeout` | Retry policy |
|---|---|---|
| `record_no_hold_outcome` | 10s | default (SDK's built-in retry) |
| `generate_explanation` | 30s | explicit `RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))` — the LLM call is the one most likely to be flaky in a demo (local model, cold start), and it's a good, visible way to show Temporal's retry behavior in the Web UI |
| `place_hold` | 10s | default |
| `notify_customer` | 10s | default |
| `release` / `block` | 10s | default |
| `escalate` | 10s | default |

## FastAPI (`app/main.py`)

- `POST /transactions/hold` — body: `Transaction`. Starts
  `FraudHoldWorkflow.run` via the Temporal client, `id=transaction.transaction_id`,
  `task_queue=settings.task_queue`. Returns `{"workflow_id": ...}`.
- `POST /transactions/{transaction_id}/respond` — body: `CustomerResponse`.
  Gets a workflow handle by `transaction_id` and sends the `customer_responded`
  signal. Returns `{"status": "signal_sent"}`.

### Duplicate transaction handling (idempotency)

Using `transaction_id` as the Workflow ID is intentional — it's a natural
idempotency key. If the fraud engine submits the same `transaction_id` twice
(e.g. a retried webhook), the second `client.start_workflow(...)` call raises
`WorkflowAlreadyStartedError`, imported as
`from temporalio.exceptions import WorkflowAlreadyStartedError` (this is where
the SDK defines it — `temporalio.client` only references it, it doesn't
re-export it, so importing it from `temporalio.client` fails).
`POST /transactions/hold` catches that specific exception and returns the
*existing* workflow's ID with
`{"workflow_id": ..., "status": "already_started"}` instead of a 500. This is a
deliberate demonstration of Temporal's dedup guarantee via Workflow ID, not an
edge case being brushed aside — it's worth calling out explicitly since it's one
of the more visible durable-execution properties in play.

## Worker (`app/worker.py`)

Connects to `settings.temporal_address`, creates a `Worker` registered with
`FraudHoldWorkflow` and all four activity modules' functions, polls
`settings.task_queue`.

## `scripts/send_signal.py`

Standalone script: connects a Temporal client directly (no FastAPI dependency),
takes `transaction_id` and `response` as CLI args, sends the signal. Demonstrates
that signal delivery doesn't require going through the API.

## Docker Compose

Three services: `temporal` (`temporalio/temporal:1.8.2`, running
`temporal server start-dev --ip 0.0.0.0`, exposes 7233 + 8233 UI), `api` (build
from `Dockerfile`, runs uvicorn, port 8000, depends on `temporal`), `worker`
(same image as `api`, runs `python -m app.worker`, depends on `temporal`). Both
`api` and `worker` get `OLLAMA_BASE_URL=http://host.docker.internal:11434/v1`
by default in `.env.example`, and `extra_hosts: host.docker.internal:host-gateway`
for Linux Docker compatibility.

`temporalio/temporal` is the Temporal CLI image — `temporal server start-dev`
bundles the server and the Web UI in one process/container, both listening
where the CLI's dev-server normally puts them (7233 for the frontend service,
8233 for the Web UI). This replaces the earlier `temporalio/auto-setup` choice:
`auto-setup` only runs the server against a preconfigured database and does
**not** ship the Web UI — getting the UI that way would need a second
`temporalio/ui` container. Using the CLI dev-server image instead keeps this
demo to a single Temporal container.

## Tests

A short test module (e.g. `tests/test_fraud_hold_workflow.py`) using Temporal's
Python test framework — `temporalio.testing.WorkflowEnvironment` with
time-skipping (`start_time_skipping()`) so the 24h timeout test doesn't actually
wait 24 hours. All activities are mocked (no real Ollama call in tests). Five
cases, each short:

1. **Below threshold** — `fraud_score=50` → workflow completes without calling
   `place_hold`/`notify_customer`; `record_no_hold_outcome` was called.
2. **`it_was_me`** — above threshold, send `customer_responded` signal with
   `it_was_me` → `release` activity called, not `block`/`escalate`.
3. **`not_me`** — above threshold, send signal with `not_me` → `block` activity
   called.
4. **Timeout → escalate** — above threshold, no signal sent, time-skip past 24h
   → `escalate` activity called.
5. **`generate_explanation` failure/fallback** — above threshold, the mocked
   `generate_explanation` activity raises on every call (simulating exhausted
   retries) → the workflow catches the resulting `ActivityError`, falls back to
   a fixed `InvestigationSummary`, and still notifies/waits/resolves normally
   after a `customer_responded` signal (added post-launch to close a real
   reliability gap — see `app/workflows/fraud_hold_workflow.py`).

This is meant to demonstrate the workflow is genuinely testable without a real
Temporal server or LLM, not to be a full suite.

> Narrative note (2026-08-11): `generate_explanation` was later upgraded from a
> single LLM call to a tool-using PydanticAI Agent (two read-only mock tools, an
> explicit `usage_limits` bound), which added a sixth test-file,
> `tests/test_generate_explanation_agent.py` (six Agent-level tests, using
> `TestModel`/`FunctionModel` — no real Ollama), bringing the suite to 11 tests
> total. The five tests described above are unchanged. See `AGENTS.md` and
> `docs/LEARNING_GUIDE.md` Sections 2.6/2.12 for the current design and tests;
> this section is left as a record of the 2026-08-04 scope.

## Locked Decisions

- Fraud score threshold: **70**.
- Dependency/image pins: `temporalio==1.31.0`, `fastapi==0.141.1`,
  `uvicorn==0.52.1`, `pydantic==2.13.4`, `pydantic-settings==2.14.2`,
  `pydantic-ai-slim[openai]==2.22.0` (not the full `pydantic-ai` — see Stack
  above), `temporalio/temporal:1.8.2` — all latest stable as of 2026-08-04.

## Out of Scope

No database, no auth, no real payment/notification integrations. The five
workflow tests above were, as of 2026-08-04, the extent of test coverage — no
API-layer or activity-internals tests. (As the 2026-08-11 note above
describes, activity-internals tests for the Agent were later added
deliberately, to close a real coverage gap the Agent upgrade introduced — not
as scope creep past this boundary.) README already documents the run/demo
steps; this spec does not duplicate them.
