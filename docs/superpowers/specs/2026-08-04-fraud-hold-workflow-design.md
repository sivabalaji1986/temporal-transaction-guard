# Fraud Hold Workflow — Design Spec

## Purpose

A minimal, readable Python demo of Temporal's durable execution, applied to a banking
fraud-hold scenario. An existing fraud engine has already scored a transaction and
handed it off with a `fraudScore` and `triggerReason`. This project is only
responsible for what happens next: decide whether to hold, explain the hold to the
customer via a local LLM, notify them, wait durably for their response (with a
timeout), and resolve the case. This is a teaching demo, not production code —
every external effect is mocked with a print statement and a short sleep.

## Stack

- Python 3.11+
- Temporal Python SDK (`temporalio`), latest stable, unpinned
- FastAPI (two endpoints only)
- Pydantic models for all data passed between workflow/activities/API
- pydantic-ai, using a local Ollama model via its OpenAI-compatible endpoint
  (`OLLAMA_BASE_URL`, default `http://localhost:11434/v1`), model name from
  `OLLAMA_MODEL` env var (default `llama3.1`)
- Docker + docker-compose for: Temporal dev server (`temporalio/auto-setup`,
  unpinned), the FastAPI app, and the worker. Ollama runs on the host; the worker
  reaches it via `host.docker.internal`.

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
│       ├── investigate.py
│       ├── hold.py
│       ├── notify.py
│       └── log_outcome.py
└── scripts/
    └── send_signal.py
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

Pydantic `BaseSettings` reading from env (see `.env.example`):

- `ollama_base_url` (default `http://localhost:11434/v1`)
- `ollama_model` (default `llama3.1`)
- `temporal_address` (default `localhost:7233`)
- `task_queue` (default `fraud-hold-task-queue`)
- `fraud_score_threshold` (default `70`)

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
   a. Call `investigate` activity → `InvestigationSummary`.
   b. Call `place_hold` activity.
   c. Call `notify_customer` activity with the investigation's customer-facing
      explanation and notification type.
   d. `workflow.wait_condition` on a signal flag, wrapped with
      `asyncio.wait_for`-style Temporal timeout (24h) via
      `workflow.wait_condition(..., timeout=timedelta(hours=24))`. Signal name:
      `customer_responded`, payload is `CustomerResponse`.
   e. Resolve:
      - `it_was_me` → `release` activity
      - `not_me` → `block` activity
      - timeout (no signal) → `escalate` activity
5. Workflow returns a short result dict/string describing the outcome (for
   visibility in `workflow describe` / the Web UI).

## Activities

All activities are a handful of lines: a `print(...)` describing the mocked effect
and `time.sleep(1)` (activities are sync, run in Temporal's default thread-pool
executor).

- `investigate.py`: builds a pydantic-ai `Agent` with an `OpenAIModel` +
  `OpenAIProvider`(base_url=`OLLAMA_BASE_URL`, api_key="ollama") pointed at Ollama,
  `output_type=InvestigationSummary`. Prompt includes `fraud_score` and
  `trigger_reason`, explicitly instructs the model to *explain*, not *decide*.
- `hold.py`: `place_hold(transaction_id)`, `release(transaction_id)`,
  `block(transaction_id)`, `escalate(transaction_id)` — each prints + sleeps.
- `notify.py`: `notify_customer(customer_id, message, notification_type)` — prints
  + sleeps.
- `log_outcome.py`: `record_no_hold_outcome(transaction_id, fraud_score)` — prints
  + sleeps.

## FastAPI (`app/main.py`)

- `POST /transactions/hold` — body: `Transaction`. Starts
  `FraudHoldWorkflow.run` via the Temporal client, `id=transaction.transaction_id`,
  `task_queue=settings.task_queue`. Returns `{"workflow_id": ...}`.
- `POST /transactions/{transaction_id}/respond` — body: `CustomerResponse`.
  Gets a workflow handle by `transaction_id` and sends the `customer_responded`
  signal. Returns `{"status": "signal_sent"}`.

## Worker (`app/worker.py`)

Connects to `settings.temporal_address`, creates a `Worker` registered with
`FraudHoldWorkflow` and all four activity modules' functions, polls
`settings.task_queue`.

## `scripts/send_signal.py`

Standalone script: connects a Temporal client directly (no FastAPI dependency),
takes `transaction_id` and `response` as CLI args, sends the signal. Demonstrates
that signal delivery doesn't require going through the API.

## Docker Compose

Three services: `temporal` (`temporalio/auto-setup`, exposes 7233 + 8233 UI),
`api` (build from `Dockerfile`, runs uvicorn, port 8000, depends on `temporal`),
`worker` (same image as `api`, runs `python -m app.worker`, depends on `temporal`).
Both `api` and `worker` get `OLLAMA_BASE_URL=http://host.docker.internal:11434/v1`
by default in `.env.example`, and `extra_hosts: host.docker.internal:host-gateway`
for Linux Docker compatibility.

## Locked Decisions

- Fraud score threshold: **70**.
- Temporal SDK / dev-server image: current stable / latest, unpinned.

## Out of Scope

No database, no auth, no tests beyond what's trivial to include, no real
payment/notification integrations. README already documents the run/demo steps;
this spec does not duplicate them.
