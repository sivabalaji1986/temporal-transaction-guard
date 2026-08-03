# Fraud Hold Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `temporal-transaction-guard` — a minimal, readable Python demo of Temporal's durable execution applied to a banking fraud-hold scenario (explain → hold → notify → wait → resolve).

**Architecture:** A FastAPI app starts a Temporal workflow per transaction. The workflow does a deterministic threshold check, then (if above threshold) places the hold, calls a pydantic-ai/Ollama activity to generate customer/ops explanations, notifies the customer, and durably waits up to 24h for a `customer_responded` signal before resolving to release/block/escalate. A separate worker process executes the workflow and all activities; killing and restarting it mid-hold must not lose or duplicate any side effect.

**Tech Stack:** Python 3.11+, `temporalio` 1.31.0, FastAPI 0.141.1 + uvicorn 0.52.1, `pydantic` 2.13.4, `pydantic-settings` 2.14.2, `pydantic-ai` 2.22.0 (via Ollama's OpenAI-compatible endpoint), Docker + docker-compose (`temporalio/temporal:1.8.2` dev-server image).

Reference spec: `docs/superpowers/specs/2026-08-04-fraud-hold-workflow-design.md`

## Global Constraints

- Python 3.11+. All dependency/image versions are pinned exactly (no `latest`, no ranges): `temporalio==1.31.0`, `fastapi==0.141.1`, `uvicorn==0.52.1`, `pydantic==2.13.4`, `pydantic-settings==2.14.2`, `pydantic-ai==2.22.0`, `pytest==9.1.1`, `pytest-asyncio==1.4.0`, Docker image `temporalio/temporal:1.8.2`.
- Fraud score threshold is **70**, passed into the workflow as a start argument (not read from env inside the workflow) — this keeps the workflow deterministic on replay.
- The threshold check is plain `if` logic **inside** the workflow, never an activity. Comment above it must explain why (determinism/replay-safety, not just cost).
- Every activity is `async def`, using `await asyncio.sleep(1)` plus a `print(...)` for its mocked effect. No `activity_executor` is configured on the Worker — this only works because every activity is async.
- Every `workflow.execute_activity(...)` call must pass `start_to_close_timeout`; `generate_explanation` additionally gets `RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))`.
- Every `temporalio.client.Client` (in `main.py`, `worker.py`, `scripts/send_signal.py`, and the test file's `WorkflowEnvironment.start_time_skipping(...)`) must pass `data_converter=pydantic_data_converter` from `temporalio.contrib.pydantic`, or Pydantic models fail to serialize across the workflow boundary.
- Catch `WorkflowAlreadyStartedError` from `temporalio.exceptions` (not `temporalio.client`) in the hold endpoint. Pass `id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE` (from `temporalio.common`) on `start_workflow` so a duplicate `transaction_id` is rejected the same way whether the original execution is still running or has already completed — the default `ALLOW_DUPLICATE` only catches the former.
- In the above-threshold branch, `place_hold` runs **before** `generate_explanation`: the hold protects funds and must not depend on the LLM call succeeding or being fast.
- The Docker crash-resume demo (Task 10) proves the *workflow's* durable progress and correct replay — not exactly-once *activity* execution. Activities are at-least-once; a real (non-mocked) hold/release/block integration must be idempotent on its own (e.g. keyed by `transaction_id`).
- `investigate` is named `generate_explanation` everywhere (file, function, activity name) — the agent explains a decision already made, it does not re-decide.
- **No tests beyond the four workflow tests** specified in Task 6 (below-threshold, it_was_me, not_me, timeout→escalate). Do not add tests for models, config, individual activities, or the FastAPI layer — verify those manually (a `python -c` snippet, `docker compose config`, or `curl`), per the spec's explicit scope limit.
- No database, no auth, no real payment/notification integrations — every activity's "effect" is a print statement and a sleep.
- `README.md` already documents run/demo instructions in full — do not duplicate them in code comments or docstrings.

---

## File Structure

```
temporal-transaction-guard/
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── app/
│   ├── __init__.py
│   ├── main.py                        # FastAPI app (Task 8)
│   ├── models.py                      # Transaction, InvestigationSummary, CustomerResponse (Task 2)
│   ├── config.py                      # Settings (Task 3)
│   ├── worker.py                      # Temporal worker entrypoint (Task 7)
│   ├── workflows/
│   │   ├── __init__.py
│   │   └── fraud_hold_workflow.py     # FraudHoldWorkflow (Task 6)
│   └── activities/
│       ├── __init__.py
│       ├── generate_explanation.py    # pydantic-ai / Ollama (Task 5)
│       ├── hold.py                    # place_hold, release, block, escalate (Task 4)
│       ├── notify.py                  # notify_customer (Task 4)
│       └── log_outcome.py             # record_no_hold_outcome (Task 4)
├── scripts/
│   ├── __init__.py
│   └── send_signal.py                 # standalone signal sender (Task 9)
└── tests/
    └── test_fraud_hold_workflow.py    # the 4 required workflow tests (Task 6)
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `app/__init__.py`, `app/workflows/__init__.py`, `app/activities/__init__.py`, `scripts/__init__.py` (all empty)

**Interfaces:**
- Produces: a `pip install -r requirements.txt` environment with every pinned dependency; a `Dockerfile` image usable by both the `api` and `worker` docker-compose services; env var names later tasks read via `app.config.Settings` (`OLLAMA_BASE_URL`, `OLLAMA_MODEL`, `TEMPORAL_ADDRESS`, `TASK_QUEUE`, `FRAUD_SCORE_THRESHOLD`).

- [ ] **Step 1: Create `requirements.txt`**

```
temporalio==1.31.0
fastapi==0.141.1
uvicorn==0.52.1
pydantic==2.13.4
pydantic-settings==2.14.2
pydantic-ai==2.22.0
pytest==9.1.1
pytest-asyncio==1.4.0
```

- [ ] **Step 2: Create `.env.example`**

```
# Ollama runs on the host, not in Docker. This default is for running the
# app natively (outside Docker). docker-compose.yml overrides this to
# http://host.docker.internal:11434/v1 for the api/worker containers.
OLLAMA_BASE_URL=http://localhost:11434/v1
OLLAMA_MODEL=llama3.1

# Default is for running natively. docker-compose.yml overrides this to
# temporal:7233 for the api/worker containers (the compose service name).
TEMPORAL_ADDRESS=localhost:7233

TASK_QUEUE=fraud-hold-task-queue
FRAUD_SCORE_THRESHOLD=70
```

- [ ] **Step 3: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY scripts/ ./scripts/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 4: Create `docker-compose.yml`**

```yaml
services:
  temporal:
    image: temporalio/temporal:1.8.2
    command: server start-dev --ip 0.0.0.0
    ports:
      - "7233:7233"
      - "8233:8233"
    healthcheck:
      # `temporal` is the same CLI binary running the server (it's this
      # image's ENTRYPOINT), so it's available to re-invoke here against the
      # server it's hosting. This only confirms the frontend service is
      # accepting requests -- not that persistence or the namespace cache is
      # fully warm -- but that's sufficient to gate api/worker startup.
      test: ["CMD", "temporal", "operator", "cluster", "health"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 10s

  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
      - TASK_QUEUE=${TASK_QUEUE:-fraud-hold-task-queue}
      - OLLAMA_BASE_URL=${OLLAMA_BASE_URL:-http://host.docker.internal:11434/v1}
      - OLLAMA_MODEL=${OLLAMA_MODEL:-llama3.1}
      - FRAUD_SCORE_THRESHOLD=${FRAUD_SCORE_THRESHOLD:-70}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      temporal:
        condition: service_healthy

  worker:
    build: .
    command: python -m app.worker
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
      - TASK_QUEUE=${TASK_QUEUE:-fraud-hold-task-queue}
      - OLLAMA_BASE_URL=${OLLAMA_BASE_URL:-http://host.docker.internal:11434/v1}
      - OLLAMA_MODEL=${OLLAMA_MODEL:-llama3.1}
      - FRAUD_SCORE_THRESHOLD=${FRAUD_SCORE_THRESHOLD:-70}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      temporal:
        condition: service_healthy
```

- [ ] **Step 5: Create the empty package init files**

```bash
mkdir -p app/workflows app/activities scripts tests
touch app/__init__.py app/workflows/__init__.py app/activities/__init__.py scripts/__init__.py
```

- [ ] **Step 6: Verify scaffolding**

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose config >/dev/null && echo "compose file valid"
docker build -t temporal-transaction-guard-app . && echo "image builds"
```

Expected: `pip install` succeeds, `docker compose config` prints no errors (exits 0), `docker build` succeeds (the app/ and scripts/ directories only contain `__init__.py` at this point — that's fine, the image doesn't need to run yet).

- [ ] **Step 7: Commit**

```bash
git add requirements.txt .env.example Dockerfile docker-compose.yml app/__init__.py app/workflows/__init__.py app/activities/__init__.py scripts/__init__.py
git commit -m "Scaffold project: pinned deps, Docker, package skeleton"
```

---

### Task 2: Data models

**Files:**
- Create: `app/models.py`

**Interfaces:**
- Consumes: nothing (leaf module).
- Produces: `Transaction(transaction_id: str, fraud_score: float, trigger_reason: str, customer_id: str)`, `InvestigationSummary(customer_explanation: str, ops_summary: str, notification_type: Literal["sms", "email", "push"])`, `CustomerResponse(response: Literal["it_was_me", "not_me"])` — all Pydantic `BaseModel`s, imported by every later task.

- [ ] **Step 1: Write `app/models.py`**

```python
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Transaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    transaction_id: str = Field(alias="transactionId")
    fraud_score: float = Field(alias="fraudScore")
    trigger_reason: str = Field(alias="triggerReason")
    customer_id: str = Field(alias="customerId")


class InvestigationSummary(BaseModel):
    customer_explanation: str
    ops_summary: str
    notification_type: Literal["sms", "email", "push"]


class CustomerResponse(BaseModel):
    response: Literal["it_was_me", "not_me"]
```

- [ ] **Step 2: Verify manually**

```bash
python3 -c "
from app.models import Transaction, InvestigationSummary, CustomerResponse

t = Transaction.model_validate({
    'transactionId': 'TXN-1001',
    'fraudScore': 78,
    'triggerReason': 'UNUSUAL_LOCATION',
    'customerId': 'CUST-101',
})
print(t)
print(InvestigationSummary(customer_explanation='x', ops_summary='y', notification_type='sms'))
print(CustomerResponse(response='it_was_me'))
"
```

Expected: all three print without error; `t.fraud_score == 78` and `t.transaction_id == 'TXN-1001'`.

- [ ] **Step 3: Commit**

```bash
git add app/models.py
git commit -m "Add Transaction, InvestigationSummary, CustomerResponse models"
```

---

### Task 3: Config

**Files:**
- Create: `app/config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: a module-level `settings: Settings` instance with fields `ollama_base_url: str`, `ollama_model: str`, `temporal_address: str`, `task_queue: str`, `fraud_score_threshold: float`, populated from env vars (case-insensitive match) or `.env`.

- [ ] **Step 1: Write `app/config.py`**

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ollama_base_url: str = "http://localhost:11434/v1"
    ollama_model: str = "llama3.1"
    temporal_address: str = "localhost:7233"
    task_queue: str = "fraud-hold-task-queue"
    fraud_score_threshold: float = 70


settings = Settings()
```

- [ ] **Step 2: Verify manually**

```bash
python3 -c "from app.config import settings; print(settings)"
FRAUD_SCORE_THRESHOLD=55 python3 -c "from app.config import settings; print(settings.fraud_score_threshold)"
```

Expected: first command prints defaults including `fraud_score_threshold=70.0`; second prints `55.0`, confirming env override works.

- [ ] **Step 3: Commit**

```bash
git add app/config.py
git commit -m "Add env-driven Settings"
```

---

### Task 4: Mocked side-effect activities (hold, notify, log_outcome)

**Files:**
- Create: `app/activities/log_outcome.py`
- Create: `app/activities/hold.py`
- Create: `app/activities/notify.py`

**Interfaces:**
- Consumes: nothing but stdlib `asyncio` and `temporalio.activity`.
- Produces: `record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None`, `place_hold(transaction_id: str) -> None`, `release(transaction_id: str) -> None`, `block(transaction_id: str) -> None`, `escalate(transaction_id: str) -> None`, `notify_customer(customer_id: str, message: str, notification_type: str) -> None` — all `@activity.defn`, all consumed by Task 6's workflow and Task 7's worker registration.

- [ ] **Step 1: Write `app/activities/log_outcome.py`**

```python
import asyncio

from temporalio import activity


@activity.defn
async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
    print(f"[log_outcome] {transaction_id}: fraud_score={fraud_score} below threshold, no hold placed")
    await asyncio.sleep(1)
```

- [ ] **Step 2: Write `app/activities/hold.py`**

```python
import asyncio

from temporalio import activity


@activity.defn
async def place_hold(transaction_id: str) -> None:
    print(f"[hold] placing temporary hold on {transaction_id}")
    await asyncio.sleep(1)


@activity.defn
async def release(transaction_id: str) -> None:
    print(f"[hold] releasing hold on {transaction_id}: customer confirmed it was them")
    await asyncio.sleep(1)


@activity.defn
async def block(transaction_id: str) -> None:
    print(f"[hold] blocking {transaction_id}: customer says this wasn't them")
    await asyncio.sleep(1)


@activity.defn
async def escalate(transaction_id: str) -> None:
    print(f"[hold] escalating {transaction_id} for manual review: no customer response")
    await asyncio.sleep(1)
```

- [ ] **Step 3: Write `app/activities/notify.py`**

```python
import asyncio

from temporalio import activity


@activity.defn
async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
    print(f"[notify] sending {notification_type} to {customer_id}: {message}")
    await asyncio.sleep(1)
```

- [ ] **Step 4: Verify manually**

```bash
python3 -c "
import asyncio
from app.activities.hold import place_hold, release, block, escalate
from app.activities.notify import notify_customer
from app.activities.log_outcome import record_no_hold_outcome

async def main():
    await place_hold('TXN-1')
    await release('TXN-1')
    await block('TXN-1')
    await escalate('TXN-1')
    await notify_customer('CUST-1', 'hi', 'sms')
    await record_no_hold_outcome('TXN-1', 40)

asyncio.run(main())
"
```

Expected: six `print(...)` lines, one per activity, each after a ~1s pause, no exceptions.

- [ ] **Step 5: Commit**

```bash
git add app/activities/log_outcome.py app/activities/hold.py app/activities/notify.py
git commit -m "Add mocked hold/notify/log_outcome activities"
```

---

### Task 5: `generate_explanation` activity (pydantic-ai + Ollama)

**Files:**
- Create: `app/activities/generate_explanation.py`

**Interfaces:**
- Consumes: `app.config.settings` (Task 3), `app.models.InvestigationSummary` (Task 2).
- Produces: `generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary`, an `@activity.defn`, consumed by Task 6's workflow and Task 7's worker registration.

- [ ] **Step 1: Write `app/activities/generate_explanation.py`**

```python
from temporalio import activity

from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from app.config import settings
from app.models import InvestigationSummary

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    output_type=InvestigationSummary,
    system_prompt=(
        "A bank's fraud engine has already decided to hold a transaction. "
        "Your job is only to EXPLAIN that decision clearly -- to the customer "
        "in plain language, and to fraud-ops staff as a short internal summary "
        "-- and to pick the best notification channel. You do NOT decide "
        "whether to hold the transaction; that decision has already been made."
    ),
)


@activity.defn
async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
    result = await _agent.run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Write the customer_explanation, ops_summary, and notification_type."
    )
    return result.output
```

- [ ] **Step 2: Verify manually (requires a running local Ollama with the configured model pulled)**

```bash
ollama pull llama3.1   # if not already pulled
python3 -c "
import asyncio
from app.activities.generate_explanation import generate_explanation

async def main():
    summary = await generate_explanation(78, 'UNUSUAL_LOCATION')
    print(summary)

asyncio.run(main())
"
```

Expected: prints an `InvestigationSummary` with non-empty `customer_explanation`/`ops_summary` and a valid `notification_type`. If Ollama isn't running locally, this step will fail with a connection error — that's expected in an environment without Ollama; note it and move on, since Task 6's tests mock this activity entirely and don't depend on a live Ollama.

- [ ] **Step 3: Commit**

```bash
git add app/activities/generate_explanation.py
git commit -m "Add generate_explanation activity (pydantic-ai via Ollama)"
```

---

### Task 6: Workflow + the four required tests

**Files:**
- Create: `app/workflows/fraud_hold_workflow.py`
- Create: `tests/test_fraud_hold_workflow.py`

**Interfaces:**
- Consumes: `Transaction`, `InvestigationSummary`, `CustomerResponse` (Task 2); `record_no_hold_outcome`, `generate_explanation`, `place_hold`, `release`, `block`, `escalate`, `notify_customer` (Tasks 4 & 5).
- Produces: `FraudHoldWorkflow` — a `@workflow.defn` class with `@workflow.run async def run(self, transaction: Transaction, fraud_score_threshold: float) -> str` and `@workflow.signal def customer_responded(self, response: CustomerResponse) -> None` — consumed by Task 7 (worker registration), Task 8 (FastAPI starts/signals it), and Task 9 (script signals it).

- [ ] **Step 1: Write the test file first**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_fraud_hold_workflow.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'app.workflows.fraud_hold_workflow'` (the module doesn't exist yet). Note: the first run of `WorkflowEnvironment.start_time_skipping()` on this machine downloads a test-server binary, which requires internet access once; it's cached afterward.

- [ ] **Step 3: Write `app/workflows/fraud_hold_workflow.py`**

```python
"""FraudHoldWorkflow orchestrates what happens after an existing fraud engine
flags a transaction: a deterministic score check decides whether to place a
hold; if held, the transaction is held immediately (so fund protection never
waits on the LLM), then a pydantic-ai agent (via a local Ollama model)
generates a customer-facing explanation and an internal ops summary, the
customer is notified, and the workflow durably waits up to 24 hours for the
customer's "it was me" / "not me" response -- resolving to release, block, or
escalate on timeout. Because Temporal persists workflow progress independently
of the worker process, this all resumes correctly even if the worker is
killed and restarted mid-hold.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

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

        investigation: InvestigationSummary = await workflow.execute_activity(
            generate_explanation,
            args=[transaction.fraud_score, transaction.trigger_reason],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3, initial_interval=timedelta(seconds=1)
            ),
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_fraud_hold_workflow.py -v
```

Expected: PASS — all 4 tests (`test_below_threshold_records_no_hold_outcome`, `test_it_was_me_releases`, `test_not_me_blocks`, `test_timeout_escalates`) pass. The timeout test completes in seconds (not 24 real hours) via the environment's time-skipping.

- [ ] **Step 5: Commit**

```bash
git add app/workflows/fraud_hold_workflow.py tests/test_fraud_hold_workflow.py
git commit -m "Add FraudHoldWorkflow with the 4 required workflow tests"
```

---

### Task 7: Worker entrypoint

**Files:**
- Create: `app/worker.py`

**Interfaces:**
- Consumes: `settings` (Task 3), `FraudHoldWorkflow` (Task 6), all seven activity functions (Tasks 4 & 5).
- Produces: a runnable `python -m app.worker` process; no other task depends on this module's contents (it's an entrypoint), but Task 10's manual verification depends on it running.

- [ ] **Step 1: Write `app/worker.py`**

```python
import asyncio

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from app.activities.generate_explanation import generate_explanation
from app.activities.hold import block, escalate, place_hold, release
from app.activities.log_outcome import record_no_hold_outcome
from app.activities.notify import notify_customer
from app.config import settings
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address, data_converter=pydantic_data_converter
    )
    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[FraudHoldWorkflow],
        activities=[
            record_no_hold_outcome,
            generate_explanation,
            place_hold,
            release,
            block,
            escalate,
            notify_customer,
        ],
    )
    print(f"Worker started, polling task queue '{settings.task_queue}'...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Verify manually (requires a local Temporal dev server)**

```bash
temporal server start-dev &   # or: docker compose up temporal
sleep 3
python -m app.worker &
sleep 2
kill %2   # stop the worker
wait %1 2>/dev/null; kill %1 2>/dev/null
```

Expected: worker prints `Worker started, polling task queue 'fraud-hold-task-queue'...` with no exceptions before being killed.

- [ ] **Step 3: Commit**

```bash
git add app/worker.py
git commit -m "Add Temporal worker entrypoint"
```

---

### Task 8: FastAPI app

**Files:**
- Create: `app/main.py`

**Interfaces:**
- Consumes: `settings` (Task 3), `Transaction`/`CustomerResponse` (Task 2), `FraudHoldWorkflow` (Task 6).
- Produces: `app` (FastAPI instance) with `POST /transactions/hold` and `POST /transactions/{transaction_id}/respond`, run via `uvicorn app.main:app`.

- [ ] **Step 1: Write `app/main.py`**

```python
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.config import settings
from app.models import CustomerResponse, Transaction
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

_client: Optional[Client] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = await Client.connect(
        settings.temporal_address, data_converter=pydantic_data_converter
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
    await handle.signal(FraudHoldWorkflow.customer_responded, response)
    return {"status": "signal_sent"}
```

- [ ] **Step 2: Verify manually — duplicate submission while the workflow is still running**

```bash
temporal server start-dev &
sleep 3
python -m app.worker &
sleep 2
uvicorn app.main:app --port 8000 &
sleep 2

# fraud_score=90 is above threshold, so this workflow stays running (waiting
# on the signal) instead of completing immediately.
curl -s -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-2001", "fraudScore": 90, "triggerReason": "TEST", "customerId": "CUST-1"}'
echo
sleep 2
# repeat immediately, while the first execution is still running
curl -s -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-2001", "fraudScore": 90, "triggerReason": "TEST", "customerId": "CUST-1"}'
echo
```

Expected: first curl returns `{"workflow_id":"TXN-2001","status":"started"}`; second returns `{"workflow_id":"TXN-2001","status":"already_started"}` — this exercises the default `id_conflict_policy` path (a currently-running execution with the same ID).

- [ ] **Step 3: Verify manually — duplicate submission after the workflow has already completed**

```bash
curl -s -X POST http://localhost:8000/transactions/TXN-2001/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "it_was_me"}'
echo
sleep 2
# TXN-2001's workflow has now completed. Submit the same transaction_id again.
curl -s -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-2001", "fraudScore": 90, "triggerReason": "TEST", "customerId": "CUST-1"}'
echo

kill %3 %2 %1 2>/dev/null
```

Expected: `{"workflow_id":"TXN-2001","status":"already_started"}` — this exercises the `id_reuse_policy=REJECT_DUPLICATE` path (a *closed* execution with the same ID). Without that policy, this call would instead return `{"status":"started"}` and silently start a second, unrelated execution reusing the same Workflow ID.

- [ ] **Step 4: Commit**

```bash
git add app/main.py
git commit -m "Add FastAPI endpoints for starting and signaling the workflow"
```

---

### Task 9: `scripts/send_signal.py`

**Files:**
- Create: `scripts/send_signal.py`

**Interfaces:**
- Consumes: `settings` (Task 3), `CustomerResponse` (Task 2), `FraudHoldWorkflow` (Task 6).
- Produces: a standalone CLI, run as `python -m scripts.send_signal <transaction_id> <it_was_me|not_me>`.

- [ ] **Step 1: Write `scripts/send_signal.py`**

```python
import argparse
import asyncio

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from app.config import settings
from app.models import CustomerResponse
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow


async def send_signal(transaction_id: str, response: str) -> None:
    client = await Client.connect(
        settings.temporal_address, data_converter=pydantic_data_converter
    )
    handle = client.get_workflow_handle(transaction_id)
    await handle.signal(
        FraudHoldWorkflow.customer_responded, CustomerResponse(response=response)
    )
    print(f"Sent '{response}' signal to workflow '{transaction_id}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate a customer's response to a fraud hold.")
    parser.add_argument("transaction_id")
    parser.add_argument("response", choices=["it_was_me", "not_me"])
    args = parser.parse_args()
    asyncio.run(send_signal(args.transaction_id, args.response))
```

- [ ] **Step 2: Verify manually (requires a local Temporal dev server, worker, and a held transaction)**

```bash
temporal server start-dev &
sleep 3
python -m app.worker &
sleep 2
uvicorn app.main:app --port 8000 &
sleep 2

curl -s -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-3001", "fraudScore": 90, "triggerReason": "TEST", "customerId": "CUST-1"}'
echo
sleep 2
python -m scripts.send_signal TXN-3001 it_was_me

kill %3 %2 %1 2>/dev/null
```

Expected: `curl` returns `{"workflow_id":"TXN-3001","status":"started"}`; the script prints `Sent 'it_was_me' signal to workflow 'TXN-3001'`; the worker's terminal output shows the `release` activity's print line shortly after.

- [ ] **Step 3: Commit**

```bash
git add scripts/send_signal.py
git commit -m "Add standalone script to simulate a customer's signal response"
```

---

### Task 10: End-to-end Docker verification (crash-resume demo)

**Files:** none (verification only — proves Tasks 1–9 work together exactly as `README.md`'s "Running locally" and "The crash-resume demo" sections describe).

**Interfaces:**
- Consumes: everything built in Tasks 1–9.
- Produces: nothing new — this is the final proof that the system works, including surviving a worker restart mid-hold.

- [ ] **Step 1: Bring up the full stack**

```bash
docker compose up --build -d
sleep 15
docker compose ps
```

Expected: `temporal` shows status `healthy` (via its `temporal operator cluster health` healthcheck); `api` and `worker` show as running, and only started polling/serving once `temporal` reported healthy (that's what `depends_on: condition: service_healthy` enforces) — so their logs should show no initial connection-refused errors against `temporal:7233`.

- [ ] **Step 2: Trigger a hold-worthy transaction**

```bash
curl -s -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-DEMO-1", "fraudScore": 85, "triggerReason": "UNUSUAL_LOCATION", "customerId": "CUST-DEMO"}'
echo
```

Expected: `{"workflow_id":"TXN-DEMO-1","status":"started"}`.

- [ ] **Step 3: Confirm the workflow is durably waiting**

Open `http://localhost:8233` (the Temporal Web UI), find the `TXN-DEMO-1` workflow, and confirm its status shows it's running and waiting (no pending activity — it's parked in `wait_condition`).

- [ ] **Step 4: Kill and restart the worker mid-hold**

```bash
docker compose stop worker
sleep 2
docker compose start worker
sleep 2
docker compose logs worker --tail 20
```

Expected: no errors in the fresh worker's logs; the workflow is still visible as running in the Web UI (its history was persisted by the Temporal server, independent of the worker process).

- [ ] **Step 5: Resolve the case and confirm successful, idempotent resolution after restart**

```bash
curl -s -X POST http://localhost:8000/transactions/TXN-DEMO-1/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "it_was_me"}'
echo
docker compose logs worker --tail 20
```

Expected: `{"status":"signal_sent"}`; the worker's logs show exactly one `[hold] releasing hold on TXN-DEMO-1...` line (not duplicated), and the Web UI shows the workflow as completed with result `"released"`.

Note on what this actually proves: Temporal guarantees the *workflow's* durable progress and correct replay from history — it does not give the *activities* exactly-once execution. Activities have at-least-once semantics (they can retry after a timeout or a worker crash mid-execution, potentially running again). This demo's activities are safe to see printed more than once because they're mocked no-ops; a real (non-mocked) hold/release/block integration must be idempotent in its own right — typically by using `transaction_id` as an idempotency key against the downstream system — since Temporal will not deduplicate an activity that legitimately retries.

- [ ] **Step 6: Tear down**

```bash
docker compose down
```

No commit for this task — it's verification of already-committed work.

---

## Self-Review Notes

- **Spec coverage:** every spec section maps to a task — scaffolding/pins (Task 1), models (Task 2), config (Task 3), mocked activities (Task 4), `generate_explanation` (Task 5), workflow + the exact 4 tests (Task 6), worker (Task 7), FastAPI + idempotency (Task 8), `send_signal.py` (Task 9), docker-compose crash-resume demo (Task 10).
- **Type consistency checked:** `Transaction`/`InvestigationSummary`/`CustomerResponse` field names match between Task 2's definition and every later task's usage (`transaction.fraud_score`, `investigation.customer_explanation`, `investigation.notification_type`, `response.response`). Activity function names (`record_no_hold_outcome`, `generate_explanation`, `place_hold`, `release`, `block`, `escalate`, `notify_customer`) match between Tasks 4/5's definitions, Task 6's workflow imports and mock-test names, and Task 7's worker registration list.
- **No placeholders:** every step has complete, runnable code — no TODOs.
- **Forward-dependency check (found and fixed):** Task 8's original "duplicate after completion" verification called `scripts/send_signal.py`, which doesn't exist until Task 9. Fixed to use the `/transactions/{id}/respond` endpoint Task 8 already defines, so each task's verification only depends on earlier tasks.
- **Ordering consistency:** Task 6's workflow code, docstring, and the two order-sensitive test assertions (`test_it_was_me_releases`, `test_not_me_blocks`) all now agree that `place_hold` runs before `generate_explanation`.
