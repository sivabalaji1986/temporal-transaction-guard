# temporal-transaction-guard

A durable **hold → generate_explanation → notify → wait → resolve** workflow for suspicious transactions, built on [Temporal](https://temporal.io), [FastAPI](https://fastapi.tiangolo.com), [PydanticAI](https://ai.pydantic.dev), and a local [Ollama](https://ollama.com) model.

> **What this project is *not*:** a fraud-detection engine. We assume a bank's existing fraud system has already flagged a transaction and handed it to us with a score and a reason. This project is only responsible for what happens *after* that: placing a hold, explaining it to the customer, waiting durably for a response, and resolving the case — correctly, even if a server crashes in the middle.

---

## Why this exists

Most fraud-hold logic is written as request/response code: check a score, call an API, maybe write a row to a database saying "waiting for customer." That approach has a real weakness — if the process crashes while a hold is open and a customer is expected to respond hours or days later, someone has to rebuild "where was this case?" by hand from whatever state made it to the database.

Temporal removes that problem. A workflow's progress is recorded as an event history on the Temporal server, not just in your process's memory. If the worker process dies — mid-hold, mid-wait, doesn't matter — a new worker can pick the workflow back up and continue exactly where it left off, with no lost cases and no re-deciding what already happened. (This is a guarantee about the *workflow's* durable progress and correct replay, not about individual activities — those are at-least-once and can legitimately retry, so a real hold/release/notify integration still needs to be idempotent on its own.)

This repo is a small, runnable demonstration of that guarantee, applied to a believable banking scenario.

---

## Architecture

```
Existing fraud engine flags transaction
        |
        v
POST /transactions/hold  (FastAPI)
        |                              <- resubmitting the same transaction_id
        v                                 (still running OR already completed)
FastAPI starts FraudHoldWorkflow          returns {"status": "already_started"}
(Temporal client call)                    instead of starting a duplicate
        |
        v
Deterministic threshold check          <- pure workflow logic, NOT an activity
        |
   -----+-----------------------------------
   |                                        |
no hold needed                        hold needed
   |                                        |
   v                                        v
Record no-hold outcome              Place temporary hold (Activity)
(Activity) -> workflow completes    <- funds are protected first; this
                                        must not wait on the LLM below
                                                |
                                                v
                                     PydanticAI generates:
                                     - customer-friendly explanation
                                     - operations summary
                                     - notification content
                                     (Activity, calls local Ollama model)
                                     <- falls back to a fixed, deterministic
                                        explanation instead of failing the
                                        workflow if this fails after retries
                                        (e.g. Ollama is unreachable)
                                                |
                                                v
                                     Notify customer (Activity)
                                                |
                                                v
                                     Wait for signal, 24h timeout
                                     (durable — holds no thread/memory)
                                     <- POST /transactions/{id}/respond
                                        returns 404 for an unknown or
                                        already-resolved transaction_id
                                                |
                        ------------------------------------------------
                        |                       |                      |
                 "It was me"              no response in 24h      "Not me"
                        |                       |                      |
                        v                       v                      v
                     Release                Escalate for review     Block
```

### Design rules this project follows

- **The threshold check lives inside the Workflow, not an Activity.** It's pure logic over data already in workflow memory — no external call, no non-determinism — so it's safe to replay.
- **Everything that touches the outside world is an Activity:** calling the LLM, placing a hold, sending a notification, releasing/blocking funds, logging the no-hold outcome. Activities are the only things that can fail, retry, and have side effects.
- **The AI's job is explanation, not judgment.** The PydanticAI agent turns an already-computed fraud score and trigger reason into a structured, human-readable explanation and notification copy. It does **not** decide whether to hold — that decision is a deterministic threshold check the bank's own score already justifies. This keeps the article/demo focused on durable orchestration, not on whether an LLM makes good fraud judgments.

---

## Components

| Component | Role |
|---|---|
| **FastAPI (`app/main.py`)** | Entry and exit point. `POST /transactions/hold` receives the handoff from the fraud engine and starts a workflow — resubmitting the same `transaction_id` (whether still running or already completed) returns `{"status": "already_started"}` instead of starting a duplicate. `POST /transactions/{id}/respond` delivers the customer's response as a Temporal Signal, returning a 404 if `transaction_id` doesn't match a known workflow. |
| **Workflow (`app/workflows/fraud_hold_workflow.py`)** | Orchestrates the whole case: threshold check, activity calls, the durable wait with timeout, and the final branch (release / block / escalate). |
| **Activities (`app/activities/`)** | `generate_explanation.py` (PydanticAI + Ollama; falls back to a fixed explanation instead of failing the workflow if the call fails after retries), `hold.py` (place hold / release / block / escalate), `notify.py` (customer notification), `log_outcome.py` (no-hold logging). All mocked for the demo — swap in real integrations later. |
| **Worker (`app/worker.py`)** | Connects to the Temporal server, registers the workflow and activities, and executes tasks from the queue. This is the process we deliberately kill mid-demo. |
| **`scripts/send_signal.py`** | Simulates a customer replying, independent of the FastAPI process — useful for testing signal delivery directly. |

---

## Input contract

The fraud engine hands off a transaction like this:

```json
{
  "transactionId": "TXN-1001",
  "fraudScore": 78,
  "triggerReason": "UNUSUAL_LOCATION",
  "customerId": "CUST-101"
}
```

`fraudScore` and `triggerReason` are treated as already-decided facts from an external system — this project doesn't recompute or second-guess them.

---

## Running locally

### Prerequisites

- Python 3.11+
- [Ollama](https://ollama.com) running locally with a model pulled, e.g. `ollama pull llama3.1`
- Docker (optional — see below)

### Option A: Docker (recommended)

```bash
docker compose up --build
```

This brings up:
- a Temporal dev server (with its Web UI at `http://localhost:8233`)
- the FastAPI app (`http://localhost:8000`)
- a Temporal worker

Ollama is expected to run on your **host machine**, not inside Docker — the worker container talks to it via `http://host.docker.internal:11434/v1` (already wired into `docker-compose.yml`). This avoids bundling model weights into the container and lets you swap models without rebuilding.

### Option B: Run it all natively

```bash
pip install -r requirements.txt

# terminal 1: start a local Temporal dev server
temporal server start-dev

# terminal 2: start the worker
python -m app.worker

# terminal 3: start the API
uvicorn app.main:app --reload
```

### Connecting to Ollama

Set these in `.env` (see `.env.example`):

```
OLLAMA_BASE_URL=http://localhost:11434/v1
OLLAMA_MODEL=llama3.1
```

The `generate_explanation` activity uses PydanticAI's OpenAI-compatible provider pointed at Ollama's local endpoint — no external API key, no data leaving your machine.

---

## Triggering the flow

```bash
curl -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "TXN-1001",
    "fraudScore": 78,
    "triggerReason": "UNUSUAL_LOCATION",
    "customerId": "CUST-101"
  }'
```

Simulate the customer's reply:

```bash
curl -X POST http://localhost:8000/transactions/TXN-1001/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "it_was_me"}'
```

## The crash-resume demo

1. Start the worker and fire the `hold` request above.
2. Confirm in the Temporal Web UI (`http://localhost:8233`) that the workflow is now sitting in **"waiting for signal."**
3. Kill the worker process (`Ctrl+C` or `docker compose stop worker`).
4. Restart it.
5. Send the `respond` request from above.

The workflow resumes from exactly where it paused and resolves the case correctly — no lost progress — even though the process that was running it died in the middle. (Temporal guarantees the workflow's durable progress and correct replay; it does not give activities exactly-once execution — they're at-least-once, so a real, non-mocked hold/release/block integration would need to be idempotent on its own, e.g. keyed by `transaction_id`.)

---

## What this project deliberately leaves out

- Real fraud scoring — assumed to come from an existing system.
- Real payment rails for hold/release/block — mocked for clarity.
- Auth, persistence beyond Temporal's own history, and production-grade error handling.

The goal is to isolate and demonstrate one thing clearly: **durable execution for a long-running, wait-on-a-human workflow** — not to be a production fraud system.