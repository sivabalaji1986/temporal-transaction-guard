# temporal-transaction-guard

A durable **hold → generate_explanation → notify → wait → resolve** workflow for suspicious transactions, built on [Temporal](https://temporal.io), [FastAPI](https://fastapi.tiangolo.com), a tool-using [PydanticAI](https://ai.pydantic.dev) Agent, and a local [Ollama](https://ollama.com) model.

> **Agentic investigation, deterministic action.** The AI Agent may autonomously gather additional read-only context before writing its explanation, but it never decides whether to hold, release, block, or escalate a transaction — that stays entirely in the Temporal Workflow.

> **What this project is *not*:** a fraud-detection engine. An upstream fraud engine doesn't send us every transaction — it identifies candidate transactions that may require further action and hands each one to us with a `fraudScore`, `triggerReason`, and `customerId`. We don't recompute or second-guess that score; we apply a simple deterministic hold policy on top of it (hold if the score is at/above a configured threshold, otherwise don't) and handle what happens *after* that decision: placing a hold, explaining it to the customer, waiting durably for a response, and resolving the case — correctly, even if a server crashes in the middle.

---

## Why this exists

Most fraud-hold logic is written as request/response code: check a score, call an API, maybe write a row to a database saying "waiting for customer." That approach has a real weakness — if the process crashes while a hold is open and a customer is expected to respond hours or days later, someone has to rebuild "where was this case?" by hand from whatever state made it to the database.

Temporal removes that problem. A workflow's progress is recorded as an event history on the Temporal server, not just in your process's memory. If the worker process dies — mid-hold, mid-wait, doesn't matter — a new worker can pick the workflow back up and continue exactly where it left off, with no lost cases and no re-deciding what already happened. (This is a guarantee about the *workflow's* durable progress and correct replay, not about individual activities — those are at-least-once and can legitimately retry, so a real hold/release/notify integration still needs to be idempotent on its own.)

This repo is a small, runnable demonstration of that guarantee, applied to a believable banking scenario.

---

## Architecture

```
Upstream fraud engine identifies a candidate transaction
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
                                     PydanticAI Agent Activity:
                                     - may call lookup_recent_transactions()
                                     - may call lookup_customer_channel_
                                       preference()
                                       (both read-only, mocked, optional --
                                        the Agent decides if/when to call
                                        them, bounded by an explicit
                                        request/tool-call limit)
                                     - reasons over whatever it gathered
                                     - produces InvestigationSummary:
                                       customer-friendly explanation,
                                       operations summary, notification type
                                     (single Activity, calls local Ollama
                                      model; a retry re-runs the whole Agent
                                      loop incl. any tool calls -- safe
                                      because the tools are read-only)
                                     <- falls back to a fixed, deterministic
                                        explanation instead of failing the
                                        workflow if this fails after retries
                                        (e.g. Ollama is unreachable, or the
                                        Agent's execution bound is hit)
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
- **Everything that touches the outside world is an Activity:** calling the LLM (and its tools), placing a hold, sending a notification, releasing/blocking funds, logging the no-hold outcome. Activities are the only things that can fail, retry, and have side effects.
- **Agentic investigation, deterministic action.** The PydanticAI Agent inside `generate_explanation` may call two read-only tools — `lookup_recent_transactions` and `lookup_customer_channel_preference` — to gather extra context about the customer before writing its response. It decides for itself whether, and in what order, to call them. What it can **never** do is decide whether to hold, release, block, or escalate, recompute the fraud score, or call a write-side/banking tool — those stay entirely in the Workflow and its other Activities. This keeps the demo focused on durable orchestration around a genuinely agentic AI component, without letting the AI touch anything that actually moves money.
- **The Agent's tools are read-only, and both live inside the single `generate_explanation` Activity** — they are not separate Temporal Activities. This means a Temporal retry of `generate_explanation` re-runs the *entire* Agent loop, including any tool calls already made. That's only safe because the tools are read-only and naturally idempotent (looking up the same customer's data twice has no side effect); a future tool with a real side effect would need its own Activity boundary instead.
- **The Agent's tool-calling loop is explicitly bounded**, not left to run indefinitely. `generate_explanation.py` passes `usage_limits=UsageLimits(request_limit=6, tool_calls_limit=4)` to the Agent run — roughly double the normal path's expected 3 requests / 2 tool calls, but still far short of unbounded — so a pathological loop can't consume the whole Activity timeout. Exceeding either limit is just another Agent failure, handled by the same deterministic fallback described below.
- **Customer-facing text must summarize, not leak, internal context.** The Agent's system prompt explicitly instructs it to turn whatever it gathers (recent transactions, channel preference, the raw trigger reason) into a plain-language `customer_explanation` — never to paste raw tool output or internal identifiers into it. `ops_summary` is the place for more internal detail.

### How this shows up in Temporal's Event History

Open any workflow in the Temporal Web UI (`http://localhost:8233`) and click its **Event History** tab, and you'll see a repeating pattern rather than one line per activity. That's expected — here's how to read it:

- **Workflow Task** — the worker waking up to run/replay `FraudHoldWorkflow.run` up to its next decision point, then going back to sleep. Every activity call, every signal, and the timer firing/being canceled each trigger one of these. Recorded as 3 events: `WorkflowTaskScheduled` → `WorkflowTaskStarted` → `WorkflowTaskCompleted`.
- **Activity Task** — the actual execution of one activity (`place_hold`, `generate_explanation`, `notify_customer`, `release`/`block`/`escalate`). Also 3 events — `ActivityTaskScheduled` → `ActivityTaskStarted` → `ActivityTaskCompleted` — plus one extra `ActivityTaskStarted` per retry attempt if it fails and gets retried (check the `Attempt` field, and `Last Failure` on that event for why the previous attempt didn't succeed).
- **Timer** — the durable 24h wait: `TimerStarted` when `wait_condition`'s timeout kicks in, then either `TimerCanceled` (a signal arrived first) or `TimerFired` (24h passed with no response).
- **Signal** — `WorkflowExecutionSignaled` the moment `/respond` (or `send_signal.py`) delivers `customer_responded`.
- **Completion** — `WorkflowExecutionCompleted`, the terminal event, carrying the final result string.

Stacked together, an above-threshold hold that gets resolved via signal looks like this:

```
WorkflowExecutionStarted
  Workflow Task  -> decide: call place_hold
  Activity Task: place_hold
  Workflow Task  -> decide: call generate_explanation
  Activity Task: generate_explanation
  Workflow Task  -> decide: call notify_customer
  Activity Task: notify_customer
  Workflow Task  -> decide: start the 24h wait
  TimerStarted
  ...workflow durably parked here -- no thread, no process, just this
     recorded state -- until one of the two below happens...
  WorkflowExecutionSignaled (customer_responded)   <- or TimerFired, if 24h passes first
  Workflow Task  -> decide: signal arrived, cancel the timer, call release/block
  TimerCanceled
  Activity Task: release  (or: block)
  Workflow Task  -> decide: return the result
  WorkflowExecutionCompleted ("released")
```

Since each "Workflow Task" and "Activity Task" line above is really 3 history events, a single resolved hold typically ends up as ~30-35 total events. That's normal, not a sign anything's wrong — every one of those events is a durability checkpoint permanently recorded by the Temporal *server*, independent of the worker process. That's exactly why killing and restarting the worker mid-hold (see [the crash-resume demo](#the-crash-resume-demo) below) doesn't lose any progress: a new worker just picks up where this history left off.

---

## Components

| Component | Role |
|---|---|
| **FastAPI (`app/main.py`)** | Entry and exit point. `POST /transactions/hold` receives the handoff from the fraud engine and starts a workflow — resubmitting the same `transaction_id` (whether still running or already completed) returns `{"status": "already_started"}` instead of starting a duplicate. `POST /transactions/{id}/respond` delivers the customer's response as a Temporal Signal, returning a 404 if `transaction_id` doesn't match a known workflow. |
| **Workflow (`app/workflows/fraud_hold_workflow.py`)** | Orchestrates the whole case: threshold check, activity calls, the durable wait with timeout, and the final branch (release / block / escalate). |
| **Activities (`app/activities/`)** | `generate_explanation.py` (a tool-using PydanticAI Agent talking to Ollama — may call two read-only mock tools, bounded by an explicit request/tool-call limit; falls back to a fixed explanation instead of failing the workflow if the call fails after retries), `hold.py` (place hold / release / block / escalate), `notify.py` (customer notification), `log_outcome.py` (no-hold logging). All mocked for the demo — swap in real integrations later. |
| **Worker (`app/worker.py`)** | Connects to the Temporal server, registers the workflow and activities, and executes tasks from the queue. This is the process we deliberately kill mid-demo. |
| **`scripts/send_signal.py`** | Simulates a customer replying, independent of the FastAPI process — useful for testing signal delivery directly. |

---

## Repository structure

```
temporal-transaction-guard/
├── requirements.txt
├── requirements-dev.txt
├── ruff.toml
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── pytest.ini
├── LICENSE
├── AGENTS.md
├── .github/
│   └── workflows/
│       └── tests.yml
├── app/
│   ├── main.py
│   ├── models.py
│   ├── config.py
│   ├── worker.py
│   ├── workflows/
│   │   └── fraud_hold_workflow.py
│   └── activities/
│       ├── generate_explanation.py    # tool-using PydanticAI Agent
│       ├── hold.py
│       ├── notify.py
│       └── log_outcome.py
├── scripts/
│   └── send_signal.py
└── tests/
    ├── test_fraud_hold_workflow.py         # Workflow-level, mocked activities
    └── test_generate_explanation_agent.py  # Agent-level, TestModel/FunctionModel
```

---

## Input contract

The upstream fraud engine doesn't hand us every transaction — only candidate transactions it flags as potentially needing further action, sent like this:

```json
{
  "transactionId": "TXN-1001",
  "fraudScore": 78,
  "triggerReason": "UNUSUAL_LOCATION",
  "customerId": "CUST-101"
}
```

`fraudScore` and `triggerReason` are treated as facts from an external system — this project doesn't recompute or second-guess the score itself. What it *does* decide, independently, is whether that score clears our own configured hold threshold; see [Architecture](#architecture) for that check.

---

## Setup and Verification

Get the repo ready to work with, and confirm it's in a working state, before running anything that needs Temporal, Docker, or Ollama.

1. Clone the repo:

   ```bash
   git clone https://github.com/sivabalaji1986/temporal-transaction-guard.git
   cd temporal-transaction-guard
   ```

2. Create and activate a persistent virtual environment. This is **not** the same as a throwaway venv you might spin up just to run a one-off command — this one is meant to stay for ongoing work in the repo:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   ```

   `.venv` is gitignored and won't exist after a fresh clone — this step is required every time you clone the repo. If you skip it, `source .venv/bin/activate` will fail with "no such file or directory."

3. Install pinned dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the test suite. This does **not** require Temporal, Docker, or Ollama to be running — all 11 tests use mocked activities, Temporal's in-memory time-skipping test environment, and PydanticAI's deterministic `TestModel`/`FunctionModel` test doubles instead of a real Ollama call:

   ```bash
   pytest tests/ -v
   ```

   Expected: `11 passed` — 5 in `tests/test_fraud_hold_workflow.py` (Workflow-level orchestration, with the whole `generate_explanation` Activity mocked out) and 6 in `tests/test_generate_explanation_agent.py` (Agent-level: tool registration/calling, real tool-return-value influence on the final output, invalid-output rejection, no raw-data leakage, and the bounded tool-calling loop). If you instead see `ModuleNotFoundError: No module named 'temporalio'`, you're running `pytest` with a different Python than the one in `.venv` — check that `which python3` and `which pytest` both point inside `.venv/bin` (a common cause is running `pytest` via a system or IDE-default Python instead of the activated venv).

5. Copy the example environment file:

   ```bash
   cp .env.example .env        # Windows: Copy-Item .env.example .env
   ```

   The Ollama and Temporal environment variables referenced throughout this README (`OLLAMA_BASE_URL`, `OLLAMA_MODEL`, `TEMPORAL_ADDRESS`, etc.) won't take effect until this file exists — `.env.example` is a template, not something the app reads directly.

6. Confirm Ollama is running and has a model pulled:

   ```bash
   # Ollama needs to be running before you can pull a model or use the app.
   # On Mac, the Ollama desktop app already runs this in the background --
   # only run it manually (e.g. on Linux, or if `ollama list` below fails
   # to connect) if it's not already running. `ollama serve` runs in the
   # foreground and blocks, so give it its own terminal (or start it as a
   # background service) -- running the commands below in that same
   # terminal would just hang waiting for `ollama serve` to return:
   ollama serve

   # (in a separate terminal, once ollama serve is running)
   ollama pull qwen3.5:latest
   ollama list
   curl http://localhost:11434/api/tags
   ```

   `ollama list` and the `curl` should both show `qwen3.5:latest` (or whatever you set `OLLAMA_MODEL` to) once it's pulled. If either fails to connect instead, Ollama isn't running yet — run `ollama serve` above first.

7. Only after tests pass, proceed to "Running locally" below to actually run the app — that part does require Temporal, and either Docker or a native Temporal dev server.

---

## Running locally

### Prerequisites

- Python 3.11+
- [Ollama](https://ollama.com) running locally with a model pulled, e.g. `ollama pull qwen3.5:latest`
- Docker (optional — see below)
- [Temporal CLI](https://docs.temporal.io/cli#install) — required for Option B (native `temporal server start-dev`); **not** required for Option A (Docker), which bundles its own Temporal dev server

(See [Setup and Verification](#setup-and-verification) above if you haven't already created a virtual environment and confirmed tests pass.)

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
# If you haven't already (see Setup and Verification above):
pip install -r requirements.txt

# terminal 1: start a local Temporal dev server
temporal server start-dev

# terminal 2: start the worker
python -m app.worker

# terminal 3: start the API
uvicorn app.main:app --reload
```

### Verify it's running

**Docker (Option A):**

```bash
docker compose ps              # temporal, api, worker should all show as Up
docker compose logs -f worker  # look for "Worker started, polling task queue..."
```

Then open `http://localhost:8233` in your browser for the Temporal Web UI.

**Native (Option B):**

Check the worker's own terminal (terminal 2 above) for `Worker started, polling task queue '...'`, then open `http://localhost:8233` (Temporal Web UI — `temporal server start-dev` serves this too) and `http://localhost:8000/docs` (FastAPI's interactive docs) in your browser to confirm both processes are up.

There's no `/health` endpoint on the API today — `/docs` (FastAPI's built-in Swagger UI, on by default) is the honest way to confirm it's responding without one. A dedicated health-check endpoint would be a reasonable future addition, but that's out of scope for this doc-only pass.

### Connecting to Ollama

Set these in `.env` (see `.env.example`):

```
OLLAMA_BASE_URL=http://localhost:11434/v1
OLLAMA_MODEL=qwen3.5:latest
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

Response:

```json
{"workflow_id":"TXN-1001","status":"started"}
```

Simulate the customer's reply:

```bash
curl -X POST http://localhost:8000/transactions/TXN-1001/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "it_was_me"}'
```

Response:

```json
{"status":"signal_sent"}
```

### More examples

A transaction below the fraud-score threshold (default `70`, see `FRAUD_SCORE_THRESHOLD` in `.env.example`) — the response shape is identical to a held transaction; the difference (no hold placed, no notification sent) only shows up in the worker's logs or the Temporal Web UI, not in this response:

```bash
curl -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-2001", "fraudScore": 40, "triggerReason": "MINOR_ANOMALY", "customerId": "CUST-202"}'
```

```json
{"workflow_id":"TXN-2001","status":"started"}
```

A `"not_me"` response — also `{"status":"signal_sent"}`, since this endpoint only confirms the signal was delivered; whether the workflow resolves to `release` or `block` isn't reflected here, only in the logs/Web UI:

```bash
curl -X POST http://localhost:8000/transactions/TXN-1001/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "not_me"}'
```

Submitting the same `transaction_id` a second time (idempotency — see [Architecture](#architecture)):

```bash
curl -X POST http://localhost:8000/transactions/hold \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "TXN-1001", "fraudScore": 78, "triggerReason": "UNUSUAL_LOCATION", "customerId": "CUST-101"}'
```

```json
{"workflow_id":"TXN-1001","status":"already_started"}
```

Responding to a `transaction_id` that was never submitted (or was already resolved):

```bash
curl -i -X POST http://localhost:8000/transactions/TXN-DOES-NOT-EXIST/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "it_was_me"}'
```

```
HTTP/1.1 404 Not Found
```
```json
{"detail":"No transaction found for 'TXN-DOES-NOT-EXIST'"}
```

## The crash-resume demo

1. Start the worker and fire the `hold` request above.
2. Confirm in the Temporal Web UI (`http://localhost:8233`) that the workflow is now sitting in **"waiting for signal."** Open its **Event History** tab while you're there — see [How this shows up in Temporal's Event History](#how-this-shows-up-in-temporals-event-history) above for how to read it.
3. Kill the worker process (`Ctrl+C` or `docker compose stop worker`).
4. Restart it.
5. Send the `respond` request from above.

The workflow resumes from exactly where it paused and resolves the case correctly — no lost progress — even though the process that was running it died in the middle. (Temporal guarantees the workflow's durable progress and correct replay; it does not give activities exactly-once execution — they're at-least-once, so a real, non-mocked hold/release/block integration would need to be idempotent on its own, e.g. keyed by `transaction_id`.)

### Stopping everything

- **Docker (Option A):** `docker compose down` (add `-v` for a full reset that also wipes Temporal's dev-server data — namespaces, workflow history, everything. Only needed if you want a completely clean slate, not for routine shutdown)
- **Native (Option B):** stop each of the three terminals with `Ctrl+C`, in any order

---

## Troubleshooting

**Ollama isn't running, or the wrong model is configured.** `generate_explanation` will retry 3 times against `OLLAMA_BASE_URL`/`OLLAMA_MODEL` (from `.env`) and then fall back to a fixed explanation rather than failing the workflow — see the Architecture diagram above. Each attempt may itself involve several model/tool round trips (the Agent deciding whether to call its read-only tools), bounded by the `usage_limits` in `generate_explanation.py` so a single attempt can't run away — but a full attempt is still one Activity attempt, and Temporal still retries the whole thing, tool calls included, on failure. So a broken Ollama setup won't crash a held transaction, but it will mean every hold gets the generic fallback message instead of a real explanation. Confirm with `ollama list` and `curl http://localhost:11434/api/tags` (see [Setup and Verification](#setup-and-verification)); double-check `OLLAMA_MODEL` in `.env` matches a model you've actually pulled.

**Port already in use (`8000`, `7233`, or `8233`).** These are used by the API, the Temporal frontend service, and the Temporal Web UI respectively. Find whatever's already bound to the port (e.g. `lsof -i :8000` on Mac/Linux) and stop it, or change the mapping in `docker-compose.yml`'s `ports:` (Docker) — for native mode, `uvicorn app.main:app --port <other-port>` and `temporal server start-dev --ui-port <other-port>` accept alternate ports directly.

**A workflow seems stuck** (no response after `/hold`, or `/respond` doesn't seem to do anything). Start with `docker compose logs worker` (or the worker's own terminal in native mode) — every activity prints when it runs, so you can see exactly how far the workflow got. Then check the workflow's state directly in the Temporal Web UI (`http://localhost:8233`): open the workflow by its `transaction_id` and look at its event history — a workflow parked in `wait_condition` is normal and expected until a `/respond` signal (or the 24h timeout) arrives.

---

## What this project deliberately leaves out

- Real fraud scoring — assumed to come from an existing system.
- Real payment rails for hold/release/block — mocked for clarity.
- Real customer-history/notification-preference systems — the Agent's two tools are mocked, read-only, and return fixed data.
- Multi-agent architectures, MCP, A2A, RAG/vector databases, or any other agent-framework machinery beyond a single tool-using PydanticAI Agent.
- Auth, persistence beyond Temporal's own history, and production-grade error handling.

The goal is to isolate and demonstrate two things clearly: **durable execution for a long-running, wait-on-a-human workflow**, and **a genuinely agentic AI component kept safely inside a deterministic action boundary** — not to be a production fraud system.