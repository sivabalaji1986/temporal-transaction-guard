# Learning Guide: temporal-transaction-guard

This is **not** setup documentation — see `README.md` for how to install and run the project. This file exists purely to teach you Python and this codebase's design, assuming you can follow simple logic but haven't written much (or any) Python before. Every Python concept is explained the first time it actually shows up in the code, not in an upfront glossary — so it's worth reading in order, at least the first time through.

---

## 1. The Big Picture First

Before looking at a single line of code, here's the story of what happens, in plain English. Keep this narrative in your head — everything in the rest of this guide is just filling in *how* each step is actually written in Python.

1. **An upstream fraud engine identifies a candidate transaction and calls our API.** Somewhere else in the bank's systems, a fraud-detection engine (not part of this project) doesn't send us every transaction — only ones it flags as candidates that may require further action — and sends us a message: "here's a transaction, here's a score, here's why, here's who it belongs to." That message arrives as an HTTP request into `app/main.py`. We don't recompute that score; we just compare it to our own threshold in the next step.

2. **`app/main.py` starts a Temporal workflow.** It doesn't do the fraud-hold logic itself — it just hands the transaction off to `app/workflows/fraud_hold_workflow.py` and asks Temporal to start running it.

3. **The workflow checks the fraud score against a threshold — directly in its own code, no separate file involved.** This is a deliberate design choice: comparing two numbers is simple enough, and safe enough (no network calls, no randomness), that it doesn't need to be handled specially. We'll explain exactly *why* this matters — it's about Temporal's "replay" mechanism — when we get to the workflow file in Section 2, and again in Section 4.

4. **If a hold is needed**, the workflow calls three things, one after another:
   - **`place_hold`** (in `hold.py`) — puts a temporary hold on the transaction.
   - **`generate_explanation`** (in `generate_explanation.py`) — asks a local AI model (via PydanticAI and Ollama) to write a customer-friendly explanation and an internal summary.
   - **`notify_customer`** (in `notify.py`) — sends that explanation to the customer.

5. **The workflow then pauses — durably.** It waits for one of two things: the customer replying ("it was me" / "not me"), delivered as something Temporal calls a **Signal**, or 24 hours passing with no reply. "Durably" is the key word here: the workflow can sit in this paused state for hours or days, and it isn't tying up a worker thread or keeping a process running the whole time — but its state is still durably stored by the Temporal server for that entire time, which is exactly why it can survive a worker restart. We'll unpack exactly what that means in Section 4.

6. **A separate process — the worker (`app/worker.py`) — is what actually runs all of this.** This is an important distinction. `app/main.py` (the API) never runs the workflow's own code; it just talks to the Temporal *server*, which schedules work. The **worker** is a different, long-running Python process that connects to that same Temporal server, and it's the one that actually executes the workflow's code and the activities (`place_hold`, `generate_explanation`, etc.) when the server tells it to. Why split these into two processes? Because this project's whole point is to demonstrate that the worker can be killed and restarted *without losing any progress* — and that only works because the worker doesn't hold the important state itself, the Temporal server does. If the API and the worker were the same process, restarting it to prove that point would also take down the API.

7. **The customer's response arrives** either through the API (`app/main.py`, a second endpoint) or through a small standalone script (`scripts/send_signal.py`) that talks to Temporal directly, without going through the API at all. Either way, it's delivered to the paused workflow as a Signal.

8. **The workflow resolves the case** — back in `hold.py` again — either releasing the hold ("it was me"), blocking it ("not me"), or escalating it for manual review (the 24-hour timeout fired with no reply).

That's the whole system. Before diving into each file in detail, here's a quick-scan reference table — not a replacement for the walkthrough that follows, just something to glance back at while you're in it:

| File | Role | Flow step(s) |
|---|---|---|
| `app/models.py` | Data model | Defines the shapes used at every step |
| `app/config.py` | Configuration | Supplies settings used in steps 2, 4, 6 |
| `app/activities/log_outcome.py` | Activity | Below-threshold outcome (alternative to step 4) |
| `app/activities/hold.py` | Activity | Place hold, release, block, escalate (steps 4, 8) |
| `app/activities/notify.py` | Activity | Notify customer (step 4) |
| `app/activities/generate_explanation.py` | Activity | Generate AI explanation (step 4) |
| `app/workflows/fraud_hold_workflow.py` | Workflow orchestration | Threshold check, activity calls, wait, resolve (steps 2–8) |
| `app/worker.py` | Worker entrypoint | Actually executes the workflow + activities (step 6) |
| `app/main.py` | FastAPI endpoints | Request in (steps 1–2), Signal in (step 7) |
| `scripts/send_signal.py` | Standalone script | Signal in — an alternative path for step 7 |
| `tests/test_fraud_hold_workflow.py` | Test | Exercises steps 2–8 without a real server or AI |

Now let's go file by file and see how each piece of this is actually written.

---

## 2. File-by-File Walkthrough

For every file below, we'll cover: what it's for, what *category* of thing it is (this categorization is one of the main things worth learning here — Data model, Configuration, Activity, Workflow, Worker entrypoint, FastAPI endpoint, Standalone script, or Test), its important functions, any new Python concept it introduces, what it calls into / is called by, and how data flows through it.

### 2.1 `app/models.py` — **Data model**

**Job:** Defines the shapes of data that move around this system — what a `Transaction` looks like, what the AI's output looks like, what a customer's reply looks like. Nothing in this file *does* anything; it only describes *shapes*.

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

**New concepts, explained:**

- **`import`** — the first line, `from typing import Literal`, is Python's way of saying "I want to use some code that lives in another file/library." `typing` is part of Python's **standard library** — it ships with Python itself, nothing to install. `pydantic`, imported on the next line, is different: it's an **external library** — a separate package this project depends on and has to install (see `requirements.txt`). (Coming from Java: a Python *module* — one `.py` file, like this one — is roughly analogous to a single Java source file, and `import` here plays a similar role to Java's `import`.)
- **`class`** — a class is a blueprint for a "thing" that bundles data together. `class Transaction(BaseModel):` says "here's a new kind of thing called `Transaction`, and it works like Pydantic's `BaseModel`." Once defined, you can create an actual `Transaction` by calling it like a function: `Transaction(transaction_id="TXN-1", ...)`.
- **`BaseModel` (Pydantic)** — this is the one external concept worth understanding early, because it's used everywhere in this project. A Pydantic `BaseModel` is a class where you declare the fields you expect (with their types), and Pydantic automatically: validates incoming data matches those types, converts JSON into real Python objects and back, and raises a clear error if something doesn't fit. Every "shape of data" in this project (`Transaction`, `InvestigationSummary`, `CustomerResponse`) is a Pydantic model for exactly this reason. (If you know Java: a Pydantic `BaseModel` is roughly comparable to a Java DTO/record used together with JSON binding (e.g. Jackson) and Bean Validation (`@NotNull`, `@Min`, etc.) — Java's fields are already type-safe at compile time, so that half isn't new to you. What Pydantic adds on top is *runtime* validation of data arriving from *outside* the type system entirely — a JSON request body, an env var, a signal payload — producing structured validation errors, and handling serialization/deserialization to and from that untyped data automatically. It also does some type coercion by default: a field typed as `int` would happily accept the string `"25"` and store it as the integer `25`, since it's an unambiguous match for the declared type — this can be turned off with Pydantic's strict mode if you want it to reject anything that isn't already the exact declared type.)
- **Type hints** — `transaction_id: str` means "this field is expected to be a string." `fraud_score: float` means a decimal number. These aren't just documentation — Pydantic actually *enforces* them at runtime (if you tried to create a `Transaction` with `fraud_score="not a number"`, it would raise an error).
- **`Literal["sms", "email", "push"]`** — this is a type hint that means "not just any string — specifically one of these exact values." `InvestigationSummary.notification_type` can only ever be `"sms"`, `"email"`, or `"push"`; anything else is rejected. (If you know Java: this is loosely comparable to using a Java `enum` — a fixed, closed set of allowed values — except `Literal` values are plain strings under the hood rather than a distinct enum type.)
- **`Field(alias="transactionId")`** — the fraud engine that calls our API sends JSON with camelCase keys (`transactionId`, `fraudScore`), but Python convention is snake_case (`transaction_id`, `fraud_score`). `alias=` tells Pydantic "when reading JSON, look for this key instead of the field's Python name." `model_config = ConfigDict(populate_by_name=True)` additionally allows constructing a `Transaction` using the Python names directly (useful in tests — see Section 2.11), not just the aliases.

**Calls out to:** nothing — this is a leaf file, it has no dependencies on the rest of the project.
**Called by:** `generate_explanation.py`, `fraud_hold_workflow.py`, `main.py`, `send_signal.py`, and the tests.

**Data flow:** nothing flows *through* this file at runtime — it just defines the containers other files put data into.

---

### 2.2 `app/config.py` — **Configuration**

**Job:** Defines every setting this project reads from the environment (URLs, model names, the fraud threshold), with sensible defaults, in one place.

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ollama_base_url: str = "http://localhost:11434/v1"
    ollama_model: str = "qwen3.5:latest"
    temporal_address: str = "localhost:7233"
    task_queue: str = "fraud-hold-task-queue"
    fraud_score_threshold: float = 70


settings = Settings()
```

**New concepts, explained:**

- **`BaseSettings` (from `pydantic-settings`, a separate package from plain `pydantic`)** — this is `BaseModel`'s cousin, specifically for configuration. Every field gets its value from an environment variable of the *same name* (case-insensitively — `ollama_base_url` reads the `OLLAMA_BASE_URL` environment variable) if one is set, and otherwise falls back to the default shown after the `=`.
- **`model_config = SettingsConfigDict(env_file=".env", ...)`** — tells it to also read from a `.env` file if one exists (that's the file `README.md`'s Setup instructions have you copy from `.env.example`).
- **The last line, `settings = Settings()`, is worth noticing carefully.** This isn't inside the class — it's a plain instruction that runs once, the first time this file is imported anywhere in the project, creating a single shared `Settings` object. Every other file that needs a config value does `from app.config import settings` and then reads e.g. `settings.fraud_score_threshold` — they're all sharing this *one* object, not creating their own.

**Calls out to:** nothing project-specific.
**Called by:** `generate_explanation.py`, `fraud_hold_workflow.py` (indirectly, via the threshold being passed in — see 2.7), `worker.py`, `main.py`, `send_signal.py`.

**Data flow:** environment variables / `.env` file → in → a single shared `settings` object → out to whoever imports it.

---

### 2.3 `app/activities/log_outcome.py` — **Activity**

**Job:** Records that a transaction was checked and didn't need a hold. This is the smallest possible activity, and a good place to introduce what an "Activity" *is* before we see more of them.

```python
import asyncio

from temporalio import activity


@activity.defn
async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
    print(
        f"[log_outcome] {transaction_id}: fraud_score={fraud_score} "
        "below threshold, no hold placed"
    )
    await asyncio.sleep(1)
```

**What's an "Activity"?** In Temporal's vocabulary, an Activity is a single unit of work that touches the outside world — calling an API, writing to a database, sending a notification — anything that *isn't* guaranteed to produce the exact same result if you ran it again (unlike, say, adding two numbers). Temporal tracks whether each Activity succeeded, and can automatically retry it if it fails. In this demo, none of the activities do anything real — each one just prints a line and pauses for a second, standing in for what a real integration would do.

**New concepts, explained:**

- **Decorators (`@activity.defn`)** — a decorator is a line starting with `@`, placed directly above a function, that *wraps* that function to add extra behavior without changing the function's own code. Here, `@activity.defn` doesn't change what `record_no_hold_outcome` does when you call it directly — it marks the function as *eligible* to be run as a Temporal Activity (attaching the metadata Temporal needs to recognize it as one, like its name). That's only half the story, though: `@activity.defn` by itself does **not** register this function with any actual running Worker — a decorated function that no `Worker` ever loads still can't be executed by Temporal. The second, separate step — actually making a Worker able to run it — happens in `app/worker.py`, when the function is explicitly included in that `Worker`'s `activities=[...]` list (Section 2.8). Both steps are required: `@activity.defn` here, *and* being listed in a `Worker` there. (If you know Java: a decorator is similar in spirit to an annotation like `@Override` or `@Transactional` — a marker on a piece of code that some other framework logic looks for and acts on. The difference is where that "acting" happens: `@activity.defn`'s registration-eligibility logic runs immediately, but the "actually able to run it" part still needs the separate `Worker` step above — there's no single annotation here that does everything Spring's `@Transactional` might do in one place.)
- **`async def` / `await`** — `async def` marks a function as *asynchronous*: it's allowed to pause partway through (at an `await`) and let other work happen, instead of blocking everything until it finishes. `await asyncio.sleep(1)` pauses this specific function for 1 second without freezing the rest of the program. You can only use `await` inside a function defined with `async def`. Temporal activities and workflows in this project are always written as `async def` — this lets a single worker process handle many activities "at once" (really: taking turns whenever one of them is waiting on something, like a sleep or a network call). (If you know Java: this is a rough analogue to chaining `CompletableFuture`s — both let one thread juggle multiple pending operations instead of blocking on each one — but it's an approximation, not an exact match; Python's `async`/`await` reads more like ordinary sequential code, with the pause points made explicit by `await`.)
- **`-> None`** — a type hint on the *return value* this time, meaning "this function doesn't return anything meaningful."
- **f-strings (`f"..."`)** — the `f` right before a string means you can drop variables directly inside it using `{curly braces}`, e.g. `f"...{transaction_id}..."` inserts the actual value of `transaction_id` into the text.

**Calls out to:** nothing — just `print` and `asyncio.sleep`, both from Python's standard library.
**Called by:** `fraud_hold_workflow.py` (below-threshold path), `worker.py` (registers it so it *can* be called).

**Data flow:** in — `transaction_id` and `fraud_score` (plain strings/numbers, not the full `Transaction` object). Out — nothing (`None`); its only real "output" is the printed line.

---

### 2.4 `app/activities/hold.py` — **Activity**

**Job:** The four activities that change a hold's state: placing one, and the three ways to resolve it.

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

No new Python concepts here — it's the same pattern as `log_outcome.py`, repeated four times. Worth noticing: **one file can define several activities.** They don't have to live one-per-file; they're grouped here because they're all "things that happen to a hold."

**Calls out to:** nothing.
**Called by:** `fraud_hold_workflow.py` (`place_hold` always, then exactly one of `release`/`block`/`escalate` depending on how the case resolves), `worker.py`.

**Data flow:** in — `transaction_id` only. Out — nothing.

---

### 2.5 `app/activities/notify.py` — **Activity**

**Job:** Sends the (real or fallback) explanation to the customer.

```python
import asyncio

from temporalio import activity


@activity.defn
async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
    print(f"[notify] sending {notification_type} to {customer_id}: {message}")
    await asyncio.sleep(1)
```

Same pattern again. The one thing worth noting: this function takes *three* plain arguments (`customer_id`, `message`, `notification_type`) rather than a whole object — we'll see in Section 2.7 that the workflow pulls these three values out of an `InvestigationSummary` before calling this.

**Calls out to:** nothing.
**Called by:** `fraud_hold_workflow.py`, `worker.py`.

---

### 2.6 `app/activities/generate_explanation.py` — **Activity**

**Job:** The one activity that calls out to an AI model (via [PydanticAI](https://ai.pydantic.dev), talking to a locally-running [Ollama](https://ollama.com) model) to turn a fraud score and reason into human-readable text.

```python
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from temporalio import activity

from app.config import settings
from app.models import InvestigationSummary

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    output_type=InvestigationSummary,
    system_prompt=(
        "An upstream fraud-detection system identifies candidate suspicious "
        "transactions and provides a fraud score and trigger reason for each "
        "one. Our Temporal Workflow -- not this agent -- performs the "
        "deterministic threshold check on that score, decides whether to "
        "hold, and places the hold. You are only called after that hold "
        "decision has already been made and the hold is already in place. "
        "Your only job is to generate three things: a customer-friendly "
        "explanation, a short internal fraud-ops summary, and the best "
        "notification channel. You must never decide whether to hold, "
        "release, block, or escalate a transaction -- those decisions "
        "belong entirely to the Workflow, not to you."
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

**New concepts, explained:**

- **Module-level code that isn't inside a function** — the `_agent = Agent(...)` block runs *once*, the moment this file is first imported (same idea as `settings = Settings()` in `config.py`). It builds one `Agent` object and every call to `generate_explanation` reuses it, rather than rebuilding it from scratch on every call. The leading underscore in `_agent` is a Python convention meaning "this is private to this file, other files shouldn't reach in and use it directly" — it's not enforced by the language, just a signal to readers.
- **`Agent(model, output_type=..., system_prompt=...)`** — this is PydanticAI's core building block: an `Agent` wraps a language model and, critically, `output_type=InvestigationSummary` tells PydanticAI "don't just give me back free text — force the model's answer into this exact Pydantic shape." This is why `InvestigationSummary` (defined in `models.py`) shows up here: the AI's raw response gets validated and structured into it automatically.
- **`OpenAIChatModel` + `OpenAIProvider`** — Ollama exposes an API that's compatible with OpenAI's own API format, so PydanticAI can talk to it using its "OpenAI" support, just pointed at a different `base_url` (Ollama's local address instead of OpenAI's servers) with a throwaway `api_key` (Ollama doesn't check it).
- **`await _agent.run(...)`** — actually sends the prompt and waits for (and validates) the response. `result.output` is the resulting `InvestigationSummary` object.
- Notice this function returns `InvestigationSummary`, not `None` like every activity so far — activities can return real data, and Temporal will deliver that return value back to whatever workflow code called it (we'll see this land in a variable in Section 2.7).
- **What happens to `ops_summary`?** The AI agent generates all three `InvestigationSummary` fields, including `ops_summary`, and it's genuinely present in this activity's result. But if you follow it forward into Section 2.7, you'll see the workflow only ever reads `investigation.customer_explanation` and `investigation.notification_type` when calling `notify_customer` — `ops_summary` isn't passed anywhere further in this demo. It exists as a hook for a future audit-log or internal-ops integration, not because anything in this project currently reads it.

**Calls out to:** `app/config.py` (for the Ollama URL/model), `app/models.py` (for the `InvestigationSummary` shape); PydanticAI/Ollama externally.
**Called by:** `fraud_hold_workflow.py`, `worker.py`.

**Data flow:** in — `fraud_score`, `trigger_reason` (plain values). Out — a full `InvestigationSummary` object.

---

### 2.7 `app/workflows/fraud_hold_workflow.py` — **Workflow**

**Job:** This is the file that orchestrates everything — it's the "recipe" that decides, in order, which activities to call and when to pause. This is the most important file in the project, so we'll go through it carefully.

**What's a "Workflow," and how is it different from an "Activity"?** A Workflow is the *orchestration* logic — the sequence of decisions and activity calls. Temporal records everything a workflow does as a history of events on its own server. If the worker process that's running a workflow dies, a new worker can pick that history back up and continue exactly where it left off, by **replaying** the workflow's code against that recorded history: instead of actually re-executing already-completed activities, it just feeds back their already-recorded results instantly, fast-forwarding until it reaches the exact point where it left off — then continues normally from there. This is *why* the workflow's own code has a strict rule: it must be **deterministic** — given the same history, it must make the exact same decisions every time it's replayed. That's why anything unpredictable (calling an AI model, reading the current time, random numbers) is pushed out into an Activity instead, and the workflow only ever contains plain, predictable logic plus calls out to activities.

Let's look at the imports first:

```python
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
```

- **`timedelta`** — Python's standard-library way of representing a *duration* (as opposed to a specific point in time), e.g. `timedelta(hours=24)` or `timedelta(seconds=10)`.
- **`with workflow.unsafe.imports_passed_through():`** — this is a Temporal-specific detail, not a general Python concept. Because workflow code gets replayed (see above), Temporal normally restricts what a workflow file is allowed to import, to catch accidentally non-deterministic code early. The activity files we're importing here (especially `generate_explanation.py`, which pulls in an AI library) don't actually run *inside* the workflow's replayed logic — the workflow only needs to hold a *reference* to the function so it can tell Temporal "go run this one, elsewhere." This block tells Temporal "trust me, these imports are safe to pass through without the usual restrictions."
- **`with ... :`** itself is Python's **context manager** syntax — a way of saying "do some setup, run this block of code, then do some cleanup afterward, no matter what happens inside." We'll see a more hands-on example of this in Section 2.11, when tests use `async with`.

Now the workflow class itself:

```python
@workflow.defn
class FraudHoldWorkflow:
    def __init__(self) -> None:
        self._response: CustomerResponse | None = None

    @workflow.signal
    def customer_responded(self, response: CustomerResponse) -> None:
        self._response = response
```

- **`@workflow.defn`** — same idea as `@activity.defn`, but registers this whole *class* as a workflow Temporal is allowed to run.
- **`def __init__(self) -> None:`** — every class can define an `__init__` method, which runs automatically whenever you create a new instance of that class. It's where you set up the object's starting state. `self` refers to "this particular instance" — every method on a class takes `self` as its first parameter, by convention, so it can read and change that instance's own data.
- **`self._response: CustomerResponse | None = None`** — sets up one piece of state this workflow instance remembers: "the customer's response, if we've gotten one yet." `CustomerResponse | None` is a type hint meaning "either a real `CustomerResponse`, or nothing at all" — and it starts out as `None` (nothing yet).
- **`@workflow.signal`** — marks `customer_responded` as a method that can be triggered from *outside* the workflow, asynchronously, while the workflow is running (or paused). This is Temporal's **Signal** mechanism. When a signal arrives, this method runs and just stores the response — it doesn't do anything else. (We'll see what actually *reacts* to that stored value next.)

Now the main logic:

```python
    @workflow.run
    async def run(self, transaction: Transaction, fraud_score_threshold: float) -> str:
        # Deterministic threshold check: this stays as plain workflow logic,
        # not an activity. ...
        if transaction.fraud_score < fraud_score_threshold:
            await workflow.execute_activity(
                record_no_hold_outcome,
                args=[transaction.transaction_id, transaction.fraud_score],
                start_to_close_timeout=timedelta(seconds=10),
            )
            return "no_hold_needed"
```

- **`@workflow.run`** — marks this as the workflow's main entry point — the method Temporal actually calls when a `FraudHoldWorkflow` is started. A workflow class can only have one of these.
- **`workflow.execute_activity(record_no_hold_outcome, args=[...], start_to_close_timeout=...)`** — this is how a workflow calls an activity: give it the activity function itself (imported above), the arguments to pass, and — always required — a `start_to_close_timeout`, telling Temporal the longest this activity is allowed to take before being considered failed.
- This is the "threshold check" from Section 1, step 3: a plain `if` comparing two numbers already handed to the workflow. Nothing about it can vary between replays, so it's safe as ordinary code, with no activity needed.

If the score is at or above the threshold, the hold happens first, deliberately before asking the AI for an explanation:

```python
        await workflow.execute_activity(
            place_hold,
            transaction.transaction_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        try:
            investigation: InvestigationSummary = await workflow.execute_activity(
                generate_explanation,
                args=[transaction.fraud_score, transaction.trigger_reason],
                start_to_close_timeout=timedelta(seconds=90),
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
```

- **`try` / `except ActivityError:`** — this is Python's error-handling syntax, appearing here for the first time. Code inside `try:` runs normally; if it raises an *exception* (an error), instead of crashing the whole program, execution jumps straight to the matching `except` block. `except ActivityError:` specifically only catches Temporal's `ActivityError` — the error Temporal raises when an activity has *exhausted all its retries and given up* (here, after 3 attempts per the `RetryPolicy` above). Any other kind of error would still crash the workflow — this only handles this one specific, expected failure mode.
- **`RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))`** — tells Temporal: if this activity fails, automatically try it again, up to 3 times total, waiting at least 1 second between attempts. This is entirely handled by Temporal — the workflow code doesn't write any retry loop itself.
- **Why the fallback matters:** by this point, `place_hold` has already run — the transaction is genuinely on hold. If `generate_explanation` fails even after retries (say, Ollama isn't running) and that error were allowed to crash the workflow, the transaction would be stuck on hold forever with no notification and no way to resolve it, short of someone manually intervening in Temporal. Catching the error and substituting a fixed, generic `InvestigationSummary` means the rest of the flow (notify, wait, resolve) still runs normally either way.
- Notice `investigation` ends up holding a real `InvestigationSummary` either way — either the AI's real one, or this hand-built fallback — and the `notify_customer` call right after doesn't know or care which.

Finally, the durable wait and resolution:

```python
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
```

- **`workflow.wait_condition(lambda: self._response is not None, timeout=...)`** — pauses the workflow until the given condition becomes true, or the timeout expires. This is *durable* pausing: it isn't tying up a worker thread or keeping a process running the whole time — but its state is still durably stored by the Temporal server, which is exactly why it can survive a worker restart (see Section 4). It's simply recorded that this workflow is waiting, and it will be woken back up either when the `customer_responded` signal sets `self._response` (making the condition true) or when 24 hours pass.
- **`lambda: self._response is not None`** — a `lambda` is a tiny, unnamed function written inline, used here because `wait_condition` needs *a function it can call repeatedly* to check the condition, not just a one-time value. `lambda: self._response is not None` means "a function with no arguments that returns whether `self._response` currently isn't `None`."
- **`except TimeoutError:`** — `wait_condition`'s timeout expiring raises this specific error, caught here to trigger the escalate path. (This used to be written as `asyncio.TimeoutError` — as of Python 3.11, `asyncio.TimeoutError` is just an alias for the builtin `TimeoutError`, so `ruff` flags the qualified form as an unnecessary import and rewrites it to the plain builtin. Same error, same behavior.)
- **`assert self._response is not None`** — a sanity check, not really "error handling": at this exact point in the code, we know `wait_condition` only returned normally (didn't raise `TimeoutError`) because the condition became true, so `self._response` genuinely can't be `None` here. `assert` documents that guarantee and would raise loudly if it were ever somehow wrong.

**Calls out to:** `models.py` (for the data shapes), every activity file. **Called by:** `worker.py` (registers it), `main.py` (starts and signals it), `send_signal.py` (signals it), the tests (runs it directly).

**Data flow:** in — a `Transaction` and a threshold number, given once at start. Out — a short string (`"no_hold_needed"`, `"released"`, `"blocked"`, or `"escalated_no_response"`) describing how the case ended.

---

### 2.8 `app/worker.py` — **Worker entrypoint**

**Job:** Connects to Temporal and starts the long-running process that actually executes `FraudHoldWorkflow` and all its activities.

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

**New concepts, explained:**

- **`Client.connect(...)`** — opens a connection to the Temporal *server* (a separate piece of software this project runs via Docker or `temporal server start-dev` — see `README.md`). Everything this project does with Temporal goes through a `Client` like this one.
- **`pydantic_data_converter`** — by default, Temporal's SDK only knows how to send plain, simple data types back and forth to its server. Since this whole project passes Pydantic models (`Transaction`, `InvestigationSummary`, `CustomerResponse`) as workflow/activity/signal arguments, every `Client` in this project passes this converter so Temporal knows how to properly turn those objects into JSON and back.
- **`Worker(client, task_queue=..., workflows=[...], activities=[...])`** — this is the object that does the actual work: it repeatedly asks the Temporal server "anything for me to run?" on the given `task_queue`, and when the answer is "run `FraudHoldWorkflow`" or "run `place_hold`," it's this `Worker` object that executes the matching registered function/class.
- **`await worker.run()`** — starts that polling loop, and doesn't return until the worker is stopped (e.g. `Ctrl+C`). This is why this process just sits there printing nothing further, once started — it's waiting for work.
- **`if __name__ == "__main__":`** — a very common Python idiom, appearing here for the first time. `__name__` is a special variable Python sets automatically; it equals `"__main__"` only when this file is the one you *ran directly* (e.g. `python -m app.worker`), and something else when this file is merely *imported* by another file. This line means "only actually start the worker if someone ran this file directly — don't start it just because some other file imported something from it." `asyncio.run(main())` is the standard way to kick off an `async def` function from ordinary (non-async) code — every `async` chain in this project ultimately starts from a call like this one.

**Calls out to:** every activity file, `fraud_hold_workflow.py`, `config.py`.
**Called by:** nobody (in Python terms) — it's started directly as a process (`python -m app.worker`, or via `docker-compose.yml`'s `worker` service).

---

### 2.9 `app/main.py` — **FastAPI endpoint**

**Job:** The HTTP-facing entry and exit points of the whole system — the only file that speaks the "web request" language.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from app.config import settings
from app.models import CustomerResponse, Transaction
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

_client: Client | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = await Client.connect(
        settings.temporal_address, data_converter=pydantic_data_converter
    )
    yield


app = FastAPI(lifespan=lifespan)
```

**New concepts, explained:**

- **`FastAPI`** — the web framework this project uses to turn Python functions into HTTP endpoints. `app = FastAPI(...)` creates "the application" — the object that will receive and route incoming web requests.
- **`_client: Client | None = None`** at the top level (not inside a function) — a variable shared by the whole file, starting out empty. It gets filled in once, when the app starts.
- **`@asynccontextmanager`** and **`lifespan`** — FastAPI lets you register a function that runs setup code, then `yield`s (pauses, handing control to the running application), and would run cleanup code after that `yield` if there were any here. This is FastAPI's way of saying "connect to Temporal once, when the app starts, and keep that one connection alive for as long as the app runs" — rather than reconnecting on every single request.
- **`global _client`** — inside a function, Python normally treats `_client = ...` as creating a brand-new *local* variable, even if a variable of that name already exists outside the function. `global _client` tells Python "no, I mean the one defined at the top of the file" — so this line actually updates the shared variable every other function in this file reads from.

Now the two endpoints:

```python
@app.post("/transactions/hold")
async def hold_transaction(transaction: Transaction) -> dict:
    assert _client is not None
    try:
        handle = await _client.start_workflow(
            FraudHoldWorkflow.run,
            args=[transaction, settings.fraud_score_threshold],
            id=transaction.transaction_id,
            task_queue=settings.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        return {"workflow_id": handle.id, "status": "started"}
    except WorkflowAlreadyStartedError:
        return {"workflow_id": transaction.transaction_id, "status": "already_started"}


@app.post("/transactions/{transaction_id}/respond")
async def respond_to_transaction(transaction_id: str, response: CustomerResponse) -> dict:
    assert _client is not None
    handle = _client.get_workflow_handle(transaction_id)
    try:
        await handle.signal(FraudHoldWorkflow.customer_responded, response)
    except RPCError:
        raise HTTPException(status_code=404, detail=f"No transaction found for {transaction_id!r}")
    return {"status": "signal_sent"}
```

- **`@app.post("/transactions/hold")`** — a decorator (there's that concept again) that registers the function right below it as the handler for `POST` requests to this exact URL path. When a request comes in for `/transactions/hold`, FastAPI calls `hold_transaction`.
- **`async def hold_transaction(transaction: Transaction) -> dict:`** — notice the parameter is typed as `Transaction` (our Pydantic model from `models.py`). This isn't decoration — FastAPI actually reads the incoming JSON body, validates it against `Transaction`'s fields (including the `alias=` mappings from Section 2.1), and hands you a real, already-validated `Transaction` object. If the JSON doesn't match, FastAPI automatically rejects the request before your function even runs — this is FastAPI's dependency-injection-flavored request handling: you declare *what you need*, and the framework supplies it.
- **`_client.start_workflow(FraudHoldWorkflow.run, args=[...], id=transaction.transaction_id, ...)`** — this is where step 2 from Section 1 actually happens: telling the Temporal server "start running `FraudHoldWorkflow`." Notice `id=transaction.transaction_id` — the transaction's own ID *is* the workflow's ID. That's deliberate (see Section 4.6, Idempotency).
- **`id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE`** — tells Temporal to refuse starting a new workflow reusing an ID that's ever been used before, even by a workflow that's already finished.
- **`except WorkflowAlreadyStartedError:`** — if the same `transaction_id` is submitted twice (either while the first one is still running, or after it's already finished, thanks to the policy above), Temporal raises this specific error instead of starting a duplicate. This is caught and turned into a clean `{"status": "already_started"}` response instead of crashing.
- **`respond_to_transaction`** — notice `transaction_id` appears both in the URL path (`{transaction_id}`) and as a function parameter of the same name — FastAPI automatically fills that parameter in from the URL. `handle.signal(FraudHoldWorkflow.customer_responded, response)` is how the API delivers a Signal to an already-running (or paused) workflow — this is what actually wakes up the `wait_condition` we saw in Section 2.7.
- **`except RPCError:`** paired with **`raise HTTPException(status_code=404, ...)`** — in this demo, an `RPCError` here is converted to an HTTP 404, mainly to handle an unknown or already-resolved `transaction_id`. But `RPCError` is a broad exception type — it's Temporal's general category for "the RPC call to the Temporal service failed," which could also mean something unrelated, like the Temporal server being unreachable. Production code would want to distinguish a genuine "workflow not found/closed" error from an unrelated infrastructure failure before deciding what HTTP status to return; this demo doesn't make that distinction, for simplicity. `raise` is how you deliberately trigger an exception yourself, rather than one happening naturally — `HTTPException` is FastAPI's own exception type specifically for "stop, and send this HTTP error status back to the caller."

**Calls out to:** `config.py`, `models.py`, `fraud_hold_workflow.py`.
**Called by:** nobody (in Python terms) — it's the process an ASGI server (`uvicorn`) runs directly.

---

### 2.10 `scripts/send_signal.py` — **Standalone script**

**Job:** A small command-line tool that sends a `customer_responded` signal directly, without going through the API at all — proving that Signal delivery doesn't require the FastAPI process to be involved.

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

**New concepts, explained:**

- **`argparse`** — Python's standard-library tool for reading command-line arguments. `parser.add_argument("transaction_id")` declares a required positional argument; `choices=["it_was_me", "not_me"]` on the second one means argparse itself rejects any other value before the script's own code even runs. `args = parser.parse_args()` actually reads whatever was typed after the script name on the command line (e.g. `python -m scripts.send_signal TXN-1001 it_was_me`) into `args.transaction_id` and `args.response`.
- Everything else here — `Client.connect`, `get_workflow_handle`, `.signal(...)`, `if __name__ == "__main__":` — is the exact same pattern already introduced in `worker.py` and `main.py`. The core logic — connect, get a handle by ID, signal it — is functionally identical to what `respond_to_transaction` does in `main.py`, just reached from a terminal instead of an HTTP request.

**Calls out to:** `config.py`, `models.py`, `fraud_hold_workflow.py`.
**Called by:** nobody — run directly from a terminal.

---

### 2.11 `tests/test_fraud_hold_workflow.py` — **Test**

**Job:** Five automated tests that exercise `FraudHoldWorkflow` end-to-end, without needing a real Temporal server, real activities, or a real Ollama running.

```python
import uuid
from collections.abc import Callable

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.models import CustomerResponse, InvestigationSummary, Transaction
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

FRAUD_SCORE_THRESHOLD = 70


def make_mock_activities(
    generate_explanation_fails: bool = False,
) -> tuple[list[str], list[str], list[Callable]]:
    calls: list[str] = []
    notify_messages: list[str] = []

    @activity.defn(name="record_no_hold_outcome")
    async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
        calls.append("record_no_hold_outcome")

    @activity.defn(name="generate_explanation")
    async def generate_explanation(fraud_score: float, trigger_reason: str) -> InvestigationSummary:
        calls.append("generate_explanation")
        if generate_explanation_fails:
            raise ValueError("simulated Ollama outage")
        return InvestigationSummary(
            customer_explanation="test explanation",
            ops_summary="test ops summary",
            notification_type="sms",
        )

    # ... place_hold, notify_customer, release, block, escalate follow the
    # same pattern, each just appending its own name to `calls` ...

    return calls, notify_messages, [
        record_no_hold_outcome,
        generate_explanation,
        # ... the rest ...
    ]
```

**New concepts, explained:**

- **What is "mocking," and why replace real activities with fake ones?** A *mock* is a fake, simplified stand-in for a piece of real code, used in tests so you can check "did my logic call the right things, in the right order, with the right data?" without actually doing the real (slow, unpredictable, or expensive) thing. Here, the real `generate_explanation` activity calls an actual AI model — far too slow and non-deterministic for a fast automated test. So the tests define their *own* versions of every activity — matching names, matching signatures — that just record what happened (by appending to the shared `calls` list) instead of doing real work. Crucially, `@activity.defn(name="generate_explanation")` registers this fake function under the *same name* the real one uses — Temporal matches activities by name, so the workflow code doesn't need to know or care whether it's talking to the real activity or a test's stand-in.
- **Closures** — `record_no_hold_outcome` and `generate_explanation` are defined *inside* `make_mock_activities`, and both refer to `calls` (and `notify_messages`), a variable that belongs to the outer function. This works because of a Python feature called a *closure*: an inner function can "remember" and modify variables from the function that defined it, even after that outer function returns. This is exactly how the tests can later inspect `calls` and see, after the fact, which activities actually ran.
- **`generate_explanation_fails: bool = False`** — a function parameter with a default value. Most tests call `make_mock_activities()` with no arguments (using the default, `False` — the AI mock succeeds normally); one test (Section 5.7) calls `make_mock_activities(generate_explanation_fails=True)` to make the mock always fail instead, simulating an Ollama outage.
- **`raise ValueError("simulated Ollama outage")`** — deliberately triggers an error, the same way `main.py` deliberately raises `HTTPException`. This is what eventually becomes the `ActivityError` that `fraud_hold_workflow.py`'s `except ActivityError:` catches (Section 2.7) — Temporal wraps whatever error an activity raises.
- **`Callable` (from `collections.abc`)** — a type hint meaning "any function." `list[Callable]` means "a list of functions" — used here because `make_mock_activities` returns the whole list of fake activity functions, ready to hand to a test `Worker`. `Callable` used to live in `typing`; as of Python 3.9+, the standard-library convention moved these container/callable type hints to `collections.abc`, and `typing.Callable` is now just a deprecated alias for the same thing — `ruff` flags the old import path and rewrites it to this one.
- **Tuples and unpacking** — `return calls, notify_messages, [...]` returns three things at once, bundled into a *tuple*. `calls, notify_messages, activities = make_mock_activities()` on the calling side is called *unpacking*: Python matches each name on the left to the corresponding item in the tuple on the right, in order.

Here's one full test, showing the pattern every test follows:

```python
@pytest.mark.asyncio
async def test_it_was_me_releases():
    calls, notify_messages, activities = make_mock_activities()
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
    assert calls.index("place_hold") < calls.index("generate_explanation")
```

- **`@pytest.mark.asyncio`** — pytest (the test-running tool) normally expects plain functions, not `async def` ones. This decorator tells pytest "this test is asynchronous — run it properly using `asyncio`, awaiting it instead of just calling it."
- **`async with ... as env:`** — this is the concept flagged as a forward reference back in Section 2.7: `async with` is Python's syntax for using a **context manager** — something with defined setup and teardown behavior wrapped around a block of code, guaranteeing the teardown runs even if something inside the block fails. Here, `WorkflowEnvironment.start_time_skipping()` spins up a temporary, isolated Temporal test environment with time-skipping enabled; the `as env` part gives you a handle to it (`env`) to use inside the block; and when the block ends, that temporary environment is automatically shut down — you never have to remember to clean it up yourself. The `Worker(...)` right below does the same thing for the test's worker process — it starts polling for work the moment you enter the block, and stops automatically when you leave it.
- **`WorkflowEnvironment.start_time_skipping()` — what is it, and why does this project need it?** One of the five tests (Section 5.6) needs to prove the workflow correctly times out after **24 real hours** of no response. Actually waiting 24 hours for a test to pass obviously isn't practical. This special test environment solves that: it keeps track of a *simulated* clock, and whenever the test is just waiting for a result (like `await handle.result()`), it automatically fast-forwards that simulated clock straight to the next thing that's scheduled to happen — collapsing a real 24-hour wait into a fraction of a second of actual test time, without changing a single line of the workflow's own code.
- **`uuid.uuid4()`** — generates a random, essentially-guaranteed-unique identifier. Every test generates a fresh one for its task queue and workflow ID, so tests never collide with each other, even if run repeatedly or in parallel.
- **`assert result == "released"`** — pytest's way of checking an expectation; if the condition is false, the test fails and reports exactly this line.

**Calls out to:** `fraud_hold_workflow.py`, `models.py` — this file exercises the *real* workflow code, with only the activities faked.
**Called by:** nobody — run via `pytest`.

---

### 2.12 The `__init__.py` files

`app/__init__.py`, `app/activities/__init__.py`, `app/workflows/__init__.py`, and `scripts/__init__.py` are all completely empty. These empty files explicitly mark these directories as regular Python packages — not strictly required in every modern Python setup (Python 3.3+ also supports "implicit namespace packages," folders with no `__init__.py` at all), but doing it explicitly here keeps imports and tooling behavior predictable, and is what makes writing `from app.activities.hold import place_hold` (instead of some more awkward path-based import) possible throughout this project. There's nothing to read inside them. (If you know Java: a Python package — a folder containing an `__init__.py` — plays roughly the same organizing role as a Java package, and `app.activities.hold` reads much like a Java package path such as `app.activities.hold`, just with dots instead of matching directory-and-namespace declarations.)

---

## 3. Complete Runtime Walkthrough

Now that every file and every concept has been introduced, let's trace two complete, real examples through the *exact* sequence of files and functions involved — no new concepts here, just following the thread from end to end.

### 3.1 Scenario A: A hold that gets released after "it was me"

1. A `POST /transactions/hold` request arrives → **`app/main.py`**, `hold_transaction(...)`.
2. FastAPI validates the JSON body into a `Transaction` (**`app/models.py`**).
3. `hold_transaction` calls `_client.start_workflow(FraudHoldWorkflow.run, ...)` — this talks to the Temporal *server*, telling it to start a workflow. `hold_transaction` returns `{"workflow_id": ..., "status": "started"}` immediately — it does **not** wait for the workflow to finish.
4. Separately, the **worker process** (running **`app/worker.py`**) — which has been polling the Temporal server the whole time — picks up the new workflow and starts executing **`app/workflows/fraud_hold_workflow.py`**, `FraudHoldWorkflow.run(...)`.
5. Inside `run`: the threshold check (`transaction.fraud_score < fraud_score_threshold`) is false — a hold is needed.
6. The workflow calls `place_hold` → the worker executes **`app/activities/hold.py`**, `place_hold(...)` — prints, sleeps 1 second.
7. The workflow calls `generate_explanation` → the worker executes **`app/activities/generate_explanation.py`**, `generate_explanation(...)` — this succeeds, returning a real `InvestigationSummary`.
8. The workflow calls `notify_customer` with that summary's fields → the worker executes **`app/activities/notify.py`**, `notify_customer(...)`.
9. The workflow reaches `wait_condition(...)` and pauses — durably. At this point, even if the worker process were killed and restarted, the workflow would still be exactly here when it comes back (see Section 4.1).
10. Later, a `POST /transactions/TXN-.../respond` request with `{"response": "it_was_me"}` arrives → **`app/main.py`**, `respond_to_transaction(...)`.
11. It calls `handle.signal(FraudHoldWorkflow.customer_responded, response)` — this delivers a Signal to the paused workflow.
12. Back in the worker process, `FraudHoldWorkflow.customer_responded(...)` (in **`fraud_hold_workflow.py`**) runs, setting `self._response`.
13. That makes `wait_condition`'s condition true, waking up `run` right where it left off.
14. `self._response.response == "it_was_me"` is true, so the workflow calls `release` → the worker executes **`app/activities/hold.py`**, `release(...)`.
15. `run` returns `"released"` — the workflow is now complete.

### 3.2 Scenario B: The below-threshold path

1. A `POST /transactions/hold` request arrives with a low `fraudScore` → **`app/main.py`**, `hold_transaction(...)`.
2. Same as before: validated into a `Transaction`, `start_workflow` called, `{"status": "started"}` returned immediately.
3. The worker picks it up and starts **`app/workflows/fraud_hold_workflow.py`**, `FraudHoldWorkflow.run(...)`.
4. The threshold check (`transaction.fraud_score < fraud_score_threshold`) is **true** this time.
5. The workflow calls `record_no_hold_outcome` → the worker executes **`app/activities/log_outcome.py`**, `record_no_hold_outcome(...)`.
6. `run` immediately returns `"no_hold_needed"`. No hold was ever placed, no AI call happened, no notification was sent — it completes without ever entering the long-running customer-response wait.

Notice how much of `FraudHoldWorkflow.run` — the AI call, the hold, the notification, the entire wait-for-signal mechanism — Scenario B simply never touches. That's the whole point of the threshold check living right at the top of the function.

---

## 4. Where the Durability Concepts Live in the Code

Each of Temporal's core guarantees shows up as a specific, small piece of code in this project. Here's exactly where.

### 4.1 Crash recovery / replay

**Where:** the entire body of `FraudHoldWorkflow.run` in `app/workflows/fraud_hold_workflow.py`, and specifically the fact that it contains no unpredictable logic outside of activity calls.

**How it works, conceptually:** every meaningful step a workflow takes (each activity call starting, each activity call finishing, each signal arriving) gets permanently recorded by the Temporal *server* as an **event history** — independent of whatever worker process happens to be running at the time. If a worker process dies mid-workflow (say, right after `place_hold` but before `generate_explanation`), nothing is lost, because that worker process was never the thing holding the workflow's state — the server was. When a (possibly brand new) worker picks the workflow back up, Temporal has it **replay**: it re-runs the workflow's own code from the very beginning, but instead of actually re-executing already-completed activities, it just feeds back their already-recorded results instantly, fast-forwarding until it reaches the exact point where it left off — then continues normally from there. This is only safe because the workflow's own code (outside of activity calls) is required to be deterministic — see the threshold-check comment in `fraud_hold_workflow.py`, lines 42–48, which explains this exact reasoning.

### 4.2 Retries

**Where:** `fraud_hold_workflow.py`, the `retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))` argument on the `generate_explanation` activity call.

**What it does:** if `generate_explanation` fails, Temporal automatically retries it (up to 3 attempts total, at least 1 second apart) *without any retry loop written in this project's own code*. Every other activity call in this file has no explicit `retry_policy=`, which means it falls back to Temporal's default Activity retry policy rather than any specific fixed number of attempts. `generate_explanation` gets a deliberately small, explicit policy because it's the one call most likely to be genuinely flaky (a local AI model — response time varies a lot run to run, and can be substantial depending on which model `OLLAMA_MODEL` points at) — in a real, production system, every activity would generally want its own deliberately chosen retry limit, timeout, and idempotency behavior, rather than being left on Temporal's defaults by accident; this demo mostly leans on the defaults for everything except this one call, for simplicity.

**A concrete example of why this matters:** this call's `start_to_close_timeout` is 90 seconds — deliberately generous. It used to be 30 seconds, tuned around a smaller, faster default model. After switching the default `OLLAMA_MODEL`, direct timing showed single calls regularly taking 30-50+ seconds (this wasn't just a one-off "cold start" cost either — a second, back-to-back call on an already-loaded model took *longer* than the first). At 30 seconds, attempt 1 was timing out on essentially every real transaction, and the workflow only succeeded because the `RetryPolicy` above quietly absorbed a full, wasted retry cycle (and the extra latency that came with it) on every single hold. This is exactly the kind of thing Temporal's Event History makes visible that a normal application log might not: an `ActivityTaskStarted` event with `Attempt: 2`, and a `Last Failure` panel showing `"timeoutType": "TIMEOUT_TYPE_START_TO_CLOSE"` from attempt 1, tells you precisely what happened and when — open any workflow's Event History in the Temporal Web UI (`http://localhost:8233`) and look for the same pattern if you want to see it directly.

### 4.3 Signals

**Where:** the `@workflow.signal` decorator on `customer_responded` in `fraud_hold_workflow.py`, and the two places that trigger it — `handle.signal(...)` in `app/main.py`'s `respond_to_transaction`, and the identical call in `scripts/send_signal.py`.

**What it does:** a Signal is how something *outside* a running workflow — here, an HTTP request or a CLI script — delivers a message *into* it asynchronously, without needing to know or care whether the workflow happens to be actively running or durably paused at that exact moment. Temporal handles the delivery either way.

### 4.4 Timeout

**Where:** `fraud_hold_workflow.py`, the `timeout=timedelta(hours=24)` argument to `workflow.wait_condition(...)`, and the paired `except TimeoutError:` block right after it.

**What it does:** bounds how long the workflow will wait for a Signal before giving up and moving on (to `escalate`) on its own. Note this is a completely different mechanism from the `RetryPolicy` above — this isn't retrying anything, it's a maximum wait duration on a pause.

### 4.5 Fallback messaging

**Where:** `fraud_hold_workflow.py`, the `try:` / `except ActivityError:` wrapped directly around the `generate_explanation` call.

**What it does:** turns "the AI call failed even after every retry" from a workflow-ending crash into a handled, expected case — the workflow substitutes a fixed `InvestigationSummary` and continues exactly as if the AI call had succeeded, all the way through to the final resolution. See `tests/test_fraud_hold_workflow.py`'s `test_ai_failure_falls_back_and_still_resolves` (Section 5.7) for the automated proof that this actually works.

### 4.6 Idempotency

**Where:** `app/main.py`, the `id=transaction.transaction_id` argument to `start_workflow`, combined with `id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE`, and the `except WorkflowAlreadyStartedError:` block right after.

**What it does:** using the transaction's own ID as the workflow's ID means Temporal itself becomes the mechanism that prevents duplicate processing — if the same transaction is submitted twice (say, a retried webhook from the fraud engine), the *second* `start_workflow` call fails with `WorkflowAlreadyStartedError`, whether the first workflow is still running (Temporal's default behavior already covers this case) or has already finished (which specifically needs `REJECT_DUPLICATE` to catch, since the default would otherwise allow starting a fresh execution reusing that same ID). Either way, `main.py` catches that error and returns the existing workflow's ID instead of silently starting a second, duplicate execution.

---

## 5. The Tests, Explained

### 5.1 What is mocking, and why do the tests replace real activities with fake ones?

(Covered in detail in Section 2.11 — brief recap here.) A mock is a fake stand-in for real code, used so a test can check "did the right things happen, in the right order?" without doing the real, slow, or unpredictable work. Every test in this file builds its own set of fake activities via `make_mock_activities()`, each one just recording its own name into a shared `calls` list instead of actually holding funds or calling an AI.

### 5.2 What is `WorkflowEnvironment.start_time_skipping`, and why is it needed?

(Also covered in Section 2.11.) It's a temporary, isolated Temporal test environment with time-skipping enabled — a *simulated* clock that automatically fast-forwards whenever a test is just waiting for a result. Without it, `test_timeout_escalates` (below) would need to actually wait 24 real hours to pass.

Now, the five tests themselves:

### 5.3 `test_below_threshold_records_no_hold_outcome`

**Scenario:** a `Transaction` with `fraud_score=50` (below the test's threshold of 70) is run through the workflow, with no signal ever sent.

**Asserts:** the workflow's result is `"no_hold_needed"`, and `calls` equals *exactly* `["record_no_hold_outcome"]` — not just "contains" it, but the *entire* list of everything that ran, nothing more.

**What it proves:** the below-threshold branch genuinely skips `place_hold`, `generate_explanation`, and `notify_customer` entirely — this is a strong assertion, since checking the full list rules out any activity firing that shouldn't.

### 5.4 `test_it_was_me_releases`

**Scenario:** a `Transaction` with `fraud_score=90` (above threshold) is started, then immediately signaled with `CustomerResponse(response="it_was_me")`.

**Asserts:** the result is `"released"`; `"release"` ran but `"block"` and `"escalate"` did not; and — importantly — `calls.index("place_hold") < calls.index("generate_explanation")`, proving `place_hold` genuinely ran *before* `generate_explanation`, not just that both ran.

**What it proves:** the full above-threshold happy path works end to end, and specifically that the fund-protecting hold really does happen before the (potentially slow/unreliable) AI call, not after it.

### 5.5 `test_not_me_blocks`

**Scenario:** identical to 5.4, but signaled with `response="not_me"`.

**Asserts:** the result is `"blocked"`; `"block"` ran, `"release"` and `"escalate"` did not; same ordering check as above.

**What it proves:** the workflow correctly branches on the *content* of the signal, not just on a signal having arrived at all.

### 5.6 `test_timeout_escalates`

**Scenario:** an above-threshold transaction is started, and — deliberately — **no signal is ever sent**.

**Asserts:** the result is `"escalated_no_response"`, and `"escalate"` ran.

**What it proves:** the 24-hour timeout branch works correctly, without the test actually taking 24 hours (thanks to time-skipping — see 5.2).

### 5.7 `test_ai_failure_falls_back_and_still_resolves`

**Scenario:** `make_mock_activities(generate_explanation_fails=True)` — the fake `generate_explanation` raises `ValueError` on *every* call, simulating Ollama being completely unreachable even after retries. The transaction is above threshold, and gets signaled with `"it_was_me"`.

**Asserts:** the result is still `"released"`; `"generate_explanation"` did run (it wasn't skipped — it was attempted and failed); `"notify_customer"` still ran; and, most specifically, the actual message captured in `notify_messages` is **not** the AI mock's normal success text (`"test explanation"`) and **does** contain `"temporarily paused"` — the exact fallback wording from `fraud_hold_workflow.py`.

**What it proves:** this is the automated proof for Section 4.5 — that a total, repeated AI failure doesn't crash the workflow or leave the hold stuck, and that the customer genuinely receives the fallback message (not just that *some* message was sent).

---

## If you want to read this in stages

Everything in this guide is worth reading eventually — nothing here is filler. But if a full first pass feels like a lot in one sitting, here's one way to split it into a couple of visits rather than trying to absorb it all at once:

- **A first pass**, to get oriented: Section 1 (the big picture), the quick-reference table right after it, and Section 2's entries for `models.py`, the activity files (2.3–2.6), and `fraud_hold_workflow.py` (2.7) — that's enough to understand what the system does and how the core orchestration is written.
- **A second pass**, to see it all connect: the rest of Section 2 (`worker.py`, `main.py`, `send_signal.py`, the tests), then Section 3's two traced-through scenarios — by this point the file-by-file pieces should click into a single mental model of a request's full journey.
- **A third pass**, to consolidate: Section 4 (where each durability concept actually lives in the code) and Section 5 (what each test proves) — these two sections mostly point back at code you've already read, tying it to the specific Temporal concepts and correctness guarantees it demonstrates.

Come back to any earlier section as needed — the file-by-file walkthrough in particular is meant to double as a reference, not just a one-time read.
