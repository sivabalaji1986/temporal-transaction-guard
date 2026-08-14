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
   - **the Agent in `generate_explanation.py`** — hands the case to a local AI **Agent** (via PydanticAI and Ollama), called *directly* from the workflow's own code. This isn't just one question-and-answer call: the Agent can decide, on its own, to call one or both of two small read-only lookup tools first (recent transactions, notification preference) before writing a customer-friendly explanation and an internal summary. Every one of those steps — each time the Agent asks the model something, each tool call — runs as its *own*, separately durable Temporal Activity under the hood, not all bundled into one. Section 2.6 covers what makes this an *Agent* rather than a single LLM call, exactly what it is and isn't allowed to do, and what "each step is its own Activity" actually buys you.
   - **`notify_customer`** (in `notify.py`) — sends that explanation to the customer.

5. **The workflow then pauses — durably.** It waits for one of two things: the customer replying ("it was me" / "not me"), delivered as something Temporal calls a **Signal**, or 24 hours passing with no reply. "Durably" is the key word here: the workflow can sit in this paused state for hours or days, and it isn't tying up a worker thread or keeping a process running the whole time — but its state is still durably stored by the Temporal server for that entire time, which is exactly why it can survive a worker restart. We'll unpack exactly what that means in Section 4.

6. **A separate process — the worker (`app/worker.py`) — is what actually runs all of this.** This is an important distinction. `app/main.py` (the API) never runs the workflow's own code; it just talks to the Temporal *server*, which schedules work. The **worker** is a different, long-running Python process that connects to that same Temporal server, and it's the one that actually executes the workflow's code and every Activity — `place_hold`, the Agent's individual model-request and tool-call steps, `notify_customer`, and so on — when the server tells it to. Why split these into two processes? Because this project's whole point is to demonstrate that the worker can be killed and restarted *without losing any progress* — and that only works because the worker doesn't hold the important state itself, the Temporal server does. If the API and the worker were the same process, restarting it to prove that point would also take down the API. This project actually goes one step further: you can run *two or more* worker processes at once, all polling the same Temporal task queue, and kill any one of them mid-Activity — Temporal just hands the remaining work to whichever worker is still alive. That two-worker scenario is "Demo C" in `README.md`; Section 4.7 of this guide covers where the code that makes it observable lives.

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
| `app/activities/generate_explanation.py` | Agent definition (fine-grained Activities) | Tool-using AI Agent generates the explanation (step 4) |
| `app/workflows/fraud_hold_workflow.py` | Workflow orchestration | Threshold check, activity calls, direct Agent call, wait, resolve (steps 2–8) |
| `app/worker.py` | Worker entrypoint | Actually executes the workflow + activities (step 6) |
| `app/main.py` | FastAPI endpoints | Request in (steps 1–2), Signal in (step 7) |
| `scripts/send_signal.py` | Standalone script | Signal in — an alternative path for step 7 |
| `tests/test_fraud_hold_workflow.py` | Test | Exercises steps 2–8 without a real server or AI |
| `tests/test_generate_explanation_agent.py` | Test | Exercises the Agent and its tools directly, without a real Ollama, without Temporal |
| `tests/test_generate_explanation_agent_durability.py` | Test | Proves the fine-grained per-step durability behavior, through a real (test) Temporal Workflow |

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

### 2.6 `app/activities/generate_explanation.py` — **Agent definition (fine-grained Activities)**

**Job:** Defines the AI **Agent** (via [PydanticAI](https://ai.pydantic.dev), talking to a locally-running [Ollama](https://ollama.com) model) that turns a fraud score and reason into human-readable text — and, unlike a single API call, lets that Agent decide for itself whether it needs to look up more context first. This file doesn't define an `@activity.defn` function; instead, the Agent's `capabilities=[TemporalDurability(...)]` setting (covered below) is what turns its model requests and tool calls into Temporal Activities.

**What makes this an *Agent* rather than "just calling an LLM"?** A single LLM call is a one-shot round trip: you send a prompt, you get back text (or, with `output_type`, structured data), and that's the entire interaction — the model can't go do anything else in between. An **Agent** here means something more specific: the model is given a set of **tools** (real Python functions it can choose to invoke) and is allowed to keep going — call a tool, look at what it returned, decide whether it needs another tool or already has enough to answer, and only *then* produce its final structured output. The model itself decides how many of those turns it needs, if any. That decision-making loop — not the mere presence of an LLM — is what "agentic" means in this codebase, and it's why the locked principle for this file is **agentic investigation, deterministic action**: the Agent gets real autonomy over *how it investigates*, and zero autonomy over *what happens to the transaction*.

**In one sentence:** the Agent itself carries a `capabilities=[TemporalDurability(...)]` setting, and PydanticAI's own Temporal integration turns *each* model request and *each* tool call into its own separate Temporal Activity, automatically, whenever `_agent.run(...)` is called from inside a Workflow.

```python
import asyncio
from datetime import timedelta
from typing import Literal

from pydantic_ai import Agent, RunContext
from pydantic_ai.durable_exec.temporal import TemporalDurability
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import UsageLimits
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from app.config import settings
from app.models import InvestigationSummary

_AGENT_USAGE_LIMITS = UsageLimits(request_limit=6, tool_calls_limit=4)

_BASE_ACTIVITY_CONFIG: ActivityConfig = {
    "start_to_close_timeout": timedelta(seconds=10),
    "retry_policy": RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1)),
}
_MODEL_ACTIVITY_CONFIG: ActivityConfig = {
    "start_to_close_timeout": timedelta(seconds=60),
    "retry_policy": RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1)),
}

_agent = Agent(
    OpenAIChatModel(
        settings.ollama_model,
        provider=OpenAIProvider(base_url=settings.ollama_base_url, api_key="ollama"),
    ),
    name="fraud_hold_investigator",
    output_type=InvestigationSummary,
    deps_type=str,
    system_prompt=(
        "An upstream fraud-detection system identifies candidate suspicious "
        "transactions and provides a fraud score and trigger reason for each "
        "one. Our Temporal Workflow -- not this agent -- performs the "
        "deterministic threshold check on that score, decides whether to "
        "hold, and places the hold. You are only called after that hold "
        "decision has already been made and the hold is already in place. "
        "You may call your read-only tools, in any order, zero or more "
        "times, to gather extra context about this customer before writing "
        "your response -- for example their recent transaction activity or "
        "their preferred notification channel. Your only job is to produce "
        "three things: a customer-friendly explanation, a short internal "
        "fraud-ops summary, and the best notification channel. You must "
        "never decide whether to hold, release, block, or escalate a "
        "transaction -- those decisions belong entirely to the Workflow, "
        "not to you. The customer-facing explanation must stay concise and "
        "appropriate for an end customer: never paste raw tool output, "
        "internal identifiers, or other internal-only details directly "
        "into it -- summarize what's relevant in plain language instead."
    ),
    capabilities=[
        TemporalDurability(
            activity_config=_BASE_ACTIVITY_CONFIG,
            model_activity_config=_MODEL_ACTIVITY_CONFIG,
        )
    ],
)


_MOCK_RECENT_TRANSACTIONS: dict[str, list[dict[str, str | float]]] = {
    "CUST-101": [
        {"merchant": "Acme Coffee", "amount": 4.50, "currency": "USD", "country": "US"},
        {"merchant": "Riverside Grocer", "amount": 62.10, "currency": "USD", "country": "US"},
        {"merchant": "Unnamed Kiosk", "amount": 310.00, "currency": "EUR", "country": "DE"},
    ],
    "CUST-202": [
        {"merchant": "Downtown Pharmacy", "amount": 18.25, "currency": "USD", "country": "US"},
        {"merchant": "Lakeside Diner", "amount": 27.80, "currency": "USD", "country": "US"},
    ],
}

_MOCK_CHANNEL_PREFERENCES: dict[str, Literal["sms", "email", "push"]] = {
    "CUST-101": "email",
    "CUST-202": "push",
}

_DEFAULT_CHANNEL_PREFERENCE: Literal["sms", "email", "push"] = "sms"


@_agent.tool
async def lookup_recent_transactions(ctx: RunContext[str]) -> list[dict[str, str | float]]:
    """Read-only mock: this customer's recent transactions (merchant, amount, currency, country)."""
    return _MOCK_RECENT_TRANSACTIONS.get(ctx.deps, [])


@_agent.tool
async def lookup_customer_channel_preference(
    ctx: RunContext[str],
) -> Literal["sms", "email", "push"]:
    """Read-only mock: this customer's preferred notification channel."""
    if settings.demo_failover_delay_seconds > 0:
        await asyncio.sleep(settings.demo_failover_delay_seconds)
    return _MOCK_CHANNEL_PREFERENCES.get(ctx.deps, _DEFAULT_CHANNEL_PREFERENCE)
```

**New concepts, explained:**

- **Module-level code that isn't inside a function** — the `_agent = Agent(...)` block runs *once*, the moment this file is first imported (same idea as `settings = Settings()` in `config.py`). It builds one `Agent` object, registers its tools on it, and every call site (the Workflow, the tests) reuses that same object rather than rebuilding it from scratch. The leading underscore in `_agent` is a Python convention meaning "this is private to this file, other files shouldn't reach in and use it directly" — it's not enforced by the language, just a signal to readers. (There is one deliberate, documented exception to "other files shouldn't reach in": Section 2.11's tests use pytest's `monkeypatch` to temporarily swap this exact `_agent` binding out for a test double — that's a testing technique, not a normal call site, and it's explained in full there.)
- **`Agent(model, name=..., output_type=..., deps_type=..., system_prompt=..., capabilities=[...])`** — this is PydanticAI's core building block: an `Agent` wraps a language model and, critically, `output_type=InvestigationSummary` tells PydanticAI "don't just give me back free text — force the model's final answer into this exact Pydantic shape." `deps_type=str` declares that this Agent expects one piece of "dependency" data — here, the `customer_id` — that tools can read but the model itself never sees or supplies. `name=` and `capabilities=` are new in this version of the file — both covered below.
- **`OpenAIChatModel` + `OpenAIProvider`** — Ollama exposes an API that's compatible with OpenAI's own API format, so PydanticAI can talk to it using its "OpenAI" support, just pointed at a different `base_url` (Ollama's local address instead of OpenAI's servers) with a throwaway `api_key` (Ollama doesn't check it).
- **`name="fraud_hold_investigator"` — why does the Agent need an explicit name now?** Once an Agent has `TemporalDurability` capabilities, PydanticAI needs a *stable* string to build the Temporal Activity type names it registers for this Agent's model requests and tool calls — you'll see exactly what those names look like below. If this string ever changed, any Workflow execution already in progress (or being replayed from history — Section 4.1) would reference Activity type names that no longer exist. Treat a rename here the same way you'd treat renaming a database column an in-flight process depends on — see `AGENTS.md` rule 7.
- **`capabilities=[TemporalDurability(activity_config=..., model_activity_config=...)]` — the mechanism that turns the Agent's model requests and tool calls into Temporal Activities.** `TemporalDurability` is a capability PydanticAI's `pydantic_ai.durable_exec.temporal` module provides specifically for running an Agent inside a Temporal Workflow. When this Agent's `.run(...)` is called from Workflow code (Section 2.7), PydanticAI transparently turns *every* model request into its own Temporal Activity (named `agent__fraud_hold_investigator__model_request`) and *every* tool call into its own Temporal Activity (named `agent__fraud_hold_investigator__toolset__<agent>__call_tool`) — each with its own independently tracked attempt history, timeout, and retry policy. Outside a Workflow (e.g. when a test calls `_agent.run(...)` directly, Section 2.12), this capability has no effect at all — it behaves like a plain, non-durable Agent.
- **`activity_config=` vs. `model_activity_config=`** — `TemporalDurability` takes two separate configs. `activity_config` is the base config, which applies to tool-call Activities (`_BASE_ACTIVITY_CONFIG`: 10-second timeout, 2 attempts). `model_activity_config` is shallow-merged on top of it specifically for model-request Activities (`_MODEL_ACTIVITY_CONFIG`: 60-second timeout, same retry shape) — model requests are given a longer timeout because they're the slower of the two step types (an actual Ollama call vs. a fast in-memory dict lookup). Both are plain Python dicts typed as `temporalio.workflow.ActivityConfig` — the same shape you'd otherwise pass as keyword arguments to `workflow.execute_activity(...)` (Section 2.7 has one of those too, for the *other*, non-Agent activities).
- **Why explicit `RetryPolicy(maximum_attempts=2, ...)` on both configs, instead of leaving it out?** Temporal's own default `RetryPolicy` has `maximum_attempts=0`, meaning *unlimited* retries — left unset, a single failing model request or tool call could in principle retry forever, which would make the whole Agent investigation's wall-clock time unbounded. Setting `maximum_attempts=2` explicitly is what makes it possible to calculate a real worst-case duration for the whole Agent phase — see the comment in the actual source file (elided here for length) and `AGENTS.md` rule 6 for that full calculation: 6 possible model requests × up to 121s each, plus 4 possible tool calls × up to 21s each, comes to 810 seconds — under the 900-second ceiling this project targets for the whole Agent investigation.
- **What is a PydanticAI "tool"?** A tool is a normal Python `async def` function that you register on an `Agent` so the model can choose to call it *during* a run — a bounded, specific capability you hand the model, not a way for the model to run arbitrary code. It's not the same thing as an ordinary Python function call elsewhere in your app: the model only "knows about" a tool through a schema PydanticAI generates from its type hints (name, parameters, return type), and the model decides on its own, based on the conversation so far, whether calling it is useful.
- **`@_agent.tool`** — the decorator that registers a function as one of `_agent`'s tools (same decorator pattern as `@activity.defn` in Section 2.3: it doesn't change what the function does when called directly, it attaches it to something else's system — here, the Agent's tool list). Under `TemporalDurability`, each call PydanticAI makes to one of these functions is itself wrapped as a Temporal Activity — but the function's own code, shown above, doesn't know or care about that; it's plain Python code with nothing Activity-specific in it.
- **`RunContext[str]`** — the first parameter of a context-aware tool. `ctx.deps` gives the tool access to whatever was passed as `deps=` to `_agent.run(...)` — here, the `customer_id` string. Because `ctx` isn't a normal argument the *model* fills in (PydanticAI excludes it from the tool's schema), the model can never choose or override which customer's data these tools look up. This holds even now that tool calls cross a real Temporal Activity boundary: `deps` rides to the Activity as its own separate, typed parameter, never folded into the model-visible `tool_args` — see `AGENTS.md` rule 9.
- **The two tools are deliberately read-only and return customer-scoped mock data** — `lookup_recent_transactions` and `lookup_customer_channel_preference` don't call any real system; they look up `ctx.deps` (the `customer_id`) in the small, hardcoded `_MOCK_RECENT_TRANSACTIONS`/`_MOCK_CHANNEL_PREFERENCES` dicts above, so `CUST-101` and `CUST-202` genuinely get different results, with a safe default (empty list / `"sms"`) for any other customer_id.
- **The `asyncio.sleep` inside `lookup_customer_channel_preference` — what is that, and is it always active?** No — `settings.demo_failover_delay_seconds` defaults to `0`, meaning this `if` is false and the sleep never happens in normal operation, including every automated test. It's a demo-only hook: setting the `DEMO_FAILOVER_DELAY_SECONDS` environment variable gives a manual two-Worker failover demo (README's "Demo C") a wide, predictable window where this specific tool-call Activity is genuinely in-flight, long enough to reliably kill one Worker process and watch a second one pick up the retry. It only ever delays this one Activity — it changes nothing about Workflow code, `customer_id` handling, or the `TemporalDurability` boundary itself.
- **Why does a Temporal retry only re-run the failed step, not the whole Agent loop?** Each model request and each tool call is tracked as its own independent Activity in Temporal's Event History: if, say, `lookup_recent_transactions` already succeeded and got recorded, and the *next* model request then fails, only that failing model-request Activity retries — the already-completed tool call isn't re-invoked. `tests/test_generate_explanation_agent_durability.py` (Section 2.13) proves this directly, by scripting exactly this scenario and asserting the tool only ran once.
- **Why can't the Agent perform an actual banking action?** Structurally, not just by instruction: the only tools registered on `_agent` are the two read-only lookups above. There is no `release`, `block`, `escalate`, or `place_hold` tool anywhere near the Agent's tool list — those functions live in `hold.py` and are only ever called directly by the *Workflow* (Section 2.7), never passed to `Agent(...)`. Even a fully "jailbroken" model talking to this Agent has nothing it could call to move money.
- **`UsageLimits(request_limit=6, tool_calls_limit=4)`** — the explicit bound on the Agent's loop, passed to `_agent.run(...)` as `usage_limits=` (Section 2.7 shows exactly where). Without it, a model that kept deciding "call one more tool" could in principle loop for a very long time. `request_limit` caps how many separate calls to the model itself can happen in one run; `tool_calls_limit` caps how many tool invocations total. Exceeding either raises `pydantic_ai.exceptions.UsageLimitExceeded`, a subclass of `pydantic_ai.exceptions.AgentRunError` — which, unlike a plain Temporal `ActivityError`, propagates *directly* from `_agent.run(...)` in Workflow code (Section 2.7 explains why the Workflow's `except` clause now needs to catch both types).
- **Keeping customer-facing text separate from internal context** — the system prompt explicitly tells the model to summarize what it learns from its tools in plain language for `customer_explanation`, and never to paste raw tool output or internal identifiers into it directly; `ops_summary` is where more internal detail belongs. This is a prompt-level instruction, not something the code enforces after the fact. Section 2.12 covers how the tests build a *scripted* stand-in model to check this contract deterministically, and why that's a meaningfully different (and more limited) guarantee than "a live LLM will always comply."
- **What happens to `ops_summary`?** The Agent generates all three `InvestigationSummary` fields, including `ops_summary`. But if you follow it forward into Section 2.7, you'll see the workflow only ever reads `investigation.customer_explanation` and `investigation.notification_type` when calling `notify_customer` — `ops_summary` isn't passed anywhere further in this demo. It exists as a hook for a future audit-log or internal-ops integration.

**If you know Java:** think of `InvestigationSummary` the same way Section 2.1 described `Transaction` — roughly a DTO/record, but with runtime binding and validation built in rather than hand-written. `@_agent.tool` is conceptually similar to an annotation-based registration mechanism (like `@Component` making a Spring bean discoverable). `capabilities=[TemporalDurability(...)]` is closer to a cross-cutting concern applied declaratively to the whole Agent — somewhat like wrapping every method of a Spring bean in its own `@Transactional` boundary, except here it's Temporal's durable-execution boundary being applied per model-request and per tool-call, automatically, rather than one boundary per method you write by hand.

**Calls out to:** `app/config.py` (for the Ollama URL/model and the demo delay setting), `app/models.py` (for the `InvestigationSummary` shape); PydanticAI/Ollama externally.
**Called by:** `fraud_hold_workflow.py` (directly calls `_agent.run(...)` — see Section 2.7), `tests/test_generate_explanation_agent.py`, `tests/test_generate_explanation_agent_durability.py`, `tests/test_fraud_hold_workflow.py` (imports `_BASE_ACTIVITY_CONFIG`/`_MODEL_ACTIVITY_CONFIG` to build a matching test Agent).

**Data flow:** in — a prompt string built from `fraud_score`/`trigger_reason`, plus `deps=customer_id` (Section 2.7 shows exactly where these come from). Out — a full `InvestigationSummary` object, or an exception (`ActivityError` from an exhausted model/tool Activity, or `AgentRunError`/`UsageLimitExceeded` from the Agent's own loop) that the Workflow's fallback handles (Section 2.7, 4.5).

---

### 2.7 `app/workflows/fraud_hold_workflow.py` — **Workflow**

**Job:** This is the file that orchestrates everything — it's the "recipe" that decides, in order, which activities to call and when to pause. This is the most important file in the project, so we'll go through it carefully.

**What's a "Workflow," and how is it different from an "Activity"?** A Workflow is the *orchestration* logic — the sequence of decisions and activity calls. Temporal records everything a workflow does as a history of events on its own server. If the worker process that's running a workflow dies, a new worker can pick that history back up and continue exactly where it left off, by **replaying** the workflow's code against that recorded history: instead of actually re-executing already-completed activities, it just feeds back their already-recorded results instantly, fast-forwarding until it reaches the exact point where it left off — then continues normally from there. This is *why* the workflow's own code has a strict rule: it must be **deterministic** — given the same history, it must make the exact same decisions every time it's replayed. That's why anything unpredictable (calling an AI model, reading the current time, random numbers) is pushed out into an Activity instead, and the workflow only ever contains plain, predictable logic plus calls out to activities.

Let's look at the imports first:

```python
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
```

- **`timedelta`** — Python's standard-library way of representing a *duration* (as opposed to a specific point in time), e.g. `timedelta(hours=24)` or `timedelta(seconds=10)`.
- **`with workflow.unsafe.imports_passed_through():`** — this is a Temporal-specific detail, not a general Python concept. Because workflow code gets replayed (see above), Temporal normally restricts what a workflow file is allowed to import, to catch accidentally non-deterministic code early. The activity files we're importing here (especially `generate_explanation.py`, which pulls in an AI library) don't actually run *inside* the workflow's replayed logic — the workflow only needs to hold a *reference* to the function (or, for `_agent`, to the whole Agent object) so it can tell Temporal "go run this, elsewhere." This block tells Temporal "trust me, these imports are safe to pass through without the usual restrictions."
- **`with ... :`** itself is Python's **context manager** syntax — a way of saying "do some setup, run this block of code, then do some cleanup afterward, no matter what happens inside." We'll see a more hands-on example of this in Section 2.11, when tests use `async with`.
- **This file imports the whole `_agent` object, not an Activity function.** It imports `_agent` (and its `_AGENT_USAGE_LIMITS`) straight from `generate_explanation.py`, and calls `_agent.run(...)` directly, further down — the Agent itself is the thing that gets called, not a separately importable `generate_explanation` function. `RetryPolicy` isn't imported here either: the Agent's own retry policies live where the Agent is defined (`_BASE_ACTIVITY_CONFIG`/`_MODEL_ACTIVITY_CONFIG` in `generate_explanation.py`, Section 2.6), not at each call site.
- **`PydanticAIWorkflow` and `AgentRunError`** — both new imports, both from PydanticAI's `pydantic_ai.durable_exec.temporal`/`pydantic_ai.exceptions` modules rather than from this project's own code. `PydanticAIWorkflow` is a base class the workflow now inherits from (see below); `AgentRunError` is a new exception type this workflow's fallback needs to catch, alongside the already-familiar `ActivityError`.

Now the workflow class itself:

```python
@workflow.defn
class FraudHoldWorkflow(PydanticAIWorkflow):
    __pydantic_ai_agents__ = [_agent]

    def __init__(self) -> None:
        self._response: CustomerResponse | None = None

    @workflow.signal
    def customer_responded(self, response: CustomerResponse) -> None:
        self._response = response
```

- **`@workflow.defn`** — same idea as `@activity.defn`, but registers this whole *class* as a workflow Temporal is allowed to run.
- **`class FraudHoldWorkflow(PydanticAIWorkflow):`** — the class now inherits from `PydanticAIWorkflow` instead of being a plain class. `PydanticAIWorkflow` is a small mixin PydanticAI provides that lets a Workflow class safely call an Agent's `.run(...)` method directly from its own code (you'll see that call further down) and cooperates with the Worker-side registration described in the next bullet.
- **`__pydantic_ai_agents__ = [_agent]`** — a class attribute (not inside `__init__`, so it belongs to the class itself, not to any one instance of it) listing every PydanticAI Agent this workflow uses. `app/worker.py`'s `PydanticAIPlugin` (Section 2.8) reads this exact list, at the moment the `Worker` object is constructed, to figure out which Temporal Activities it needs to auto-register for the Agent's model requests and tool calls — the same way `worker.py`'s own `activities=[...]` list registers `place_hold`, `notify_customer`, and so on, just driven by this attribute instead of being listed by hand.
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
```

- **`await _agent.run(...)` — called directly from Workflow code, no `workflow.execute_activity(...)` wrapper.** There's no activity function to name or pass here — `_agent` (imported straight from `generate_explanation.py`, Section 2.6) is called exactly the way any PydanticAI Agent is called, with a prompt, `deps=`, and `usage_limits=`. What makes this safe to call directly from Workflow code — normally reserved for pure, deterministic logic (see the "What's a Workflow" explanation above) — is the `capabilities=[TemporalDurability(...)]` set on `_agent` itself (Section 2.6) plus this class inheriting from `PydanticAIWorkflow` and declaring `__pydantic_ai_agents__`. Underneath, every model request and tool call this triggers still becomes its own real, tracked Temporal Activity — it just doesn't look like one at this call site the way `place_hold`/`notify_customer` do.
- **`try` / `except (ActivityError, AgentRunError):`** — this is Python's error-handling syntax, appearing here for the first time in this walkthrough (though it's used the same way at Section 2.9's `except RPCError:`, further down). Code inside `try:` runs normally; if it raises an *exception* (an error), instead of crashing the whole program, execution jumps straight to the matching `except` block. `except (a, b):` with a tuple catches *either* type. Two different types are caught here on purpose, because the Agent can now fail in two structurally different ways:
  - **`ActivityError`** — raised when one specific model-request or tool-call Activity exhausts its own 2 attempts (per `_MODEL_ACTIVITY_CONFIG`/`_BASE_ACTIVITY_CONFIG` in Section 2.6) — e.g. Ollama stays unreachable across both attempts.
  - **`AgentRunError`** (and its subclasses, like `UsageLimitExceeded` or `UnexpectedModelBehavior`) — raised *directly* by the Agent's own continuation loop running as part of this Workflow's code, not wrapped in an Activity failure at all. This happens, for example, if the Agent's `usage_limits` bound (Section 2.6) is exceeded by a runaway tool-calling loop, or if the model's final output still doesn't match `InvestigationSummary` after PydanticAI's own internal retries.

  Neither an unrelated Temporal infrastructure failure nor `asyncio.CancelledError` is caught here — only these two specific, expected Agent failure modes.
- **Why the fallback matters:** by this point, `place_hold` has already run — the transaction is genuinely on hold. If the Agent investigation fails even after its Activities' retries (say, Ollama isn't running, or the Agent hits its execution bound) and that error were allowed to crash the workflow, the transaction would be stuck on hold forever with no notification and no way to resolve it, short of someone manually intervening in Temporal. Catching the error and substituting a fixed, generic `InvestigationSummary` means the rest of the flow (notify, wait, resolve) still runs normally either way.
- Notice `investigation` ends up holding a real `InvestigationSummary` either way — either the Agent's real one (`result.output`), or this hand-built fallback — and the `notify_customer` call right after doesn't know or care which.
- **Where do the retry/timeout settings for the Agent's Activities live, if not here?** They live where the Agent itself is defined: `_MODEL_ACTIVITY_CONFIG`/`_BASE_ACTIVITY_CONFIG` in `generate_explanation.py` (Section 2.6). That's because there isn't one single Activity call site to attach them to — a single investigation can involve up to 6 model-request Activities and up to 4 tool-call Activities, each independently configured and independently retried. Section 4.2 walks through the worst-case timing math this produces.

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

**Calls out to:** `models.py` (for the data shapes), every activity file, and `generate_explanation.py`'s `_agent`/`_AGENT_USAGE_LIMITS` directly (not through an Activity wrapper). **Called by:** `worker.py` (registers it), `main.py` (starts and signals it), `send_signal.py` (signals it), the tests (runs it directly).

**Data flow:** in — a `Transaction` and a threshold number, given once at start. Out — a short string (`"no_hold_needed"`, `"released"`, `"blocked"`, or `"escalated_no_response"`) describing how the case ended.

---

### 2.8 `app/worker.py` — **Worker entrypoint**

**Job:** Connects to Temporal and starts the long-running process that actually executes `FraudHoldWorkflow` and all its activities.

```python
import asyncio
import socket

from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from app.activities.hold import block, escalate, place_hold, release
from app.activities.log_outcome import record_no_hold_outcome
from app.activities.notify import notify_customer
from app.config import settings
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

WORKER_IDENTITY = f"worker-{socket.gethostname()}"


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        data_converter=pydantic_data_converter,
        plugins=[PydanticAIPlugin()],
        identity=WORKER_IDENTITY,
    )
    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[FraudHoldWorkflow],
        activities=[
            record_no_hold_outcome,
            place_hold,
            release,
            block,
            escalate,
            notify_customer,
        ],
    )
    print(
        f"Worker started ({WORKER_IDENTITY}), polling task queue "
        f"'{settings.task_queue}'..."
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

**New concepts, explained:**

- **`Client.connect(...)`** — opens a connection to the Temporal *server* (a separate piece of software this project runs via Docker or `temporal server start-dev` — see `README.md`). Everything this project does with Temporal goes through a `Client` like this one.
- **`pydantic_data_converter`** — by default, Temporal's SDK only knows how to send plain, simple data types back and forth to its server. Since this whole project passes Pydantic models (`Transaction`, `InvestigationSummary`, `CustomerResponse`) as workflow/activity/signal arguments, every `Client` in this project passes this converter so Temporal knows how to properly turn those objects into JSON and back.
- **`plugins=[PydanticAIPlugin()]` — the piece that makes `__pydantic_ai_agents__` actually do something.** `PydanticAIPlugin`, imported from `pydantic_ai.durable_exec.temporal`, is a Temporal client/worker plugin. At the moment the `Worker` object below is constructed, it walks every workflow class listed in `workflows=[...]` (here, just `FraudHoldWorkflow`), reads each one's `__pydantic_ai_agents__` list (Section 2.7), and automatically registers the Temporal Activities each listed Agent needs — the model-request and tool-call Activities described in Section 2.6 — on this `Worker`. That's why `generate_explanation.py`'s Agent activities aren't in the `activities=[...]` list below the way `place_hold`/`notify_customer` are: this plugin adds them on its own, reading straight from the Workflow class rather than needing them listed here by hand. The plugin is passed to `Client.connect(...)`, not to `Worker(...)` directly — passing it in both places causes a duplicate-registration error.
- **`identity=WORKER_IDENTITY`, where `WORKER_IDENTITY = f"worker-{socket.gethostname()}"`** — an ordinary `Client.connect(...)` parameter, not specific to PydanticAI. Every Activity attempt this Worker executes gets tagged with this identity string, and it's visible in Temporal's Event History (the "Identity" field on each `ActivityTaskStarted` event) and in the Temporal Web UI. `socket.gethostname()` — a standard-library call — returns this container's hostname; Docker assigns each container a distinct hostname by default, so when this project is run with `docker compose up --scale worker=2` (README's "Demo C"), the two Worker processes end up with two visibly different identities in the same workflow's Event History, without any custom instrumentation. This is the mechanism that makes it possible to tell, after the fact, *which* Worker replica actually executed a given step — including the Agent's own internal model-request/tool-call Activities, which this project's own code never directly logs a line for.
- **`Worker(client, task_queue=..., workflows=[...], activities=[...])`** — this is the object that does the actual work: it repeatedly asks the Temporal server "anything for me to run?" on the given `task_queue`, and when the answer is "run `FraudHoldWorkflow`" or "run `place_hold`," it's this `Worker` object that executes the matching registered function/class — plus, now, whatever Agent activities `PydanticAIPlugin` auto-registered above.
- **`await worker.run()`** — starts that polling loop, and doesn't return until the worker is stopped (e.g. `Ctrl+C`). This is why this process just sits there printing nothing further, once started — it's waiting for work.
- **`if __name__ == "__main__":`** — a very common Python idiom, appearing here for the first time. `__name__` is a special variable Python sets automatically; it equals `"__main__"` only when this file is the one you *ran directly* (e.g. `python -m app.worker`), and something else when this file is merely *imported* by another file. This line means "only actually start the worker if someone ran this file directly — don't start it just because some other file imported something from it." `asyncio.run(main())` is the standard way to kick off an `async def` function from ordinary (non-async) code — every `async` chain in this project ultimately starts from a call like this one.

**Calls out to:** every activity file, `fraud_hold_workflow.py`, `config.py`, PydanticAI's `durable_exec.temporal` module.
**Called by:** nobody (in Python terms) — it's started directly as a process (`python -m app.worker`, or via `docker-compose.yml`'s `worker` service, which can be scaled to run several of these processes at once — see README's "Demo C").

---

### 2.9 `app/main.py` — **FastAPI endpoint**

**Job:** The HTTP-facing entry and exit points of the whole system — the only file that speaks the "web request" language.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
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
        settings.temporal_address,
        data_converter=pydantic_data_converter,
        plugins=[PydanticAIPlugin()],
    )
    yield


app = FastAPI(lifespan=lifespan)
```

**New concepts, explained:**

- **`FastAPI`** — the web framework this project uses to turn Python functions into HTTP endpoints. `app = FastAPI(...)` creates "the application" — the object that will receive and route incoming web requests.
- **`_client: Client | None = None`** at the top level (not inside a function) — a variable shared by the whole file, starting out empty. It gets filled in once, when the app starts.
- **`@asynccontextmanager`** and **`lifespan`** — FastAPI lets you register a function that runs setup code, then `yield`s (pauses, handing control to the running application), and would run cleanup code after that `yield` if there were any here. This is FastAPI's way of saying "connect to Temporal once, when the app starts, and keep that one connection alive for as long as the app runs" — rather than reconnecting on every single request.
- **`global _client`** — inside a function, Python normally treats `_client = ...` as creating a brand-new *local* variable, even if a variable of that name already exists outside the function. `global _client` tells Python "no, I mean the one defined at the top of the file" — so this line actually updates the shared variable every other function in this file reads from.
- **`plugins=[PydanticAIPlugin()]`** — the same plugin `worker.py` passes to *its* `Client.connect(...)` call (Section 2.8). Every Temporal `Client` this project constructs — the API's, the worker's, `send_signal.py`'s — passes this plugin now, consistently, wherever a `Client` interacts with `FraudHoldWorkflow` (starting it, signaling it, or — only for the worker's `Client` — actually executing it). Note this `Client` never itself runs any Activity; only `worker.py`'s `Worker` does that. Passing the plugin here keeps this `Client`'s configuration matched to the worker's rather than drifting apart, which is the safer default PydanticAI's own docs recommend for any Client touching a Workflow that uses `TemporalDurability`.

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
    try:
        await handle.signal(FraudHoldWorkflow.customer_responded, response)
    except RPCError:
        # Signaling a transaction_id with no matching workflow (unknown or
        # already-completed) raises here rather than returning a normal
        # response -- surface it as a clean 404 instead of an unhandled 500.
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

from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from app.config import settings
from app.models import CustomerResponse
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow


async def send_signal(transaction_id: str, response: str) -> None:
    client = await Client.connect(
        settings.temporal_address,
        data_converter=pydantic_data_converter,
        plugins=[PydanticAIPlugin()],
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
- Everything else here — `Client.connect` (now with `plugins=[PydanticAIPlugin()]`, same as `main.py`'s client — see Section 2.9), `get_workflow_handle`, `.signal(...)`, `if __name__ == "__main__":` — is the exact same pattern already introduced in `worker.py` and `main.py`. The core logic — connect, get a handle by ID, signal it — is functionally identical to what `respond_to_transaction` does in `main.py`, just reached from a terminal instead of an HTTP request.

**Calls out to:** `config.py`, `models.py`, `fraud_hold_workflow.py`.
**Called by:** nobody — run directly from a terminal.

---

### 2.11 `tests/test_fraud_hold_workflow.py` — **Test**

**Job:** Five automated tests that exercise `FraudHoldWorkflow` end-to-end, without needing a real Temporal server, a real AI/Ollama, or Docker. (The full suite is 15 tests across three files — Section 2.12 covers seven that test the Agent directly, Section 2.13 covers three fine-grained-durability proofs.)

**Why does this file replace the whole `_agent` object rather than faking a single named Activity?** The real `_agent.run(...)` (Section 2.7) generates a *variable* number of model-request and tool-call Activities, named after the Agent itself — there's no single Activity name a test could register a fake function under, the way `place_hold` or `notify_customer` can be faked by name. So instead, these tests swap out the whole `_agent` object the Workflow calls — using pytest's `monkeypatch` fixture — for a test-only Agent built the same way (`TemporalDurability`, same activity configs) but with a scripted `FunctionModel` instead of a real Ollama-backed one. Getting this working reliably required one extra, non-obvious piece, explained below: `workflow_runner=UnsandboxedWorkflowRunner()`.

```python
import uuid
from collections.abc import Callable

import pytest
from pydantic_ai import Agent
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin, TemporalDurability
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from app.activities.generate_explanation import _BASE_ACTIVITY_CONFIG, _MODEL_ACTIVITY_CONFIG
from app.models import CustomerResponse, InvestigationSummary, Transaction
from app.workflows import fraud_hold_workflow as fraud_hold_workflow_module
from app.workflows.fraud_hold_workflow import FraudHoldWorkflow

FRAUD_SCORE_THRESHOLD = 70


def make_test_agent(calls: list[str], fail: bool = False) -> Agent:
    def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append("agent_investigation")
        if fail:
            raise RuntimeError("simulated Ollama outage")
        output_tool = info.output_tools[0]
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=output_tool.name,
                    args={
                        "customer_explanation": "test explanation",
                        "ops_summary": "test ops summary",
                        "notification_type": "sms",
                    },
                )
            ]
        )

    return Agent(
        FunctionModel(scripted),
        name="test_fraud_hold_investigator",
        deps_type=str,
        output_type=InvestigationSummary,
        system_prompt="test",
        capabilities=[
            TemporalDurability(
                activity_config=_BASE_ACTIVITY_CONFIG,
                model_activity_config=_MODEL_ACTIVITY_CONFIG,
            )
        ],
    )
```

**New concepts, explained:**

- **What is "mocking," and why replace the real Agent with a fake one?** A *mock* is a fake, simplified stand-in for a piece of real code, used in tests so you can check "did my logic call the right things, in the right order, with the right data?" without actually doing the real (slow, unpredictable, or expensive) thing. Here, the real `_agent` talks to Ollama — far too slow and non-deterministic for a fast automated test. `make_test_agent` builds a *complete replacement* Agent — its own `FunctionModel`, its own `TemporalDurability` capabilities (reusing the real `_BASE_ACTIVITY_CONFIG`/`_MODEL_ACTIVITY_CONFIG` from `generate_explanation.py` rather than restating them, so the timing behavior under test matches production) — that always goes straight to a scripted final answer, appending `"agent_investigation"` to the shared `calls` list so the test can check *when* it ran relative to the other activities.
- **`FunctionModel(scripted)`** — the same deterministic stand-in model used in Section 2.12's Agent-level tests, here driving a whole test-only `Agent`, not just standing in for the production one via `.override(...)`. `scripted` is a plain function that receives the conversation so far (`messages`) and info about the run (`info`, including `info.output_tools`) and returns a `ModelResponse` — here, always a `ToolCallPart` invoking the Agent's structured-output tool directly, skipping any read-only tool calls (these tests are about Workflow orchestration — did `place_hold` run before the Agent, did the fallback kick in — not about the Agent's own tool-calling behavior, which Sections 2.12 and 2.13 already cover).
- **`fail: bool = False`** — a function parameter with a default value. Most tests call `make_test_agent(calls)` (the AI mock succeeds normally); one test (the AI-failure test) calls `make_test_agent(calls, fail=True)` so the scripted model always raises instead, simulating an Ollama outage.
- **`Callable` (from `collections.abc`)** — a type hint meaning "any function," used elsewhere in this file for the plain (non-Agent) mock activities. `Callable` used to live in `typing`; as of Python 3.9+, the standard-library convention moved these container/callable type hints to `collections.abc`, and `typing.Callable` is now just a deprecated alias for the same thing — `ruff` flags the old import path and rewrites it to this one.

Now, how each test actually wires this test Agent into a real Workflow run:

```python
@pytest.mark.asyncio
async def test_it_was_me_releases(monkeypatch):
    calls, notify_messages, activities = make_mock_activities()
    test_agent = make_test_agent(calls)
    monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)
    monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])

    task_queue = str(uuid.uuid4())
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter, plugins=[PydanticAIPlugin()]
    ) as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[FraudHoldWorkflow],
            activities=activities,
            # Test-only: required so pytest monkeypatch of the production Agent is visible
            # to Workflow execution. Production Worker remains sandboxed; replay/determinism
            # is validated separately using the normal Temporal runner.
            workflow_runner=UnsandboxedWorkflowRunner(),
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
    assert calls.index("place_hold") < calls.index("agent_investigation")
```

- **`monkeypatch` (a pytest fixture, passed as a test parameter)** — pytest's built-in tool for temporarily replacing an attribute (on a module, a class, an object) for the duration of one test, and automatically restoring the original value afterward — even if the test fails partway through. `pytest.fixture` in general is pytest's mechanism for handing a test some prepared resource as a function parameter (you'll see `monkeypatch` show up as a plain parameter name, and pytest supplies it); this project doesn't define any of its own fixtures, but uses pytest's built-in ones.
- **`monkeypatch.setattr(fraud_hold_workflow_module, "_agent", test_agent)`** — replaces the `_agent` name inside the `app.workflows.fraud_hold_workflow` module (imported here as `fraud_hold_workflow_module` specifically so this line can reach into it) with the test Agent. This is the exact binding `FraudHoldWorkflow.run` reads when it calls `await _agent.run(...)` (Section 2.7) — so, for the rest of this test, that call runs against the scripted test Agent instead of the real Ollama-backed one.
- **`monkeypatch.setattr(FraudHoldWorkflow, "__pydantic_ai_agents__", [test_agent])`** — separately patches the class attribute `PydanticAIPlugin` reads (Section 2.7, 2.8) to decide which Agent's Activities to register on a `Worker`. Both patches are needed: one controls what the Workflow code actually *calls*, the other controls what the test `Worker`, built a few lines below, actually *registers*.
- **`workflow_runner=UnsandboxedWorkflowRunner()` — why this is required, not optional.** Temporal's *default* workflow runner (`SandboxedWorkflowRunner`) re-imports every workflow-defining module into its own isolated copy, specifically to catch accidentally non-deterministic workflow code early (this is the same sandboxing mechanism behind `workflow.unsafe.imports_passed_through()` in Section 2.7). The problem: `monkeypatch.setattr(fraud_hold_workflow_module, ...)` above patches the *outer*, test-process copy of that module — but the sandboxed runner's `FraudHoldWorkflow.run` executes against its *own*, separately re-imported copy, which never sees the patch. Without `UnsandboxedWorkflowRunner()`, these tests fail with a `NotFoundError` naming the *production* Agent's Activity names (`agent__fraud_hold_investigator__model_request`), proving the Workflow was still calling the real, unpatched `_agent` even though the patch had definitely been applied. Passing `workflow_runner=UnsandboxedWorkflowRunner()` makes this test `Worker` run workflow code in-process, in the same Python module instances the test itself patched — so the monkeypatch is visible where it needs to be. Production `app/worker.py` (Section 2.8) is never touched by this — it stays on the normal sandboxed runner. Separately, `tests/test_generate_explanation_agent_durability.py`'s `test_replay_determinism` (Section 2.13) validates that the *production* Workflow shape still replays safely under the normal sandboxed runner — this file's use of `UnsandboxedWorkflowRunner()` is a test-only convenience for reaching a monkeypatched Agent, not a weakening of that separate safety check. See `AGENTS.md` rule 10 for why a future cleanup shouldn't remove this.
- **`plugins=[PydanticAIPlugin()]` on `WorkflowEnvironment.start_time_skipping(...)`** — the test environment's own internal `Client` needs this plugin too, for the same reason `main.py`'s and `worker.py`'s do (Sections 2.8–2.9) — anywhere a `Client` interacts with a Workflow using `TemporalDurability`.
- **`calls.index("place_hold") < calls.index("agent_investigation")`** — this replaces the old `calls.index("place_hold") < calls.index("generate_explanation")` assertion. Same idea (the hold must be placed before the AI investigation runs — Section 4.5/`AGENTS.md` rule 2), just checking for the new marker string the scripted model appends instead of the name of a since-removed Activity function.

**Calls out to:** `fraud_hold_workflow.py`, `generate_explanation.py` (for the real activity configs), `models.py` — this file exercises the *real* production `FraudHoldWorkflow`, with the Agent it calls monkeypatched and the plain activities faked.
**Called by:** nobody — run via `pytest`.

---

### 2.12 `tests/test_generate_explanation_agent.py` — **Test**

**Job:** Seven tests that exercise the Agent inside `generate_explanation.py` directly — calling `_agent.run(...)` itself, no Workflow, no Worker, no Temporal server involved at all — using PydanticAI's own deterministic test models instead of a real Ollama.

**Why does this need a different approach than Section 2.11's tests?** Section 2.11's tests prove the *Workflow* behaves correctly by replacing the whole Agent with a scripted stand-in reached through a real (test) Temporal Worker. That's the right tool for testing orchestration (does the Workflow call things in the right order, does it fall back correctly), but it can't easily tell you much about the Agent's *internal* behavior — whether it actually registers its tools, whether a tool's real return value actually reaches the final answer, whether its execution bound actually stops a runaway loop — without a lot of Temporal test-harness overhead for each case. For that, these tests exercise the real `_agent` object from `generate_explanation.py` directly, outside any Workflow, with its language model swapped out for something deterministic — the simplest, fastest layer for testing the Agent's own logic in isolation.

```python
import pytest
from pydantic_ai.models.test import TestModel

from app.activities.generate_explanation import _AGENT_USAGE_LIMITS, _agent


async def _run_agent(fraud_score: float, trigger_reason: str, customer_id: str) -> InvestigationSummary:
    result = await _agent.run(
        f"fraud_score={fraud_score}, trigger_reason={trigger_reason}. "
        "Gather any useful context via your tools, then write the "
        "customer_explanation, ops_summary, and notification_type.",
        deps=customer_id,
        usage_limits=_AGENT_USAGE_LIMITS,
    )
    return result.output


@pytest.mark.asyncio
async def test_agent_can_invoke_both_read_only_tools():
    # TestModel's default call_tools="all" calls every registered tool once
    # before producing a schema-valid InvestigationSummary -- this proves
    # both tools are actually registered on the Agent and callable, and that
    # the Agent's output still satisfies the InvestigationSummary schema
    # once tools are in the loop.
    with _agent.override(model=TestModel()):
        result = await _run_agent(85, "UNUSUAL_LOCATION", "CUST-101")
    assert result.notification_type in {"sms", "email", "push"}
    assert isinstance(result.customer_explanation, str)
    assert isinstance(result.ops_summary, str)
```

- **`_run_agent(...)` — a small test-local helper, not part of the production code.** There's no importable Activity function that builds the exact prompt `fraud_hold_workflow.py` uses and calls `_agent.run(...)` the same way — the Workflow calls `_agent.run(...)` inline, in its own code (Section 2.7), not through a separately importable function. So this file defines `_run_agent` as its own small helper that reconstructs that same call — deliberately mirroring the real call in `fraud_hold_workflow.py`'s `run` method (Section 2.7), including passing the real, imported `_AGENT_USAGE_LIMITS` rather than a test-local copy, so these tests exercise the Agent under the same bound production actually uses. It returns `InvestigationSummary` directly (`result.output`), not the full `AgentRunResult` PydanticAI's `.run(...)` returns, matching what every test in this file actually wants to assert against.
- **`_agent.override(model=...)`** — a context manager PydanticAI provides specifically for testing: inside the `with` block, `_agent` behaves exactly as it does in production (same tools, same `system_prompt`, same `output_type`, same `usage_limits` passed at call time) *except* its language model is temporarily replaced. Outside the block, `_agent` goes back to using the real Ollama-backed model. **This only works reliably outside a Temporal Workflow, which is exactly the situation here** — `_run_agent` calls `_agent.run(...)` as a plain, ordinary Python call, not from inside Workflow code, so `.override(...)`'s usual context-local behavior applies cleanly. (Section 2.11's tests need the Agent swapped out *inside* a real Workflow execution instead, which is why they use `monkeypatch` on the whole `_agent` binding rather than `.override(...)` — `.override(...)`'s temporary substitution doesn't reliably cross the Temporal Activity boundary `TemporalDurability` introduces, which is exactly why the fine-grained-durability test file, Section 2.13, and Section 2.11's tests exist as two different, deliberately chosen testing strategies for two different layers.)
- **`TestModel()`** — one of two deterministic stand-in "models" used here. By default, it calls *every* tool registered on the Agent once, then invents a schema-valid dummy value for each output field. That's enough to prove the tools are genuinely registered and callable, and that a schema-valid `InvestigationSummary` comes back — but its dummy values don't depend on what the tools actually returned, so it can't prove tool *results* influenced the answer.
- **`FunctionModel(...)`** — the other stand-in, used in the other five tests. Instead of picking tool calls and output automatically, you hand it a plain Python function that receives the conversation so far and decides what the "model" does next — call a specific tool, or produce final output. This is what lets a test script an exact scenario: "call `lookup_customer_channel_preference`, read its *real* return value back out of the conversation, then use that real value when producing the final `InvestigationSummary`" — which is the only way to actually prove tool-result influence rather than just asserting a hardcoded expectation.
- **Reading a tool's real result back out of the conversation** — when a tool runs, PydanticAI appends a `ToolReturnPart` (from `pydantic_ai.messages`) holding its return value to the message history before asking the "model" what to do next. A `FunctionModel` function can inspect that history directly, which is exactly how `test_real_tool_return_value_influences_final_output` proves the real `lookup_customer_channel_preference` tool's answer (`"email"`) — not a value the test invented — ends up as `result.notification_type`.
- **Proving tool data is genuinely scoped by customer, not just registered** — `test_tool_results_are_scoped_by_customer_id_via_deps_only` calls `_run_agent` twice with the same scripted `FunctionModel`, once for `customer_id="CUST-101"` and once for `"CUST-202"`, and asserts the two calls get different real `notification_type` values back (`"email"` vs. `"push"`, per `_MOCK_CHANNEL_PREFERENCES` in Section 2.6). It also asserts the tool's JSON schema has no `customer_id` property — proving the model itself never sees or supplies it, so it can't ask about a customer other than the one this run was actually called for.
- **Proving the invalid-output case fails loudly** — one test scripts a `FunctionModel` that always tries to return an out-of-range `notification_type` (`"carrier_pigeon"`, not `sms`/`email`/`push`). PydanticAI's own output validation retries once against the model, and since this scripted model never corrects itself, the run ultimately raises `pydantic_ai.exceptions.UnexpectedModelBehavior` — proving bad structured output can never quietly become a "successful" result.
- **Proving the loop is bounded** — another test scripts a `FunctionModel` that always calls a tool again and never produces final output (a pathological loop). It asserts `pydantic_ai.exceptions.UsageLimitExceeded` is raised, using the *real*, imported `_AGENT_USAGE_LIMITS` from `generate_explanation.py` rather than reimplementing the bound in the test — so this test breaks loudly if someone changes the real limit without updating the test alongside it. (Section 2.13's fine-grained-durability tests prove something related but distinct: that this same limit is still enforced correctly *through* a real Temporal Workflow/Activity boundary, not just in a plain, un-durable call like this one.)
- **An honest limitation, worth stating plainly:** the "no raw tool data leaks into `customer_explanation`" test scripts a *compliant* `FunctionModel` response (a clean summary, the behavior the system prompt asks for) and asserts the raw tool payload doesn't appear in it. That's a real regression test — it would catch a future code change that, say, dumped a tool's raw return value straight into `customer_explanation` — but it cannot prove a live LLM will always follow the system prompt's instruction, because in this test *the test itself* controls what the scripted model returns. Proving that against a real, non-scripted model is an evaluation concern, genuinely out of scope for this demo.

**Calls out to:** `app/activities/generate_explanation.py` — this file exercises the *real* Agent and tools directly, in-process, with no Temporal Workflow involved.
**Called by:** nobody — run via `pytest`.

---

### 2.13 `tests/test_generate_explanation_agent_durability.py` — **Test**

**Job:** Three tests proving the fine-grained durability claims from Section 2.6 are actually true, not just architecturally plausible — using a small, dedicated test-only Agent and Workflow, run through a real (test) Temporal `Worker`.

**Why does this need yet another, different testing approach from Sections 2.11 and 2.12?** Section 2.12's tests prove the Agent's *own* logic works (tools registered, results used, loop bounded) but never touch a real Temporal Activity boundary at all — `_agent.run(...)` there runs as a plain, non-durable Python call. Section 2.11's tests prove the *Workflow's* orchestration is correct, but they replace the Agent with one that never calls a tool and never fails mid-loop, so they can't observe anything about *fine-grained* durability specifically — whether a completed tool call really is skipped on a later retry, whether `UsageLimitExceeded` genuinely still propagates correctly once it has to cross a real Activity boundary, whether the whole thing replays safely. This file is the one place in the test suite that deliberately drives a real Temporal `Worker`, `WorkflowEnvironment`, and Agent activities together, specifically to make those fine-grained claims fall out of an *observed* run rather than an inferred one.

```python
_current_script: list = [None]


def _delegating_model_fn(messages, info) -> ModelResponse:
    return _current_script[0](messages, info)


_test_agent = Agent(
    FunctionModel(_delegating_model_fn),
    name="durability_prototype_agent",
    deps_type=str,
    output_type=_Summary,
    system_prompt="test",
    capabilities=[TemporalDurability()],
)


@_test_agent.tool
async def lookup(ctx: RunContext[str]) -> str:
    _TOOL_CALL_LOG.append(f"lookup({ctx.deps})")
    return f"data-for-{ctx.deps}"


@workflow.defn
class _DurabilityTestWorkflow(PydanticAIWorkflow):
    __pydantic_ai_agents__ = [_test_agent]

    @workflow.run
    async def run(self, customer_id: str) -> str:
        result = await _test_agent.run(
            "investigate", deps=customer_id,
            usage_limits=UsageLimits(request_limit=6, tool_calls_limit=4),
        )
        return result.output.notification_type
```

**New concepts, explained:**

- **`_current_script: list = [None]`, and the "delegating model" pattern** — this file needs a *different* scripted response per test (one test needs "tool call succeeds, then the next model turn fails once"; another needs "always call the tool again, forever"), but the Agent driving all of them, `_test_agent`, is built once at module level — the same "build the Agent once, share it everywhere" pattern from Section 2.6's `_agent`. `_current_script` is a one-element list used purely as a mutable box: each test reassigns `_current_script[0]` to its own scripted function *before* running its scenario, and `_delegating_model_fn` — the actual function wired into `FunctionModel` — just calls whatever function currently sits in that box. A plain module-level variable would work almost the same way; a one-element list is used here mainly to make "this is intentionally mutable, shared state, reassigned per test" visually explicit at the point of reassignment. This works safely because Activity code (unlike Workflow code — see Section 2.11's `UnsandboxedWorkflowRunner()` explanation) is never re-imported into a separate sandboxed copy, so a test's reassignment in the outer process is exactly what the Activity execution sees.
- **`TemporalDurability()` with no arguments** — unlike `generate_explanation.py`'s real `_agent` (Section 2.6), this test Agent doesn't pass `activity_config=`/`model_activity_config=`, so it falls back to `TemporalDurability`'s own defaults. That's fine here: these tests aren't validating specific timeout/retry numbers (Section 2.11's tests already exercise the real, production-matching configs for that), they're validating the *behavioral* claims — does a retry skip an already-completed step, does a limit-exceeded error propagate, does replay work — which hold regardless of the exact timeout values in play.
- **`_DurabilityTestWorkflow` — a small, dedicated test-only Workflow, unlike Section 2.11's approach.** Section 2.11 goes out of its way to test the *real*, production `FraudHoldWorkflow` (via monkeypatch) rather than a parallel test Workflow, specifically so its tests can't drift from what production code actually does. This file takes a different, equally deliberate choice: since it's proving something about PydanticAI's `TemporalDurability` mechanism itself — not about `FraudHoldWorkflow`'s own orchestration logic — a minimal, purpose-built Workflow (`_DurabilityTestWorkflow`) keeps each test focused on exactly the fine-grained-durability behavior being proved, without unrelated hold/notify/wait-condition noise. Both choices are documented, deliberate testing strategies for two different things being tested — not an inconsistency.

Here's the first test, proving the headline fine-grained-durability claim:

```python
@pytest.mark.asyncio
async def test_completed_tool_activity_is_not_reexecuted_on_later_failure():
    ...
    # scripted: turn 1 calls the tool; turn 2 fails once, then succeeds
    ...
    assert _TOOL_CALL_LOG == ["lookup(CUST-101)"]
    tool_schedules = count_activity_task_scheduled(history, "...call_tool")
    model_schedules = count_activity_task_scheduled(history, "...model_request")
    assert tool_schedules == 1
    assert model_schedules == 2
```

- **Scripting a mid-loop failure, then checking two different kinds of evidence** — the scripted script has the model call `lookup` on its first turn, then deliberately fail (raise an exception) on its *second* turn once, succeeding only on the retry. This test checks two independent kinds of evidence that the already-completed `lookup` call is *not* re-invoked when that later model-request Activity fails and retries: `_TOOL_CALL_LOG == ["lookup(CUST-101)"]` (the tool's own Python-level call count — a plain list a real invocation appends to, checked exactly, not just "at least once"), *and* `handle.fetch_history_events(...)` — a direct query against the Temporal server's own recorded Event History (Section 4.1) — counted for `ActivityTaskScheduled` events per Activity type name. Checking both matters: the first proves the tool genuinely wasn't re-invoked at the Python level; the second proves that's not a coincidence of this particular mock, but is exactly what Temporal itself records as having happened — one tool-call Activity scheduled ever, two model-request Activities scheduled (the failing one and its successful retry).
- **`WorkflowHandle.fetch_history()` / `fetch_history_events(...)`** — methods on a workflow handle that query the Temporal server directly for everything it has durably recorded about a given workflow execution — the same Event History mechanism Section 4.1 describes powering crash recovery and replay. This is a stronger, more direct kind of proof than only checking application-level side effects (like `_TOOL_CALL_LOG`) — it's asking Temporal's own server-side bookkeeping "what actually got scheduled," independent of anything this test's own Python code tracked.

The second test proves the Agent's `UsageLimits` bound (Section 2.6) still works correctly once crossing a real Activity boundary:

- **Scripting an infinite tool-call loop, and catching the failure on the *Workflow* side** — the scripted model always calls `lookup` again, never producing final output, the same pathological scenario Section 2.12 tests in a plain, non-durable call. Run through `_DurabilityTestWorkflow` instead, the resulting `UsageLimitExceeded` has to cross from inside the Workflow's `_test_agent.run(...)` call, through Temporal's own workflow-failure machinery, out to whatever is awaiting `handle.result()` in the test. What actually lands there is not the original Python `UsageLimitExceeded` instance — `await handle.result()` raises `WorkflowFailureError` (or similar), and `str(exc_info.value)` is only the generic `"Workflow execution failed"`. The real information survives one level deeper, on `cause = getattr(exc_info.value, "cause", None)` — a Temporal `ApplicationError` carrying the original exception's type name and message as text. The test asserts `"UsageLimitExceeded" in str(cause)` and `"tool_calls_limit" in str(cause)` — checking the *substance* of the original failure survived the trip across the boundary, even though its exact Python type didn't.
- **Why check `cause`, not just that *some* error was raised?** A test that only asserted "an exception happened" wouldn't distinguish this specific, expected failure (the loop being correctly bounded) from some unrelated bug also happening to crash the workflow. Digging into `cause`'s text is what makes this a precise regression test for the right failure mode, not a vague one.

The third test validates replay safety for this fine-grained shape specifically:

- **`Replayer(workflows=[_DurabilityTestWorkflow], plugins=[PydanticAIPlugin()])`** — `temporalio.worker.Replayer` is Temporal's own tool for re-running a workflow's code against a previously recorded Event History (`history = await handle.fetch_history()`) and confirming it produces the exact same sequence of commands the original execution did — a direct, automated check of the determinism requirement Section 4.1 describes conceptually. `test_replay_determinism` runs `_DurabilityTestWorkflow` once for real, fetches its history, then calls `await replayer.replay_workflow(history)` and asserts it completes without raising. Just like `Worker` and `WorkflowEnvironment` elsewhere in this project, `Replayer` also needs `plugins=[PydanticAIPlugin()]` passed directly to it — without it, replaying a workflow that uses `TemporalDurability` fails with the same kind of sandbox-restriction error `Worker`/`WorkflowEnvironment` would raise if the plugin were missing there.
- **Why does this matter specifically for the fine-grained shape, beyond "replay should always work"?** A single investigation can produce a *variable* number of model-request and tool-call events, depending on how many turns the Agent actually took. This test is the concrete proof that this variable-shaped history replays cleanly, rather than something merely assumed to be true.

**Calls out to:** `pydantic_ai.durable_exec.temporal` (`PydanticAIWorkflow`, `PydanticAIPlugin`), `temporalio.worker.Replayer` — this file is deliberately self-contained rather than importing `FraudHoldWorkflow` or the real `_agent`, since it's testing PydanticAI's Temporal mechanism itself, not this project's specific Workflow.
**Called by:** nobody — run via `pytest`.

---

### 2.14 The `__init__.py` files

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
7. The workflow calls `await _agent.run(...)` directly (**`app/workflows/fraud_hold_workflow.py`**, Section 2.7) — no separate Activity function to hand off to first. PydanticAI's `TemporalDurability` capability (Section 2.6) turns this into a sequence of individually tracked Temporal Activities: one `agent__fraud_hold_investigator__model_request` Activity for each turn the model takes, and one `agent__fraud_hold_investigator__toolset__<agent>__call_tool` Activity for each of `lookup_recent_transactions`/`lookup_customer_channel_preference` the Agent decides to call — the worker executes each of these as they're scheduled, same as any other Activity, just automatically registered rather than listed by hand in `worker.py`. The Agent's own loop (how many turns, which tools, in what order) is its own decision, bounded by `_AGENT_USAGE_LIMITS`, before it produces a real `InvestigationSummary`.
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

**How it works, conceptually:** every meaningful step a workflow takes (each activity call starting, each activity call finishing, each signal arriving) gets permanently recorded by the Temporal *server* as an **event history** — independent of whatever worker process happens to be running at the time. If a worker process dies mid-workflow (say, right after `place_hold` but partway through the Agent investigation), nothing is lost, because that worker process was never the thing holding the workflow's state — the server was. When a (possibly brand new) worker picks the workflow back up, Temporal has it **replay**: it re-runs the workflow's own code from the very beginning, but instead of actually re-executing already-completed activities, it just feeds back their already-recorded results instantly, fast-forwarding until it reaches the exact point where it left off — then continues normally from there. This is only safe because the workflow's own code (outside of activity calls) is required to be deterministic — see the threshold-check comment in `fraud_hold_workflow.py`, which explains this exact reasoning.

**Fine-grained recovery:** because each model request and each tool call is its own Activity (Section 2.6), a worker restart can land *in the middle* of an investigation — say, after `lookup_recent_transactions` succeeded but before the next model turn completed — and replay handles this at that same fine grain: the completed tool-call Activity's result is fed back instantly from history, and only the interrupted model-request Activity actually re-executes. `tests/test_generate_explanation_agent_durability.py`'s `test_completed_tool_activity_is_not_reexecuted_on_later_failure` (Section 2.13) is the automated proof of exactly this. `test_replay_determinism` in the same file is the automated proof that this variable-length history shape replays cleanly.

### 4.2 Retries

**Where:** `generate_explanation.py`'s `_BASE_ACTIVITY_CONFIG` and `_MODEL_ACTIVITY_CONFIG` (Section 2.6), passed into `_agent`'s `capabilities=[TemporalDurability(activity_config=..., model_activity_config=...)]`.

**What it does:** if a model-request or tool-call Activity fails, Temporal automatically retries it (up to 2 attempts total per Activity, at least 1 second apart) *without any retry loop written in this project's own code* — each individual model request or tool call is retried independently, not the whole investigation at once (Section 4.1 above). Every other activity call in `fraud_hold_workflow.py` (`place_hold`, `notify_customer`, and the rest) has no explicit `retry_policy=`, which means it falls back to Temporal's default Activity retry policy rather than any specific fixed number of attempts. The Agent's model/tool Activities get a deliberately explicit policy because Temporal's own *default* retry policy has `maximum_attempts=0` — meaning *unlimited* retries — which would leave the whole Agent investigation's wall-clock time completely unbounded if left unset; in a real, production system, every activity would generally want its own deliberately chosen retry limit, timeout, and idempotency behavior, rather than being left on Temporal's defaults by accident, but this demo mostly leans on the defaults for everything except the Agent's activities, for simplicity.

**A concrete example of why this matters:** `_MODEL_ACTIVITY_CONFIG`'s `start_to_close_timeout` is 60 seconds per model-request Activity, `_BASE_ACTIVITY_CONFIG`'s is 10 seconds per tool-call Activity — both deliberately tied to real numbers, not picked out of thin air. Real, forced-cold-start local measurements against `qwen3.5:latest` (this project's configured default — see `.env.example`), using the actual system prompt and tool schemas, showed 13.6–17.1 seconds worst observed — so 60 seconds gives roughly 3.5x headroom over what was actually measured, not just a guess. Multiplied out against `_AGENT_USAGE_LIMITS` (Section 2.6: up to 6 model requests, up to 4 tool calls, each up to 2 attempts with a 1-second backoff), the worst-case Agent-phase budget comes to 6 × (60 + 1 + 60) + 4 × (10 + 1 + 10) = 726 + 84 = **810 seconds**, kept under this project's 900-second ceiling for the whole investigation. If you raise the Agent's `request_limit`/`tool_calls_limit` or change which Ollama model is configured, this calculation needs re-checking against the new worst case — see `AGENTS.md` rule 6. This is exactly the kind of thing Temporal's Event History makes visible that a normal application log might not: an `ActivityTaskStarted` event with `Attempt: 2`, and a `Last Failure` panel showing `"timeoutType": "TIMEOUT_TYPE_START_TO_CLOSE"` from attempt 1, tells you precisely what happened and when, for *each* individual model-request or tool-call Activity — open any workflow's Event History in the Temporal Web UI (`http://localhost:8233`) and look for the same pattern if you want to see it directly.

### 4.3 Signals

**Where:** the `@workflow.signal` decorator on `customer_responded` in `fraud_hold_workflow.py`, and the two places that trigger it — `handle.signal(...)` in `app/main.py`'s `respond_to_transaction`, and the identical call in `scripts/send_signal.py`.

**What it does:** a Signal is how something *outside* a running workflow — here, an HTTP request or a CLI script — delivers a message *into* it asynchronously, without needing to know or care whether the workflow happens to be actively running or durably paused at that exact moment. Temporal handles the delivery either way.

### 4.4 Timeout

**Where:** `fraud_hold_workflow.py`, the `timeout=timedelta(hours=24)` argument to `workflow.wait_condition(...)`, and the paired `except TimeoutError:` block right after it.

**What it does:** bounds how long the workflow will wait for a Signal before giving up and moving on (to `escalate`) on its own. Note this is a completely different mechanism from the `RetryPolicy` above — this isn't retrying anything, it's a maximum wait duration on a pause.

### 4.5 Fallback messaging

**Where:** `fraud_hold_workflow.py`, the `try:` / `except (ActivityError, AgentRunError):` wrapped directly around the `await _agent.run(...)` call.

**What it does:** turns "the Agent investigation failed even after every retry" — whether that's Ollama being unreachable across a model-request Activity's retries, the model never producing valid structured output, or the Agent's `usage_limits` bound being exceeded (Section 2.6) — from a workflow-ending crash into a handled, expected case. The workflow substitutes a fixed `InvestigationSummary` and continues exactly as if the Agent had succeeded, all the way through to the final resolution. See `tests/test_fraud_hold_workflow.py`'s `test_ai_failure_falls_back_and_still_resolves` (Section 5.7) for the automated proof that this actually works.

### 4.6 Idempotency

**Where:** `app/main.py`, the `id=transaction.transaction_id` argument to `start_workflow`, combined with `id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE`, and the `except WorkflowAlreadyStartedError:` block right after.

**What it does:** using the transaction's own ID as the workflow's ID means Temporal itself becomes the mechanism that prevents duplicate processing — if the same transaction is submitted twice (say, a retried webhook from the fraud engine), the *second* `start_workflow` call fails with `WorkflowAlreadyStartedError`, whether the first workflow is still running (Temporal's default behavior already covers this case) or has already finished (which specifically needs `REJECT_DUPLICATE` to catch, since the default would otherwise allow starting a fresh execution reusing that same ID). Either way, `main.py` catches that error and returns the existing workflow's ID instead of silently starting a second, duplicate execution.

### 4.7 Multiple Workers and Worker identity

**Where:** `app/worker.py`'s `WORKER_IDENTITY = f"worker-{socket.gethostname()}"` and `identity=WORKER_IDENTITY` on `Client.connect(...)` (Section 2.8); `docker-compose.yml`'s `worker` service, deliberately built with no `container_name` and no fixed host port so it can be scaled.

**What it does:** this project's durability guarantee — a killed worker doesn't lose progress — holds even with only *one* worker process (a restarted single worker picks its own old work back up, as Sections 4.1 and 4.2 describe). Running `docker compose up --scale worker=2` goes one step further: two independent worker processes poll the *same* task queue at once, and Temporal itself decides which one picks up which piece of work, including retrying a step on whichever worker is available if the one that started it is killed mid-Activity. `WORKER_IDENTITY` is what makes this observable rather than just theoretically true — Docker gives each scaled replica a distinct container hostname by default, so each worker's `Client.connect(identity=...)` ends up visibly different, and every Activity attempt any worker executes is tagged with its identity in Temporal's Event History (the "Identity" field on `ActivityTaskStarted` events) — including the Agent's own model-request/tool-call Activities, which this project's own Python code never writes a log line for. `README.md`'s "Demo C" is the guided, manual walkthrough of killing one replica mid-investigation (using the `DEMO_FAILOVER_DELAY_SECONDS` hook from Section 2.6) and confirming, from the Event History, that a *different* worker identity picked up the retry.

**An honest scope note, carried over from `README.md`:** the reproducible interruption point Demo C actually uses is a tool-call Activity (`lookup_customer_channel_preference`), not a model-request Activity specifically — delaying a model-request Activity on demand would require patching PydanticAI's own internal activity functions, which is out of scope (private API, not something this project's code should reach into). The failover behavior being demonstrated — a different worker identity resuming an in-flight Agent investigation after one worker is killed — is the same either way; only which *specific* step gets artificially slowed down for the demo differs from the most literal reading of "delay the model call."

---

## 5. The Tests, Explained

This project's test suite is 15 tests across three files, each testing a different layer: `tests/test_fraud_hold_workflow.py` (5 tests — the real `FraudHoldWorkflow`'s orchestration, Agent monkeypatched), `tests/test_generate_explanation_agent.py` (7 tests — the real Agent's own logic, no Workflow involved), and `tests/test_generate_explanation_agent_durability.py` (3 tests — the fine-grained `TemporalDurability` mechanism itself, through a dedicated test Workflow).

### 5.1 What is mocking, and why do these tests replace real code with fake stand-ins?

(Covered in detail in Sections 2.11–2.13.) A mock is a fake stand-in for real code, used so a test can check "did the right things happen, in the right order?" without doing the real, slow, or unpredictable work. `test_fraud_hold_workflow.py` builds its own set of fake plain activities via `make_mock_activities()` (each just recording its own name into a shared `calls` list) *and* a fake Agent via `make_test_agent()`, monkeypatched in to replace the real one for the duration of each test.

### 5.2 What is `WorkflowEnvironment.start_time_skipping`, and why is it needed?

(Also covered in Section 2.11.) It's a temporary, isolated Temporal test environment with time-skipping enabled — a *simulated* clock that automatically fast-forwards whenever a test is just waiting for a result. Without it, `test_timeout_escalates` (below) would need to actually wait 24 real hours to pass.

Now, the five Workflow-level tests themselves (Section 5.8 covers the seven Agent-level tests, Section 5.9 covers the three fine-grained-durability tests):

### 5.3 `test_below_threshold_records_no_hold_outcome`

**Scenario:** a `Transaction` with `fraud_score=50` (below the test's threshold of 70) is run through the workflow, with no signal ever sent.

**Asserts:** the workflow's result is `"no_hold_needed"`, and `calls` equals *exactly* `["record_no_hold_outcome"]` — not just "contains" it, but the *entire* list of everything that ran, nothing more.

**What it proves:** the below-threshold branch genuinely skips `place_hold`, the Agent investigation, and `notify_customer` entirely — this is a strong assertion, since checking the full list rules out any activity firing that shouldn't.

### 5.4 `test_it_was_me_releases`

**Scenario:** a `Transaction` with `fraud_score=90` (above threshold) is started, then immediately signaled with `CustomerResponse(response="it_was_me")`.

**Asserts:** the result is `"released"`; `"release"` ran but `"block"` and `"escalate"` did not; and — importantly — `calls.index("place_hold") < calls.index("agent_investigation")`, proving `place_hold` genuinely ran *before* the Agent investigation, not just that both ran.

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

**Scenario:** `make_test_agent(calls, fail=True)` — the test Agent's scripted model raises `RuntimeError` on *every* call, simulating Ollama being completely unreachable even after retries. The transaction is above threshold, and gets signaled with `"it_was_me"`.

**Asserts:** the result is still `"released"`; `"agent_investigation"` did run (it wasn't skipped — it was attempted and failed); `"notify_customer"` still ran; and, most specifically, the actual message captured in `notify_messages` is **not** the AI mock's normal success text (`"test explanation"`) and **does** contain `"temporarily paused"` — the exact fallback wording from `fraud_hold_workflow.py`.

**What it proves:** this is the automated proof for Section 4.5 — that a total, repeated AI failure doesn't crash the workflow or leave the hold stuck, and that the customer genuinely receives the fallback message (not just that *some* message was sent). Note this fails via `ActivityError` here (the scripted model always raises inside its own model-request Activity, exhausting its retries) — Section 5.9's `test_agent_loop_is_bounded_through_the_durable_boundary` is the companion proof for the *other* caught exception type, `AgentRunError`.

### 5.8 `tests/test_generate_explanation_agent.py` — the seven Agent-level tests

(Full walkthrough in Section 2.12; this is a quick-reference summary.) These seven call `_run_agent(...)` (a small test helper that calls `_agent.run(...)` the same way `fraud_hold_workflow.py` does), swapping `_agent`'s model for `TestModel` or `FunctionModel` via `_agent.override(...)` — no Workflow, no Worker, no Ollama, no Temporal server at all.

| Test | What it proves |
|---|---|
| `test_agent_can_invoke_both_read_only_tools` | Both tools are genuinely registered and callable, and output stays schema-valid with tools in the loop. |
| `test_real_tool_return_value_influences_final_output` | A tool's *actual* return value (not a value the test invented) flows into the final `InvestigationSummary` — the one thing `TestModel` alone can't prove. |
| `test_tool_results_are_scoped_by_customer_id_via_deps_only` | Two different `customer_id` values genuinely get different scoped tool results, and the tool's schema has no `customer_id` parameter — the model can't ask about a different customer than the one this run was called for. |
| `test_invalid_notification_type_is_rejected_not_silently_accepted` | An out-of-range `notification_type` can never quietly succeed — PydanticAI's output validation retries, then raises. |
| `test_customer_explanation_does_not_leak_raw_tool_data` | A compliant response keeps raw tool payload out of `customer_explanation` while allowing it in `ops_summary` — a regression guard and contract statement, not proof of live-model behavior (see Section 2.12's honesty note about this). |
| `test_agent_loop_is_bounded` | A pathological, always-call-another-tool loop is actually stopped by the real, imported `_AGENT_USAGE_LIMITS` — not an unbounded loop — outside a Workflow. |
| `test_tools_are_importable_and_registered` | The tool functions referenced throughout the tests and docs are the same ones actually registered on `_agent`. |

### 5.9 `tests/test_generate_explanation_agent_durability.py` — the three fine-grained-durability tests

(Full walkthrough in Section 2.13; this is a quick-reference summary.) These three run a dedicated test-only Agent and Workflow through a real (test) Temporal `Worker`, specifically to prove PydanticAI's `TemporalDurability` mechanism itself behaves correctly — not `FraudHoldWorkflow`'s own orchestration, which Section 5.3–5.7 already cover.

| Test | What it proves |
|---|---|
| `test_completed_tool_activity_is_not_reexecuted_on_later_failure` | The headline fine-grained-durability claim: when a model-request Activity fails and retries, an already-completed tool-call Activity is *not* re-executed — verified both via a Python-level call counter and via Temporal's own Event History (`ActivityTaskScheduled` counts). |
| `test_agent_loop_is_bounded_through_the_durable_boundary` | The Agent's `UsageLimits` bound still stops a pathological loop when running through real Temporal Activities, and the resulting `UsageLimitExceeded` genuinely propagates out to the workflow's caller (as a Temporal `ApplicationError` cause) rather than being silently swallowed or retried forever. |
| `test_replay_determinism` | A completed run's real Event History replays cleanly via `temporalio.worker.Replayer` — proving the variable-length (model-request/tool-call) history shape doesn't introduce nondeterminism, verified directly rather than assumed. |

---

## If you want to read this in stages

Everything in this guide is worth reading eventually — nothing here is filler. But if a full first pass feels like a lot in one sitting, here's one way to split it into a couple of visits rather than trying to absorb it all at once:

- **A first pass**, to get oriented: Section 1 (the big picture), the quick-reference table right after it, and Section 2's entries for `models.py`, the activity files (2.3–2.6), and `fraud_hold_workflow.py` (2.7) — that's enough to understand what the system does and how the core orchestration is written.
- **A second pass**, to see it all connect: the rest of Section 2 (`worker.py`, `main.py`, `send_signal.py`, the tests), then Section 3's two traced-through scenarios — by this point the file-by-file pieces should click into a single mental model of a request's full journey.
- **A third pass**, to consolidate: Section 4 (where each durability concept actually lives in the code) and Section 5 (what each test proves) — these two sections mostly point back at code you've already read, tying it to the specific Temporal concepts and correctness guarantees it demonstrates.

Come back to any earlier section as needed — the file-by-file walkthrough in particular is meant to double as a reference, not just a one-time read.
