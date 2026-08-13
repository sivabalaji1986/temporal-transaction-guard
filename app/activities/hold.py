import asyncio
import socket

from temporalio import activity

# Included in every print below so `docker compose logs -f worker` (with
# multiple Worker replicas, e.g. `--scale worker=2`) shows which container
# handled which call -- see README's "Demo C" (Worker-pod failover). Purely
# informational: reading the hostname here has no effect on Workflow
# determinism (this runs inside an Activity, never inside Workflow code).
_WORKER_HOSTNAME = socket.gethostname()


@activity.defn
async def place_hold(transaction_id: str) -> None:
    print(f"[hold:{_WORKER_HOSTNAME}] placing temporary hold on {transaction_id}")
    await asyncio.sleep(1)


@activity.defn
async def release(transaction_id: str) -> None:
    print(
        f"[hold:{_WORKER_HOSTNAME}] releasing hold on {transaction_id}: "
        "customer confirmed it was them"
    )
    await asyncio.sleep(1)


@activity.defn
async def block(transaction_id: str) -> None:
    print(f"[hold:{_WORKER_HOSTNAME}] blocking {transaction_id}: customer says this wasn't them")
    await asyncio.sleep(1)


@activity.defn
async def escalate(transaction_id: str) -> None:
    print(
        f"[hold:{_WORKER_HOSTNAME}] escalating {transaction_id} for manual "
        "review: no customer response"
    )
    await asyncio.sleep(1)
