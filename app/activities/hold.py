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
