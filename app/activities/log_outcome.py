import asyncio

from temporalio import activity


@activity.defn
async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
    print(
        f"[log_outcome] {transaction_id}: fraud_score={fraud_score} "
        "below threshold, no hold placed"
    )
    await asyncio.sleep(1)
