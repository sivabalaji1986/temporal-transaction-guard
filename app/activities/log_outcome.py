import asyncio
import socket

from temporalio import activity

_WORKER_HOSTNAME = socket.gethostname()


@activity.defn
async def record_no_hold_outcome(transaction_id: str, fraud_score: float) -> None:
    print(
        f"[log_outcome:{_WORKER_HOSTNAME}] {transaction_id}: fraud_score={fraud_score} "
        "below threshold, no hold placed"
    )
    await asyncio.sleep(1)
