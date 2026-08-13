import asyncio
import socket

from temporalio import activity

_WORKER_HOSTNAME = socket.gethostname()


@activity.defn
async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
    print(f"[notify:{_WORKER_HOSTNAME}] sending {notification_type} to {customer_id}: {message}")
    await asyncio.sleep(1)
