import asyncio

from temporalio import activity


@activity.defn
async def notify_customer(customer_id: str, message: str, notification_type: str) -> None:
    print(f"[notify] sending {notification_type} to {customer_id}: {message}")
    await asyncio.sleep(1)
