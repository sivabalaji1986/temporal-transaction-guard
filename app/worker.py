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
