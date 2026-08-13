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

# Docker sets each container's hostname to its (short) container ID by
# default, so this gives every Activity attempt an identity that's visibly
# distinct per Worker replica in the Temporal Web UI's Event History (the
# "Identity" field on each ActivityTaskStarted event) -- including the
# Agent's own model-request/tool-call activities, which this project's own
# code never directly logs. Used by the two-Worker failover demo (README,
# "Demo C") to show which container handled which attempt; harmless and
# informative in the single-Worker case too.
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
        # generate_explanation's Agent activities (model requests, tool
        # calls) are NOT listed here -- PydanticAIPlugin auto-registers them
        # by reading FraudHoldWorkflow.__pydantic_ai_agents__.
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
