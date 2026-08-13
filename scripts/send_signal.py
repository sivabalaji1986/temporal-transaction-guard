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
