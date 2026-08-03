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
