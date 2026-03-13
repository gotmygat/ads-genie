from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from memory.decision_memory import DecisionMemory
from orchestration.models.decision_log import DecisionLog


class MonthlyClientReport(BaseModel):
    customer_id: str
    action_count: int
    approved_actions: int
    dismissed_actions: int
    key_learnings: list[str] = Field(default_factory=list)


def generate_monthly_client_report(customer_id: str) -> MonthlyClientReport:
    decision_memory = DecisionMemory()
    context = decision_memory.get_context_for_account(customer_id, lookback_days=30)
    approved_actions = len([item for item in context.recent_actions if item])
    dismissed_actions = context.repeated_dismissals
    key_learnings = context.successful_patterns[:5] + context.constraints[:3] + context.seasonal_notes[:3]
    return MonthlyClientReport(
        customer_id=customer_id,
        action_count=context.decision_count,
        approved_actions=approved_actions,
        dismissed_actions=dismissed_actions,
        key_learnings=key_learnings[:10],
    )
