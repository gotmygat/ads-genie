from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from pydantic import BaseModel, Field


class AccountContext(BaseModel):
    customer_id: str
    lookback_days: int
    decision_count: int
    recent_actions: list[str] = Field(default_factory=list)
    repeated_dismissals: int = 0
    successful_patterns: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    seasonal_notes: list[str] = Field(default_factory=list)


class DecisionMemory:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_DECISIONS_TABLE", "")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self._memory: list[dict[str, Any]] = []
        resource = boto3.resource("dynamodb", region_name=self.region_name) if boto3 is not None and self.table_name else None
        self.table = resource.Table(self.table_name) if resource is not None else None

    def _load_records(self, customer_id: str, lookback_days: int) -> list[dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        if self.table is None:
            records = [record for record in self._memory if record.get("customer_id") == customer_id]
        else:
            records = [item for item in self.table.scan().get("Items", []) if item.get("customer_id") == customer_id]
        filtered: list[dict[str, Any]] = []
        for record in records:
            timestamp = str(record.get("timestamp", ""))
            try:
                if datetime.fromisoformat(timestamp) >= cutoff:
                    filtered.append(record)
            except ValueError:
                continue
        return filtered

    def get_context_for_account(self, customer_id: str, lookback_days: int = 90) -> AccountContext:
        records = self._load_records(customer_id, lookback_days)
        recent_actions = [str(record.get("action_type", "unknown")) for record in records[-10:]]
        repeated_dismissals = sum(1 for record in records if record.get("human_decision") == "dismissed")
        successful_patterns = [
            f"{record.get('action_type')} -> {record.get('execution_result')}"
            for record in records
            if record.get("human_decision") in {"approved", "auto_executed"}
            and record.get("execution_result")
        ][:5]
        constraints = [
            str(record.get("modified_action", {}).get("instructions"))
            for record in records
            if isinstance(record.get("modified_action"), dict)
            and record.get("modified_action", {}).get("instructions")
        ][:5]
        seasonal_notes = [
            str(record.get("tool_call_outputs", {}).get("seasonal_note"))
            for record in records
            if isinstance(record.get("tool_call_outputs"), dict)
            and record.get("tool_call_outputs", {}).get("seasonal_note")
        ][:5]
        return AccountContext(
            customer_id=customer_id,
            lookback_days=lookback_days,
            decision_count=len(records),
            recent_actions=recent_actions,
            repeated_dismissals=repeated_dismissals,
            successful_patterns=successful_patterns,
            constraints=constraints,
            seasonal_notes=seasonal_notes,
        )
