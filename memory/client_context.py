from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from pydantic import BaseModel, Field


class ClientContextRecord(BaseModel):
    customer_id: str
    seasonal_patterns: list[str] = Field(default_factory=list)
    standing_constraints: list[str] = Field(default_factory=list)
    past_outcomes: list[str] = Field(default_factory=list)
    updated_at: str


class ClientContext:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_CACHE_TABLE", "")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self._memory: dict[str, dict[str, Any]] = {}
        resource = boto3.resource("dynamodb", region_name=self.region_name) if boto3 is not None and self.table_name else None
        self.table = resource.Table(self.table_name) if resource is not None else None

    def _key(self, customer_id: str) -> str:
        return f"client_context#{customer_id}"

    def get(self, customer_id: str) -> ClientContextRecord:
        if self.table is not None:
            item = self.table.get_item(Key={"cache_key": self._key(customer_id)}).get("Item") or {}
            payload = item.get("payload", {})
        else:
            payload = self._memory.get(customer_id, {})
        return ClientContextRecord(
            customer_id=customer_id,
            seasonal_patterns=list(payload.get("seasonal_patterns", [])),
            standing_constraints=list(payload.get("standing_constraints", [])),
            past_outcomes=list(payload.get("past_outcomes", [])),
            updated_at=str(payload.get("updated_at", datetime.now(timezone.utc).isoformat())),
        )

    def update_after_action(
        self,
        customer_id: str,
        *,
        seasonal_note: str | None = None,
        standing_constraint: str | None = None,
        past_outcome: str | None = None,
    ) -> ClientContextRecord:
        current = self.get(customer_id)
        if seasonal_note:
            current.seasonal_patterns = [seasonal_note, *current.seasonal_patterns][:10]
        if standing_constraint:
            current.standing_constraints = [standing_constraint, *current.standing_constraints][:10]
        if past_outcome:
            current.past_outcomes = [past_outcome, *current.past_outcomes][:10]
        current.updated_at = datetime.now(timezone.utc).isoformat()
        payload = current.model_dump()
        if self.table is not None:
            self.table.put_item(
                Item={
                    "cache_key": self._key(customer_id),
                    "customer_id": customer_id,
                    "query_hash": "client_context",
                    "payload": payload,
                    "ttl_epoch": int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp()),
                }
            )
        else:
            self._memory[customer_id] = payload
        return current
