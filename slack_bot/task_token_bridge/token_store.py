from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class TokenRecord:
    message_ts: str
    execution_arn: str
    task_token: str
    customer_id: str
    action_type: str
    expires_at: int


class TokenStore:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_TASK_TOKENS_TABLE", "ads_genie_task_tokens")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self._memory: dict[str, dict] = {}
        resource = boto3.resource("dynamodb", region_name=self.region_name) if boto3 is not None and self.table_name else None
        self.table = resource.Table(self.table_name) if resource is not None else None

    def store_token(
        self,
        message_ts: str,
        execution_arn: str,
        task_token: str,
        customer_id: str,
        action_type: str,
    ) -> TokenRecord:
        expires_at = int((datetime.now(timezone.utc) + timedelta(hours=25)).timestamp())
        item = {
            "message_ts": message_ts,
            "execution_arn": execution_arn,
            "task_token": task_token,
            "customer_id": customer_id,
            "action_type": action_type,
            "expires_at": expires_at,
        }
        if self.table is not None:
            self.table.put_item(Item=item)
        else:
            self._memory[message_ts] = item
        return TokenRecord(**item)

    def retrieve_token(self, message_ts: str) -> TokenRecord | None:
        item = None
        if self.table is not None:
            item = self.table.get_item(Key={"message_ts": message_ts}).get("Item")
        else:
            item = self._memory.get(message_ts)
        if not item:
            return None
        return TokenRecord(**item)
