from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class AccountRecord:
    customer_id: str
    account_name: str
    vertical: str
    slack_channel_id: str
    autonomy_config_id: str
    is_active: bool
    mcc_id: str
    time_zone: str
    created_at: str
    updated_at: str


class AccountRegistry:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_ACCOUNTS_TABLE", "")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.table = (
            boto3.resource("dynamodb", region_name=self.region_name).Table(self.table_name)
            if self.table_name and boto3 is not None
            else None
        )

    def get_account(self, customer_id: str) -> dict[str, Any] | None:
        if self.table is None:
            return None
        return self.table.get_item(Key={"customer_id": customer_id}).get("Item")

    def list_active_accounts(self) -> list[dict[str, Any]]:
        if self.table is None:
            return []
        items = self.table.scan().get("Items", [])
        return [item for item in items if item.get("is_active", True)]

    def verify_channel_for_account(self, customer_id: str, channel_id: str) -> bool:
        account = self.get_account(customer_id)
        if not account:
            return False
        return str(account.get("slack_channel_id", "")) == str(channel_id)
