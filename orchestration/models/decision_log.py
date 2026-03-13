from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4
from typing import Any
import json
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]


class DecisionLog:
    def __init__(
        self,
        table_name: str | None = None,
        bucket_name: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_DECISIONS_TABLE", "")
        self.bucket_name = bucket_name or os.getenv("S3_AUDIT_BUCKET", "")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        resource = boto3.resource("dynamodb", region_name=self.region_name) if boto3 is not None else None
        self.table = resource.Table(self.table_name) if resource is not None and self.table_name else None
        self.s3 = boto3.client("s3", region_name=self.region_name) if boto3 is not None and self.bucket_name else None

    def write(self, record: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        payload = dict(record)
        payload.setdefault("decision_id", str(uuid4()))
        payload.setdefault("timestamp", now.isoformat())

        if self.table is not None:
            self.table.put_item(Item=payload)

        if self.s3 is not None:
            key = f"decision-log/{payload['customer_id']}/{payload['decision_id']}.json"
            kwargs = {
                "Bucket": self.bucket_name,
                "Key": key,
                "Body": json.dumps(payload).encode("utf-8"),
                "ContentType": "application/json",
            }
            retain_until = now + timedelta(days=365)
            kwargs["ObjectLockMode"] = "COMPLIANCE"
            kwargs["ObjectLockRetainUntilDate"] = retain_until
            self.s3.put_object(**kwargs)

        return payload
