from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from typing import Any
from decimal import Decimal
import json
import os
import time

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class CacheHit:
    value: list[dict[str, Any]]
    hit: bool


class DynamoDBCache:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_CACHE_TABLE", "").strip()
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self._lock = RLock()
        self._memory_cache: dict[str, tuple[int, list[dict[str, Any]]]] = {}
        self._table = None
        if self.table_name and boto3 is not None:
            resource = boto3.resource("dynamodb", region_name=self.region_name)
            self._table = resource.Table(self.table_name)

    def _key(self, customer_id: str, query: str) -> str:
        digest = sha256(f"{customer_id}:{query}".encode("utf-8")).hexdigest()
        return digest

    def get(self, customer_id: str, query: str) -> CacheHit:
        key = self._key(customer_id, query)
        now_epoch = int(time.time())
        if self._table is None:
            cached = self._memory_cache.get(key)
            if not cached or cached[0] <= now_epoch:
                return CacheHit(value=[], hit=False)
            return CacheHit(value=cached[1], hit=True)

        item = self._table.get_item(Key={"cache_key": key}).get("Item")
        if not item or int(item.get("ttl_epoch", 0)) <= now_epoch:
            return CacheHit(value=[], hit=False)
        return CacheHit(value=item.get("payload", []), hit=True)

    def put(self, customer_id: str, query: str, payload: list[dict[str, Any]], ttl_seconds: int) -> None:
        key = self._key(customer_id, query)
        ttl_epoch = int(time.time()) + ttl_seconds
        if self._table is None:
            with self._lock:
                self._memory_cache[key] = (ttl_epoch, payload)
            return
        self._table.put_item(
            Item={
                "cache_key": key,
                "customer_id": customer_id,
                "query_hash": sha256(query.encode("utf-8")).hexdigest(),
                "payload": json.loads(json.dumps(payload), parse_float=Decimal),
                "ttl_epoch": ttl_epoch,
            }
        )
