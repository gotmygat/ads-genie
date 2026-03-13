from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]


class MCCAggregator:
    def __init__(self, region_name: str | None = None) -> None:
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        resource = boto3.resource("dynamodb", region_name=self.region_name) if boto3 is not None else None
        self.benchmarks_table = (
            resource.Table(os.getenv("DYNAMODB_MCC_BENCHMARKS_TABLE", "ads_genie_mcc_benchmarks")) if resource is not None else None
        )
        self.negatives_table = (
            resource.Table(os.getenv("DYNAMODB_MCC_NEGATIVES_TABLE", "ads_genie_mcc_negatives")) if resource is not None else None
        )

    def aggregate_benchmarks(self, snapshots: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        for snapshot in snapshots:
            vertical = str(snapshot.get("vertical", "unknown"))
            grouped[vertical]["roas"].append(float(snapshot.get("roas", 0.0)))
            grouped[vertical]["cpa"].append(float(snapshot.get("cpa", 0.0)))
            grouped[vertical]["ctr"].append(float(snapshot.get("ctr", 0.0)))
        aggregated: dict[str, dict[str, float]] = {}
        for vertical, metrics in grouped.items():
            aggregated[vertical] = {
                "account_count": float(len(metrics["roas"])),
                "avg_roas": round(mean(metrics["roas"]), 4) if metrics["roas"] else 0.0,
                "avg_cpa": round(mean(metrics["cpa"]), 4) if metrics["cpa"] else 0.0,
                "avg_ctr": round(mean(metrics["ctr"]), 4) if metrics["ctr"] else 0.0,
            }
            if self.benchmarks_table is not None:
                self.benchmarks_table.put_item(Item={"vertical": vertical, **aggregated[vertical]})
        return aggregated

    def aggregate_confirmed_negatives(self, records: list[dict[str, Any]]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        for record in records:
            if str(record.get("action_type")) != "add_negative_keywords":
                continue
            vertical = str(record.get("vertical", "unknown"))
            terms = record.get("recommended_action", {}).get("negative_keywords", []) if isinstance(record.get("recommended_action"), dict) else []
            grouped[vertical].extend(str(item.get("term", "")) for item in terms if item.get("term"))
        deduped = {vertical: sorted(set(values)) for vertical, values in grouped.items()}
        if self.negatives_table is not None:
            for vertical, negatives in deduped.items():
                self.negatives_table.put_item(Item={"vertical": vertical, "negative_terms": negatives})
        return deduped
