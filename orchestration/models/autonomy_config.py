from __future__ import annotations

from typing import Any
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]

from orchestration.models.autonomy_levels import LEVEL_ORDER, normalize_autonomy_level


class AutonomyPolicyViolation(RuntimeError):
    pass


class AutonomyConfig:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None:
        self.table_name = table_name or os.getenv("DYNAMODB_AUTONOMY_TABLE", "")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.table = (
            boto3.resource("dynamodb", region_name=self.region_name).Table(self.table_name)
            if self.table_name and boto3 is not None
            else None
        )

    def get_config(self, config_id: str) -> dict[str, Any]:
        if self.table is None:
            return {}
        return self.table.get_item(Key={"config_id": config_id}).get("Item", {})

    def get_action_policy(self, config_id: str, action_type: str) -> dict[str, Any]:
        config = self.get_config(config_id)
        return dict(config.get(action_type, {}))

    def validate_action(
        self,
        config_id: str,
        action_type: str,
        minimum_level: str,
        requested_change_pct: float | None = None,
    ) -> dict[str, Any]:
        policy = self.get_action_policy(config_id, action_type)
        current_level = normalize_autonomy_level(policy.get("level", "escalate"))
        required_level = normalize_autonomy_level(minimum_level)
        policy["level"] = current_level
        if LEVEL_ORDER.get(current_level, 99) < LEVEL_ORDER.get(required_level, 99):
            raise AutonomyPolicyViolation(
                f"Action '{action_type}' configured as '{current_level}', below required minimum '{required_level}'."
            )
        if requested_change_pct is not None and "max_change_pct" in policy:
            requested_change_pct = max(-float(policy["max_change_pct"]), min(float(policy["max_change_pct"]), requested_change_pct))
            policy["clamped_change_pct"] = requested_change_pct
        return policy
