from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from mcp_server.auth.google_oauth import GoogleAdsAuth
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.autonomy_config import AutonomyConfig
from orchestration.models.decision_log import DecisionLog


class AccountNotFoundError(LookupError):
    pass


class WriteActionResult(BaseModel):
    success: bool
    added_count: int = 0
    failed_count: int = 0
    google_ads_operation_ids: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    action_type: str
    target_resource: str | None = None
    preview_validated: bool = False
    requested_change_pct: float | None = None
    applied_change_pct: float | None = None
    details: dict[str, Any] = Field(default_factory=dict)


def resolve_account(
    customer_id: str,
    registry: AccountRegistry | None = None,
) -> tuple[AccountRegistry, dict[str, Any]]:
    registry = registry or AccountRegistry()
    account = registry.get_account(customer_id)
    if not account:
        raise AccountNotFoundError(f"Customer '{customer_id}' was not found in ads_genie_accounts.")
    return registry, dict(account)


def resolve_policy(
    customer_id: str,
    action_type: str,
    minimum_level: str,
    registry: AccountRegistry | None = None,
    autonomy: AutonomyConfig | None = None,
    requested_change_pct: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any], AccountRegistry, AutonomyConfig]:
    registry, account = resolve_account(customer_id, registry=registry)
    autonomy = autonomy or AutonomyConfig()
    config_id = str(account.get("autonomy_config_id", "")).strip()
    policy = autonomy.validate_action(
        config_id=config_id,
        action_type=action_type,
        minimum_level=minimum_level,
        requested_change_pct=requested_change_pct,
    )
    return account, policy, registry, autonomy


def extract_operation_ids(response: Any) -> list[str]:
    results = getattr(response, "results", None)
    if results is None and isinstance(response, dict):
        results = response.get("results", [])
    operation_ids: list[str] = []
    for result in results or []:
        resource_name = getattr(result, "resource_name", None)
        if resource_name is None and isinstance(result, dict):
            resource_name = result.get("resource_name")
        if resource_name:
            operation_ids.append(str(resource_name))
    return operation_ids


def decision_state_from_policy(policy: dict[str, Any]) -> str:
    return "auto_executed" if str(policy.get("level", "")).strip() == "auto_execute" else "approved"


def write_pre_execution_log(
    *,
    customer_id: str,
    action_type: str,
    inputs: dict[str, Any],
    policy: dict[str, Any],
    decision_log: DecisionLog | None = None,
    step_functions_execution_id: str | None = None,
) -> dict[str, Any]:
    decision_log = decision_log or DecisionLog()
    return decision_log.write(
        {
            "customer_id": customer_id,
            "action_type": action_type,
            "recommended_action": inputs,
            "human_decision": decision_state_from_policy(policy),
            "execution_result": {"status": "pending"},
            "tool_call_inputs": inputs,
            "tool_call_outputs": {},
            "step_functions_execution_id": step_functions_execution_id,
        }
    )


def write_post_execution_log(
    *,
    customer_id: str,
    action_type: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    policy: dict[str, Any],
    decision_log: DecisionLog | None = None,
    step_functions_execution_id: str | None = None,
    decision_id: str | None = None,
) -> dict[str, Any]:
    decision_log = decision_log or DecisionLog()
    payload = {
        "customer_id": customer_id,
        "action_type": action_type,
        "recommended_action": inputs,
        "human_decision": decision_state_from_policy(policy),
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "execution_result": outputs,
        "tool_call_inputs": inputs,
        "tool_call_outputs": outputs,
        "step_functions_execution_id": step_functions_execution_id,
    }
    if decision_id:
        payload["decision_id"] = decision_id
    return decision_log.write(payload)


def enum_member(client: Any, enum_name: str, member_name: str) -> Any:
    container = getattr(client.enums, enum_name)
    if hasattr(container, member_name):
        return getattr(container, member_name)
    try:
        return container[member_name]
    except Exception:
        return member_name


def append_update_mask(operation: Any, field_path: str) -> None:
    paths = getattr(getattr(operation, "update_mask", None), "paths", None)
    if paths is not None:
        paths.append(field_path)


def make_ad_group_resource_name(service: Any, customer_id: str, ad_group_id: str) -> str:
    if hasattr(service, "ad_group_path"):
        return service.ad_group_path(customer_id, ad_group_id)
    return f"customers/{customer_id}/adGroups/{ad_group_id}"


def make_campaign_resource_name(service: Any, customer_id: str, campaign_id: str) -> str:
    if hasattr(service, "campaign_path"):
        return service.campaign_path(customer_id, campaign_id)
    return f"customers/{customer_id}/campaigns/{campaign_id}"


__all__ = [
    "AccountNotFoundError",
    "GoogleAdsAuth",
    "WriteActionResult",
    "append_update_mask",
    "decision_state_from_policy",
    "enum_member",
    "extract_operation_ids",
    "make_ad_group_resource_name",
    "make_campaign_resource_name",
    "resolve_account",
    "resolve_policy",
    "write_post_execution_log",
    "write_pre_execution_log",
]
