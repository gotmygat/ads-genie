from __future__ import annotations

from typing import Any

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.write_actions import (
    WriteActionResult,
    append_update_mask,
    enum_member,
    extract_operation_ids,
    make_ad_group_resource_name,
    resolve_policy,
    write_post_execution_log,
    write_pre_execution_log,
)
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.autonomy_config import AutonomyConfig
from orchestration.models.decision_log import DecisionLog


def enable_ad_group(
    customer_id: str,
    ad_group_id: str,
    *,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
    registry: AccountRegistry | None = None,
    autonomy: AutonomyConfig | None = None,
    decision_log: DecisionLog | None = None,
    step_functions_execution_id: str | None = None,
) -> WriteActionResult:
    """
    Re-enables an ad group under the same safety controls as pause_ad_group: account registry
    lookup, autonomy enforcement, validate_only preview, and immutable execution logging.
    """

    inputs = {"customer_id": customer_id, "ad_group_id": ad_group_id, "target_status": "ENABLED"}
    _account, policy, _registry, _autonomy = resolve_policy(
        customer_id=customer_id,
        action_type="pause_ad_group",
        minimum_level="propose_and_wait",
        registry=registry,
        autonomy=autonomy,
    )
    pre_log = write_pre_execution_log(
        customer_id=customer_id,
        action_type="enable_ad_group",
        inputs=inputs,
        policy=policy,
        decision_log=decision_log,
        step_functions_execution_id=step_functions_execution_id,
    )

    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)
    service = client.get_service("AdGroupService")
    operation = client.get_type("AdGroupOperation")
    operation.update.resource_name = make_ad_group_resource_name(service, customer_id, ad_group_id)
    operation.update.status = enum_member(client, "AdGroupStatusEnum", "ENABLED")
    append_update_mask(operation, "status")

    try:
        service.mutate_ad_groups(customer_id=customer_id, operations=[operation], validate_only=True)
        response = service.mutate_ad_groups(customer_id=customer_id, operations=[operation], validate_only=False)
        result = WriteActionResult(
            success=True,
            added_count=1,
            failed_count=0,
            google_ads_operation_ids=extract_operation_ids(response),
            action_type="enable_ad_group",
            target_resource=operation.update.resource_name,
            preview_validated=True,
        )
        write_post_execution_log(
            customer_id=customer_id,
            action_type="enable_ad_group",
            inputs=inputs,
            outputs=result.model_dump(mode="json"),
            policy=policy,
            decision_log=decision_log,
            step_functions_execution_id=step_functions_execution_id,
            decision_id=pre_log.get("decision_id"),
        )
        return result
    except Exception as exc:
        write_post_execution_log(
            customer_id=customer_id,
            action_type="enable_ad_group",
            inputs=inputs,
            outputs={"success": False, "error": str(exc)},
            policy=policy,
            decision_log=decision_log,
            step_functions_execution_id=step_functions_execution_id,
            decision_id=pre_log.get("decision_id"),
        )
        raise
