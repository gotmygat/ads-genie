from __future__ import annotations

from typing import Any
import logging

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.write_actions import (
    WriteActionResult,
    append_update_mask,
    extract_operation_ids,
    make_ad_group_resource_name,
    resolve_policy,
    write_post_execution_log,
    write_pre_execution_log,
)
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.autonomy_config import AutonomyConfig
from orchestration.models.decision_log import DecisionLog


LOGGER = logging.getLogger(__name__)


def adjust_bids(
    customer_id: str,
    ad_group_id: str,
    requested_change_pct: float,
    current_cpc_bid_micros: int,
    *,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
    registry: AccountRegistry | None = None,
    autonomy: AutonomyConfig | None = None,
    decision_log: DecisionLog | None = None,
    step_functions_execution_id: str | None = None,
) -> WriteActionResult:
    """
    Adjusts ad-group bids with explicit max-change clamping from autonomy config, validate_only
    preview, and immutable before/after execution logging.
    """

    _account, policy, _registry, _autonomy = resolve_policy(
        customer_id=customer_id,
        action_type="adjust_bids",
        minimum_level="propose_and_wait",
        registry=registry,
        autonomy=autonomy,
        requested_change_pct=requested_change_pct,
    )
    applied_change_pct = float(policy.get("clamped_change_pct", requested_change_pct))
    if applied_change_pct != requested_change_pct:
        LOGGER.warning(
            "bid_change_clamped",
            extra={
                "customer_id": customer_id,
                "ad_group_id": ad_group_id,
                "requested_change_pct": requested_change_pct,
                "applied_change_pct": applied_change_pct,
            },
        )

    new_bid_micros = max(1, int(round(current_cpc_bid_micros * (1 + (applied_change_pct / 100.0)))))
    inputs = {
        "customer_id": customer_id,
        "ad_group_id": ad_group_id,
        "requested_change_pct": requested_change_pct,
        "applied_change_pct": applied_change_pct,
        "current_cpc_bid_micros": current_cpc_bid_micros,
        "new_cpc_bid_micros": new_bid_micros,
    }
    pre_log = write_pre_execution_log(
        customer_id=customer_id,
        action_type="adjust_bids",
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
    operation.update.cpc_bid_micros = new_bid_micros
    append_update_mask(operation, "cpc_bid_micros")

    try:
        service.mutate_ad_groups(customer_id=customer_id, operations=[operation], validate_only=True)
        response = service.mutate_ad_groups(customer_id=customer_id, operations=[operation], validate_only=False)
        result = WriteActionResult(
            success=True,
            added_count=1,
            failed_count=0,
            google_ads_operation_ids=extract_operation_ids(response),
            action_type="adjust_bids",
            target_resource=operation.update.resource_name,
            preview_validated=True,
            requested_change_pct=requested_change_pct,
            applied_change_pct=applied_change_pct,
            details={
                "current_cpc_bid_micros": current_cpc_bid_micros,
                "new_cpc_bid_micros": new_bid_micros,
            },
        )
        write_post_execution_log(
            customer_id=customer_id,
            action_type="adjust_bids",
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
            action_type="adjust_bids",
            inputs=inputs,
            outputs={"success": False, "error": str(exc)},
            policy=policy,
            decision_log=decision_log,
            step_functions_execution_id=step_functions_execution_id,
            decision_id=pre_log.get("decision_id"),
        )
        raise
