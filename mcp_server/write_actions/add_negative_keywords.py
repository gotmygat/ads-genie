from __future__ import annotations

from typing import Any

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.write_actions import (
    WriteActionResult,
    enum_member,
    extract_operation_ids,
    make_campaign_resource_name,
    resolve_policy,
    write_post_execution_log,
    write_pre_execution_log,
)
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.autonomy_config import AutonomyConfig
from orchestration.models.decision_log import DecisionLog


def _campaign_negative_operations(client: Any, customer_id: str, campaign_id: str, negative_keywords: list[dict[str, Any]]) -> list[Any]:
    campaign_service = client.get_service("CampaignService")
    operations: list[Any] = []
    for keyword in negative_keywords:
        operation = client.get_type("CampaignCriterionOperation")
        criterion = operation.create
        criterion.campaign = make_campaign_resource_name(campaign_service, customer_id, campaign_id)
        criterion.negative = True
        criterion.keyword.text = str(keyword.get("term", "")).strip()
        criterion.keyword.match_type = enum_member(
            client,
            "KeywordMatchTypeEnum",
            str(keyword.get("match_type", "PHRASE")).upper(),
        )
        operations.append(operation)
    return operations


def _account_negative_operations(client: Any, negative_keywords: list[dict[str, Any]]) -> list[Any]:
    operations: list[Any] = []
    for keyword in negative_keywords:
        operation = client.get_type("CustomerNegativeCriterionOperation")
        criterion = operation.create
        criterion.keyword.text = str(keyword.get("term", "")).strip()
        criterion.keyword.match_type = enum_member(
            client,
            "KeywordMatchTypeEnum",
            str(keyword.get("match_type", "PHRASE")).upper(),
        )
        operations.append(operation)
    return operations


def _ad_group_negative_operations(client: Any, customer_id: str, ad_group_id: str, negative_keywords: list[dict[str, Any]]) -> list[Any]:
    ad_group_service = client.get_service("AdGroupService")
    operations: list[Any] = []
    for keyword in negative_keywords:
        operation = client.get_type("AdGroupCriterionOperation")
        criterion = operation.create
        criterion.ad_group = ad_group_service.ad_group_path(customer_id, ad_group_id)
        criterion.negative = True
        criterion.keyword.text = str(keyword.get("term", "")).strip()
        criterion.keyword.match_type = enum_member(
            client,
            "KeywordMatchTypeEnum",
            str(keyword.get("match_type", "PHRASE")).upper(),
        )
        operations.append(operation)
    return operations


def add_negative_keywords(
    customer_id: str,
    campaign_id: str,
    negative_keywords: list[dict[str, Any]],
    scope: str,
    *,
    ad_group_id: str | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
    registry: AccountRegistry | None = None,
    autonomy: AutonomyConfig | None = None,
    decision_log: DecisionLog | None = None,
    step_functions_execution_id: str | None = None,
) -> WriteActionResult:
    """
    Adds negative keywords only after account registry validation, autonomy policy checks,
    preview validation, and immutable before/after decision logging.
    """

    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"account", "campaign", "ad_group"}:
        raise ValueError("scope must be one of: account, campaign, ad_group")
    if normalized_scope == "ad_group" and not ad_group_id:
        raise ValueError("ad_group_id is required when scope='ad_group'")

    inputs = {
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "ad_group_id": ad_group_id,
        "negative_keywords": negative_keywords,
        "scope": normalized_scope,
    }
    _account, policy, _registry, _autonomy = resolve_policy(
        customer_id=customer_id,
        action_type="add_negative_keywords",
        minimum_level="auto_execute",
        registry=registry,
        autonomy=autonomy,
    )
    pre_log = write_pre_execution_log(
        customer_id=customer_id,
        action_type="add_negative_keywords",
        inputs=inputs,
        policy=policy,
        decision_log=decision_log,
        step_functions_execution_id=step_functions_execution_id,
    )

    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)

    if normalized_scope == "account":
        service = client.get_service("CustomerNegativeCriterionService")
        operations = _account_negative_operations(client, negative_keywords)
        execute = lambda validate_only: service.mutate_customer_negative_criteria(  # noqa: E731
            customer_id=customer_id,
            operations=operations,
            validate_only=validate_only,
        )
        target_resource = f"customers/{customer_id}"
    elif normalized_scope == "ad_group":
        service = client.get_service("AdGroupCriterionService")
        operations = _ad_group_negative_operations(client, customer_id, str(ad_group_id), negative_keywords)
        execute = lambda validate_only: service.mutate_ad_group_criteria(  # noqa: E731
            customer_id=customer_id,
            operations=operations,
            validate_only=validate_only,
        )
        target_resource = client.get_service("AdGroupService").ad_group_path(customer_id, str(ad_group_id))
    else:
        service = client.get_service("CampaignCriterionService")
        operations = _campaign_negative_operations(client, customer_id, campaign_id, negative_keywords)
        execute = lambda validate_only: service.mutate_campaign_criteria(  # noqa: E731
            customer_id=customer_id,
            operations=operations,
            validate_only=validate_only,
        )
        target_resource = make_campaign_resource_name(client.get_service("CampaignService"), customer_id, campaign_id)

    try:
        execute(True)
        response = execute(False)
        result = WriteActionResult(
            success=True,
            added_count=len(extract_operation_ids(response)) or len(negative_keywords),
            failed_count=max(0, len(negative_keywords) - len(extract_operation_ids(response))),
            google_ads_operation_ids=extract_operation_ids(response),
            action_type="add_negative_keywords",
            target_resource=target_resource,
            preview_validated=True,
            details={"scope": normalized_scope, "requested_terms": len(negative_keywords)},
        )
        write_post_execution_log(
            customer_id=customer_id,
            action_type="add_negative_keywords",
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
            action_type="add_negative_keywords",
            inputs=inputs,
            outputs={"success": False, "error": str(exc)},
            policy=policy,
            decision_log=decision_log,
            step_functions_execution_id=step_functions_execution_id,
            decision_id=pre_log.get("decision_id"),
        )
        raise
