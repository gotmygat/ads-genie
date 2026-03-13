from __future__ import annotations

from types import SimpleNamespace

import pytest

from mcp_server.write_actions.add_negative_keywords import add_negative_keywords
from mcp_server.write_actions.adjust_bids import adjust_bids
from mcp_server.write_actions.pause_ad_group import pause_ad_group
from orchestration.models.autonomy_config import AutonomyPolicyViolation


class FakeRegistry:
    def __init__(self, account: dict | None) -> None:
        self.account = account

    def get_account(self, customer_id: str):  # noqa: ANN001
        if self.account and self.account.get("customer_id") == customer_id:
            return self.account
        return None


class FakeAutonomy:
    def __init__(self, policies: dict[str, dict]) -> None:
        self.policies = policies

    def validate_action(self, config_id: str, action_type: str, minimum_level: str, requested_change_pct: float | None = None):
        del config_id
        order = {
            "auto_execute": 0,
            "propose_and_wait": 1,
            "draft_and_review": 2,
            "escalate": 3,
        }
        policy = dict(self.policies[action_type])
        if order[policy["level"]] < order[minimum_level]:
            raise AutonomyPolicyViolation("policy too permissive for requested action")
        if requested_change_pct is not None and "max_change_pct" in policy:
            cap = float(policy["max_change_pct"])
            policy["clamped_change_pct"] = max(-cap, min(cap, float(requested_change_pct)))
        return policy


class FakeDecisionLog:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def write(self, record: dict):
        payload = dict(record)
        payload.setdefault("decision_id", f"decision-{len(self.records)+1}")
        self.records.append(payload)
        return payload


class FakeOperation:
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.create = SimpleNamespace(keyword=SimpleNamespace())
        self.update = SimpleNamespace()
        self.update_mask = SimpleNamespace(paths=[])


class FakeMutationService:
    def __init__(self, path_builder=None) -> None:
        self.calls: list[dict] = []
        self._path_builder = path_builder

    def campaign_path(self, customer_id: str, campaign_id: str) -> str:
        return f"customers/{customer_id}/campaigns/{campaign_id}"

    def ad_group_path(self, customer_id: str, ad_group_id: str) -> str:
        return f"customers/{customer_id}/adGroups/{ad_group_id}"

    def mutate_campaign_criteria(self, **kwargs):
        self.calls.append({"method": "mutate_campaign_criteria", **kwargs})
        ops = kwargs["operations"]
        return SimpleNamespace(results=[SimpleNamespace(resource_name=f"campaign-op-{index+1}") for index, _ in enumerate(ops)])

    def mutate_customer_negative_criteria(self, **kwargs):
        self.calls.append({"method": "mutate_customer_negative_criteria", **kwargs})
        ops = kwargs["operations"]
        return SimpleNamespace(results=[SimpleNamespace(resource_name=f"customer-op-{index+1}") for index, _ in enumerate(ops)])

    def mutate_ad_group_criteria(self, **kwargs):
        self.calls.append({"method": "mutate_ad_group_criteria", **kwargs})
        ops = kwargs["operations"]
        return SimpleNamespace(results=[SimpleNamespace(resource_name=f"criterion-op-{index+1}") for index, _ in enumerate(ops)])

    def mutate_ad_groups(self, **kwargs):
        self.calls.append({"method": "mutate_ad_groups", **kwargs})
        ops = kwargs["operations"]
        return SimpleNamespace(results=[SimpleNamespace(resource_name=f"ad-group-op-{index+1}") for index, _ in enumerate(ops)])


class FakeClient:
    def __init__(self) -> None:
        self.services = {
            "CampaignService": FakeMutationService(),
            "CampaignCriterionService": FakeMutationService(),
            "CustomerNegativeCriterionService": FakeMutationService(),
            "AdGroupService": FakeMutationService(),
            "AdGroupCriterionService": FakeMutationService(),
        }
        self.enums = SimpleNamespace(
            KeywordMatchTypeEnum=SimpleNamespace(EXACT="EXACT", PHRASE="PHRASE", BROAD="BROAD"),
            AdGroupStatusEnum=SimpleNamespace(PAUSED="PAUSED", ENABLED="ENABLED"),
        )

    def get_service(self, name: str):
        return self.services[name]

    def get_type(self, name: str):
        return FakeOperation(name)


class FakeAuth:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def get_client(self, customer_id: str):  # noqa: ANN001
        del customer_id
        return self.client


@pytest.fixture
def account() -> dict:
    return {
        "customer_id": "123",
        "autonomy_config_id": "config-1",
    }


def test_add_negative_keywords_previews_executes_and_logs(account) -> None:
    client = FakeClient()
    decision_log = FakeDecisionLog()
    result = add_negative_keywords(
        customer_id="123",
        campaign_id="456",
        negative_keywords=[{"term": "free spa jobs", "match_type": "phrase"}],
        scope="campaign",
        auth=FakeAuth(client),
        registry=FakeRegistry(account),
        autonomy=FakeAutonomy({"add_negative_keywords": {"level": "auto_execute"}}),
        decision_log=decision_log,
    )
    mutate_calls = client.get_service("CampaignCriterionService").calls
    assert [call["validate_only"] for call in mutate_calls] == [True, False]
    assert result.success is True
    assert result.added_count == 1
    assert len(decision_log.records) == 2


def test_pause_ad_group_rejects_auto_execute_policy(account) -> None:
    with pytest.raises(AutonomyPolicyViolation):
        pause_ad_group(
            customer_id="123",
            ad_group_id="789",
            auth=FakeAuth(FakeClient()),
            registry=FakeRegistry(account),
            autonomy=FakeAutonomy({"pause_ad_group": {"level": "auto_execute"}}),
            decision_log=FakeDecisionLog(),
        )


def test_adjust_bids_clamps_to_policy_limit(account) -> None:
    client = FakeClient()
    decision_log = FakeDecisionLog()
    result = adjust_bids(
        customer_id="123",
        ad_group_id="789",
        requested_change_pct=25.0,
        current_cpc_bid_micros=1_000_000,
        auth=FakeAuth(client),
        registry=FakeRegistry(account),
        autonomy=FakeAutonomy({"adjust_bids": {"level": "propose_and_wait", "max_change_pct": 10.0}}),
        decision_log=decision_log,
    )
    mutate_calls = client.get_service("AdGroupService").calls
    assert [call["validate_only"] for call in mutate_calls] == [True, False]
    operation = mutate_calls[0]["operations"][0]
    assert operation.update.cpc_bid_micros == 1_100_000
    assert result.applied_change_pct == 10.0
    assert len(decision_log.records) == 2
