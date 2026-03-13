from __future__ import annotations

from mcp_server.tools.cross_mcc_anomalies import cross_mcc_anomalies


class PerCustomerQueryExecutor:
    def __init__(self, payloads):
        self.payloads = payloads

    def run(self, client, customer_id: str, query: str):  # noqa: ANN001, ARG002
        return self.payloads[customer_id]


def test_cross_mcc_anomalies_flags_cost_spike(dummy_auth) -> None:
    accounts = [{"customer_id": "123", "account_name": "Alpha", "vertical": "day_spa", "is_active": True}]
    payloads = {
        "123": [
            {"metrics": {"cost": 100.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 102.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 98.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 101.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 99.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 97.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 103.0, "all_conversions_value": 300.0, "conversions": 6.0}},
            {"metrics": {"cost": 240.0, "all_conversions_value": 300.0, "conversions": 6.0}},
        ]
    }
    result = cross_mcc_anomalies(
        account_loader=lambda _vertical: accounts,
        query_executor=PerCustomerQueryExecutor(payloads),
        auth=dummy_auth,
    )
    assert any(item.anomaly_type == "cost_spike" for item in result.anomalies)
