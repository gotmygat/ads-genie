from __future__ import annotations

from typing import Any

from orchestration.models.decision_log import DecisionLog


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    record = dict(event)
    record.setdefault("step_functions_execution_id", event.get("execution_arn") or getattr(context, "aws_request_id", None))
    written = DecisionLog().write(record)
    return {"logged": True, "decision_id": written.get("decision_id"), "timestamp": written.get("timestamp")}
