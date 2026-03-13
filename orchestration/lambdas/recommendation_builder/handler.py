from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json
import os

try:
    from anthropic import Anthropic
except ImportError:  # pragma: no cover
    Anthropic = None  # type: ignore[assignment]

from slack_bot.messages.alert_blocks import AlertPayload, build_alert_blocks


RECOMMENDATION_SCHEMA = {
    "type": "object",
    "properties": {
        "root_cause_summary": {"type": "string"},
        "recommended_action": {"type": "string"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
    },
    "required": ["root_cause_summary", "recommended_action", "confidence"],
}


def _fallback_recommendation(event: dict[str, Any]) -> dict[str, str]:
    analysis = event.get("analysis_result", {})
    if event.get("analysis_tool") == "diagnose_roas_drop":
        return {
            "root_cause_summary": ", ".join(analysis.get("supporting_evidence", [])) or str(analysis.get("primary_cause", "ROAS drop detected")),
            "recommended_action": str(analysis.get("recommended_fix", "Review search terms, quality, and bids.")),
            "confidence": str(analysis.get("confidence", "medium")),
        }
    if event.get("analysis_tool") == "analyze_budget_waste":
        return {
            "root_cause_summary": f"Detected ${analysis.get('total_wasted_spend', 0)} in potential waste across campaigns.",
            "recommended_action": "Review irrelevant search terms, starving campaigns, and low quality score drag.",
            "confidence": "high",
        }
    return {
        "root_cause_summary": "The account health check found a material performance issue requiring review.",
        "recommended_action": "; ".join(event.get("health_check", {}).get("recommended_actions", [])[:3]) or "Review the account diagnostics.",
        "confidence": "medium",
    }


def _anthropic_recommendation(event: dict[str, Any]) -> dict[str, str]:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key or Anthropic is None:
        return _fallback_recommendation(event)
    prompt = (
        "Return JSON matching this schema exactly: "
        f"{json.dumps(RECOMMENDATION_SCHEMA)}\n\n"
        f"Account name: {event.get('account_name')}\n"
        f"Vertical: {event.get('vertical')}\n"
        f"Specific metrics and tool outputs: {json.dumps(event.get('analysis_result', {}))}\n"
        f"Health check output: {json.dumps(event.get('health_check', {}))}\n"
        f"Recommended action type: {event.get('action_type')}\n"
        "Produce concise executive wording."
    )
    try:
        client = Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-3-7-sonnet-latest",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(block.text for block in response.content if getattr(block, "type", "") == "text")
        payload = json.loads(text)
        return {
            "root_cause_summary": str(payload["root_cause_summary"]),
            "recommended_action": str(payload["recommended_action"]),
            "confidence": str(payload["confidence"]),
        }
    except Exception:
        return _fallback_recommendation(event)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    recommendation = _anthropic_recommendation(event)
    alert = AlertPayload(
        customer_id=str(event["customer_id"]),
        account_name=str(event.get("account_name", event["customer_id"])),
        alert_type=str(event.get("analysis_tool", "analysis_result")),
        root_cause_summary=recommendation["root_cause_summary"],
        supporting_data=event.get("analysis_result", {}),
        recommended_action=recommendation["recommended_action"],
        confidence=recommendation["confidence"],
        timestamp=datetime.now(timezone.utc).isoformat(),
        vertical=str(event.get("vertical", "")),
        slack_channel_id=str(event.get("slack_channel_id", "")),
        action_type=str(event.get("action_type", "")),
        execution_arn=str(event.get("execution_arn", "")) if event.get("execution_arn") else None,
    )
    response = dict(event)
    response["recommendation"] = recommendation
    response["slack_message_payload"] = {
        "channel": alert.slack_channel_id,
        "text": f"{alert.account_name} — {alert.alert_type}",
        "blocks": build_alert_blocks(alert),
    }
    response["recommendation_request_id"] = getattr(context, "aws_request_id", None)
    return response
