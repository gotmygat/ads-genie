from __future__ import annotations

import json
from pathlib import Path


WORKFLOW_PATH = Path("orchestration/step_functions/anomaly_workflow.json")


def _workflow() -> dict:
    return json.loads(WORKFLOW_PATH.read_text())


def test_auto_execute_route_skips_wait_state() -> None:
    workflow = _workflow()
    choices = workflow["States"]["RouteByAutonomyLevel"]["Choices"]
    auto_choice = next(choice for choice in choices if choice["Variable"] == "$.autonomy_level" and choice["StringEquals"] == "auto_execute")
    assert auto_choice["Next"] == "ExecuteAction"


def test_propose_and_wait_uses_task_token_with_24h_timeout() -> None:
    workflow = _workflow()
    wait_state = workflow["States"]["WaitForApproval"]
    assert wait_state["Resource"].endswith("waitForTaskToken")
    assert wait_state["TimeoutSeconds"] == 86400
    choices = workflow["States"]["RouteByAutonomyLevel"]["Choices"]
    review_choice = next(choice for choice in choices if choice["StringEquals"] == "propose_and_wait")
    assert review_choice["Next"] == "SendSlackAlert"


def test_draft_and_review_wait_state_has_72h_timeout() -> None:
    workflow = _workflow()
    wait_state = workflow["States"]["WaitForReview"]
    assert wait_state["Resource"].endswith("waitForTaskToken")
    assert wait_state["TimeoutSeconds"] == 259200
