from __future__ import annotations

from typing import Any


AUTONOMY_ALIASES = {
    "auto_execute": "auto_execute",
    "propose_wait": "propose_and_wait",
    "propose_and_wait": "propose_and_wait",
    "draft_review": "draft_and_review",
    "draft_and_review": "draft_and_review",
    "escalate": "escalate",
}

LEVEL_ORDER = {
    "auto_execute": 0,
    "propose_and_wait": 1,
    "draft_and_review": 2,
    "escalate": 3,
}


def normalize_autonomy_level(value: Any, default: str = "escalate") -> str:
    candidate = str(value or "").strip().lower()
    return AUTONOMY_ALIASES.get(candidate, default)

