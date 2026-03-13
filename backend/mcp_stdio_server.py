from __future__ import annotations

import json
import sys
from typing import Any

from .actions import ActionExecutor
from .ads_client import GoogleAdsAdapter
from .config import load_settings
from .db import Database
from .orchestrator import Orchestrator
from .reports import ReportService
from .slack_bridge import SlackBridge
from .tools import ToolEngine


class MCPStdioServer:
    def __init__(self) -> None:
        settings = load_settings()
        db = Database(settings.db_path)
        db.init_schema()
        if settings.auto_seed:
            db.seed_demo_data()

        ads = GoogleAdsAdapter(settings, db)
        tools = ToolEngine(db, ads)
        reports = ReportService(db, tools, settings.timezone)
        actions = ActionExecutor(db)
        slack = SlackBridge(settings)
        orchestrator = Orchestrator(db, tools, actions, reports, slack, settings.timezone)

        self.db = db
        self.tools = tools
        self.orchestrator = orchestrator

    def _handle_initialize(self, _params: dict[str, Any]) -> dict[str, Any]:
        return {
            "protocolVersion": "2024-11-05",
            "serverInfo": {
                "name": "ads-genie-mcp",
                "version": "0.1.0",
            },
            "capabilities": {
                "tools": {},
            },
        }

    def _tool_schema(self) -> list[dict[str, Any]]:
        entries = []
        for tool in self.tools.list_tools():
            entries.append(
                {
                    "name": tool["name"],
                    "description": tool["description"],
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "account_id": {"type": "integer"},
                            "params": {"type": "object"},
                        },
                    },
                }
            )
        entries.append(
            {
                "name": "run_monitoring_cycle",
                "description": "Run detect -> analyze -> recommend -> approve/execute -> log pipeline.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "account_id": {"type": "integer"},
                    },
                },
            }
        )
        return entries

    def _handle_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = str(params.get("name", "")).strip()
        arguments = params.get("arguments", {}) or {}

        if name == "run_monitoring_cycle":
            account_id = arguments.get("account_id")
            result = self.orchestrator.run_monitoring_cycle(
                account_id=int(account_id) if account_id is not None else None,
                triggered_by="mcp",
            )
            payload = json.dumps(result, indent=2)
            return {"content": [{"type": "text", "text": payload}], "isError": False}

        account_id = arguments.get("account_id")
        run_params = arguments.get("params", {})
        result = self.tools.run_tool(name, account_id=account_id, params=run_params)
        payload = json.dumps(result, indent=2)
        return {"content": [{"type": "text", "text": payload}], "isError": False}

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        method = request.get("method")
        if method == "initialize":
            return self._handle_initialize(request.get("params", {}))
        if method == "tools/list":
            return {"tools": self._tool_schema()}
        if method == "tools/call":
            return self._handle_tools_call(request.get("params", {}))
        if method == "ping":
            return {"pong": True}
        raise ValueError(f"Unsupported method: {method}")

    def run(self) -> None:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
                request_id = request.get("id")
                result = self.handle(request)
                response = {"jsonrpc": "2.0", "id": request_id, "result": result}
            except Exception as exc:
                response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32000, "message": str(exc)},
                }
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()


def main() -> None:
    MCPStdioServer().run()


if __name__ == "__main__":
    main()
