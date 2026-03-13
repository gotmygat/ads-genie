from __future__ import annotations

try:
    from aws_cdk import App
except ImportError:  # pragma: no cover
    App = None  # type: ignore[assignment]

from infrastructure.stacks.database_stack import DatabaseStack
from infrastructure.stacks.mcp_server_stack import MCPServerStack
from infrastructure.stacks.orchestration_stack import OrchestrationStack
from infrastructure.stacks.slack_bot_stack import SlackBotStack


def main() -> None:
    if App is None:
        raise RuntimeError("aws-cdk-lib is required to synthesize the Ads Genie infrastructure")
    app = App()
    DatabaseStack(app, "AdsGenieDatabaseStack")
    MCPServerStack(app, "AdsGenieMCPServerStack")
    OrchestrationStack(app, "AdsGenieOrchestrationStack")
    SlackBotStack(app, "AdsGenieSlackBotStack")
    app.synth()


if __name__ == "__main__":
    main()
