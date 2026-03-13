from __future__ import annotations

from hashlib import sha256
from hmac import compare_digest, new as hmac_new
from typing import Any
import json
import os
import time

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from slack_bot.handlers.approval_handler import (
    MODIFY_MODAL_CALLBACK_ID,
    approve_action,
    dismiss_action,
    open_modify_modal,
    submit_modify_action,
)
from slack_bot.handlers.query_handler import handle_query_message
from orchestration.models.account_registry import AccountRegistry

try:
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler
except ImportError:  # pragma: no cover
    App = None  # type: ignore[assignment]
    SocketModeHandler = None  # type: ignore[assignment]


def _load_secret_payload(secret_name: str) -> dict[str, str]:
    if boto3 is None:
        raise RuntimeError("boto3 is required for Secrets Manager access")
    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
    secret = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])


def _slack_credentials() -> dict[str, str]:
    if os.getenv("ENV", "local").strip().lower() == "production":
        payload = _load_secret_payload(os.getenv("SLACK_SECRET_NAME", "ads-genie/slack"))
        return {
            "SLACK_BOT_TOKEN": str(payload["SLACK_BOT_TOKEN"]),
            "SLACK_APP_TOKEN": str(payload["SLACK_APP_TOKEN"]),
            "SLACK_SIGNING_SECRET": str(payload["SLACK_SIGNING_SECRET"]),
        }
    return {
        "SLACK_BOT_TOKEN": os.getenv("SLACK_BOT_TOKEN", ""),
        "SLACK_APP_TOKEN": os.getenv("SLACK_APP_TOKEN", ""),
        "SLACK_SIGNING_SECRET": os.getenv("SLACK_SIGNING_SECRET", ""),
    }


def verify_channel_for_account(customer_id: str, channel_id: str) -> bool:
    return AccountRegistry().verify_channel_for_account(customer_id, channel_id)


def verify_slack_signature(headers: dict[str, str], raw_body: bytes, signing_secret: str) -> bool:
    timestamp = headers.get("X-Slack-Request-Timestamp", "")
    signature = headers.get("X-Slack-Signature", "")
    if not timestamp or not signature:
        return False
    if abs(time.time() - int(timestamp)) > 300:
        return False
    base_string = f"v0:{timestamp}:{raw_body.decode('utf-8')}"
    expected = "v0=" + hmac_new(signing_secret.encode("utf-8"), base_string.encode("utf-8"), sha256).hexdigest()
    return compare_digest(expected, signature)


def create_slack_app() -> Any:
    if App is None:
        raise RuntimeError("slack-bolt is required to run the Slack app")
    creds = _slack_credentials()
    app = App(token=creds["SLACK_BOT_TOKEN"], signing_secret=creds["SLACK_SIGNING_SECRET"])

    @app.event("app_mention")
    def on_app_mention(body, say, logger):  # noqa: ANN001
        event = body.get("event", {})
        response = handle_query_message(channel_id=event.get("channel", ""), text=event.get("text", ""))
        logger.info("app_mention", channel_id=event.get("channel"), response_text=response.get("text"))
        say(**response)

    @app.action("approve_action")
    def on_approve(ack, body, client, logger):  # noqa: ANN001
        ack()
        approve_action(body=body, client=client)
        logger.info("approve_action", channel_id=body.get("channel", {}).get("id"))

    @app.action("modify_action")
    def on_modify(ack, body, client, logger):  # noqa: ANN001
        ack()
        open_modify_modal(body=body, client=client)
        logger.info("modify_action", channel_id=body.get("channel", {}).get("id"))

    @app.action("dismiss_action")
    def on_dismiss(ack, body, client, logger):  # noqa: ANN001
        ack()
        dismiss_action(body=body, client=client)
        logger.info("dismiss_action", channel_id=body.get("channel", {}).get("id"))

    @app.view(MODIFY_MODAL_CALLBACK_ID)
    def on_modify_submit(ack, body, client, logger):  # noqa: ANN001
        ack()
        submit_modify_action(body=body, client=client)
        logger.info("modify_action_submit")

    return app


def run() -> None:
    if SocketModeHandler is None:
        raise RuntimeError("slack-bolt socket mode dependencies are required")
    creds = _slack_credentials()
    app = create_slack_app()
    SocketModeHandler(app, creds["SLACK_APP_TOKEN"]).start()


if __name__ == "__main__":
    run()
