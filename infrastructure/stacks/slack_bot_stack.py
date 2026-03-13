from __future__ import annotations

try:
    from aws_cdk import Stack, aws_iam as iam
    from constructs import Construct
except ImportError:  # pragma: no cover
    Stack = object  # type: ignore[assignment]
    iam = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]

from infrastructure.constructs.lambda_construct import LambdaConstruct


class SlackBotStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        if iam is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize SlackBotStack")
        super().__init__(scope, construct_id, **kwargs)
        # Least privilege: task-token table access, decision log writes, Slack secret read, Step Functions callback only.
        role = iam.Role(
            self,
            "SlackBotRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        self.slack_bot = LambdaConstruct(
            self,
            "SlackBot",
            code_path=".",
            handler="slack_bot.app.run",
            environment={"ENV": "production"},
            role=role,
        ).function
