from __future__ import annotations

try:
    from aws_cdk import Stack, aws_iam as iam
    from constructs import Construct
except ImportError:  # pragma: no cover
    Stack = object  # type: ignore[assignment]
    iam = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]

from infrastructure.constructs.lambda_construct import LambdaConstruct


class MCPServerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        if iam is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize MCPServerStack")
        super().__init__(scope, construct_id, **kwargs)
        # Least privilege: DynamoDB only on Ads Genie tables, Secrets Manager only for required secrets, CloudWatch logs write.
        role = iam.Role(
            self,
            "McpServerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        self.mcp_server = LambdaConstruct(
            self,
            "McpServer",
            code_path=".",
            handler="mcp_server.server.handler",
            environment={"ENV": "production"},
            role=role,
        ).function
