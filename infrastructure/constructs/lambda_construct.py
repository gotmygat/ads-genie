from __future__ import annotations

try:
    from aws_cdk import Duration, aws_iam as iam, aws_lambda as lambda_
    from constructs import Construct
except ImportError:  # pragma: no cover
    Duration = None  # type: ignore[assignment]
    iam = None  # type: ignore[assignment]
    lambda_ = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]


class LambdaConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, code_path: str, handler: str, environment: dict[str, str], role=None):
        if lambda_ is None or Duration is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize Lambda constructs")
        super().__init__(scope, construct_id)
        self.function = lambda_.Function(
            self,
            "Function",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler=handler,
            code=lambda_.Code.from_asset(code_path),
            timeout=Duration.minutes(5),
            environment=environment,
            role=role,
            tracing=lambda_.Tracing.ACTIVE,
        )
