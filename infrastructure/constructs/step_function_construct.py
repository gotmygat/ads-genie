from __future__ import annotations

from pathlib import Path

try:
    from aws_cdk import aws_stepfunctions as sfn
    from constructs import Construct
except ImportError:  # pragma: no cover
    sfn = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]


class StepFunctionConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, definition_path: str):
        if sfn is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize Step Functions")
        super().__init__(scope, construct_id)
        self.state_machine = sfn.CfnStateMachine(
            self,
            "StateMachine",
            role_arn="${StateMachineRoleArn}",
            definition_string=Path(definition_path).read_text(),
        )
