from __future__ import annotations

from pathlib import Path

try:
    from aws_cdk import Duration, Stack, aws_events as events, aws_events_targets as targets, aws_iam as iam, aws_stepfunctions as sfn
    from constructs import Construct
except ImportError:  # pragma: no cover
    Duration = None  # type: ignore[assignment]
    Stack = object  # type: ignore[assignment]
    events = None  # type: ignore[assignment]
    targets = None  # type: ignore[assignment]
    iam = None  # type: ignore[assignment]
    sfn = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]

from infrastructure.constructs.lambda_construct import LambdaConstruct


class OrchestrationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        if Duration is None or events is None or targets is None or iam is None or sfn is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize OrchestrationStack")
        super().__init__(scope, construct_id, **kwargs)
        env = {"ENV": "production"}
        # Separate roles per Lambda to preserve least privilege boundaries.
        self.health_check_trigger = LambdaConstruct(self, "HealthCheckTrigger", code_path=".", handler="orchestration.lambdas.health_check_trigger.handler.lambda_handler", environment=env).function
        self.anomaly_detector = LambdaConstruct(self, "AnomalyDetector", code_path=".", handler="orchestration.lambdas.anomaly_detector.handler.lambda_handler", environment=env).function
        self.analysis_runner = LambdaConstruct(self, "AnalysisRunner", code_path=".", handler="orchestration.lambdas.analysis_runner.handler.lambda_handler", environment=env).function
        self.recommendation_builder = LambdaConstruct(self, "RecommendationBuilder", code_path=".", handler="orchestration.lambdas.recommendation_builder.handler.lambda_handler", environment=env).function
        self.action_executor = LambdaConstruct(self, "ActionExecutor", code_path=".", handler="orchestration.lambdas.action_executor.handler.lambda_handler", environment=env).function
        self.decision_logger = LambdaConstruct(self, "DecisionLogger", code_path=".", handler="orchestration.lambdas.decision_logger.handler.lambda_handler", environment=env).function
        self.state_machine = sfn.CfnStateMachine(
            self,
            "AnomalyWorkflow",
            role_arn="${StateMachineRoleArn}",
            definition_string=Path("orchestration/step_functions/anomaly_workflow.json").read_text(encoding="utf-8"),
        )
        for rule_id, schedule, target_lambda in [
            ("HourlyHealthChecks", events.Schedule.rate(Duration.hours(1)), self.health_check_trigger),
            ("MondayMccReport", events.Schedule.cron(minute="0", hour="8", week_day="MON"), self.decision_logger),
            ("DailyAnomalySummary", events.Schedule.cron(minute="0", hour="6"), self.decision_logger),
        ]:
            role = iam.Role(
                self,
                f"{rule_id}Role",
                assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
            )
            target_lambda.grant_invoke(role)
            events.Rule(
                self,
                rule_id,
                schedule=schedule,
                targets=[targets.LambdaFunction(target_lambda, event_role=role)],
            )
