from __future__ import annotations

try:
    from aws_cdk import RemovalPolicy, Stack, aws_dynamodb as dynamodb, aws_s3 as s3
    from constructs import Construct
except ImportError:  # pragma: no cover
    RemovalPolicy = None  # type: ignore[assignment]
    Stack = object  # type: ignore[assignment]
    dynamodb = None  # type: ignore[assignment]
    s3 = None  # type: ignore[assignment]
    Construct = object  # type: ignore[assignment]


class DatabaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        if dynamodb is None or s3 is None:
            raise RuntimeError("aws-cdk-lib is required to synthesize DatabaseStack")
        super().__init__(scope, construct_id, **kwargs)
        table_kwargs = {
            "billing_mode": dynamodb.BillingMode.PAY_PER_REQUEST,
            "point_in_time_recovery": True,
            "removal_policy": RemovalPolicy.RETAIN,
        }
        self.accounts_table = dynamodb.Table(
            self,
            "AccountsTable",
            table_name="ads_genie_accounts",
            partition_key=dynamodb.Attribute(name="customer_id", type=dynamodb.AttributeType.STRING),
            **table_kwargs,
        )
        self.autonomy_table = dynamodb.Table(
            self,
            "AutonomyTable",
            table_name="ads_genie_autonomy",
            partition_key=dynamodb.Attribute(name="config_id", type=dynamodb.AttributeType.STRING),
            **table_kwargs,
        )
        self.decisions_table = dynamodb.Table(
            self,
            "DecisionsTable",
            table_name="ads_genie_decisions",
            partition_key=dynamodb.Attribute(name="decision_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            **table_kwargs,
        )
        self.cache_table = dynamodb.Table(
            self,
            "CacheTable",
            table_name="ads_genie_cache",
            partition_key=dynamodb.Attribute(name="cache_key", type=dynamodb.AttributeType.STRING),
            time_to_live_attribute="ttl_epoch",
            **table_kwargs,
        )
        self.task_tokens_table = dynamodb.Table(
            self,
            "TaskTokensTable",
            table_name="ads_genie_task_tokens",
            partition_key=dynamodb.Attribute(name="message_ts", type=dynamodb.AttributeType.STRING),
            time_to_live_attribute="expires_at",
            **table_kwargs,
        )
        self.mcc_benchmarks_table = dynamodb.Table(
            self,
            "MccBenchmarksTable",
            table_name="ads_genie_mcc_benchmarks",
            partition_key=dynamodb.Attribute(name="vertical", type=dynamodb.AttributeType.STRING),
            **table_kwargs,
        )
        self.mcc_negatives_table = dynamodb.Table(
            self,
            "MccNegativesTable",
            table_name="ads_genie_mcc_negatives",
            partition_key=dynamodb.Attribute(name="vertical", type=dynamodb.AttributeType.STRING),
            **table_kwargs,
        )
        self.audit_bucket = s3.Bucket(
            self,
            "AuditBucket",
            bucket_name="ads-genie-audit-bucket",
            object_lock_enabled=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
