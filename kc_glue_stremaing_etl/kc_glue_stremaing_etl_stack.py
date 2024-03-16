from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    # aws_glue as glue,
    aws_glue_alpha as gluea,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lakeformation as lf,
    aws_athena as athena,
    aws_kinesis as kinesis,
    aws_iot as iot,
    aws_logs as logs
)
from constructs import Construct

class KcGlueStremaingEtlStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env, project_name: str, logging: bool, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Region=env.region
        AccountId=env.account
        
        ingestion_role = iam.Role(
            self,
            'IngestionRole',
            description='Role to run lf lambda function',
            role_name=f'{project_name}-ingestion-role',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('lambda.amazonaws.com'),
                iam.ServicePrincipal('glue.amazonaws.com'),
                iam.ServicePrincipal('kinesis.amazonaws.com'),
                iam.ServicePrincipal('iot.amazonaws.com'),
            ),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('AWSLambdaInvocation-DynamoDB'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('AWSXrayWriteOnlyAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('AWSGlueConsoleFullAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('AmazonKinesisFullAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                              iam.ManagedPolicy.from_aws_managed_policy_name('IAMFullAccess')],
            inline_policies={
                'KinesisInlinePolicy': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=['kinesis:PutRecord'],
                            resources=[f'arn:aws:kinesis:{Region}:{AccountId}:stream/{project_name}-Kinesis-Stream']
                        )
                    ]
                )
            }
        )
        ingestion_role.apply_removal_policy(policy=RemovalPolicy.DESTROY)

        bucket = s3.Bucket(self,
            'Bucket',
            bucket_name=f'{project_name}-{AccountId}-{Region}-target',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            auto_delete_objects=True, # set to False in prod  
            removal_policy=RemovalPolicy.DESTROY, # set to RETAIN in prod  
            versioned=False,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
        )

        stream = kinesis.Stream(self, "KinesisStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            stream_name=f'{project_name}-Kinesis-Stream',
            retention_period=Duration.hours(24),
        )

        
        glue_bucket = s3.Bucket(
            self,
            'GlueBucket',
            bucket_name=f'{project_name}-glue-assets-{AccountId}-{Region}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            versioned=False,
            lifecycle_rules=[s3.LifecycleRule(enabled=True,abort_incomplete_multipart_upload_after=Duration.days(5))],
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED
        )

        job = gluea.Job(self, 'StreamingJob',
            job_name=f'{project_name}_streaming_etl',
            spark_ui=gluea.SparkUIProps(
                enabled=False
            ),
            continuous_logging=gluea.ContinuousLoggingProps(enabled=logging),
            default_arguments={
                '--job-bookmark-option': 'job-bookmark-disable',  # enable in prod
                '--TempDir': f's3://{glue_bucket.bucket_name}/temporary/',
                '--enable-job-insights': 'true',
                '--extra-py-files': 's3://aws-glue-studio-transforms-244479516193-prod-eu-west-1/gs_common.py,s3://aws-glue-studio-transforms-244479516193-prod-eu-west-1/gs_format_timestamp.py,s3://aws-glue-studio-transforms-244479516193-prod-eu-west-1/gs_to_timestamp.py',
                '--spark-event-logs-path': f's3://{glue_bucket.bucket_name}/sparkHistoryLogs/',
                '--enable-auto-scaling': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-metrics': 'true'
            },
            enable_profiling_metrics=True,
            role=ingestion_role,
            worker_count=5,
            worker_type=gluea.WorkerType.G_025_X,
            executable=gluea.JobExecutable.python_streaming(
                glue_version=gluea.GlueVersion.V4_0,
                python_version=gluea.PythonVersion.THREE,
                script=gluea.Code.from_asset("./src/job.py")
            )
        )

        error_log_group = logs.LogGroup(self, 
            'RuleLogGroup',
            log_group_name=f'{project_name}-iot-rule-error',
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy=RemovalPolicy.DESTROY
        )

        topic_rule = iot.CfnTopicRule(self, "TopicRule",
            rule_name=f'{project_name}_rule',
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                actions=[iot.CfnTopicRule.ActionProperty(
                    kinesis=iot.CfnTopicRule.KinesisActionProperty(
                        role_arn=ingestion_role.role_arn,
                        stream_name=stream.stream_name,
                        partition_key="${timestamp()}"
                    )
                )],
                sql="SELECT * FROM 'sdk/test/python'", # Adapt to actual device
                error_action=iot.CfnTopicRule.ActionProperty(
                    cloudwatch_logs=iot.CfnTopicRule.CloudwatchLogsActionProperty(
                        log_group_name=error_log_group.log_group_name,
                        role_arn=ingestion_role.role_arn,
                    )
                )
            )
        )
        query_bucket = s3.Bucket(
            self,
            'QueryBucket',
            bucket_name=f'{project_name}-athena-query-results-{AccountId}-{Region}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            versioned=False,
            lifecycle_rules=[s3.LifecycleRule(enabled=True,abort_incomplete_multipart_upload_after=Duration.days(5))],
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED
        )

        athena_workgroup = athena.CfnWorkGroup(
            self,
            'AthenaWorkgroup',
            name=f'{project_name}-workgroup',
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                bytes_scanned_cutoff_per_query=None,
                enforce_work_group_configuration=False,
                publish_cloud_watch_metrics_enabled=False,
                requester_pays_enabled=False,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option='SSE_S3'
                    ),
                    output_location='s3://' + query_bucket.bucket_name + '/'
                )
            )
        )
