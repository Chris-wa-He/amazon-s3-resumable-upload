from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_sqs as sqs


class CdkResourceStack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, src_bucket, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        self.src_s3 = s3.Bucket.from_bucket_name(self, "s3", bucket_name=src_bucket)

        self.ddb_file_list = ddb.Table(self, "ddb",
                                       table_name="s3_upload_file_list-" + src_bucket,
                                       partition_key=ddb.Attribute(name="filekey", type=ddb.AttributeType.STRING),
                                       billing_mode=ddb.BillingMode.PAY_PER_REQUEST)

        self.sqs_queue_DLQ = sqs.Queue(self, "sqs_DLQ",
                                       queue_name="s3_upload_file_list-" + src_bucket + '-DLQ',
                                       visibility_timeout=core.Duration.hours(1),
                                       retention_period=core.Duration.days(14)
                                       )
        self.sqs_queue = sqs.Queue(self, "sqs_queue",
                                   queue_name="s3_upload_file_list-" + src_bucket,
                                   visibility_timeout=core.Duration.hours(1),
                                   retention_period=core.Duration.days(14),
                                   dead_letter_queue=sqs.DeadLetterQueue(
                                       max_receive_count=3,
                                       queue=self.sqs_queue_DLQ
                                   )
                                   )

