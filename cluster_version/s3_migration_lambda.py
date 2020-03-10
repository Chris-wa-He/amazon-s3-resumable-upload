import json, os, urllib, ssl
import boto3
from s3_migration_lib import step_function, wait_sqs_available
from botocore.config import Config

# 常量
Des_bucket = "hawkey999"
Des_prefix = "test-from-tokyo-lambda-day2"
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']

JobType = "PUT"
table_queue_name = "s3_migrate_file_list"
ssm_parameter_bucket = "s3_migrate_bucket_para"
ssm_parameter_credentials = "s3_migrate_credentials"
StorageClass = "STANDARD"
ifVerifyMD5Twice = False

ChunkSize = 5 * 1024 * 1024
ResumableThreshold = 10
MaxRetry = 10
MaxThread = 200
MaxParallelFile = 1
JobTimeout = 3000

CleanUnfinishedUpload = False
LoggingLevel = "INFO"
LocalProfileMode = False

# Set environment
s3_config = Config(max_pool_connections=200)  # boto default 10

region = "ap-northeast-1"
instance_id = "Lambda"
sqs = boto3.client('sqs', region)
dynamodb = boto3.resource('dynamodb', region)
ssm = boto3.client('ssm', region)

# 取另一个Account的credentials
credentials_session = boto3.session.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="cn-north-1"
)

s3_src_client = boto3.client('s3', region, config=s3_config)
s3_des_client = credentials_session.client('s3', config=s3_config)

table = dynamodb.Table(table_queue_name)
table.wait_until_exists()
sqs_queue = wait_sqs_available(sqs, table_queue_name)


class TimeoutOrMaxRetry(Exception):
    pass


def lambda_handler(event, context):
    print(json.dumps(event, default=str))

    trigger_body = event['Records'][0]['body']
    sqs_message = json.loads(trigger_body)
    print(json.dumps(sqs_message, default=str))

    if "Records" in sqs_message:
        src_bucket = sqs_message['Records'][0]['s3']['bucket']['name']
        src_key = sqs_message['Records'][0]['s3']['object']['key']
        src_key = urllib.parse.unquote_plus(src_key)
        size = sqs_message['Records'][0]['s3']['object']['size']
        job = {
            'Src_bucket': src_bucket,
            'Src_key': src_key,
            'Size': size,
            'Des_bucket': Des_bucket,
            'Des_key': Des_prefix + src_key
        }

        # context = ssl._create_unverified_context()
        # response = urllib.request.urlopen(urllib.request.Request("https://checkip.amazonaws.com"), context=context).read()
        # print(response)
        upload_etag_full = step_function(job, table, s3_src_client, s3_des_client, instance_id,
                                         StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
                                         JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload)

        if upload_etag_full != "TIMEOUT" and upload_etag_full != "ERR":
            return {
                'statusCode': 200,
                'body': upload_etag_full
            }
        else:
            raise TimeoutOrMaxRetry
    else:
        return {
            'statusCode': 200,
            'body': "OK"
        }


# Main
if __name__ == '__main__':
    #######
    # Program start processing here
    #######
    with open("./lambda_test.json") as f:
        event = f.read()

    lambda_handler(event=event)
    pass