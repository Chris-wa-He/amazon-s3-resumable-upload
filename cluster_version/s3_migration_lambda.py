import json, os, urllib, ssl, logging, time
import boto3
from s3_migration_lib import step_function, wait_sqs_available
from botocore.config import Config
from pathlib import PurePosixPath

# 常量
Des_bucket = os.environ['Des_bucket']
Des_prefix = os.environ['Des_prefix']
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']

table_queue_name = "s3_migrate_file_list"
ssm_parameter_bucket = "s3_migrate_bucket_para"
ssm_parameter_credentials = "s3_migrate_credentials"
StorageClass = os.environ['StorageClass']
ifVerifyMD5Twice = False

ChunkSize = 5 * 1024 * 1024
ResumableThreshold = 10
MaxRetry = 10
MaxThread = 200
MaxParallelFile = 1
JobTimeout = 3000

CleanUnfinishedUpload = False
LocalProfileMode = False

# Set environment
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_config = Config(max_pool_connections=200)  # boto default 10

region = os.environ['Des_region']
instance_id = "Lambda"
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# 取另一个Account的credentials
credentials_session = boto3.session.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region
)

s3_src_client = boto3.client('s3', config=s3_config)
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
        Src_bucket = sqs_message['Records'][0]['s3']['bucket']['name']
        Src_key = sqs_message['Records'][0]['s3']['object']['key']
        Src_key = urllib.parse.unquote_plus(Src_key)
        Size = sqs_message['Records'][0]['s3']['object']['size']
        if Size == 0:
            return {
                'statusCode': 200,
                'body': "Zero size file"
            }

        job = {
            'Src_bucket': Src_bucket,
            'Src_key': Src_key,
            'Size': Size,
            'Des_bucket': Des_bucket,
            'Des_key': str(PurePosixPath(Des_prefix) / Src_key)
        }

        context = ssl._create_unverified_context()
        response = urllib.request.urlopen(urllib.request.Request("https://checkip.amazonaws.com"), context=context).read()
        print("Lambda IP Address:", response.decode('utf-8'))

        logger.info(f'Write log to DDB in first round of job: {Src_bucket}/{Src_key}')
        with table.batch_writer() as ddb_batch:
            # write to ddb, auto batch
            for retry in range(MaxRetry + 1):
                try:
                    ddb_key = str(PurePosixPath(job["Src_bucket"]) / job["Src_key"])
                    ddb_batch.put_item(Item={
                        "Key": ddb_key,
                        "Src_bucket": job["Src_bucket"],
                        "Des_bucket": job["Des_bucket"],
                        "Des_key": job["Des_key"],
                        "Size": job["Size"]
                    })
                    break
                except Exception as e:
                    logger.error(f'Fail writing to DDB: {ddb_key}, {str(e)}')
                    if retry >= MaxRetry:
                        logger.error(f'Fail writing to DDB: {ddb_key}')
                    else:
                        time.sleep(5 * retry)

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
