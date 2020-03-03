import boto3
import json
import logging
from s3_upload_utility import get_s3_file_list, job_upload_sqs_ddb


# Main
if __name__ == '__main__':
    # Configure logging
    LoggingLevel = 'INFO'

    logger = logging.getLogger()
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(streamHandler)
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    ############

    # Set environment
    s3_src_bucket = 'broad-references'
    s3_src_prefix = ''
    src_session = boto3.session.Session(profile_name='us')
    s3_src_client = src_session.client('s3')

    sqs_queue = 'https://sqs.us-west-2.amazonaws.com/968464439421/s3-open-data'
    sqs = src_session.client('sqs')
    dynamodb = src_session.resource('dynamodb')
    table = dynamodb.Table('s3-upload-file-list')
    ################

    file_list = get_s3_file_list(s3_src_client, s3_src_bucket, s3_src_prefix)

    # with open(s3_src_bucket+'-file_list.json', 'w') as f:  # 写入文件做备份，下面并不使用
    #     json.dump(file_list, f)

    job_upload_sqs_ddb(sqs, sqs_queue, table, file_list)

