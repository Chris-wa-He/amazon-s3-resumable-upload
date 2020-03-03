import json
import logging
import os
import boto3
import requests

from s3_upload_utility import get_s3_file_list, job_upload_sqs_ddb

# Main
if __name__ == '__main__':
    # Set Source s3 bucket
    s3_src_bucket = 'broad-references'
    s3_src_prefix = ''
    ##############

    # Configure logging
    LoggingLevel = 'INFO'

    logger = logging.getLogger()
    os.system("mkdir log")
    this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    log_file_name = './log/log-'+this_file_name+'-'+s3_src_bucket+'.log'
    fileHandler = logging.FileHandler(filename=log_file_name)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(fileHandler)
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    ############
    # Set environment
    if os.uname()[0] == 'Linux':  # on EC2, use EC2 role
        region = json.loads(requests.get(
            'http://169.254.169.254/latest/dynamic/instance-identity/document').text)['region']
        s3_src_client = boto3.client('s3', region)
        sqs = boto3.client('sqs', region)
        dynamodb = boto3.resource('dynamodb', region)
    else:  # on Local machine, use aws config profile
        src_session = boto3.session.Session(profile_name='iad')
        s3_src_client = src_session.client('s3')
        sqs = src_session.client('sqs')
        dynamodb = src_session.resource('dynamodb')
    table_queue_name = 's3_upload_file_list-'+s3_src_bucket
    table = dynamodb.Table(table_queue_name)
    sqs_queue = sqs.get_queue_url(QueueName=table_queue_name)['QueueUrl']

    # Program start processing here
    file_list = get_s3_file_list(s3_src_client, s3_src_bucket, s3_src_prefix)
    job_upload_sqs_ddb(sqs, sqs_queue, table, file_list)

