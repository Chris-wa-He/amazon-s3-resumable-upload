import time
import logging

logger = logging.getLogger()


def get_s3_file_list(s3_client, bucket, S3Prefix):
    logger.info('Get s3 file list from bucket: '+bucket)
    # 如果获取 s3 list 失败则不断5秒重试
    while True:
        try:
            __des_file_list = []
            response_fileList = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=S3Prefix,
                MaxKeys=1000
            )
            if response_fileList["KeyCount"] != 0:
                for n in response_fileList["Contents"]:
                    if n["Key"][-1] != '/':      # Key以"/“结尾的是子目录，不处理
                        __des_file_list.append({
                            "Key": n["Key"],
                            "Size": n["Size"]
                        })
                while response_fileList["IsTruncated"]:
                    response_fileList = s3_client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=S3Prefix,
                        MaxKeys=1000,
                        ContinuationToken=response_fileList["NextContinuationToken"]
                    )
                    for n in response_fileList["Contents"]:
                        if (n["Size"] >= ChunkSize and IgnoreSmallFile) or not IgnoreSmallFile:
                            if n["Key"][-1] != '/':      # Key以"/“结尾的是子目录，不处理
                                __des_file_list.append({
                                    "Key": n["Key"],
                                    "Size": n["Size"]
                                })
                logger.info('Bucket list length： '+str(len(__des_file_list)))
            else:
                logger.info('File list is empty in the s3 bucket')
            break
        except Exception as err:
            logger.error(str(err))
            time.sleep(5)
            logger.error('Retry get S3 list bucket: ', bucket)
    return __des_file_list


def job_upload_sqs_ddb(sqs, sqs_queue, table, file_list):
    sqs_batch = 0
    sqs_message = []
    logger.info('Start uploading jobs to queue: '+sqs_queue)
    # create ddb writer
    with table.batch_writer() as ddb_batch:
        for index in file_list:
            try:
                # write to ddb, auto batch
                ddb_batch.put_item(Item={"filekey": str(index)})

                # construct sqs messages
                sqs_message.append({
                    "Id": str(sqs_batch),
                    "MessageBody": str(index),
                })
                sqs_batch += 1
                if sqs_batch == 10 or index == file_list[-1]:
                    sqs.send_message_batch(QueueUrl=sqs_queue, Entries=sqs_message)
                    sqs_batch = 0
                    sqs_message = []
            except Exception as e:
                logger.error(str(e)+'-->Fail to upload index: '+str(index))
    logger.info('Complete upload job to queue: ' + sqs_queue)
    return
