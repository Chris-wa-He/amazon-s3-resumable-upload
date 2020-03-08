import time
import logging
import json
import os
from pathlib import PurePosixPath
import hashlib
import concurrent.futures
import threading
import base64
import sys

import requests
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()


# Configure logging
def set_log(LoggingLevel):
    _logger = logging.getLogger()
    _logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        _logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        _logger.setLevel(logging.DEBUG)
    # File logging
    if not os.path.exists('s3_migration_log'):
        os_response = os.system("mkdir s3_migration_log")
        print('Created folder ./s3_migration_log')
    else:
        print('Folder exist ./s3_migration_log')
    this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    t = time.localtime()
    start_time = f'{t.tm_year}-{t.tm_mon}-{t.tm_mday}-{t.tm_hour}-{t.tm_min}-{t.tm_sec}'
    _log_file_name = './s3_migration_log/' + this_file_name + '-' + start_time + '.log'
    print('Logging level:', LoggingLevel, 'Log file:', os.path.abspath(_log_file_name))
    fileHandler = logging.FileHandler(filename=_log_file_name)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    _logger.addHandler(fileHandler)
    # Screen stream logging INFO 模式下在当前屏幕也输出，便于监控。生产使用建议 WARNING 模式
    if LoggingLevel == 'INFO':
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
        _logger.addHandler(streamHandler)

    return _logger, _log_file_name


# Set environment
def set_env(JobType, LocalProfileMode, table_queue_name, ssm_parameter_credentials):
    s3_config = Config(max_pool_connections=25)  # boto default 10

    if os.uname()[0] == 'Linux' and not LocalProfileMode:  # on EC2, use EC2 role
        region = json.loads(requests.get(
            'http://169.254.169.254/latest/dynamic/instance-identity/document').text)['region']
        instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text
        sqs = boto3.client('sqs', region)
        dynamodb = boto3.resource('dynamodb', region)
        ssm = boto3.client('ssm', region)

        # 取另一个Account的credentials
        credentials = json.loads(ssm.get_parameter(
            Name=ssm_parameter_credentials,
            WithDecryption=True
        )['Parameter']['Value'])
        credentials_session = boto3.session.Session(
            aws_access_key_id=credentials["aws_access_key_id"],
            aws_secret_access_key=credentials["aws_secret_access_key"],
            region_name=credentials["region"]
        )
        if JobType.upper() == "PUT":
            s3_src_client = boto3.client('s3', region, config=s3_config)
            s3_des_client = credentials_session.client('s3', config=s3_config)
        elif JobType.upper() == "GET":
            s3_des_client = boto3.client('s3', region, config=s3_config)
            s3_src_client = credentials_session.client('s3', config=s3_config)
        else:
            logger.error('Wrong JobType setting in config.ini file')
            sys.exit(0)
    # 在没有Role的环境运行，例如本地Mac测试
    else:
        instance_id = "no_instance_id"
        src_session = boto3.session.Session(profile_name='iad')
        des_session = boto3.session.Session(profile_name='zhy')
        sqs = src_session.client('sqs')
        dynamodb = src_session.resource('dynamodb')
        ssm = src_session.client('ssm')
        s3_src_client = src_session.client('s3', config=s3_config)
        s3_des_client = des_session.client('s3', config=s3_config)

    table = dynamodb.Table(table_queue_name)
    table.wait_until_exists()
    sqs_queue = wait_sqs_available(sqs, table_queue_name)

    return sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm


def get_s3_file_list(s3_client, bucket, S3Prefix):
    logger.info(f'Get s3 file list from bucket: {bucket}')

    # get s3 file list with retry every 5 sec
    while True:
        __des_file_list = []
        try:
            response_fileList = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=S3Prefix,
                MaxKeys=1000
            )
            break
        except Exception as err:
            logger.error(f'Fail to get s3 list objests: {str(err)}')
            time.sleep(5)
            logger.error(f'Retry get S3 list bucket: {bucket}')

    if response_fileList["KeyCount"] != 0:
        for n in response_fileList["Contents"]:
            if n["Size"] != 0:  # 子目录或 0 size 文件，不处理
                __des_file_list.append({
                    "Key": n["Key"],
                    "Size": n["Size"]
                })
            else:
                logger.warning(f'Zero size file, skip: {bucket}/{n["Key"]}')

        while response_fileList["IsTruncated"]:  # IsTruncated, keep getting next lists

            # Get next part of s3 list
            while True:
                try:
                    response_fileList = s3_client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=S3Prefix,
                        MaxKeys=1000,
                        ContinuationToken=response_fileList["NextContinuationToken"]
                    )
                    break
                except Exception as err:
                    logger.error(str(err))
                    time.sleep(5)
                    logger.error(f'Retry get S3 list bucket: {bucket}')

            for n in response_fileList["Contents"]:
                if n["Size"] != 0:  # 子目录或 0 size 文件，不处理
                    __des_file_list.append({
                        "Key": n["Key"],
                        "Size": n["Size"]
                    })
                else:
                    logger.warning(f'Zero size file, skip: {bucket}/{n["Key"]}')
        logger.info(f'Bucket list length：{str(len(__des_file_list))}')
    else:
        logger.info(f'File list is empty in: {bucket}')

    return __des_file_list


def delta_job_list(src_file_list, des_file_list, bucket_para):
    src_bucket = bucket_para['src_bucket']
    src_prefix = bucket_para['src_prefix']
    des_bucket = bucket_para['des_bucket']
    des_prefix = bucket_para['des_prefix']
    dp_len = len(des_prefix) + 1  # 目的bucket的 "prefix/"长度
    # Delta list
    logger.info(f'Compare source s3://{src_bucket}/{src_prefix} and '
                f'destination s3://{des_bucket}/{des_prefix}')
    start_time = int(time.time())
    job_list = []
    for src in src_file_list:
        in_list = False

        # 比对源文件是否在目标中
        for des in des_file_list:
            # 去掉目的bucket的prefix做Key对比，且Size一致，则判为存在，不加入上传列表
            if des['Key'][dp_len:] == src['Key'] and des['Size'] == src['Size']:
                in_list = True
                break
        # 单个源文件比对结束
        if in_list:
            # 下一个源文件
            continue
        # 把源文件加入job list
        else:
            job_list.append(
                {
                    "Src_bucket": src_bucket,
                    "Src_key": src["Key"],  # Src_key已经包含了Prefix
                    "Des_bucket": des_bucket,
                    "Des_key": str(PurePosixPath(des_prefix) / src["Key"]),
                    "Size": src["Size"],
                }
            )
    spent_time = int(time.time()) - start_time
    logger.info(f'Generate delta file list LENGTH: {len(job_list)} - SPENT TIME: {spent_time}S')
    return job_list


def wait_sqs_available(sqs, table_queue_name):
    while True:
        try:
            return sqs.get_queue_url(QueueName=table_queue_name)['QueueUrl']
        except Exception as e:
            logger.warning(f'Waiting for SQS availability. {str(e)}')
            time.sleep(10)


def job_upload_sqs_ddb(sqs, sqs_queue, table, job_list, MaxRetry=30):

    sqs_batch = 0
    sqs_message = []
    logger.info(f'Start uploading jobs to queue: {sqs_queue}')
    # create ddb writer
    with table.batch_writer() as ddb_batch:
        for job in job_list:
            # write to ddb, auto batch
            for retry in range(MaxRetry+1):
                try:
                    ddb_key = job["Src_bucket"] + "/" + job["Src_key"]
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
                        time.sleep(5*retry)

            # construct sqs messages
            sqs_message.append({
                "Id": str(sqs_batch),
                "MessageBody": json.dumps(job),
            })
            sqs_batch += 1

            # write to sqs in batch 10 or is last one
            if sqs_batch == 10 or job == job_list[-1]:
                for retry in range(MaxRetry+1):
                    try:
                        sqs.send_message_batch(QueueUrl=sqs_queue, Entries=sqs_message)
                        break
                    except Exception as e:
                        logger.error(f'Fail to send sqs message: {str(sqs_message)}, {str(e)}')
                        if retry >= MaxRetry:
                            logger.error(f'Fail MaxRetry {MaxRetry} send sqs message: {str(sqs_message)}')
                        else:
                            time.sleep(5*retry)
                sqs_batch = 0
                sqs_message = []

    logger.info(f'Complete upload job to queue: {sqs_queue}')
    return


# Split one file size into list of start byte position list
def split(Size, ChunkSize):
    partnumber = 1
    indexList = [0]
    if int(Size / ChunkSize) + 1 > 10000:
        ChunkSize = int(Size / 10000) + 1024  # 对于大于10000分片的大文件，自动调整Chunksize
        logger.info(f'Size excess 10000 parts limit. Auto change ChunkSize to {ChunkSize}')
    while ChunkSize * partnumber < Size:  # 如果刚好是"="，则无需再分下一part，所以这里不能用"<="
        indexList.append(ChunkSize * partnumber)
        partnumber += 1
    return indexList, ChunkSize


# Get unfinished multipart upload id from s3
def get_uploaded_list(s3_client, Des_bucket, Des_key):
    logger.info('Get unfinished multipart upload id list...')
    NextKeyMarker = ''
    IsTruncated = True
    __multipart_uploaded_list = []
    while IsTruncated:
        try:
            list_multipart_uploads = s3_client.list_multipart_uploads(
                Bucket=Des_bucket,
                Prefix=Des_key,
                MaxUploads=1000,
                KeyMarker=NextKeyMarker
            )
            # 这里的返回结果是List，文档上写是有排序的，稳妥起见还是在拼接了之后自己在check_file_exist里面排序
        except Exception as e:
            logger.error(f'Fail to list multipart upload {str(e)}')
            # 获取失败就不重试了，不影响主流程，让后面去新建好了
            return []
        IsTruncated = list_multipart_uploads["IsTruncated"]
        NextKeyMarker = list_multipart_uploads["NextKeyMarker"]
        if NextKeyMarker != '':
            for i in list_multipart_uploads["Uploads"]:
                if i["Key"] == Des_key:
                    __multipart_uploaded_list.append({
                        "Key": i["Key"],
                        "Initiated": i["Initiated"],
                        "UploadId": i["UploadId"]
                    })
                    logger.info(f'Unfinished upload, Key: {i["Key"]}, Time: {i["Initiated"]}')
    return __multipart_uploaded_list


# Check file on the list from get_uploaded_list and get created multipart id
def check_file_exist(prefix_and_key, UploadIdList):
    # 查Key是否有未完成的UploadID
    keyIDList = []
    for u in UploadIdList:
        if u["Key"] == prefix_and_key:
            keyIDList.append(u)
    # 如果找不到上传过的Upload，则从头开始传
    if not keyIDList:
        return 'UPLOAD'
    # 对同一个Key（文件）的不同Upload找出时间最晚的值
    UploadID_latest = keyIDList[0]
    for u in keyIDList:
        if u["Initiated"] > UploadID_latest["Initiated"]:
            UploadID_latest = u
    return UploadID_latest["UploadId"]


# Check uploaded part number list on Des_bucket
def checkPartnumberList(Des_bucket, Des_key, uploadId, s3_des_client):
    try:
        partnumberList = []
        PartNumberMarker = 0
        IsTruncated = True
        while IsTruncated:
            try:
                response_uploadedList = s3_des_client.list_parts(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    UploadId=uploadId,
                    MaxParts=1000,
                    PartNumberMarker=PartNumberMarker
                )
            except Exception as e:
                logger.error(f'Fail to list parts in checkPartnumberList. {str(e)}')
                # 获取失败就不重试了，不影响主流程，让后面去新建好了
                return []
            NextPartNumberMarker = response_uploadedList['NextPartNumberMarker']
            IsTruncated = response_uploadedList['IsTruncated']
            if NextPartNumberMarker > 0:
                for partnumberObject in response_uploadedList["Parts"]:
                    partnumberList.append(partnumberObject["PartNumber"])
            PartNumberMarker = NextPartNumberMarker
            # 循环完成获取list

        if partnumberList:  # 如果空则表示没有查到已上传的Part
            logger.info(f"Found uploaded partnumber: {json.dumps(partnumberList)}")
    except Exception as e:
        logger.error(str(e))
        partnumberList = []  # 出错 set null list, retry all parts
    return partnumberList


# Process one job
def job_processor(uploadId, indexList, partnumberList, job, s3_src_client, s3_des_client,
                  MaxThread, ChunkSize, MaxRetry, JobTimeout, ifVerifyMD5Twice):
    # 线程生成器，配合thread pool给出每个线程的对应关系，便于设置超时控制
    def thread_gen(woker_thread, pool,
                   stop_signal, partnumber, total, md5list, partnumberList, complete_list):
        for partStartIndex in indexList:
            # start to upload part
            if partnumber not in partnumberList:
                dryrun = False  # dryrun 是为了沿用现有的流程做出完成列表，方便后面计算 MD5
            else:
                dryrun = True
            th = pool.submit(woker_thread, stop_signal, partnumber, partStartIndex,
                             total, md5list, dryrun, complete_list)
            partnumber += 1
            yield th

    # download part from src. s3 and upload to dest. s3
    def woker_thread(stop_signal, partnumber, partStartIndex, total, md5list, dryrun, complete_list):
        if stop_signal.is_set():
            return "TIMEOUT"
        Src_bucket = job['Src_bucket']
        Src_key = job['Src_key']
        Des_bucket = job['Des_bucket']
        Des_key = job['Des_key']

        # 下载文件
        if ifVerifyMD5Twice or not dryrun:  # 如果 ifVerifyMD5Twice 则无论是否已有上传过都重新下载，作为校验整个文件用

            if not dryrun:
                logger.info(f"--->Downloading {Src_bucket}/{Src_key} - {partnumber}/{total}")
            else:
                logger.info(f"--->Downloading for verify MD5 {Src_bucket}/{Src_key} - {partnumber}/{total}")
            retryTime = 0

            # 正常工作情况下出现 stop_signal 需要退出 Thread
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    response_get_object = s3_src_client.get_object(
                        Bucket=Src_bucket,
                        Key=Src_key,
                        Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
                    )
                    getBody = response_get_object["Body"].read()
                    chunkdata_md5 = hashlib.md5(getBody)
                    md5list[partnumber - 1] = chunkdata_md5
                    break  # 完成下载，不用重试
                except Exception as err:
                    logger.warning(f"DownloadThreadFunc - {Src_bucket}/{Src_key} - Exception log: {str(err)}")
                    logger.warning(f"Download part fail, retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime > MaxRetry:
                        logger.error(f"Quit for Max Download retries: {retryTime}")
                        # 超过次数退出，改为跳下一个文件
                        stop_signal.set()
                        return "MaxRetry"  # 退出Thread
                    else:
                        time.sleep(5 * retryTime)
                        # 递增延迟，返回重试
        # 上传文件
        if not dryrun:  # 这里就不用考虑 ifVerifyMD5Twice 了，

            retryTime = 0
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    logger.info(f'--->Uploading {Des_bucket}/{Des_key} - {partnumber}/{total}')
                    s3_des_client.upload_part(
                        Body=getBody,
                        Bucket=Des_bucket,
                        Key=Des_key,
                        PartNumber=partnumber,
                        UploadId=uploadId,
                        ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
                    )
                    break
                except Exception as err:
                    logger.warning(f"UploadThreadFunc - {Des_bucket}/{Des_key} - Exception log: {str(err)}")
                    logger.warning(f"Upload part fail, retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime > MaxRetry:
                        logger.error(f"Quit for Max Download retries: {retryTime}")
                        # 原来这里是超过次数退出，改为跳下一个文件
                        stop_signal.set()
                        return "MaxRetry"
                    else:
                        time.sleep(5 * retryTime)  # 递增延迟重试

        if not stop_signal.is_set():
            complete_list.append(partnumber)
            if not dryrun:
                logger.info(
                    f'--->Complete {Src_bucket}/{Src_key} - {partnumber}/{total} {len(complete_list) / total:.2%}')
        else:
            return "TIMEOUT"
        return "Complete"

    partnumber = 1  # 当前循环要上传的Partnumber
    total = len(indexList)
    md5list = [hashlib.md5(b'')] * total
    complete_list = []

    # 线程池
    try:
        stop_signal = threading.Event()  # 用于JobTimeout终止当前文件的所有线程
        with concurrent.futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
            # 这里要用迭代器拿到threads对象
            threads = list(thread_gen(woker_thread, pool, stop_signal,
                                      partnumber, total, md5list, partnumberList, complete_list))

            result = concurrent.futures.wait(threads, timeout=JobTimeout, return_when="ALL_COMPLETED")
            if len(result[1]) > 0:
                logger.warning(f'Canceling {len(result[1])} threads...')
                stop_signal.set()
                for t in result[1]:
                    t.cancel()

        if stop_signal.is_set():
            logger.warning(f'TIMEOUT: {JobTimeout}S, Job: {job["Src_bucket"]}/{job["Src_key"]}-size:{job["Size"]}')
            return "TIMEOUT"
        # 线程池End
        logger.info(f'All parts uploaded: {job["Src_bucket"]}/{job["Src_key"]}-size:{job["Size"]}')

        # 计算所有分片列表的总etag: cal_etag
        digests = b"".join(m.digest() for m in md5list)
        md5full = hashlib.md5(digests)
        cal_etag = '"%s-%s"' % (md5full.hexdigest(), len(md5list))
    except Exception as e:
        logger.error(f'Exception in job_processor: {str(e)}')
        return "ERR"
    return cal_etag


# Complete multipart upload
# 通过查询回来的所有Part列表uploadedListParts来构建completeStructJSON
def completeUpload(uploadId, Des_bucket, Des_key, len_indexList, s3_des_client, MaxRetry):
    # 查询S3的所有Part列表uploadedListParts构建completeStructJSON
    # 发现跟checkPartnumberList有点像，但计算Etag不同，隔太久了，懒得合并了 :)
    uploadedListPartsClean = []
    PartNumberMarker = 0
    IsTruncated = True
    while IsTruncated:
        for retryTime in range(MaxRetry+1):
            try:
                response_uploadedList = s3_des_client.list_parts(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    UploadId=uploadId,
                    MaxParts=1000,
                    PartNumberMarker=PartNumberMarker
                )
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchUpload':
                    # Fail to list part list，没这个ID，则是别人已经完成这个Job了。
                    logger.error(f'Fail to list parts while completeUpload, might be duplicated job:'
                                 f' {Des_bucket}/{Des_key}, {str(e)}')
                    return "ERR"
                logger.error(f'Fail to list parts while completeUpload {Des_bucket}/{Des_key}, {str(e)}')
                if retryTime >= MaxRetry:
                    logger.error(f'Fail MaxRetry list parts while completeUpload {Des_bucket}/{Des_key}')
                    return "ERR"
                else:
                    time.sleep(5*retryTime)
            except Exception as e:
                logger.error(f'Fail to list parts while completeUpload {Des_bucket}/{Des_key}, {str(e)}')
                if retryTime >= MaxRetry:
                    logger.error(f'Fail MaxRetry list parts while completeUpload {Des_bucket}/{Des_key}')
                    return "ERR"
                else:
                    time.sleep(5*retryTime)

        NextPartNumberMarker = response_uploadedList['NextPartNumberMarker']
        IsTruncated = response_uploadedList['IsTruncated']
        # 把 ETag 加入到 Part List
        if NextPartNumberMarker > 0:
            for partObject in response_uploadedList["Parts"]:
                ETag = partObject["ETag"]
                PartNumber = partObject["PartNumber"]
                addup = {
                    "ETag": ETag,
                    "PartNumber": PartNumber
                }
                uploadedListPartsClean.append(addup)
        PartNumberMarker = NextPartNumberMarker
        # 循环获取直到拿完全部parts

    if len(uploadedListPartsClean) != len_indexList:
        logger.warning(f'Uploaded parts size not match - {Des_bucket}/{Des_key}')
        return "ERR"
    completeStructJSON = {"Parts": uploadedListPartsClean}

    # S3合并multipart upload任务
    for retryTime in range(MaxRetry+1):
        try:
            logger.info(f'Try to merge multipart upload {Des_bucket}/{Des_key}')
            response_complete = s3_des_client.complete_multipart_upload(
                Bucket=Des_bucket,
                Key=Des_key,
                UploadId=uploadId,
                MultipartUpload=completeStructJSON
            )
            result = response_complete['ETag']
            break
        except Exception as e:
            logger.error(f'Fail to complete multipart upload {Des_bucket}/{Des_key}, {str(e)}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry complete multipart upload {Des_bucket}/{Des_key}')
                return "ERR"
            else:
                time.sleep(5*retryTime)
    logger.info(f'Complete merge file {Des_bucket}/{Des_key}')
    return result


# Continuely get job message to invoke one processor per job
def job_looper(sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id,
               StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
               JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload):
    while True:
        # Get Job from sqs
        try:
            # TODO: 一次拿一批，如果是大量小文件就比较快
            logger.info('Get Job from sqs queue...')
            sqs_job = sqs.receive_message(QueueUrl=sqs_queue)
            # Empty queue message available
            if "Messages" not in sqs_job:
                # 检查是否还有其他在in-flight中的，sleep, 循环等待
                logger.info('No message in queue available, wait for inFlight message...')
                sqs_in_flight = sqs.get_queue_attributes(
                    QueueUrl=sqs_queue,
                    AttributeNames=['ApproximateNumberOfMessagesNotVisible']
                )
                if sqs_in_flight['Attributes']['ApproximateNumberOfMessagesNotVisible'] == '0':
                    logger.warning('No message in queue available and inFlight. Wait 1 min...')
                    # TODO: Empty queue, send sns notification, or use CloudWatch Alarm
                    pass
                time.sleep(60)
            # 拿到 Job message
            else:
                job = json.loads(sqs_job["Messages"][0]["Body"])
                job_receipt = sqs_job["Messages"][0]["ReceiptHandle"]  # 用于后面删除message

                Src_bucket = job['Src_bucket']
                Src_key = job['Src_key']
                Size = job['Size']
                Des_bucket = job['Des_bucket']
                Des_key = job['Des_key']
                logger.info(f'Start: {Src_bucket}/{Src_key}, Size: {Size}')

                # DynamoDB log: ADD retry time, instance-id list, SET startTime
                for retry in range(MaxRetry+1):
                    try:
                        logger.info(f'Write log to DDB via start job: {Src_bucket}/{Src_key}')
                        table.update_item(
                            Key={
                                "Key": Src_bucket + "/" + Src_key
                            },
                            UpdateExpression="ADD instance_id :id, retry_times :t "
                                             "SET start_time = :s",
                            ExpressionAttributeValues={
                                ":t": 1,
                                ":id": {instance_id},
                                ":s": int(time.time())
                            }
                        )
                        break
                    except Exception as e:
                        # 日志写不了
                        logger.error(f'Fail to put log to DDB at start {Src_bucket}/{Src_key}, {str(e)}')
                        if retry >= MaxRetry:
                            logger.error(f'Fail MaxRetry put log to DDB at start {Src_bucket}/{Src_key}')
                        else:
                            time.sleep(5*retry)

                # Size lower than limit, not to check s3 exist parts, to save time
                multipart_uploaded_list = []
                if Size > ResumableThreshold:
                    # Get dest s3 unfinish multipart upload of this file
                    multipart_uploaded_list = get_uploaded_list(s3_des_client, Des_bucket, Des_key)

                # Debug用，清理S3上现有未完成的Multipart Upload ID（不只是当前Job，而对应目标Bucket上所有的）
                if multipart_uploaded_list and CleanUnfinishedUpload:
                    logger.warning(f'You set CleanUnfinishedUpload. There are {len(multipart_uploaded_list)}.'
                                   f' Now clean them and restart!')
                    multipart_uploaded_list = get_uploaded_list(s3_des_client, Des_bucket, "")
                    for clean_i in multipart_uploaded_list:
                        try:
                            s3_des_client.abort_multipart_upload(
                                Bucket=Des_bucket,
                                Key=clean_i["Key"],
                                UploadId=clean_i["UploadId"]
                            )
                        except Exception as e:
                            logger.error(f'Fail to clean {str(e)}')
                    multipart_uploaded_list = []
                    logger.info('CLEAN FINISHED')

                # 开始 Job
                # 循环重试3次（如果MD5计算的ETag不一致）
                for md5_retry in range(3):
                    # Job 准备
                    # 检查文件没Multipart UploadID要新建, 有则 return UploadID
                    response_check_upload = check_file_exist(
                        Des_key,
                        multipart_uploaded_list
                    )
                    if response_check_upload == 'UPLOAD':
                        logger.info(f'Create multipart upload {Des_bucket}/{Des_key}')
                        try:
                            response_new_upload = s3_des_client.create_multipart_upload(
                                Bucket=Des_bucket,
                                Key=Des_key,
                                StorageClass=StorageClass
                            )
                        except Exception as e:
                            logger.error(f'Fail to create new multipart upload. {str(e)}')
                            if md5_retry >= 2:
                                upload_etag_full = "ERR"
                                break
                            else:
                                time.sleep(5*md5_retry)
                                continue
                        # logger.info("UploadId: "+response_new_upload["UploadId"])
                        reponse_uploadId = response_new_upload["UploadId"]
                        partnumberList = []
                    else:
                        reponse_uploadId = response_check_upload
                        logger.info(f'Resume upload id: {Des_bucket}/{Des_key}')
                        # 获取已上传partnumberList
                        partnumberList = checkPartnumberList(
                            Des_bucket,
                            Des_key,
                            reponse_uploadId,
                            s3_des_client
                        )

                    # 获取文件拆分片索引列表，例如[0, 10, 20]
                    indexList, ChunkSize_auto = split(
                        Size,
                        ChunkSize
                    )  # 对于大于10000分片的大文件，自动调整为Chunksize_auto

                    # Job Thread: uploadPart, 加入超时机制之后返回 "TIMEOUT"
                    upload_etag_full = job_processor(
                        reponse_uploadId,
                        indexList,
                        partnumberList,
                        job,
                        s3_src_client,
                        s3_des_client,
                        MaxThread,
                        ChunkSize_auto,  # 对单个文件使用自动调整的 Chunksize_auto
                        MaxRetry,
                        JobTimeout,
                        ifVerifyMD5Twice
                    )
                    if upload_etag_full == "TIMEOUT":
                        break  # 超时退出处理该Job，因为sqs超时会被其他EC2拿到

                    # 合并S3上的文件
                    complete_etag = completeUpload(reponse_uploadId, Des_bucket, Des_key,
                                                   len(indexList), s3_des_client, MaxRetry)
                    logger.info(f'FINISH: {Des_bucket}/{Des_key}')
                    if complete_etag == "ERR":
                        multipart_uploaded_list = []  # 清掉已上传id列表，以便重新上传
                        continue  # 循环重试

                    # 检查文件MD5
                    if ifVerifyMD5Twice:
                        if complete_etag == upload_etag_full:
                            logger.info(f'MD5 ETag Matched - {Des_bucket}/{Des_key} - {complete_etag}')
                            break  # 结束本文件，下一个sqs job
                        else:  # ETag 不匹配，删除目的S3的文件，重试
                            logger.warning(f'MD5 ETag NOT MATCHED {Des_bucket}/{Des_key}( Destination / Origin ): '
                                           f'{complete_etag} - {upload_etag_full}')
                            try:
                                s3_des_client.delete_object(
                                    Bucket=Des_bucket,
                                    Key=Des_key
                                )
                            except Exception as e:
                                logger.error(f'Fail to delete on S3. {str(e)}')
                            multipart_uploaded_list = []
                            if md5_retry >= 2:
                                logger.error(f'MD5 ETag NOT MATCHED Exceed Max Retries - {Des_bucket}/{Des_key}')
                                upload_etag_full = "ERR"
                            else:
                                logger.warning(f'Deleted and retry {Des_bucket}/{Des_key}')

                # 结束 Job
                # Delete this file's related unfinished multipart upload, 可能有多个残留
                for clean_i in multipart_uploaded_list:
                    # 其实list只查了当前Key的，不过为了安全确保其他code没被改，所以判断一下Key一致性
                    if clean_i["Key"] == Des_key:
                        try:
                            s3_des_client.abort_multipart_upload(
                                Bucket=Des_bucket,
                                Key=Des_key,
                                UploadId=clean_i["UploadId"]
                            )
                        except Exception as e:
                            logger.error(f'Fail to clean old unfinished multipart upload {str(e)}'
                                         f'{Des_bucket}/{Des_key} - UploadID: {clean_i["UploadId"]}')

                # Del Job on sqs
                if upload_etag_full != "TIMEOUT" and upload_etag_full != "ERR":
                    # 如果是超时或ERR的就不删SQS消息，是正常结束就删
                    for retry in range(MaxRetry+1):
                        try:
                            logger.info(f'Try to finsh job message on sqs.')
                            sqs.delete_message(
                                QueueUrl=sqs_queue,
                                ReceiptHandle=job_receipt
                            )
                            break
                        except Exception as e:
                            logger.error(f'Fail to delete sqs message: {Des_bucket}/{Des_key}, {str(e)}')
                            if retry >= MaxRetry:
                                logger.error(f'Fail MaxRetry delete sqs message: {Des_bucket}/{Des_key}, {str(e)}')
                            else:
                                time.sleep(5*retry)

                # DynamoDB log: ADD status: DONE/ERR(upload_etag_full)
                status = "DONE"
                if upload_etag_full == "TIMEOUT":
                    status = "TIMEOUT"
                elif upload_etag_full == "ERR":
                    status = "ERR"
                for retry in range(MaxRetry+1):
                    try:
                        table.update_item(
                            Key={
                                "Key": Src_bucket + "/" + Src_key
                            },
                            UpdateExpression="SET spent_time=:s-start_time ADD job_status :done",
                            ExpressionAttributeValues={
                                ":done": {status},
                                ":s": int(time.time())
                            }
                        )
                        break
                    except Exception as e:
                        logger.error(f'Fail to put log to DDB at end. {str(e)}')
                        if retry >= MaxRetry:
                            logger.error(f'Fail MaxRetry to put log to DDB at end. {str(e)}')
                        else:
                            time.sleep(5*retry)
        except Exception as e:
            logger.error(f'Fail. Wait for 5 seconds. ERR: {str(e)}')
            time.sleep(5)
        # Finish Job, go back to get next job in queue
