#!/usr/bin/env python3

from aws_cdk import core

from cdk.cdk_vpc_stack import CdkVpcStack
from cdk.cdk_ec2_stack import CdkEc2Stack
from cdk.cdk_resource_stack import CdkResourceStack

############
# Define bucket before deploy
bucket_para = [{
    "src_bucket": "broad-references",
    "src_prefix": "",
    "des_bucket": "s3-open-data",
    "des_prefix": "broad-references",
    }, {
    "src_bucket": "gatk-test-data",
    "src_prefix": "",
    "des_bucket": "s3-open-data",
    "des_prefix": "gatk-test-data",
    }]
key_name = "id_rsa"  # Optional if you use SSM-SessionManager
############

app = core.App()
vpc_stack = CdkVpcStack(app, "s3-migration-vpc")
vpc = vpc_stack.vpc

resource_stack = CdkResourceStack(app, "s3-migration-resource", bucket_para)

ec2_stack = CdkEc2Stack(app, "s3-migration-ec2", vpc, key_name,
                        resource_stack.ddb_file_list,
                        resource_stack.sqs_queue,
                        resource_stack.sqs_queue_DLQ,
                        resource_stack.ssm_bucket_para,
                        resource_stack.ssm_credential_para,
                        bucket_para)

app.synth()
