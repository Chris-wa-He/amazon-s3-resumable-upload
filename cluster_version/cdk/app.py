#!/usr/bin/env python3

from aws_cdk import core

from cdk.cdk_vpc_stack import CdkVpcStack
from cdk.cdk_ec2_stack import CdkEc2Stack
from cdk.cdk_resource_stack import CdkResourceStack

############
# Define the source bucket before deploy
src_bucket = "broad-references"
############

app = core.App()
vpc_stack = CdkVpcStack(app, "cdk-vpc")
vpc = vpc_stack.vpc

resource_stack = CdkResourceStack(app, "cdk-resource", src_bucket)

s3 = resource_stack.src_s3
ddb = resource_stack.ddb_file_list
sqs = resource_stack.sqs_queue
sqs_DLQ = resource_stack.sqs_queue_DLQ
ec2_stack = CdkEc2Stack(app, "cdk-ec2", vpc, s3, ddb, sqs, sqs_DLQ)

app.synth()
