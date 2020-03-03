from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_autoscaling as autoscaling
import aws_cdk.aws_iam as iam

master_type = "t3.small"
worker_type = "c5.2xlarge"
key_name = "id_rsa"
linux_ami = ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX,
                                 edition=ec2.AmazonLinuxEdition.STANDARD,
                                 virtualization=ec2.AmazonLinuxVirt.HVM,
                                 storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE
                                 )
with open("./cdk/user_data_worker.sh") as f:
    user_data_worker = f.read()
with open("./cdk/user_data_master.sh") as f:
    user_data_master = f.read()


class CdkEc2Stack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str,
                 vpc, src_s3, ddb_file_list, sqs_queue, sqs_queue_DLQ, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        # Create master node
        master = ec2.Instance(self, "master",
                              instance_name="s3_upload_cluster_master",
                              instance_type=ec2.InstanceType(instance_type_identifier=master_type),
                              machine_image=linux_ami,
                              key_name=key_name,
                              user_data=ec2.UserData.custom(user_data_master),
                              vpc=vpc,
                              vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))
        # master.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")
        master.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        # Create Autoscaling Group with fixed 2*EC2 hosts
        worker_asg = autoscaling.AutoScalingGroup(self, "myASG",
                                                  vpc=vpc,
                                                  vpc_subnets=ec2.SubnetSelection(
                                                      subnet_type=ec2.SubnetType.PUBLIC),
                                                  instance_type=ec2.InstanceType(
                                                      instance_type_identifier=worker_type),
                                                  machine_image=linux_ami,
                                                  key_name=key_name,
                                                  user_data=ec2.UserData.custom(user_data_worker),
                                                  desired_capacity=0,
                                                  min_capacity=2,
                                                  max_capacity=10,
                                                  cooldown=core.Duration.minutes(20)
                                                  )
        src_s3.grant_read(master)
        src_s3.grant_read(worker_asg)
        ddb_file_list.grant_full_access(master)
        ddb_file_list.grant_full_access(worker_asg)
        sqs_queue.grant_send_messages(master)
        sqs_queue.grant_consume_messages(worker_asg)
        sqs_queue_DLQ.grant_consume_messages(master)

        core.CfnOutput(self, "Output", value=master.instance_id)
