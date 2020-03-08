#!/bin/bash
yum update -y
yum install git -y
yum install python3 -y
pip3 install boto3 requests

cd /home/ec2-user/  || exit
git clone -b dev https://github.com/aws-samples/amazon-s3-resumable-upload.git
chown -R ec2-user:ec2-user amazon-s3-resumable-upload/

# Setup BBR
/bin/cp -rf /home/ec2-user/amazon-s3-resumable-upload/cluster_version/tcpcong.modules /etc/sysconfig/modules/tcpcong.modules
chmod 755 /etc/sysconfig/modules/tcpcong.modules
echo "net.ipv4.tcp_congestion_control = bbr" >> /etc/sysctl.d/00-tcpcong.conf
modprobe tcp_bbr
modprobe sch_fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

python3 amazon-s3-resumable-upload/cluster_version/s3_upload_cluster_master.py
# log exported to ~/log/
