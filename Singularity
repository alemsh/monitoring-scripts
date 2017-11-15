BootStrap: docker
From: centos:7

%files
condor_history_to_elasticsearch.py

%post
mkdir /data

yum -y install epel-release
yum -y upgrade ca-certificates --disablerepo=epel
yum install -y python-pip
pip install elasticsearch htcondor requests
