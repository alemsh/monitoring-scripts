FROM centos:7

RUN yum -y install epel-release; \
  yum -y upgrade ca-certificates --disablerepo=epel
RUN yum install -y python-pip
RUN pip install elasticsearch htcondor requests

COPY condor_history_to_elasticsearch.py /condor_history_to_elasticsearch.py
