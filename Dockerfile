FROM centos:7

RUN yum -y install epel-release && \
    yum -y upgrade ca-certificates --disablerepo=epel && \
    yum install -y python-pip && \
    pip install elasticsearch htcondor requests

COPY . /monitoring

WORKDIR /monitoring

ENV CONDOR_CONFIG=/monitoring/condor_config
