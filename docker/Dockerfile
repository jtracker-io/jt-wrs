FROM ubuntu:16.04

# Metadata
LABEL base.image="ubuntu:16.04"
LABEL software="JTracker Workflow Registration Service"
LABEL website="https://github.com/jtracker-io/jt-wrs"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

RUN apt-get -yqq update && \
    apt-get -yqq install curl

RUN curl -sSL -o tmp.tar.gz --retry 10 https://github.com/jtracker-io/jt-wrs/archive/0.2.0a7.tar.gz && \
    mkdir -p /srv/jt-wrs && \
    tar -C /srv/jt-wrs --strip-components 1 -zxf tmp.tar.gz && \
    apt-get -yqq install python3-pip && \
    pip3 install --upgrade pip  && \
    pip3 install -r /srv/jt-wrs/requirements.txt

CMD ["python3", "/srv/jt-wrs/app.py"]
