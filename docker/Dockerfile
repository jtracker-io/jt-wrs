FROM python:3.7

# Metadata
LABEL base.image="python:3.7"
LABEL software="JTracker Workflow Registration Service"
LABEL website="https://github.com/jtracker-io/jt-wrs"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

RUN wget -O tmp.tar.gz -t 10 https://github.com/jtracker-io/jt-wrs/archive/0.2.0a9.tar.gz && \
    mkdir -p /srv/jt-wrs && \
    tar -C /srv/jt-wrs --strip-components 1 -zxf tmp.tar.gz && \
    pip3 install -r /srv/jt-wrs/requirements.txt

CMD ["python3", "/srv/jt-wrs/app.py"]
