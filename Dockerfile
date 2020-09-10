FROM python:3

MAINTAINER Cornelis Hoeflake <c.hoeflake@gmail.com>

ENV DIR=/py-otgw-mqtt

RUN set -e && \
    python -m ensurepip --default-pip && \
    python -m pip install --upgrade pip && \
    pip install paho-mqtt && \
    mkdir $DIR && \
    cd $DIR

COPY . $DIR

CMD cd $DIR && python .
    
    