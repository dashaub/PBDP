# Docker image for transaction tracker

FROM ubuntu:18.04
LABEL maintainer="David Shaub"

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    && python-setuptools \
    && python-pip

# Install Apache Beam and InfluxDB python connectors
RUN pip install apache-beam=2.4.0 \
    && pip install influxdb==5.0.0

# Install InfluxDB
RUN wget https://dl.influxdata.com/influxdb/releases/influxdb_1.5.2_amd64.deb \
    && sudo dpkg -i influxdb_1.5.2_amd64.deb \
    && rm influxdb_1.5.2_amd64.deb

# Install Grafana
RUN wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana_5.0.4_amd64.deb \
    && apt-get install -y adduser libfontconfig \
    && dpkg -i grafana_5.0.4_amd64.deb \
    && rm grafana_5.0.4_amd64.deb

# Copy program files
COPY transaction_tracker /root/

# Cleanup
RUN rm -Rf /var/lib/apt/lists/* \
    && rm -Rf /usr/share/doc && rm -Rf /usr/share/man \
    && apt-get clean

