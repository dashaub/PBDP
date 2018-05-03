#!/bin/bash

set -euo pipefail
cd /root/transaction_tracker/

# Start the InfluxDB and Grafana service
service influxdb start
service grafana-server start

# Start collecting unconfirmed transactions
python fetch_mempool.py &

# Collect some events in the mempool before following the chain
sleep 120
python follow_chain.py &

echo "drop database blockchain;" | influx
echo "create database blockchain;" | influx
while :; do
    # Launch a Beam pipeline run every 5 minutes
    sleep 300
    timestamp=$(date -u '+%Y-%m-%dT%H%M%SZ')
    echo "Launching batch job at ${timestamp}"
    python beam_pipeline.py --timestamp="${timestamp}"

    # Insert the results in InfluxDB
    python influx_insert.py --timestamp="${timestamp}"
done
