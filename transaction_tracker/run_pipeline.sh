#!/bin/bash

set -euo pipefail

# Start collecting unconfirmed transactions
python fetch_mempool.py &

# Collect some events in the mempool before following the chain
sleep 300
python follow_chain.py &

while :; do
    # Launch a Beam pipeline run every 5 minutes
    sleep 300
    timestamp=$(date '+%Y-%m-%dT%H%M%S')
    python beam_pipeline.py --timestamp="${timestamp}"

    # Insert the results in InfluxDB
    "${timestamp}_results*"

done
