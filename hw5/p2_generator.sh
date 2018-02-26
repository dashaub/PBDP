#!/usr/bin/env bash
# This script requires 'bc', 'curl', and GNU coreutils 'sleep'

if (( "$1" != 1 )); then
    rate=$(echo "1 / $1" | bc -l)
    while true; do
        curl 'localhost:8080'
        gsleep $rate
    done
fi
echo "Must supply a numeric rate argument"
exit 1
