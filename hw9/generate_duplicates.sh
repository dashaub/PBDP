#!/bin/bash

rm data_input/*
num=0001
while true; do
    echo 'd8c6aff2-3cbb-4ecc-a909-700605db8285 2018-03-24T15:44:59.068Z http://example.com/?url=0 User_0' > data_input/dup_${num}.txt
    sleep 10
    num+=1
done
