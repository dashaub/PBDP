#!/bin/bash

if [ $# -ne 2 ]; then
    echo $0: usage: streamfiles.sh stage_dir input_dir
    return 1
fi

stage_dir=$1
input_dir=$2

input_files=`find $stage_dir -type f | sort`

for file in $input_files
do
  echo `date` Coping $file to $input_dir
  cp $file $input_dir
  sleep 2s
done
