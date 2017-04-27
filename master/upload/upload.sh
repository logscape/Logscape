#!/bin/bash
if [ $# != 1 ]
then
    echo "Usage $0 <ReleaseName>"
    exit 1
fi
trial_dir=public_html/trial
upload_dir=${trial_dir}/$1
user=logscape@logscape.com
ssh ${user} mkdir ${upload_dir}
scp dist/* ${user}:${upload_dir}
ssh  ${user} /home/logscape/link.sh $1
