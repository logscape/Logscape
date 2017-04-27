#!/bin/sh
PATH=$PATH:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin
echo "Executing renice +5 $1"
renice +5 -p $1
