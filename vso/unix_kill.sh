#!/bin/sh
SIG=$1
APID=$2

kill $SIG `ps -ef | awk -v APID=$APID '{if ($3==APID || $2==APID) printf("%s ", $2);}'`
