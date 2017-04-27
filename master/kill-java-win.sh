#!/bin/bash

echo "**************** KILLING java.exe !! ***************************** "
JPSS=`ps -Ws | grep java.exe | cut -c1-10`
for psid in $JPSS; do
  echo "killing pid $psid"
  tskill $psid
done
