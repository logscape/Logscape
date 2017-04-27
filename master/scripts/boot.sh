#!/bin/bash
#### This script is called from vscape-s.sh OR vscape.sh - which enable vscape.sh [start, stop]
####

export LC_CTYPE=C.UTF-8
export PATH=.:$PATH
CP=.:boot.jar
if [ -z "${JAVA_HOME}" ]; then
 java -DVSCAPE_BOOTSTRAP -XX:MaxPermSize=32M -Xms16M -Xmx32M -Dlog4j.configuration=./agent-log4j.properties -Xverify:none -cp $CP com.liquidlabs.boot.BootStrapper $@ &
else
 $JAVA_HOME/bin/java -DLOGSCAPE_BOOTSTRAP -XX:MaxPermSize=32M -Xms16M -Xmx32M -Dlog4j.configuration=./agent-log4j.properties -Xverify:none -cp $CP com.liquidlabs.boot.BootStrapper $@ &
fi

echo  $! > pids/agent-$!.pid

