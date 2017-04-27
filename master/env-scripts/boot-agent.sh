#!/bin/bash
export LC_CTYPE=UTF-8
export JAVA_HOME=${ENV[JAVA_HOME]} 
CP=.:boot.jar
nohup $JAVA_HOME/bin/java -Xverify:none -Dlog4j.configuration=./agent-log4j.properties -Xverify:none -cp $CP com.liquidlabs.boot.BootStrapper stcp://$LOOKUP_HOST:$LOOKUP_PORT $SLOTS
echo $! > agent.pid
