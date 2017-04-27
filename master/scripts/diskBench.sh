#!/bin/bash

 echo "** Run from logscape/scripts: i.e. >./dbDump.sh **"
 ###LS=../build/logscape
 LS=..
 LS_HOME=$LS/system-bundles
 CP=$LS_HOME/lib-1.0/thirdparty/thirdparty-all.jar:$LS_HOME/boot/lib/*:$LS_HOME/vs-log-1.0/lib/vs-log.jar
 echo "Running with CP:$CP DBDIR:$1"
 MAIN=com.liquidlabs.common.file.DiskBenchmarkTest
 #MAIN=com.liquidlabs.log.fields.FieldSets
 if [ -z ${JAVA_HOME} ]
 then
                 java -cp $CP $MAIN
         else
                         ${JAVA_HOME}/bin/java -cp $CP $MAIN
fi
