#!/bin/bash
 echo "** Run from logscape/scripts: "
 LS=..
 LS_HOME=$LS/system-bundles
 CP=$LS_HOME/lib-1.0/thirdparty/thirdparty-all.jar:$LS_HOME/boot/lib/*:$LS_HOME/lib-1.0/thirdparty/*
 MAIN=com.liquidlabs.common.compression.SnappyCompress
 if [ -z ${JAVA_HOME} ]
 then
                 java -cp $CP $MAIN $1
         else
                 ${JAVA_HOME}/bin/java -cp $CP $MAIN $1
fi
