#!/bin/bash

##
##  ======= This file reads configuration information from setup.conf and applies those changes to the installation
##

echo "Run from >logscape/scripts: i.e. ./configure.sh"

export PATH=.:$PATH

if [ -z "${JAVA_HOME}" ]; then
        java -Dlog4j.configuration=../agent-log4j.properties -cp ../boot.jar com.liquidlabs.ffilter.FileFilter ./..
else
        $JAVA_HOME/bin/java -Dlog4j.configuration=../agent-log4j.properties -cp ../boot.jar com.liquidlabs.ffilter.FileFilter ./..
fi
chmod -R +x *.sh
chmod -R +x ../*.sh